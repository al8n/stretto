use crate::error::CacheError;
use crate::metrics::{MetricType, Metrics};
use crate::policy::LFUPolicy;
use crate::store::{ShardedMap, UpdateResult};
use crate::ttl::Time;
use crate::{CacheCallback, Coster, KeyHasher, UpdateValidator, ValueCost};
use crossbeam::{
    channel::{tick, Receiver, RecvError, SendError, Sender},
    sync::WaitGroup,
};
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{spawn, JoinHandle};
use std::time::{Duration, Instant};

// TODO: find the optimal value for this or make it configurable
const SET_BUF_SIZE: usize = 32 * 1024;
const DEFAULT_BUFFER_ITEMS: u64 = 64;

pub type ItemCallback<V> = fn(&Item<V>);

#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub(crate) enum ItemFlag {
    New,
    Delete,
    Update,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct ItemMeta {
    key: u64,
    conflict: u64,
    cost: i64,
}

impl ItemMeta {
    pub(crate) fn new(k: u64, cost: i64, conflict: u64) -> Self {
        Self {
            key: k,
            conflict,
            cost,
        }
    }
}

enum Item<V> {
    New {
        key: u64,
        conflict: u64,
        cost: i64,
        value: V,
        expiration: Time,
    },
    Update {
        key: u64,
        cost: i64,
    },
    Delete {
        key: u64,
        conflict: u64,
    },
    Wait(WaitGroup),
}

impl<V> Item<V> {
    fn new(kh: u64, ch: u64, cost: i64, val: V, exp: Time) -> Self {
        Self::New {
            key: kh,
            conflict: ch,
            cost,
            value: val,
            expiration: exp,
        }
    }

    fn update(kh: u64, cost: i64) -> Self {
        Self::Update { key: kh, cost }
    }

    fn delete(kh: u64, conflict: u64) -> Self {
        Self::Delete { key: kh, conflict }
    }

    fn is_update(&self) -> bool {
        match self {
            Item::Update { .. } => true,
            _ => false,
        }
    }
}

pub struct CacheBuilder<
    K: Hash + Eq + ?Sized,
    V,
    KH: KeyHasher<K>,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
> {
    num_counters: i64,
    max_cost: i64,

    /// buffer_items determines the size of Get buffers.
    ///
    /// Unless you have a rare use case, using `64` as the BufferItems value
    /// results in good performance.
    buffer_items: u64,

    /// metrics determines whether cache statistics are kept during the cache's
    /// lifetime. There *is* some overhead to keeping statistics, so you should
    /// only set this flag to true when testing or throughput performance isn't a
    /// major factor.
    metrics: bool,

    callback: Option<CB>,

    /// key_to_hash is used to customize the key hashing algorithm.
    /// Each key will be hashed using the provided function. If keyToHash value
    /// is not set, the default keyToHash function is used.
    key_to_hash: KH,

    /// should_update is called when a value already exists in cache and is being updated.
    should_update: Option<U>,

    /// cost evaluates a value and outputs a corresponding cost. This function
    /// is ran after insert is called for a new item or an item update with a cost
    /// param of 0.
    cost: Option<C>,

    /// ignore_internal_cost set to true indicates to the cache that the cost of
    /// internally storing the value should be ignored. This is useful when the
    /// cost passed to set is not using bytes as units. Keep in mind that setting
    /// this to true will increase the memory usage.
    ignore_internal_cost: bool,

    marker: PhantomData<fn(K)>,
}

/// Cache is a thread-safe implementation of a hashmap with a TinyLFU admission
/// policy and a Sampled LFU eviction policy. You can use the same Cache instance
/// from as many threads as you want.
pub struct Cache<K: Hash + Eq + ?Sized, V, KH: KeyHasher<K>, VC: ValueCost<V>> {
    /// store is the central concurrent hashmap where key-value items are stored.
    store: Arc<ShardedMap<V>>,

    /// policy determines what gets let in to the cache and what gets kicked out.
    policy: Arc<LFUPolicy>,

    /// set_buf is a buffer allowing us to batch/drop Sets during times of high
    /// contention.
    set_buf_tx: Sender<Item<V>>,

    on_evict: Box<ItemCallback<V>>,

    on_reject: Box<ItemCallback<V>>,

    on_exit: Box<fn(&V)>,

    key_to_hash: KH,

    stop_tx: Sender<()>,
    stop_rx: Receiver<()>,

    is_closed: AtomicBool,

    cost: VC,

    ignore_internal_cost: bool,

    cleanup_duration: Duration,

    metrics: Option<Arc<Metrics>>,
}

impl<K: Hash + Eq, V, KH: KeyHasher<K>, VC: ValueCost<V>> Cache<K, V, KH, VC> {
    /// `insert` attempts to add the key-value item to the cache. If it returns false,
    /// then the `insert` was dropped and the key-value item isn't added to the cache. If
    /// it returns true, there's still a chance it could be dropped by the policy if
    /// its determined that the key-value item isn't worth keeping, but otherwise the
    /// item will be added and other items will be evicted in order to make room.
    ///
    /// To dynamically evaluate the items cost using the Config.Coster function, set
    /// the cost parameter to 0 and Coster will be ran when needed in order to find
    /// the items true cost.
    pub fn insert(&self, key: K, val: V, cost: i64) -> bool {
        self.insert_with_ttl(key, val, cost, Duration::ZERO)
    }

    /// `insert_with_ttl` works like Set but adds a key-value pair to the cache that will expire
    /// after the specified TTL (time to live) has passed. A zero value means the value never
    /// expires, which is identical to calling `insert`.
    pub fn insert_with_ttl(&self, key: K, val: V, cost: i64, ttl: Duration) -> bool {
        self.insert_in(key, val, cost, ttl, false)
    }

    /// `insert_if_present` is like `insert`, but only updates the value of an existing key. It
    /// does NOT add the key to cache if it's absent.
    ///
    ///
    pub fn insert_if_present(&self, key: K, val: V, cost: i64) -> bool {
        self.insert_in(key, val, cost, Duration::ZERO, true)
    }

    pub fn wait(&self) -> Result<(), CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        let wg = WaitGroup::new();
        let wait_item = Item::Wait(wg.clone());
        self.set_buf_tx
            .send(wait_item)
            .map(|| {
                wg.wait();
                ()
            })
            .map_err(|e| CacheError::SendError(format!("cache set buf sender: {}", e.to_string())))
    }

    pub fn remove(&self, k: &K) {
        if self.is_closed.load(Ordering::SeqCst) {
            return;
        }

        let (kh, ch) = self.key_to_hash.hash_key(&k);
        // delete immediately
        let (_, prev) = self.store.remove(kh, ch);
        match prev {
            None => {}
            Some(v) => self.on_exit.call(v),
        };

        // If we've set an item, it would be applied slightly later.
        // So we must push the same item to `setBuf` with the deletion flag.
        // This ensures that if a set is followed by a delete, it will be
        // applied in the correct order.
        let _ = self.set_buf_tx.send(Item::delete(kh, ch));
    }

    /// `close` stops all threads and closes all channels.
    pub fn close(&self) -> Result<(), CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }
        todo!()
    }

    /// `max_cost` returns the max cost of the cache.
    pub fn max_cost(&self) -> i64 {
        self.policy.max_cost()
    }

    /// `update_max_cost` updates the maxCost of an existing cache.
    pub fn update_max_cost(&self, max_cost: i64) {
        self.policy.update_max_cost(max_cost)
    }

    fn insert_in(&self, key: K, val: V, cost: i64, ttl: Duration, only_update: bool) -> bool {
        if self.is_closed.load(Ordering::SeqCst) {
            return false;
        }

        let expiration = if ttl.is_zero() {
            Time::now()
        } else {
            Time::now_with_expiration(ttl)
        };

        let (key_hash, conflict_hash) = self.key_to_hash.hash_key(&key);

        // cost is eventually updated. The expiration must also be immediately updated
        // to prevent items from being prematurely removed from the map.
        let item = match self.store.update(key_hash, val, conflict_hash, expiration) {
            UpdateResult::NotExist(v) => {
                if only_update {
                    None
                } else {
                    Some(Item::new(key_hash, conflict_hash, cost, v, expiration))
                }
            }
            UpdateResult::Conflict(v) => {
                Some(Item::new(key_hash, conflict_hash, cost, v, expiration))
            }
            UpdateResult::Update(v) => {
                self.on_evict.call(v);
                Some(Item::update(key_hash, cost))
            }
        };

        item.map_or(false, |item| {
            // Attempt to send item to policy.
            select! {
                send(self.set_buf_tx, item) -> res => {
                    res.map_or(false, |_| true)
                },
                default => {
                    if item.is_update() {
                        // Return true if this was an update operation since we've already
                        // updated the store. For all the other operations (set/delete), we
                        // return false which means the item was not inserted.
                        true
                    } else {
                        false
                    }
                }
            }
        })
    }
}

struct CacheProcessor<V> {
    set_buf_rx: Receiver<Item<V>>,
    stop_rx: Receiver<()>,
    metrics: Arc<Metrics>,
    ticker: Receiver<Instant>,
    store: Arc<ShardedMap<V>>,
    policy: Arc<LFUPolicy>,
    start_ts: HashMap<u64, Time>,
    num_to_keep: u64,
}

impl<V> CacheProcessor<V> {
    pub fn spawn(
        store: Arc<ShardedMap<V>>,
        policy: Arc<LFUPolicy>,
        set_buf_rx: Receiver<Item<V>>,
        stop_rx: Receiver<()>,
        metrics: Arc<Metrics>,
        clean_up_ticker: Receiver<Instant>,
        num_to_keep: u64,
    ) -> JoinHandle<Result<(), CacheError>> {
        let mut this = Self {
            set_buf_rx,
            stop_rx,
            metrics,
            ticker: clean_up_ticker,
            store,
            policy,
            start_ts: HashMap::<u64, Time>::new(),
            num_to_keep,
        };

        spawn(move || loop {
            select! {
                recv(this.set_buf_rx) -> res => {
                    let _ = this.handle_insert_buf(res)?;
                },
                recv(this.ticker) -> res => {
                    let _ = this.handle_clean_up(res)?;
                },
                recv(this.stop_rx) -> _ => return,
            }
        })
    }

    fn handle_insert_buf(&mut self, res: Result<Item<V>, RecvError>) -> Result<(), CacheError> {
        res.map(|item| self.handle_item(item)).map_err(|e| {
            CacheError::RecvError(format!(
                "fail to receive msg from insert buffer: {}",
                e.to_string()
            ))
        })
    }

    fn handle_item(&mut self, item: Item<V>) {
        match item {
            Item::New {
                key,
                conflict,
                cost,
                value,
                expiration,
            } => {
                // TODO: Calculate item cost value if new or update.

                let (victims, added) = self.policy.add(key, cost);

                if added {
                    self.store.insert(key, value, conflict, expiration);
                    self.metrics.add(MetricType::KeyAdd, key, 1);
                    self.track_admission(key);
                } else {
                    // TODO: onReject
                }

                victims.map(|victims| {
                    victims.iter().for_each(|victim| {
                        let (conflict, value) = self.store.remove(victim.key, 0);

                        // TODO: on_evict(victim)
                    })
                });
            }
            Item::Update { key, cost } => self.policy.update(key, cost),
            Item::Delete { key, conflict } => {
                self.policy.remove(key); // deals with metrics updates.
                let (_, val) = self.store.remove(key, conflict);
                // TODO: on_exit(val)
            }
            Item::Wait(wg) => {
                drop(wg);
            }
        }
    }

    fn track_admission(&self, key: u64) {}

    fn handle_clean_up(&self, res: Result<Instant, RecvError>) -> Result<(), CacheError> {
        res.map(|i| ()).map_err(|e| {
            CacheError::RecvError(format!(
                "fail to receive msg from ticker: {}",
                e.to_string()
            ))
        })
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_builder() {}
}
