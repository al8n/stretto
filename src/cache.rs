mod async_impl;
mod sync_impl;
#[cfg(test)]
mod test;

use crate::error::CacheError;
use crate::metrics::{MetricType, Metrics};
use crate::policy::LFUPolicy;
use crate::store::{ShardedMap, UpdateResult};
use crate::ttl::Time;
use crate::utils::{CloseableSender, ValueRef, ValueRefMut};
use crate::{
    CacheCallback, Coster, DefaultCacheCallback, DefaultCoster, DefaultUpdateValidator,
    Item as CrateItem, KeyHasher, UpdateValidator,
};
use crossbeam::{
    channel::{bounded, tick, Receiver, RecvError},
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
const DEFAULT_INSERT_BUF_SIZE: usize = 32 * 1024;
const DEFAULT_BUFFER_ITEMS: usize = 64;
const DEFAULT_CLEANUP_DURATION: Duration = Duration::from_millis(500);
pub(crate) const DEFAULT_CLEANUP_DURATION_FACTOR: u64 = 5;

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
        external_cost: i64,
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

    fn update(kh: u64, cost: i64, external_cost: i64) -> Self {
        Self::Update {
            key: kh,
            cost,
            external_cost,
        }
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
    K: Hash + Eq,
    V: Send + Sync,
    KH: KeyHasher<K>,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
> {
    /// metrics determines whether cache statistics are kept during the cache's
    /// lifetime. There *is* some overhead to keeping statistics, so you should
    /// only set this flag to true when testing or throughput performance isn't a
    /// major factor.
    metrics: bool,

    /// ignore_internal_cost set to true indicates to the cache that the cost of
    /// internally storing the value should be ignored. This is useful when the
    /// cost passed to set is not using bytes as units. Keep in mind that setting
    /// this to true will increase the memory usage.
    ignore_internal_cost: bool,

    num_counters: usize,

    max_cost: i64,

    // buffer_items determines the size of Get buffers.
    //
    // Unless you have a rare use case, using `64` as the BufferItems value
    // results in good performance.
    // buffer_items: usize,
    /// `insert_buffer_size` determines the size of insert buffers.
    ///
    /// Default is 32 * 1024 (**TODO:** need to figure out the optimal size.).
    insert_buffer_size: usize,

    /// `cleanup_duration` is the duration for internal store to cleanup expired entry.
    ///
    /// Default is 2500ms.
    cleanup_duration: Duration,

    /// key_to_hash is used to customize the key hashing algorithm.
    /// Each key will be hashed using the provided function. If keyToHash value
    /// is not set, the default keyToHash function is used.
    key_to_hash: KH,

    /// cost evaluates a value and outputs a corresponding cost. This function
    /// is ran after insert is called for a new item or an item update with a cost
    /// param of 0.
    coster: Option<C>,

    /// update_validator is called when a value already exists in cache and is being updated.
    update_validator: Option<U>,

    callback: Option<CB>,

    marker_k: PhantomData<fn(K)>,
    marker_v: PhantomData<fn(V)>,
}

impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyHasher<K>>
    CacheBuilder<K, V, KH, DefaultCoster<V>, DefaultUpdateValidator<V>, DefaultCacheCallback<V>>
{
    pub fn new(num_counters: usize, max_cost: i64, kh: KH) -> Self {
        Self {
            num_counters,
            max_cost,
            // buffer_items: DEFAULT_BUFFER_ITEMS,
            insert_buffer_size: DEFAULT_INSERT_BUF_SIZE,
            metrics: false,
            callback: Some(DefaultCacheCallback::default()),
            key_to_hash: kh,
            update_validator: Some(DefaultUpdateValidator::default()),
            coster: Some(DefaultCoster::default()),
            ignore_internal_cost: false,
            cleanup_duration: DEFAULT_CLEANUP_DURATION,
            marker_k: Default::default(),
            marker_v: Default::default(),
        }
    }
}

impl<
        K: Hash + Eq,
        V: Send + Sync + 'static,
        KH: KeyHasher<K>,
        C: Coster<V>,
        U: UpdateValidator<V>,
        CB: CacheCallback<V>,
    > CacheBuilder<K, V, KH, C, U, CB>
{
    pub fn set_num_counters(self, num_counters: usize) -> Self {
        Self {
            num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_max_cost(self, max_cost: i64) -> Self {
        Self {
            num_counters: self.num_counters,
            max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_buffer_size(self, sz: usize) -> Self {
        Self {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: sz,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_metrics(self, val: bool) -> Self {
        Self {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: val,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_ignore_internal_cost(self, val: bool) -> Self {
        Self {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: val,
            cleanup_duration: self.cleanup_duration,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_cleanup_duration(self, d: Duration) -> Self {
        Self {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: d,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_key_hasher<NKH: KeyHasher<K>>(self, kh: NKH) -> CacheBuilder<K, V, NKH, C, U, CB> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: kh,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_coster<NC: Coster<V>>(self, coster: NC) -> CacheBuilder<K, V, KH, NC, U, CB> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: Some(coster),
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_update_validator<NU: UpdateValidator<V>>(
        self,
        uv: NU,
    ) -> CacheBuilder<K, V, KH, C, NU, CB> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: Some(uv),
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_callback<NCB: CacheCallback<V>>(self, cb: NCB) -> CacheBuilder<K, V, KH, C, U, NCB> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: Some(cb),
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn finalize(self) -> Result<Cache<K, V, KH, C, U, CB>, CacheError> {
        let num_counters = self.num_counters;

        if num_counters == 0 {
            return Err(CacheError::InvalidNumCounters);
        }

        let max_cost = self.max_cost;
        if max_cost == 0 {
            return Err(CacheError::InvalidMaxCost);
        }

        let insert_buffer_size = self.insert_buffer_size;
        if insert_buffer_size == 0 {
            return Err(CacheError::InvalidBufferSize);
        }

        let (buf_tx, buf_rx) = bounded(insert_buffer_size);
        let (stop_tx, stop_rx) = bounded(0);

        let store = Arc::new(ShardedMap::with_validator(self.update_validator.unwrap()));
        let mut policy = LFUPolicy::new(num_counters, max_cost)?;
        let item_size = store.item_size();

        let coster = Arc::new(self.coster.unwrap());
        let callback = Arc::new(self.callback.unwrap());
        let metrics = if self.metrics {
            let m = Arc::new(Metrics::new_op());
            policy.collect_metrics(m.clone());
            m
        } else {
            Arc::new(Metrics::new())
        };

        let policy = Arc::new(policy);
        CacheProcessor::spawn(
            100000,
            self.ignore_internal_cost,
            self.cleanup_duration,
            store.clone(),
            policy.clone(),
            buf_rx.clone(),
            stop_rx.clone(),
            metrics.clone(),
            coster.clone(),
            callback.clone(),
        );

        let this = Cache {
            store,
            policy,
            insert_buf_tx: CloseableSender::new(buf_tx),
            insert_buf_rx: buf_rx,
            callback,
            key_to_hash: self.key_to_hash,
            stop_tx: CloseableSender::new(stop_tx),
            stop_rx,
            is_closed: AtomicBool::new(false),
            coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            metrics,
            item_size,
            _marker: Default::default(),
        };

        Ok(this)
    }
}

/// Cache is a thread-safe implementation of a hashmap with a TinyLFU admission
/// policy and a Sampled LFU eviction policy. You can use the same Cache instance
/// from as many threads as you want.
pub struct Cache<
    K: Hash + Eq,
    V: Send + Sync,
    KH: KeyHasher<K>,
    C = DefaultCoster<V>,
    U = DefaultUpdateValidator<V>,
    CB = DefaultCacheCallback<V>,
> {
    /// store is the central concurrent hashmap where key-value items are stored.
    store: Arc<ShardedMap<V, U>>,

    /// policy determines what gets let in to the cache and what gets kicked out.
    policy: Arc<LFUPolicy>,

    /// set_buf is a buffer allowing us to batch/drop Sets during times of high
    /// contention.
    insert_buf_tx: CloseableSender<Item<V>>,
    insert_buf_rx: Receiver<Item<V>>,

    callback: Arc<CB>,

    key_to_hash: KH,

    stop_tx: CloseableSender<()>,
    stop_rx: Receiver<()>,

    is_closed: AtomicBool,

    coster: Arc<C>,

    ignore_internal_cost: bool,

    cleanup_duration: Duration,

    metrics: Arc<Metrics>,

    item_size: usize,

    _marker: PhantomData<fn(K)>,
}

impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyHasher<K>> Cache<K, V, KH> {
    pub fn new(num_counters: usize, max_cost: i64, kh: KH) -> Result<Self, CacheError> {
        CacheBuilder::new(num_counters, max_cost, kh).finalize()
    }

    pub fn builder(
        num_counters: usize,
        max_cost: i64,
        kh: KH,
    ) -> CacheBuilder<K, V, KH, DefaultCoster<V>, DefaultUpdateValidator<V>, DefaultCacheCallback<V>>
    {
        CacheBuilder::new(num_counters, max_cost, kh)
    }
}

impl<
        K: Hash + Eq,
        V: Send + Sync + 'static,
        KH: KeyHasher<K>,
        C: Coster<V>,
        U: UpdateValidator<V>,
        CB: CacheCallback<V>,
    > Cache<K, V, KH, C, U, CB>
{
    /// `get` returns the value (if any) and a boolean representing whether the
    /// value was found or not. The value can be nil and the boolean can be true at
    /// the same time.
    pub fn get(&self, key: &K) -> Option<ValueRef<V>> {
        if self.is_closed.load(Ordering::SeqCst) {
            return None;
        }

        let (kh, ch) = self.key_to_hash.hash_key(key);
        match self.store.get(&kh, ch) {
            None => {
                self.metrics.add(MetricType::Hit, kh, 1);
                None
            }
            Some(v) => {
                self.metrics.add(MetricType::Miss, kh, 1);
                Some(v)
            }
        }
    }

    /// `get_mut` returns the mutable value (if any) and a boolean representing whether the
    /// value was found or not. The value can be nil and the boolean can be true at
    /// the same time.
    pub fn get_mut(&self, key: &K) -> Option<ValueRefMut<V>> {
        if self.is_closed.load(Ordering::SeqCst) {
            return None;
        }

        let (kh, ch) = self.key_to_hash.hash_key(key);
        match self.store.get_mut(&kh, ch) {
            None => {
                self.metrics.add(MetricType::Hit, kh, 1);
                None
            }
            Some(v) => {
                self.metrics.add(MetricType::Miss, kh, 1);
                Some(v)
            }
        }
    }

    // GetTTL returns the TTL for the specified key if the
    // item was found and is not expired.
    pub fn get_ttl(&self, key: &K) -> Option<Duration> {
        let (kh, ch) = self.key_to_hash.hash_key(key);
        self.store.get(&kh, ch)
            .and_then(|_| self.store.expiration(&kh)
                .map(|time| time.get_ttl())
            )
    }

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
    pub fn insert_if_present(&self, key: K, val: V, cost: i64) -> bool {
        self.insert_in(key, val, cost, Duration::ZERO, true)
    }

    pub fn wait(&self) -> Result<(), CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        let wg = WaitGroup::new();
        let wait_item = Item::Wait(wg.clone());
        self.insert_buf_tx
            .send(wait_item)
            .map(|_| {
                wg.wait();
            })
            .map_err(|e| CacheError::SendError(format!("cache set buf sender: {}", e.to_string())))
    }

    pub fn remove(&self, k: &K) {
        if self.is_closed.load(Ordering::SeqCst) {
            return;
        }

        let (kh, ch) = self.key_to_hash.hash_key(&k);
        // delete immediately
        let prev = self.store.remove(&kh, ch);

        if let Some(prev) = prev {
            self.callback.on_exit(Some(prev.value.into_inner()));
        }
        // If we've set an item, it would be applied slightly later.
        // So we must push the same item to `setBuf` with the deletion flag.
        // This ensures that if a set is followed by a delete, it will be
        // applied in the correct order.
        let _ = self.insert_buf_tx.send(Item::delete(kh, ch));
    }

    pub fn clear(&self) -> Result<(), CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        // stop the process item thread.
        self.stop_tx.send(()).map_err(|e| {
            CacheError::SendError(format!(
                "fail to send stop signal to working thread {}",
                e.to_string()
            ))
        })?;

        // clear out the insert buffer channel.
        loop {
            select! {
                recv(self.insert_buf_rx) -> res => if let Ok(item) = res {
                    match item {
                        Item::New { key, conflict, cost, value, expiration } => {
                            self.callback.on_evict(CrateItem {
                                val: Some(value),
                                key,
                                conflict,
                                cost,
                                exp: expiration,
                            })
                        }
                        Item::Delete { .. } | Item::Update { .. } => {}
                        Item::Wait(wg) => drop(wg),
                    }
                },
                default => break,
            }
        }

        self.policy.clear();
        self.store.clear();
        self.metrics.clear();

        CacheProcessor::spawn(
            100000,
            self.ignore_internal_cost,
            self.cleanup_duration,
            self.store.clone(),
            self.policy.clone(),
            self.insert_buf_rx.clone(),
            self.stop_rx.clone(),
            self.metrics.clone(),
            self.coster.clone(),
            self.callback.clone(),
        );

        Ok(())
    }

    /// `close` stops all threads and closes all channels.
    pub fn close(&self) -> Result<(), CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        self.clear()?;
        // Block until processItems thread is returned
        self.stop_tx.send(())?;
        self.stop_tx.close();
        self.insert_buf_tx.close();
        self.is_closed.store(true, Ordering::SeqCst);
        Ok(())
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
        let external_cost = if cost == 0 { self.coster.cost(&val) } else { 0 };
        match self.store.update(key_hash, val, conflict_hash, expiration) {
            UpdateResult::NotExist(v) | UpdateResult::Reject(v) | UpdateResult::Conflict(v) => {
                if only_update {
                    None
                } else {
                    Some(Item::new(key_hash, conflict_hash, cost + external_cost, v, expiration))
                }
            }
            UpdateResult::Update(v) => {
                self.callback.on_exit(Some(v));
                Some(Item::update(key_hash, cost, external_cost))
            }
        }
        .map_or(false, |item| {
            // Attempt to send item to policy.
            select! {
                send(self.insert_buf_tx.tx, item) -> res => {

                    res.map_or(false, |_| true)
                },
                default => {
                    if item.is_update() {
                        // Return true if this was an update operation since we've already
                        // updated the store. For all the other operations (set/delete), we
                        // return false which means the item was not inserted.
                        true
                    } else {
                        self.metrics.add(MetricType::DropSets, key_hash, 1);
                        false
                    }
                }
            }
        })
    }
}

struct CacheProcessor<V: Send + Sync, C: Coster<V>, U: UpdateValidator<V>, CB: CacheCallback<V>> {
    insert_buf_rx: Receiver<Item<V>>,
    stop_rx: Receiver<()>,
    metrics: Arc<Metrics>,
    ticker: Receiver<Instant>,
    store: Arc<ShardedMap<V, U>>,
    policy: Arc<LFUPolicy>,
    start_ts: HashMap<u64, Time>,
    num_to_keep: usize,
    callback: Arc<CB>,
    coster: Arc<C>,
    ignore_internal_cost: bool,
    item_size: usize,
}

impl<V: Send + Sync + 'static, C: Coster<V>, U: UpdateValidator<V>, CB: CacheCallback<V>>
    CacheProcessor<V, C, U, CB>
{
    pub fn spawn(
        num_to_keep: usize,
        ignore_internal_cost: bool,
        cleanup_duration: Duration,
        store: Arc<ShardedMap<V, U>>,
        policy: Arc<LFUPolicy>,
        insert_buf_rx: Receiver<Item<V>>,
        stop_rx: Receiver<()>,
        metrics: Arc<Metrics>,
        coster: Arc<C>,
        callback: Arc<CB>,
    ) -> JoinHandle<Result<(), CacheError>> {
        let ticker = tick(cleanup_duration);
        let item_size = store.item_size();
        let mut this = Self {
            insert_buf_rx,
            stop_rx,
            metrics,
            ticker,
            store,
            policy,
            start_ts: HashMap::<u64, Time>::new(),
            num_to_keep,
            callback,
            ignore_internal_cost,
            coster,
            item_size,
        };

        spawn(move || loop {
            select! {
                recv(this.insert_buf_rx) -> res => {
                    let _ = this.handle_insert_buf(res)?;
                },
                recv(this.ticker) -> res => {
                    let _ = this.handle_clean_up(res)?;
                },
                recv(this.stop_rx) -> _ => return Ok(()),
            }
        })
    }

    #[inline]
    fn handle_insert_buf(&mut self, res: Result<Item<V>, RecvError>) -> Result<(), CacheError> {
        res.map(|item| self.handle_item(item)).map_err(|e| {
            CacheError::RecvError(format!(
                "fail to receive msg from insert buffer: {}",
                e.to_string()
            ))
        })
    }

    #[inline]
    fn handle_item(&mut self, item: Item<V>) {
        match item {
            Item::New {
                key,
                conflict,
                cost,
                value,
                expiration,
            } => {
                let cost = self.calculate_internal_cost(cost);
                let (victims, added) = self.policy.add(key, cost);

                if added {
                    self.store.insert(key, value, conflict, expiration);
                    self.track_admission(key);
                } else {
                    self.callback.on_reject(CrateItem {
                        val: Some(value),
                        key,
                        conflict,
                        cost,
                        exp: expiration,
                    });
                }

                victims.iter().for_each(|victims| {
                    victims.iter().for_each(|victim| {
                        let sitem = self.store.remove(&victim.key, 0);
                        if let Some(sitem) = sitem {
                            let item = CrateItem {
                                key: victim.key,
                                val: Some(sitem.value.into_inner()),
                                cost: victim.cost,
                                conflict: sitem.conflict,
                                exp: sitem.expiration,
                            };
                            self.on_evict(item);
                        }
                    })
                });
            }
            Item::Update {
                key,
                cost,
                external_cost,
            } => {
                let cost = self.calculate_internal_cost(cost) + external_cost;
                self.policy.update(&key, cost)
            }
            Item::Delete { key, conflict } => {
                self.policy.remove(&key); // deals with metrics updates.
                if let Some(sitem) = self.store.remove(&key, conflict) {
                    self.callback.on_exit(Some(sitem.value.into_inner()));
                }
            }
            Item::Wait(wg) => {
                drop(wg);
            }
        }
    }

    #[inline]
    fn calculate_internal_cost(&self, cost: i64) -> i64 {
        if !self.ignore_internal_cost {
            // Add the cost of internally storing the object.
            cost + (self.item_size as i64)
        } else {
            cost
        }
    }

    #[inline]
    fn track_admission(&mut self, key: u64) {
        let added = self.metrics.add(MetricType::KeyAdd, key, 1);

        if added {
            if self.start_ts.len() > self.num_to_keep {
                self.start_ts = self.start_ts.drain().take(self.num_to_keep - 1).collect();
                self.start_ts.insert(key, Time::now());
            }
        }
    }

    #[inline]
    fn prepare_evict(&mut self, item: &CrateItem<V>) {
        if let Some(ts) = self.start_ts.get(&item.key) {
            self.metrics.track_eviction(ts.elapsed().as_secs() as i64);

            self.start_ts.remove(&item.key);
        }
    }

    #[inline]
    fn on_evict(&mut self, item: CrateItem<V>) {
        self.prepare_evict(&item);
        self.callback.on_evict(item);
    }

    #[inline]
    fn handle_clean_up(&mut self, res: Result<Instant, RecvError>) -> Result<(), CacheError> {
        res.map(|_| {
            self.store
                .clean_up(self.policy.clone())
                .into_iter()
                .for_each(|victim| {
                    self.prepare_evict(&victim);
                    self.callback.on_evict(victim);
                })
        })
        .map_err(|e| {
            CacheError::RecvError(format!(
                "fail to receive msg from ticker: {}",
                e.to_string()
            ))
        })
    }
}
