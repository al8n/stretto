use crate::cache::builder::CacheBuilderCore;
use crate::policy::LFUPolicy;
use crate::store::ShardedMap;
use crate::sync::{
    bounded, select, spawn, stop_channel, unbounded, Instant, JoinHandle, Receiver, Sender,
    UnboundedReceiver, UnboundedSender, WaitGroup,
};
use crate::ttl::{ExpirationMap, Time};
use crate::{
    metrics::MetricType, CacheCallback, CacheError, Coster, DefaultCacheCallback, DefaultCoster,
    DefaultUpdateValidator, KeyBuilder, Metrics, UpdateValidator,
};
use crossbeam_channel::{tick, RecvError};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The `CacheBuilder` struct is used when creating [`Cache`] instances if you want to customize the [`Cache`] settings.
///
/// - **num_counters**
///
///     `num_counters` is the number of 4-bit access counters to keep for admission and eviction.
///     Dgraph's developers have seen good performance in setting this to 10x the number of items
///     you expect to keep in the cache when full.
///
///     For example, if you expect each item to have a cost of 1 and `max_cost` is 100, set `num_counters` to 1,000.
///     Or, if you use variable cost values but expect the cache to hold around 10,000 items when full,
///     set num_counters to 100,000. The important thing is the *number of unique items* in the full cache,
///     not necessarily the `max_cost` value.
///
/// - **max_cost**
///
///     `max_cost` is how eviction decisions are made. For example, if max_cost is 100 and a new item
///     with a cost of 1 increases total cache cost to 101, 1 item will be evicted.
///
///     `max_cost` can also be used to denote the max size in bytes. For example,
///     if max_cost is 1,000,000 (1MB) and the cache is full with 1,000 1KB items,
///     a new item (that's accepted) would cause 5 1KB items to be evicted.
///
///     `max_cost` could be anything as long as it matches how you're using the cost values when calling [`insert`].
///
/// - **key_builder**
///
///     [`KeyBuilder`] is the hashing algorithm used for every key. In Stretto, the Cache will never store the real key.
///     The key will be processed by [`KeyBuilder`]. Stretto has two default built-in key builder,
///     one is [`TransparentKeyBuilder`], the other is [`DefaultKeyBuilder`]. If your key implements [`TransparentKey`] trait,
///     you can use [`TransparentKeyBuilder`] which is faster than [`DefaultKeyBuilder`]. Otherwise, you should use [`DefaultKeyBuilder`]
///     You can also write your own key builder for the Cache, by implementing [`KeyBuilder`] trait.
///
///     Note that if you want 128bit hashes you should use the full `(u64, u64)`,
///     otherwise just fill the `u64` at the `0` position, and it will behave like
///     any 64bit hash.
///
/// - **buffer_size**
///
///     `buffer_size` is the size of the insert buffers. The Dgraph's developers find that 32 * 1024 gives a good performance.
///
///     If for some reason you see insert performance decreasing with lots of contention (you shouldn't),
///     try increasing this value in increments of 32 * 1024. This is a fine-tuning mechanism
///     and you probably won't have to touch this.
///
/// - **metrics**
///
///     Metrics is true when you want real-time logging of a variety of stats.
///     The reason this is a [`CacheBuilder`] flag is because there's a 10% throughput performance overhead.
///
/// - **ignore_internal_cost**
///
///     Set to true indicates to the cache that the cost of
///     internally storing the value should be ignored. This is useful when the
///     cost passed to set is not using bytes as units. Keep in mind that setting
///     this to true will increase the memory usage.
///
/// - **cleanup_duration**
///
///     The [`Cache`] will cleanup the expired values every 500ms by default.
///
/// - **update_validator**
///
///     By default, the Cache will always update the value if the value already exists in the cache.
///     [`UpdateValidator`] is a trait to support customized update policy (check if the value should be updated
///     if the value already exists in the cache).
///
/// - **callback**
///
///     [`CacheCallback`] is for customize some extra operations on values when related event happens..
///
/// - **coster**
///
///     [`Coster`] is a trait you can pass to the [`CacheBuilder`] in order to evaluate
///     item cost at runtime, and only for the [`insert`] calls that aren't dropped (this is
///     useful if calculating item cost is particularly expensive, and you don't want to
///     waste time on items that will be dropped anyways).
///
///     To signal to Stretto that you'd like to use this Coster trait:
///
///     1. Set the Coster field to your own Coster implementation.
///     2. When calling [`insert`] for new items or item updates, use a cost of 0.
///
/// - **hasher**
///
///     The hasher for the [`Cache`], default is SipHasher.
///
/// [`Cache`]: struct.Cache.html
/// [`CacheBuilder`]: struct.CacheBuilder.html
/// [`TransparentKey`]: struct.TransparentKey.html
/// [`TransparentKeyBuilder`]: struct.TransparentKeyBuilder.html
/// [`DefaultKeyBuilder`]: struct.DefaultKeyBuilder.html
/// [`KeyBuilder`]: trait.KeyBuilder.html
/// [`insert`]: struct.Cache.html#method.insert
/// [`UpdateValidator`]: trait.UpdateValidator.html
/// [`CacheCallback`]: trait.CacheCallback.html
/// [`Coster`]: trait.Coster.html
pub struct CacheBuilder<
    K: Hash + Eq,
    V: Send + Sync + 'static,
    KH: KeyBuilder<K>,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    S: BuildHasher + Clone + 'static,
> {
    inner: CacheBuilderCore<K, V, KH, C, U, CB, S>,
}

impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<K>>
    CacheBuilder<
        K,
        V,
        KH,
        DefaultCoster<V>,
        DefaultUpdateValidator<V>,
        DefaultCacheCallback<V>,
        RandomState,
    >
{
    /// Create a new AsyncCacheBuilder
    #[inline]
    pub fn new(num_counters: usize, max_cost: i64, kh: KH) -> Self {
        Self {
            inner: CacheBuilderCore::new(num_counters, max_cost, kh),
        }
    }
}

impl<K, V, KH, C, U, CB, S> CacheBuilder<K, V, KH, C, U, CB, S>
where
    K: Hash + Eq,
    V: Send + Sync + 'static,
    KH: KeyBuilder<K>,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    S: BuildHasher + Clone + 'static + Send,
{
    /// Build Cache and start all threads needed by the Cache.
    #[inline]
    pub fn finalize(self) -> Result<Cache<K, V, KH, C, U, CB, S>, CacheError> {
        let num_counters = self.inner.num_counters;

        if num_counters == 0 {
            return Err(CacheError::InvalidNumCounters);
        }

        let max_cost = self.inner.max_cost;
        if max_cost == 0 {
            return Err(CacheError::InvalidMaxCost);
        }

        let insert_buffer_size = self.inner.insert_buffer_size;
        if insert_buffer_size == 0 {
            return Err(CacheError::InvalidBufferSize);
        }

        let (buf_tx, buf_rx) = bounded(insert_buffer_size);
        let (stop_tx, stop_rx) = stop_channel();
        let (clear_tx, clear_rx) = unbounded();

        let hasher = self.inner.hasher.unwrap();
        let expiration_map = ExpirationMap::with_hasher(hasher.clone());

        let store = Arc::new(ShardedMap::with_validator_and_hasher(
            expiration_map,
            self.inner.update_validator.unwrap(),
            hasher.clone(),
        ));

        let mut policy = LFUPolicy::with_hasher(num_counters, max_cost, hasher)?;

        let coster = Arc::new(self.inner.coster.unwrap());
        let callback = Arc::new(self.inner.callback.unwrap());
        let metrics = if self.inner.metrics {
            let m = Arc::new(Metrics::new_op());
            policy.collect_metrics(m.clone());
            m
        } else {
            Arc::new(Metrics::new())
        };

        let policy = Arc::new(policy);
        CacheProcessor::new(
            100000,
            self.inner.ignore_internal_cost,
            self.inner.cleanup_duration,
            store.clone(),
            policy.clone(),
            buf_rx,
            stop_rx,
            clear_rx,
            metrics.clone(),
            callback.clone(),
        )
        .spawn();

        let this = Cache {
            store,
            policy,
            insert_buf_tx: buf_tx,
            callback,
            key_to_hash: Arc::new(self.inner.key_to_hash),
            stop_tx,
            clear_tx,
            is_closed: Arc::new(AtomicBool::new(false)),
            coster,
            metrics,
            _marker: Default::default(),
        };

        Ok(this)
    }
}

pub(crate) enum Item<V> {
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
    #[inline]
    fn new(key: u64, conflict: u64, cost: i64, val: V, exp: Time) -> Self {
        Self::New {
            key,
            conflict,
            cost,
            value: val,
            expiration: exp,
        }
    }

    #[inline]
    pub(crate) fn update(key: u64, cost: i64, external_cost: i64) -> Self {
        Self::Update {
            key,
            cost,
            external_cost,
        }
    }

    #[inline]
    fn delete(key: u64, conflict: u64) -> Self {
        Self::Delete { key, conflict }
    }

    #[inline]
    fn is_update(&self) -> bool {
        matches!(self, Item::Update { .. })
    }
}

pub(crate) struct CacheProcessor<V, U, CB, S>
where
    V: Send + Sync + 'static,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    S: BuildHasher + Clone + 'static + Send,
{
    pub(crate) insert_buf_rx: Receiver<Item<V>>,
    pub(crate) stop_rx: Receiver<()>,
    pub(crate) clear_rx: UnboundedReceiver<()>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) store: Arc<ShardedMap<V, U, S, S>>,
    pub(crate) policy: Arc<LFUPolicy<S>>,
    pub(crate) start_ts: HashMap<u64, Time, S>,
    pub(crate) num_to_keep: usize,
    pub(crate) callback: Arc<CB>,
    pub(crate) ignore_internal_cost: bool,
    pub(crate) item_size: usize,
    pub(crate) cleanup_duration: Duration,
}

pub(crate) struct CacheCleaner<'a, V, U, CB, S>
where
    V: Send + Sync + 'static,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    S: BuildHasher + Clone + 'static + Send,
{
    pub(crate) processor: &'a mut CacheProcessor<V, U, CB, S>,
}

/// Cache is a thread-safe implementation of a hashmap with a TinyLFU admission
/// policy and a Sampled LFU eviction policy. You can use the same Cache instance
/// from as many threads as you want.
///
///
/// # Features
/// * **Internal Mutability** - Do not need to use `Arc<RwLock<Cache<...>>` for concurrent code, you just need `Cache<...>`
/// * **Sync and Async** - Stretto support async by `tokio` and sync by `crossbeam`.
///   * In sync, Cache starts two extra OS level threads. One is policy thread, the other is writing thread.
///   * In async, Cache starts two extra green threads. One is policy thread, the other is writing thread.
/// * **Store policy** Stretto only store the value, which means the cache does not store the key.
/// * **High Hit Ratios** - with our unique admission/eviction policy pairing, Ristretto's performance is best in class.
///     * **Eviction: SampledLFU** - on par with exact LRU and better performance on Search and Database traces.
///     * **Admission: TinyLFU** - extra performance with little memory overhead (12 bits per counter).
/// * **Fast Throughput** - we use a variety of techniques for managing contention and the result is excellent throughput.
/// * **Cost-Based Eviction** - any large new item deemed valuable can evict multiple smaller items (cost could be anything).
/// * **Fully Concurrent** - you can use as many threads as you want with little throughput degradation.
/// * **Metrics** - optional performance metrics for throughput, hit ratios, and other stats.
/// * **Simple API** - just figure out your ideal [`CacheBuilder`] values and you're off and running.
///
/// [`CacheBuilder`]: struct.CacheBuilder.html
pub struct Cache<
    K,
    V,
    KH,
    C = DefaultCoster<V>,
    U = DefaultUpdateValidator<V>,
    CB = DefaultCacheCallback<V>,
    S = RandomState,
> where
    K: Hash + Eq,
    V: Send + Sync + 'static,
    KH: KeyBuilder<K>,
{
    /// store is the central concurrent hashmap where key-value items are stored.
    pub(crate) store: Arc<ShardedMap<V, U, S, S>>,

    /// policy determines what gets let in to the cache and what gets kicked out.
    pub(crate) policy: Arc<LFUPolicy<S>>,

    /// insert_buf is a buffer allowing us to batch/drop Sets during times of high
    /// contention.
    pub(crate) insert_buf_tx: Sender<Item<V>>,

    pub(crate) stop_tx: Sender<()>,

    pub(crate) clear_tx: UnboundedSender<()>,

    pub(crate) callback: Arc<CB>,

    pub(crate) key_to_hash: Arc<KH>,

    pub(crate) is_closed: Arc<AtomicBool>,

    pub(crate) coster: Arc<C>,

    /// the metrics for the cache
    pub metrics: Arc<Metrics>,

    pub(crate) _marker: PhantomData<fn(K)>,
}

impl<K, V, KH, C, U, CB, S> Cache<K, V, KH, C, U, CB, S>
where
    K: Hash + Eq,
    V: Send + Sync + 'static,
    KH: KeyBuilder<K>,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    S: BuildHasher + Clone + 'static,
{
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

    /// wait until all the previous operations finished.
    pub fn wait(&self) -> Result<(), CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        let wg = WaitGroup::new();
        let wait_item = Item::Wait(wg.add(1));
        self.insert_buf_tx
            .send(wait_item)
            .map(|_| wg.wait())
            .map_err(|e| CacheError::SendError(format!("cache set buf sender: {}", e)))
    }

    /// remove an entry from Cache by key.
    pub fn remove(&self, k: &K) {
        if self.is_closed.load(Ordering::SeqCst) {
            return;
        }

        let (index, conflict) = self.key_to_hash.build_key(k);
        // delete immediately
        let prev = self.store.remove(&index, conflict);

        if let Some(prev) = prev {
            self.callback.on_exit(Some(prev.value.into_inner()));
        }
        // If we've set an item, it would be applied slightly later.
        // So we must push the same item to `setBuf` with the deletion flag.
        // This ensures that if a set is followed by a delete, it will be
        // applied in the correct order.
        let _ = self.insert_buf_tx.send(Item::delete(index, conflict));
    }

    /// `close` stops all threads and closes all channels.
    #[inline]
    pub fn close(&self) -> Result<(), CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        self.clear()?;
        // Block until processItems thread is returned
        self.stop_tx
            .send(())
            .map_err(|e| CacheError::SendError(format!("{}", e)))?;
        self.policy.close()?;
        self.is_closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    #[inline]
    fn insert_in(&self, key: K, val: V, cost: i64, ttl: Duration, only_update: bool) -> bool {
        if self.is_closed.load(Ordering::SeqCst) {
            return false;
        }

        self.update(key, val, cost, ttl, only_update)
            .map_or(false, |(index, item)| {
                let is_update = item.is_update();
                // Attempt to send item to policy.
                select! {
                    send(self.insert_buf_tx, item) -> res => {
                        res.map_or_else(|_| {
                            if is_update {
                                // Return true if this was an update operation since we've already
                                // updated the store. For all the other operations (set/delete), we
                                // return false which means the item was not inserted.
                                true
                            } else {
                                self.metrics.add(MetricType::DropSets, index, 1);
                                false
                            }
                        }, |_| true)
                    },
                    default => {
                        if item.is_update() {
                            // Return true if this was an update operation since we've already
                            // updated the store. For all the other operations (set/delete), we
                            // return false which means the item was not inserted.
                            true
                        } else {
                            self.metrics.add(MetricType::DropSets, index, 1);
                            false
                        }
                    }
                }
            })
    }
}

impl<V, U, CB, S> CacheProcessor<V, U, CB, S>
where
    V: Send + Sync + 'static,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    S: BuildHasher + Clone + 'static + Send,
{
    pub(crate) fn new(
        num_to_keep: usize,
        ignore_internal_cost: bool,
        cleanup_duration: Duration,
        store: Arc<ShardedMap<V, U, S, S>>,
        policy: Arc<LFUPolicy<S>>,
        insert_buf_rx: Receiver<Item<V>>,
        stop_rx: Receiver<()>,
        clear_rx: UnboundedReceiver<()>,
        metrics: Arc<Metrics>,
        callback: Arc<CB>,
    ) -> Self {
        let item_size = store.item_size();
        let hasher = store.hasher();
        Self {
            insert_buf_rx,
            stop_rx,
            clear_rx,
            metrics,
            store,
            policy,
            start_ts: HashMap::with_hasher(hasher),
            num_to_keep,
            callback,
            ignore_internal_cost,
            item_size,
            cleanup_duration,
        }
    }

    #[inline]
    pub(crate) fn spawn(mut self) -> JoinHandle<Result<(), CacheError>> {
        let ticker = tick(self.cleanup_duration);
        spawn(move || loop {
            select! {
                recv(self.insert_buf_rx) -> res => {
                    let _ = self.handle_insert_event(res)?;
                },
                recv(self.clear_rx) -> _ => {
                    let _ = self.handle_clear_event()?;
                },
                recv(ticker) -> msg => {
                    let _ = self.handle_cleanup_event(msg)?;
                },
                recv(self.stop_rx) -> _ => return Ok(()),
            }
        })
    }

    #[inline]
    pub(crate) fn handle_clear_event(&mut self) -> Result<(), CacheError> {
        CacheCleaner::new(self).clean()
    }

    #[inline]
    pub(crate) fn handle_insert_event(
        &mut self,
        msg: Result<Item<V>, RecvError>,
    ) -> Result<(), CacheError> {
        msg.map(|item| self.handle_item(item)).map_err(|e| {
            CacheError::RecvError(format!(
                "fail to receive msg from insert buffer: {}",
                e
            ))
        })
    }

    #[inline]
    pub(crate) fn handle_cleanup_event(
        &mut self,
        res: Result<Instant, RecvError>,
    ) -> Result<(), CacheError> {
        res.map(|_| {
            self.store
                .cleanup(self.policy.clone())
                .into_iter()
                .for_each(|victim| {
                    self.prepare_evict(&victim);
                    self.callback.on_evict(victim);
                })
        })
        .map_err(|e| {
            CacheError::RecvError(format!(
                "fail to receive msg from ticker: {}",
                e
            ))
        })
    }
}

impl<'a, V, U, CB, S> CacheCleaner<'a, V, U, CB, S>
where
    V: Send + Sync + 'static,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    S: BuildHasher + Clone + 'static + Send,
{
    #[inline]
    pub(crate) fn clean(mut self) -> Result<(), CacheError> {
        loop {
            select! {
                // clear out the insert buffer channel.
                recv(self.processor.insert_buf_rx) -> msg => {
                    msg.map(|item| self.handle_item(item)).map_err(|e| CacheError::RecvError(format!("fail to receive msg from insert buffer: {}", e)))?;
                },
                default => return Ok(()),
            }
        }
    }
}

impl_builder!(CacheBuilder);
impl_cache!(Cache, CacheBuilder, Item);
impl_cache_processor!(CacheProcessor, Item);
impl_cache_cleaner!(CacheCleaner, CacheProcessor, Item);
