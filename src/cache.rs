#[cfg(test)]
mod test;
mod wg;

cfg_not_async!(
    use crossbeam_channel::{tick, RecvError};
);

use crate::{
    bounded,
    policy::LFUPolicy,
    ttl::ExpirationMap,
    cache::wg::WaitGroup,
    metrics::MetricType,
    select, sleep, spawn, stop_channel,
    store::{ShardedMap, UpdateResult},
    ttl::Time,
    unbounded,
    utils::{ValueRef, ValueRefMut},
    CacheCallback, CacheError, Coster, DefaultCacheCallback, DefaultCoster, DefaultUpdateValidator,
    Instant, Item as CrateItem, JoinHandle, KeyBuilder, Metrics, Receiver, Sender,
    UnboundedReceiver, UnboundedSender, UpdateValidator,
};
use std::hash::{BuildHasher, Hash};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

// TODO: find the optimal value for this
const DEFAULT_INSERT_BUF_SIZE: usize = 32 * 1024;
// const DEFAULT_BUFFER_ITEMS: usize = 64;
const DEFAULT_CLEANUP_DURATION: Duration = Duration::from_millis(500);

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
    fn update(key: u64, cost: i64, external_cost: i64) -> Self {
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
        match self {
            Item::Update { .. } => true,
            _ => false,
        }
    }
}

/// The `CacheBuilder` struct is used when creating Cache instances if you want to customize the Cache settings.
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
///     `max_cost` could be anything as long as it matches how you're using the cost values when calling `insert`.
///
/// - **key_builder**
///
///     KeyBuilder is the hashing algorithm used for every key. In Stretto, the Cache will never store the real key.
///     The key will be processed by [`KeyBuilder`]. Stretto has two default built-in key builder,
///     one is [`TransparentKeyBuilder`], the other is [`DefaultKeyBuilder`]. If your key is `u*` or `i*`(such as `u64`, `i64`),
///     you can use [`TransparentKeyBuilder`] which is faster than [`DefaultKeyBuilder`].
///     You can also implement your own [`KeyBuilder`].
///
///     Note that if you want 128bit hashes you should use the full `[u64, u64]`,
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
///     The reason this is a CacheBuilder flag is because there's a 10% throughput performance overhead.
/// 
/// - **ignore_internal_cost**
///
///     By default, when `insert` a value in the Cache, there will always have 56 for internal cost,
///     because the size of stored item in Cache is 56(excluding the size of value).
///     Set it to true to ignore the internal cost.
/// 
/// - **update_validator**
/// 
///     By default, the Cache will always update the value if the value already exists in the cache,
///     this trait is for you to check if the value should be updated.
/// 
/// - **callback**
/// 
///     CacheCallback is for customize some extra operations on values when related event happens..
///
/// - **coster**
/// 
///     Cost is a trait you can pass to the CacheBuilder in order to evaluate
///     item cost at runtime, and only for the `insert` calls that aren't dropped (this is
///     useful if calculating item cost is particularly expensive, and you don't want to
///     waste time on items that will be dropped anyways).
/// 
///     To signal to Stretto that you'd like to use this Coster trait:
/// 
///     1. Set the Coster field to your own Coster implementation.
///     2. When calling `insert` for new items or item updates, use a `cost` of 0.
///
/// [`Cache`]: struct.Cache.html
/// [`TransparentKeyBuilder`]: struct.TransparentKeyBuilder.html
/// [`DefaultKeyBuilder`]: struct.DefaultKeyBuilder.html
/// [`KeyBuilder`]: trait.KeyBuilder.html
pub struct CacheBuilder<
    K: Hash + Eq,
    V: Send + Sync + 'static,
    KH: KeyBuilder<K>,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    PS: BuildHasher + Clone + 'static,
    ES: BuildHasher + Clone + 'static,
    SS: BuildHasher + Clone + 'static,
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
    /// Default is 500ms.
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

    policy_hasher: Option<PS>,

    expiration_hasher: Option<ES>,

    store_hasher: Option<SS>,

    marker_k: PhantomData<fn(K)>,
    marker_v: PhantomData<fn(V)>,
}

/// Cache is a thread-safe implementation of a hashmap with a TinyLFU admission
/// policy and a Sampled LFU eviction policy. You can use the same Cache instance
/// from as many threads as you want.
///
/// 
/// # Features
/// * **Internal Mutability** - Do not need to use `Arc<RwLock<Cache<...>>` for concurrent code, you just need `Arc<Cache<...>`
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
    PS = RandomState,
    ES = RandomState,
    SS = RandomState,
> where
    K: Hash + Eq,
    V: Send + Sync + 'static,
    KH: KeyBuilder<K>,
{
    /// store is the central concurrent hashmap where key-value items are stored.
    store: Arc<ShardedMap<V, U, SS, ES>>,

    /// policy determines what gets let in to the cache and what gets kicked out.
    policy: Arc<LFUPolicy<PS>>,

    /// set_buf is a buffer allowing us to batch/drop Sets during times of high
    /// contention.
    insert_buf_tx: Sender<Item<V>>,

    stop_tx: Sender<()>,

    clear_tx: UnboundedSender<()>,

    callback: Arc<CB>,

    key_to_hash: KH,

    is_closed: AtomicBool,

    coster: Arc<C>,

    /// the metrics for the cache
    pub metrics: Arc<Metrics>,

    _marker: PhantomData<fn(K)>,
}

struct CacheProcessor<V, U, CB, PS, ES, SS>
    where
        V: Send + Sync + 'static,
        U: UpdateValidator<V>,
        CB: CacheCallback<V>,
        PS: BuildHasher + Clone + 'static,
        ES: BuildHasher + Clone + 'static,
        SS: BuildHasher + Clone + 'static,
{
    insert_buf_rx: Receiver<Item<V>>,
    stop_rx: Receiver<()>,
    clear_rx: UnboundedReceiver<()>,
    metrics: Arc<Metrics>,
    store: Arc<ShardedMap<V, U, SS, ES>>,
    policy: Arc<LFUPolicy<PS>>,
    start_ts: HashMap<u64, Time>,
    num_to_keep: usize,
    callback: Arc<CB>,
    ignore_internal_cost: bool,
    item_size: usize,
    cleanup_duration: Duration,
}

struct CacheCleaner<'a, V, U, CB, PS, ES, SS>
    where
        V: Send + Sync + 'static,
        U: UpdateValidator<V>,
        CB: CacheCallback<V>,
        PS: BuildHasher + Clone + 'static,
        ES: BuildHasher + Clone + 'static,
        SS: BuildHasher + Clone + 'static,
{
    processor: &'a mut CacheProcessor<V, U, CB, PS, ES, SS>,
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
    RandomState,
    RandomState,
>
{
    /// Create a new CacheBuilder
    #[inline]
    pub fn new(num_counters: usize, max_cost: i64, index: KH) -> Self {
        Self {
            num_counters,
            max_cost,
            // buffer_items: DEFAULT_BUFFER_ITEMS,
            insert_buffer_size: DEFAULT_INSERT_BUF_SIZE,
            metrics: false,
            callback: Some(DefaultCacheCallback::default()),
            policy_hasher: Some(RandomState::default()),
            expiration_hasher: Some(RandomState::default()),
            key_to_hash: index,
            update_validator: Some(DefaultUpdateValidator::default()),
            coster: Some(DefaultCoster::default()),
            ignore_internal_cost: false,
            cleanup_duration: DEFAULT_CLEANUP_DURATION,
            marker_k: Default::default(),
            marker_v: Default::default(),
            store_hasher: Some(RandomState::default()),
        }
    }
}

impl<K, V, KH, C, U, CB, PS, ES, SS> CacheBuilder<K, V, KH, C, U, CB, PS, ES, SS>
    where
        K: Hash + Eq,
        V: Send + Sync + 'static,
        KH: KeyBuilder<K>,
        C: Coster<V>,
        U: UpdateValidator<V>,
        CB: CacheCallback<V>,
        PS: BuildHasher + Clone + 'static,
        ES: BuildHasher + Clone + 'static,
        SS: BuildHasher + Clone + 'static,
{
    ///
    #[inline]
    pub fn set_num_counters(self, num_counters: usize) -> Self {
        Self {
            num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
            store_hasher: self.store_hasher,
        }
    }

    #[inline]
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
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    #[inline]
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
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    #[inline]
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
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    #[inline]
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
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    #[inline]
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
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    #[inline]
    pub fn set_key_builder<NKH: KeyBuilder<K>>(
        self,
        index: NKH,
    ) -> CacheBuilder<K, V, NKH, C, U, CB, PS, ES, SS> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: index,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    #[inline]
    pub fn set_coster<NC: Coster<V>>(
        self,
        coster: NC,
    ) -> CacheBuilder<K, V, KH, NC, U, CB, PS, ES, SS> {
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
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    #[inline]
    pub fn set_update_validator<NU: UpdateValidator<V>>(
        self,
        uv: NU,
    ) -> CacheBuilder<K, V, KH, C, NU, CB, PS, ES, SS> {
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
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    #[inline]
    pub fn set_callback<NCB: CacheCallback<V>>(
        self,
        cb: NCB,
    ) -> CacheBuilder<K, V, KH, C, U, NCB, PS, ES, SS> {
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
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    #[inline]
    pub fn set_policy_hasher<NPS: BuildHasher + Clone + 'static>(
        self,
        hasher: NPS,
    ) -> CacheBuilder<K, V, KH, C, U, CB, NPS, ES, SS> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: Some(hasher),
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    #[inline]
    pub fn set_expiration_hasher<NES: BuildHasher + Clone + 'static>(
        self,
        hasher: NES,
    ) -> CacheBuilder<K, V, KH, C, U, CB, PS, NES, SS> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: Some(hasher),
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    #[inline]
    pub fn set_store_hasher<NSS: BuildHasher + Clone + 'static>(
        self,
        hasher: NSS,
    ) -> CacheBuilder<K, V, KH, C, U, CB, PS, ES, NSS> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: Some(hasher),
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    #[inline]
    pub fn finalize(self) -> Result<Cache<K, V, KH, C, U, CB, PS, ES, SS>, CacheError> {
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
        let (stop_tx, stop_rx) = stop_channel();
        let (clear_tx, clear_rx) = unbounded();

        let expiration_map = ExpirationMap::with_hasher(self.expiration_hasher.unwrap());

        let store = Arc::new(ShardedMap::with_validator_and_hasher(
            expiration_map,
            self.update_validator.unwrap(),
            self.store_hasher.unwrap(),
        ));

        let mut policy =
            LFUPolicy::with_hasher(num_counters, max_cost, self.policy_hasher.unwrap())?;

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
        CacheProcessor::new(
            100000,
            self.ignore_internal_cost,
            self.cleanup_duration,
            store.clone(),
            policy.clone(),
            buf_rx,
            stop_rx,
            clear_rx,
            metrics.clone(),
            callback.clone(),
        ).spawn();

        let this = Cache {
            store,
            policy,
            insert_buf_tx: buf_tx,
            callback,
            key_to_hash: self.key_to_hash,
            stop_tx,
            clear_tx,
            is_closed: AtomicBool::new(false),
            coster,
            metrics,
            _marker: Default::default(),
        };

        Ok(this)
    }
}

impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<K>> Cache<K, V, KH> {
    #[inline]
    pub fn new(num_counters: usize, max_cost: i64, index: KH) -> Result<Self, CacheError> {
        CacheBuilder::new(num_counters, max_cost, index).finalize()
    }

    #[inline]
    pub fn builder(
        num_counters: usize,
        max_cost: i64,
        index: KH,
    ) -> CacheBuilder<
        K,
        V,
        KH,
        DefaultCoster<V>,
        DefaultUpdateValidator<V>,
        DefaultCacheCallback<V>,
        RandomState,
        RandomState,
        RandomState,
    > {
        CacheBuilder::new(num_counters, max_cost, index)
    }
}

impl<K, V, KH, C, U, CB, PS, ES, SS> Cache<K, V, KH, C, U, CB, PS, ES, SS>
    where
        K: Hash + Eq,
        V: Send + Sync + 'static,
        KH: KeyBuilder<K>,
        C: Coster<V>,
        U: UpdateValidator<V>,
        CB: CacheCallback<V>,
        PS: BuildHasher + Clone + 'static,
        ES: BuildHasher + Clone + 'static,
        SS: BuildHasher + Clone + 'static,
{
    /// `get` returns the value (if any) and a boolean representing whether the
    /// value was found or not. The value can be nil and the boolean can be true at
    /// the same time.
    pub fn get(&self, key: &K) -> Option<ValueRef<V, SS>> {
        if self.is_closed.load(Ordering::SeqCst) {
            return None;
        }

        let (index, conflict) = self.key_to_hash.build_key(key);

        match self.store.get(&index, conflict) {
            None => {
                self.metrics.add(MetricType::Hit, index, 1);
                None
            }
            Some(v) => {
                self.metrics.add(MetricType::Miss, index, 1);
                Some(v)
            }
        }
    }

    /// `get_mut` returns the mutable value (if any) and a boolean representing whether the
    /// value was found or not. The value can be nil and the boolean can be true at
    /// the same time.
    pub fn get_mut(&self, key: &K) -> Option<ValueRefMut<V, SS>> {
        if self.is_closed.load(Ordering::SeqCst) {
            return None;
        }

        let (index, conflict) = self.key_to_hash.build_key(key);
        match self.store.get_mut(&index, conflict) {
            None => {
                self.metrics.add(MetricType::Hit, index, 1);
                None
            }
            Some(v) => {
                self.metrics.add(MetricType::Miss, index, 1);
                Some(v)
            }
        }
    }

    // GetTTL returns the TTL for the specified key if the
    // item was found and is not expired.
    pub fn get_ttl(&self, key: &K) -> Option<Duration> {
        let (index, conflict) = self.key_to_hash.build_key(key);
        self.store
            .get(&index, conflict)
            .and_then(|_| self.store.expiration(&index).map(|time| time.get_ttl()))
    }

    #[inline]
    pub fn clear(&self) -> Result<(), CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        // stop the process item thread.
        self.clear_tx.send(()).map_err(|e| {
            CacheError::SendError(format!(
                "fail to send clear signal to working thread {}",
                e.to_string()
            ))
        })?;

        self.policy.clear();
        self.store.clear();
        self.metrics.clear();

        Ok(())
    }

    /// `max_cost` returns the max cost of the cache.
    #[inline]
    pub fn max_cost(&self) -> i64 {
        self.policy.max_cost()
    }

    /// `update_max_cost` updates the maxCost of an existing cache.
    #[inline]
    pub fn update_max_cost(&self, max_cost: i64) {
        self.policy.update_max_cost(max_cost)
    }

    cfg_async! {
        /// `insert` attempts to add the key-value item to the cache. If it returns false,
        /// then the `insert` was dropped and the key-value item isn't added to the cache. If
        /// it returns true, there's still a chance it could be dropped by the policy if
        /// its determined that the key-value item isn't worth keeping, but otherwise the
        /// item will be added and other items will be evicted in order to make room.
        ///
        /// To dynamically evaluate the items cost using the Config.Coster function, set
        /// the cost parameter to 0 and Coster will be ran when needed in order to find
        /// the items true cost.
        pub async fn insert(&self, key: K, val: V, cost: i64) -> bool {
            self.insert_with_ttl(key, val, cost, Duration::ZERO).await
        }

        /// `insert_with_ttl` works like Set but adds a key-value pair to the cache that will expire
        /// after the specified TTL (time to live) has passed. A zero value means the value never
        /// expires, which is identical to calling `insert`.
        pub async fn insert_with_ttl(&self, key: K, val: V, cost: i64, ttl: Duration) -> bool {
            self.insert_in(key, val, cost, ttl, false).await
        }

        /// `insert_if_present` is like `insert`, but only updates the value of an existing key. It
        /// does NOT add the key to cache if it's absent.
        pub async fn insert_if_present(&self, key: K, val: V, cost: i64) -> bool {
            self.insert_in(key, val, cost, Duration::ZERO, true).await
        }

        pub async fn wait(&self) -> Result<(), CacheError> {
            if self.is_closed.load(Ordering::SeqCst) {
                return Ok(());
            }

            let wg = WaitGroup::new();
            let wait_item = Item::Wait(wg.add(1));
            match self.insert_buf_tx
                .send(wait_item)
                .await {
                Ok(_) => Ok(wg.wait().await),
                Err(e) => Err(CacheError::SendError(format!("cache set buf sender: {}", e.to_string()))),
            }
        }

        pub async fn remove(&self, k: &K) {
            if self.is_closed.load(Ordering::SeqCst) {
                return;
            }

            let (index, conflict) = self.key_to_hash.build_key(&k);
            // delete immediately
            let prev = self.store.remove(&index, conflict);

            if let Some(prev) = prev {
                self.callback.on_exit(Some(prev.value.into_inner()));
            }
            // If we've set an item, it would be applied slightly later.
            // So we must push the same item to `setBuf` with the deletion flag.
            // This ensures that if a set is followed by a delete, it will be
            // applied in the correct order.
            let _ = self.insert_buf_tx.send(Item::delete(index, conflict)).await;
        }

        /// `close` stops all threads and closes all channels.
        #[inline]
        pub async fn close(&self) -> Result<(), CacheError> {
            if self.is_closed.load(Ordering::SeqCst) {
                return Ok(());
            }

            self.clear()?;
            // Block until processItems thread is returned
            self.stop_tx.send(()).await.map_err(|e| CacheError::SendError(format!("fail to send stop signal to working thread, {}", e)))?;
            self.policy.close().await?;
            self.is_closed.store(true, Ordering::SeqCst);
            Ok(())
        }

        #[inline]
        async fn insert_in(&self, key: K, val: V, cost: i64, ttl: Duration, only_update: bool) -> bool {
            if self.is_closed.load(Ordering::SeqCst) {
                return false;
            }

            if let Some((index, item)) = self.update(key, val, cost, ttl, only_update) {
                let is_update = item.is_update();
                // Attempt to send item to policy.
                select! {
                    res = self.insert_buf_tx.send(item) => res.map_or_else(|_| {
                       if is_update {
                            // Return true if this was an update operation since we've already
                            // updated the store. For all the other operations (set/delete), we
                            // return false which means the item was not inserted.
                            true
                        } else {
                            self.metrics.add(MetricType::DropSets, index, 1);
                            false
                        }
                    }, |_| true),
                    else => {
                        if is_update {
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
            } else {
                false
            }
        }
    }

    cfg_not_async! {
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
                .map(|_| wg.wait())
                .map_err(|e| CacheError::SendError(format!("cache set buf sender: {}", e.to_string())))
        }

        pub fn remove(&self, k: &K) {
            if self.is_closed.load(Ordering::SeqCst) {
                return;
            }

            let (index, conflict) = self.key_to_hash.build_key(&k);
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
            self.stop_tx.send(()).map_err(|e| CacheError::SendError(format!("{}", e)))?;
            self.policy.close()?;
            self.is_closed.store(true, Ordering::SeqCst);
            Ok(())
        }

        #[inline]
        fn insert_in(&self, key: K, val: V, cost: i64, ttl: Duration, only_update: bool) -> bool {
            if self.is_closed.load(Ordering::SeqCst) {
                return false;
            }

            self.update(key, val, cost, ttl, only_update).map_or(false, |(index, item)| {
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

    #[inline]
    fn update(
        &self,
        key: K,
        val: V,
        cost: i64,
        ttl: Duration,
        only_update: bool,
    ) -> Option<(u64, Item<V>)> {
        let expiration = if ttl.is_zero() {
            Time::now()
        } else {
            Time::now_with_expiration(ttl)
        };

        let (index, conflict) = self.key_to_hash.build_key(&key);

        // cost is eventually updated. The expiration must also be immediately updated
        // to prevent items from being prematurely removed from the map.
        let external_cost = if cost == 0 { self.coster.cost(&val) } else { 0 };
        match self.store.update(index, val, conflict, expiration) {
            UpdateResult::NotExist(v) | UpdateResult::Reject(v) | UpdateResult::Conflict(v) => {
                if only_update {
                    None
                } else {
                    Some((
                        index,
                        Item::new(index, conflict, cost + external_cost, v, expiration),
                    ))
                }
            }
            UpdateResult::Update(v) => {
                self.callback.on_exit(Some(v));
                Some((index, Item::update(index, cost, external_cost)))
            }
        }
    }
}

impl<V, U, CB, PS, ES, SS> CacheProcessor<V, U, CB, PS, ES, SS>
    where
        V: Send + Sync + 'static,
        U: UpdateValidator<V>,
        CB: CacheCallback<V>,
        PS: BuildHasher + Clone + 'static,
        ES: BuildHasher + Clone + 'static,
        SS: BuildHasher + Clone + 'static,
{
    fn new(
        num_to_keep: usize,
        ignore_internal_cost: bool,
        cleanup_duration: Duration,
        store: Arc<ShardedMap<V, U, SS, ES>>,
        policy: Arc<LFUPolicy<PS>>,
        insert_buf_rx: Receiver<Item<V>>,
        stop_rx: Receiver<()>,
        clear_rx: UnboundedReceiver<()>,
        metrics: Arc<Metrics>,
        callback: Arc<CB>,
    ) -> Self {
        let item_size = store.item_size();
        Self {
            insert_buf_rx,
            stop_rx,
            clear_rx,
            metrics,
            store,
            policy,
            start_ts: HashMap::<u64, Time>::new(),
            num_to_keep,
            callback,
            ignore_internal_cost,
            item_size,
            cleanup_duration
        }
    }

    cfg_async! {
        #[inline]
        fn spawn(mut self) -> JoinHandle<Result<(), CacheError>> {
            spawn(async move {
                let cleanup_timer = sleep(self.cleanup_duration);
                tokio::pin!(cleanup_timer);

                loop {
                    select! {
                        item = self.insert_buf_rx.recv() => {
                            let _ = self.handle_insert_event(item)?;
                        }
                        _ = &mut cleanup_timer => {
                            cleanup_timer.as_mut().reset(Instant::now() + self.cleanup_duration);
                            let _ = self.handle_cleanup_event()?;
                        },
                        Some(_) = self.clear_rx.recv() => {
                            let _ = CacheCleaner::new(&mut self).clean().await?;
                        },
                        _ = self.stop_rx.recv() => return self.handle_close_event(),
                    }
                }
            })
        }

        #[inline]
        fn handle_close_event(&mut self) -> Result<(), CacheError> {
            self.insert_buf_rx.close();
            self.clear_rx.close();
            self.stop_rx.close();
            Ok(())
        }

        #[inline]
        fn handle_insert_event(&mut self, res: Option<Item<V>>) -> Result<(), CacheError> {
            res.map(|item| self.handle_item(item))
                .ok_or(CacheError::RecvError(format!("fail to receive msg from insert buffer")))
        }

        #[inline]
        fn handle_cleanup_event(&mut self) -> Result<(), CacheError> {
            self.store
                .cleanup(self.policy.clone())
                .into_iter()
                .for_each(|victim| {
                    self.prepare_evict(&victim);
                    self.callback.on_evict(victim);
                });
            Ok(())
        }
    }

    cfg_not_async! {
        #[inline]
        pub fn spawn(mut self) -> JoinHandle<Result<(), CacheError>> {
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
        fn handle_clear_event(&mut self) -> Result<(), CacheError> {
            CacheCleaner::new(self).clean()
        }

        #[inline]
        fn handle_insert_event(&mut self, msg: Result<Item<V>, RecvError>) -> Result<(), CacheError> {
            msg.map(|item| self.handle_item(item)).map_err(|e| {
                CacheError::RecvError(format!(
                    "fail to receive msg from insert buffer: {}",
                    e.to_string()
                ))
            })
        }

        #[inline]
        fn handle_cleanup_event(&mut self, res: Result<Instant, RecvError>) -> Result<(), CacheError> {
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
                        e.to_string()
                    ))
                })
        }
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
                        index: key,
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
                                index: victim.key,
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
    fn on_evict(&mut self, item: CrateItem<V>) {
        self.prepare_evict(&item);
        self.callback.on_evict(item);
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
        if let Some(ts) = self.start_ts.get(&item.index) {
            self.metrics.track_eviction(ts.elapsed().as_secs() as i64);
            self.start_ts.remove(&item.index);
        }
    }

}

impl<'a, V, U, CB, PS, ES, SS> CacheCleaner<'a, V, U, CB, PS, ES, SS>
    where
        V: Send + Sync + 'static,
        U: UpdateValidator<V>,
        CB: CacheCallback<V>,
        PS: BuildHasher + Clone + 'static,
        ES: BuildHasher + Clone + 'static,
        SS: BuildHasher + Clone + 'static,
{
    #[inline]
    fn new(processor: &'a mut CacheProcessor<V, U, CB, PS, ES, SS>) -> Self {
        Self { processor }
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
            } => self.processor.callback.on_evict(CrateItem {
                val: Some(value),
                index: key,
                conflict,
                cost,
                exp: expiration,
            }),
            Item::Delete { .. } | Item::Update { .. } => {}
            Item::Wait(wg) => drop(wg),
        }
    }

    cfg_async! {
        #[inline]
        async fn clean(mut self) -> Result<(), CacheError> {
            loop {
                select! {
                    // clear out the insert buffer channel.
                    Some(item) = self.processor.insert_buf_rx.recv() => {
                        self.handle_item(item);
                    },
                    else => return Ok(()),
                }
            }
        }
    }

    cfg_not_async! {
        #[inline]
        fn clean(mut self) -> Result<(), CacheError> {
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
}