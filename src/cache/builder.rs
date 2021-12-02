use crate::cache::{DEFAULT_CLEANUP_DURATION, DEFAULT_INSERT_BUF_SIZE};
use crate::{
    CacheCallback, Coster, DefaultCacheCallback, DefaultCoster, DefaultUpdateValidator, KeyBuilder,
    UpdateValidator,
};
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::time::Duration;

pub struct CacheBuilderCore<
    K: Hash + Eq,
    V: Send + Sync + 'static,
    KH: KeyBuilder<K>,
    C = DefaultCoster<V>,
    U = DefaultUpdateValidator<V>,
    CB = DefaultCacheCallback<V>,
    S = RandomState,
> {
    /// metrics determines whether cache statistics are kept during the cache's
    /// lifetime. There *is* some overhead to keeping statistics, so you should
    /// only set this flag to true when testing or throughput performance isn't a
    /// major factor.
    pub(crate) metrics: bool,

    /// ignore_internal_cost set to true indicates to the cache that the cost of
    /// internally storing the value should be ignored. This is useful when the
    /// cost passed to set is not using bytes as units. Keep in mind that setting
    /// this to true will increase the memory usage.
    pub(crate) ignore_internal_cost: bool,

    pub(crate) num_counters: usize,

    pub(crate) max_cost: i64,

    // buffer_items determines the size of Get buffers.
    //
    // Unless you have a rare use case, using `64` as the BufferItems value
    // results in good performance.
    // buffer_items: usize,
    /// `insert_buffer_size` determines the size of insert buffers.
    ///
    /// Default is 32 * 1024 (**TODO:** need to figure out the optimal size.).
    pub(crate) insert_buffer_size: usize,

    /// `cleanup_duration` is the duration for internal store to cleanup expired entry.
    ///
    /// Default is 500ms.
    pub(crate) cleanup_duration: Duration,

    /// key_to_hash is used to customize the key hashing algorithm.
    /// Each key will be hashed using the provided function. If keyToHash value
    /// is not set, the default keyToHash function is used.
    pub(crate) key_to_hash: KH,

    /// cost evaluates a value and outputs a corresponding cost. This function
    /// is ran after insert is called for a new item or an item update with a cost
    /// param of 0.
    pub(crate) coster: Option<C>,

    /// update_validator is called when a value already exists in cache and is being updated.
    pub(crate) update_validator: Option<U>,

    pub(crate) callback: Option<CB>,

    pub(crate) hasher: Option<S>,

    marker_k: PhantomData<fn(K)>,
    marker_v: PhantomData<fn(V)>,
}

impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<K>>
    CacheBuilderCore<K, V, KH>
{
    /// Create a new CacheBuilderCore
    #[inline]
    pub fn new(num_counters: usize, max_cost: i64, index: KH) -> Self {
        Self {
            num_counters,
            max_cost,
            // buffer_items: DEFAULT_BUFFER_ITEMS,
            insert_buffer_size: DEFAULT_INSERT_BUF_SIZE,
            metrics: false,
            callback: Some(DefaultCacheCallback::default()),
            key_to_hash: index,
            update_validator: Some(DefaultUpdateValidator::default()),
            coster: Some(DefaultCoster::default()),
            ignore_internal_cost: false,
            cleanup_duration: DEFAULT_CLEANUP_DURATION,
            marker_k: Default::default(),
            marker_v: Default::default(),
            hasher: Some(RandomState::default()),
        }
    }
}

impl<K, V, KH, C, U, CB, S> CacheBuilderCore<K, V, KH, C, U, CB, S>
where
    K: Hash + Eq,
    V: Send + Sync + 'static,
    KH: KeyBuilder<K>,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    S: BuildHasher + Clone + 'static,
{
    /// Set the number of counters for the Cache.
    ///
    /// `num_counters` is the number of 4-bit access counters to keep for admission and eviction.
    /// Dgraph's developers have seen good performance in setting this to 10x the number of items
    /// you expect to keep in the cache when full.
    ///
    /// For example, if you expect each item to have a cost of 1 and `max_cost` is 100, set `num_counters` to 1,000.
    /// Or, if you use variable cost values but expect the cache to hold around 10,000 items when full,
    /// set num_counters to 100,000. The important thing is the *number of unique items* in the full cache,
    /// not necessarily the `max_cost` value.
    #[inline]
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
            hasher: self.hasher,
        }
    }

    /// Set the max_cost for the Cache.
    ///
    /// `max_cost` is how eviction decisions are made. For example, if max_cost is 100 and a new item
    /// with a cost of 1 increases total cache cost to 101, 1 item will be evicted.
    ///
    /// `max_cost` can also be used to denote the max size in bytes. For example,
    /// if max_cost is 1,000,000 (1MB) and the cache is full with 1,000 1KB items,
    /// a new item (that's accepted) would cause 5 1KB items to be evicted.
    ///
    /// `max_cost` could be anything as long as it matches how you're using the cost values when calling `insert`.
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
            hasher: self.hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    /// Set the insert buffer size for the Cache.
    ///
    /// `buffer_size` is the size of the insert buffers. The Dgraph's developers find that 32 * 1024 gives a good performance.
    ///
    /// If for some reason you see insert performance decreasing with lots of contention (you shouldn't),
    /// try increasing this value in increments of 32 * 1024.
    /// This is a fine-tuning mechanism and you probably won't have to touch this.
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
            hasher: self.hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    /// Set whether record the metrics or not.
    ///
    /// Metrics is true when you want real-time logging of a variety of stats.
    /// The reason this is a CacheBuilderCore flag is because there's a 10% throughput performance overhead.
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
            hasher: self.hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    /// Set whether ignore the internal cost or not.
    ///
    /// By default, when [`insert`] a value in the Cache, there will always 56 for internal cost,
    /// because the size of stored item in Cache is 56(excluding the size of value).
    /// Set it to true to ignore the internal cost.
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
            hasher: self.hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    /// Set the cleanup ticker for Cache, each tick the Cache will clean the expired entries.
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
            hasher: self.hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    /// Set the [`KeyBuilder`] for the Cache
    ///
    /// [`KeyBuilder`] is the hashing algorithm used for every key. In Stretto, the Cache will never store the real key.
    /// The key will be processed by [`KeyBuilder`]. Stretto has two default built-in key builder,
    /// one is [`TransparentKeyBuilder`], the other is [`DefaultKeyBuilder`]. If your key implements [`TransparentKey`] trait,
    /// you can use [`TransparentKeyBuilder`] which is faster than [`DefaultKeyBuilder`]. Otherwise, you should use [`DefaultKeyBuilder`]
    /// You can also write your own key builder for the Cache, by implementing [`KeyBuilder`] trait.
    ///
    /// Note that if you want 128bit hashes you should use the full `(u64, u64)`,
    /// otherwise just fill the `u64` at the `0` position, and it will behave like
    /// any 64bit hash.
    ///
    /// [`KeyBuilder`]: trait.KeyBuilder.html
    /// [`TransparentKey`]: trait.TransparentKey.html
    /// [`TransparentKeyBuilder`]: struct.TransparentKeyBuilder.html
    /// [`DefaultKeyBuilder`]: struct.DefaultKeyBuilder.html
    #[inline]
    pub fn set_key_builder<NKH: KeyBuilder<K>>(
        self,
        index: NKH,
    ) -> CacheBuilderCore<K, V, NKH, C, U, CB, S> {
        CacheBuilderCore {
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
            hasher: self.hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    /// Set the coster for the Cache.
    ///
    /// [`Coster`] is a trait you can pass to the [`CacheBuilderCore`] in order to evaluate
    /// item cost at runtime, and only for the [`insert`] calls that aren't dropped (this is
    /// useful if calculating item cost is particularly expensive, and you don't want to
    /// waste time on items that will be dropped anyways).
    ///
    /// To signal to Stretto that you'd like to use this [`Coster`] trait:
    ///
    /// 1. Set the [`Coster`] field to your own [`Coster`] implementation.
    /// 2. When calling [`insert`] for new items or item updates, use a cost of 0.
    #[inline]
    pub fn set_coster<NC: Coster<V>>(self, coster: NC) -> CacheBuilderCore<K, V, KH, NC, U, CB, S> {
        CacheBuilderCore {
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
            hasher: self.hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    /// Set the update validator for the Cache.
    ///
    /// By default, the Cache will always update the value if the value already exists in the cache.
    /// [`UpdateValidator`] is a trait to support customized update policy (check if the value should be updated
    /// if the value already exists in the cache).
    #[inline]
    pub fn set_update_validator<NU: UpdateValidator<V>>(
        self,
        uv: NU,
    ) -> CacheBuilderCore<K, V, KH, C, NU, CB, S> {
        CacheBuilderCore {
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
            hasher: self.hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    /// Set the callbacks for the Cache.
    ///
    /// [`CacheCallback`] is for customize some extra operations on values when related event happens.
    #[inline]
    pub fn set_callback<NCB: CacheCallback<V>>(
        self,
        cb: NCB,
    ) -> CacheBuilderCore<K, V, KH, C, U, NCB, S> {
        CacheBuilderCore {
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
            hasher: self.hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    /// Set the hasher for the Cache.
    /// Default is SipHasher.
    #[inline]
    pub fn set_hasher<NS: BuildHasher + Clone + 'static>(
        self,
        hasher: NS,
    ) -> CacheBuilderCore<K, V, KH, C, U, CB, NS> {
        CacheBuilderCore {
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
            hasher: Some(hasher),
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }
}
