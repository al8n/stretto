use crate::{
  CacheCallback, Coster, DefaultCacheCallback, DefaultCoster, DefaultKeyBuilder,
  DefaultUpdateValidator, KeyBuilder, UpdateValidator,
  cache::{DEFAULT_CLEANUP_DURATION, DEFAULT_DRAIN_INTERVAL},
};
use std::{
  collections::hash_map::RandomState,
  hash::{BuildHasher, Hash},
  marker::PhantomData,
  time::Duration,
};

use super::DEFAULT_BUFFER_ITEMS;

pub struct CacheBuilderCore<
  K,
  V,
  KH = DefaultKeyBuilder<K>,
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
  ///
  /// Default is `true`. Set to `false` only when `max_cost` is measured in
  /// bytes and you need each stored item to also account for the ~56 bytes
  /// of per-entry bookkeeping (key, conflict, version, value wrapper, time).
  pub(crate) ignore_internal_cost: bool,

  pub(crate) num_counters: usize,

  pub(crate) max_cost: i64,

  pub(crate) buffer_items: usize,

  // buffer_items determines the size of Get buffers.
  //
  // Unless you have a rare use case, using `64` as the BufferItems value
  // results in good performance.
  // buffer_items: usize,
  /// High-water mark per stripe in the new striped insert buffer
  /// (`InsertStripeRing`). When a stripe accumulates this many items,
  /// the full batch is swapped out and shipped to the processor.
  /// Default `64`. Min `1`.
  pub(crate) insert_stripe_high_water: usize,

  /// Cadence for the processor's tick arm, which drains every stripe
  /// inline and runs TTL cleanup. Default `500ms`. Min `1ms`.
  pub(crate) drain_interval: Duration,

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

impl<K: Hash + Eq, V: Send + Sync + 'static> CacheBuilderCore<K, V> {
  /// Create a new CacheBuilderCore
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn new(num_counters: usize, max_cost: i64) -> Self {
    Self {
      num_counters,
      max_cost,
      buffer_items: DEFAULT_BUFFER_ITEMS,
      metrics: false,
      callback: Some(DefaultCacheCallback::default()),
      key_to_hash: DefaultKeyBuilder::<K>::default(),
      update_validator: Some(DefaultUpdateValidator::default()),
      coster: Some(DefaultCoster::default()),
      ignore_internal_cost: true,
      insert_stripe_high_water: crate::cache::insert_stripe::DEFAULT_HIGH_WATER,
      drain_interval: DEFAULT_DRAIN_INTERVAL,
      cleanup_duration: DEFAULT_CLEANUP_DURATION,
      marker_k: Default::default(),
      marker_v: Default::default(),
      hasher: Some(RandomState::default()),
    }
  }
}

impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<Key = K>> CacheBuilderCore<K, V, KH> {
  /// Create a new CacheBuilderCore
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn new_with_key_builder(num_counters: usize, max_cost: i64, index: KH) -> Self {
    Self {
      num_counters,
      max_cost,
      buffer_items: DEFAULT_BUFFER_ITEMS,
      metrics: false,
      callback: Some(DefaultCacheCallback::default()),
      key_to_hash: index,
      update_validator: Some(DefaultUpdateValidator::default()),
      coster: Some(DefaultCoster::default()),
      ignore_internal_cost: true,
      insert_stripe_high_water: crate::cache::insert_stripe::DEFAULT_HIGH_WATER,
      drain_interval: DEFAULT_DRAIN_INTERVAL,
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
  KH: KeyBuilder<Key = K>,
  C: Coster<Value = V>,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
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
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_num_counters(mut self, num_counters: usize) -> Self {
    self.num_counters = num_counters;
    self
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
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_max_cost(mut self, max_cost: i64) -> Self {
    self.max_cost = max_cost;
    self
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_buffer_items(mut self, sz: usize) -> Self {
    self.buffer_items = sz;
    self
  }

  /// Set the per-stripe high-water mark for the striped insert buffer.
  /// When a stripe accumulates this many items, the full batch is sent
  /// to the policy processor.
  ///
  /// Default `64`. Min `1`. Larger values amortize channel sends but
  /// delay admission decisions; recommended `≤ 256` for caches under
  /// ~10 K capacity.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_insert_stripe_high_water(mut self, items: usize) -> Self {
    self.insert_stripe_high_water = items.max(1);
    self
  }

  /// Set the processor's drain-tick interval. The processor wakes every
  /// `interval` to drain every stripe inline (bypassing the bounded
  /// channel) and to run TTL cleanup.
  ///
  /// Default `500ms`. Zero is silently promoted to the default.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_drain_interval(mut self, interval: Duration) -> Self {
    self.drain_interval = if interval.is_zero() {
      DEFAULT_DRAIN_INTERVAL
    } else {
      interval
    };
    self
  }

  /// Set whether record the metrics or not.
  ///
  /// Metrics is true when you want real-time logging of a variety of stats.
  /// The reason this is a CacheBuilderCore flag is because there's a 10% throughput performance overhead.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_metrics(mut self, val: bool) -> Self {
    self.metrics = val;
    self
  }

  /// Set whether ignore the internal cost or not.
  ///
  /// Default is `true`: each [`insert`] is charged only the caller-supplied cost,
  /// so `max_cost` behaves as an entry budget when you pass `1` per insert.
  ///
  /// Set to `false` when `max_cost` represents a byte budget and you need each
  /// stored item to also account for ~56 bytes of per-entry bookkeeping
  /// (key, conflict, version, value wrapper, time).
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_ignore_internal_cost(mut self, val: bool) -> Self {
    self.ignore_internal_cost = val;
    self
  }

  /// Set the cleanup ticker for Cache, each tick the Cache will clean the expired entries.
  ///
  /// A zero duration is silently promoted to the default cleanup interval: the
  /// async cache spawns a `tokio::time::interval` that panics on zero, and a
  /// zero tick has no defensible meaning for the sync cache either. Any
  /// non-zero duration is honored as-is (including sub-second values).
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_cleanup_duration(mut self, d: Duration) -> Self {
    self.cleanup_duration = if d.is_zero() {
      DEFAULT_CLEANUP_DURATION
    } else {
      d
    };
    self
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
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_key_builder<NKH: KeyBuilder<Key = K>>(
    self,
    index: NKH,
  ) -> CacheBuilderCore<K, V, NKH, C, U, CB, S> {
    CacheBuilderCore {
      num_counters: self.num_counters,
      max_cost: self.max_cost,
      buffer_items: self.buffer_items,
      metrics: self.metrics,
      callback: self.callback,
      key_to_hash: index,
      update_validator: self.update_validator,
      coster: self.coster,
      ignore_internal_cost: self.ignore_internal_cost,
      insert_stripe_high_water: self.insert_stripe_high_water,
      drain_interval: self.drain_interval,
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
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_coster<NC: Coster<Value = V>>(
    self,
    coster: NC,
  ) -> CacheBuilderCore<K, V, KH, NC, U, CB, S> {
    CacheBuilderCore {
      num_counters: self.num_counters,
      max_cost: self.max_cost,
      buffer_items: self.buffer_items,
      metrics: self.metrics,
      callback: self.callback,
      key_to_hash: self.key_to_hash,
      update_validator: self.update_validator,
      coster: Some(coster),
      ignore_internal_cost: self.ignore_internal_cost,
      insert_stripe_high_water: self.insert_stripe_high_water,
      drain_interval: self.drain_interval,
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
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_update_validator<NU: UpdateValidator<Value = V>>(
    self,
    uv: NU,
  ) -> CacheBuilderCore<K, V, KH, C, NU, CB, S> {
    CacheBuilderCore {
      num_counters: self.num_counters,
      max_cost: self.max_cost,
      buffer_items: self.buffer_items,
      metrics: self.metrics,
      callback: self.callback,
      key_to_hash: self.key_to_hash,
      update_validator: Some(uv),
      coster: self.coster,
      ignore_internal_cost: self.ignore_internal_cost,
      insert_stripe_high_water: self.insert_stripe_high_water,
      drain_interval: self.drain_interval,
      cleanup_duration: self.cleanup_duration,
      hasher: self.hasher,
      marker_k: self.marker_k,
      marker_v: self.marker_v,
    }
  }

  /// Set the callbacks for the Cache.
  ///
  /// [`CacheCallback`] is for customize some extra operations on values when related event happens.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_callback<NCB: CacheCallback<Value = V>>(
    self,
    cb: NCB,
  ) -> CacheBuilderCore<K, V, KH, C, U, NCB, S> {
    CacheBuilderCore {
      num_counters: self.num_counters,
      max_cost: self.max_cost,
      buffer_items: self.buffer_items,
      metrics: self.metrics,
      callback: Some(cb),
      key_to_hash: self.key_to_hash,
      update_validator: self.update_validator,
      coster: self.coster,
      ignore_internal_cost: self.ignore_internal_cost,
      insert_stripe_high_water: self.insert_stripe_high_water,
      drain_interval: self.drain_interval,
      cleanup_duration: self.cleanup_duration,
      hasher: self.hasher,
      marker_k: self.marker_k,
      marker_v: self.marker_v,
    }
  }

  /// Set the hasher for the Cache.
  /// Default is SipHasher.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn set_hasher<NS: BuildHasher + Clone + 'static>(
    self,
    hasher: NS,
  ) -> CacheBuilderCore<K, V, KH, C, U, CB, NS> {
    CacheBuilderCore {
      num_counters: self.num_counters,
      max_cost: self.max_cost,
      buffer_items: self.buffer_items,
      metrics: self.metrics,
      callback: self.callback,
      key_to_hash: self.key_to_hash,
      update_validator: self.update_validator,
      coster: self.coster,
      ignore_internal_cost: self.ignore_internal_cost,
      insert_stripe_high_water: self.insert_stripe_high_water,
      drain_interval: self.drain_interval,
      cleanup_duration: self.cleanup_duration,
      hasher: Some(hasher),
      marker_k: self.marker_k,
      marker_v: self.marker_v,
    }
  }
}
