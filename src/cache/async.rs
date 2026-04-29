use crate::{
  CacheCallback, CacheError, Coster, DefaultCacheCallback, DefaultCoster, DefaultKeyBuilder,
  DefaultUpdateValidator, Item as CrateItem, KeyBuilder, Metrics, UpdateValidator, ValueRef,
  ValueRefMut,
  axync::Waiter,
  cache::builder::CacheBuilderCore,
  metrics::MetricType,
  policy::{AddOutcome, AsyncLFUPolicy},
  ring::AsyncRingStripe,
  store::{ShardedMap, UpdateResult},
  ttl::{ExpirationMap, Time},
};
use agnostic_lite::RuntimeLite;
use crossbeam_channel::{Receiver, Sender, bounded as cb_bounded, select as cb_select, tick};
use std::{
  collections::{HashMap, hash_map::RandomState},
  hash::{BuildHasher, Hash},
  marker::PhantomData,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  thread::{JoinHandle, spawn as thread_spawn},
  time::Duration,
};

/// The `AsyncCacheBuilder` struct is used when creating [`AsyncCache`] instances if you want to customize the [`AsyncCache`] settings.
///
/// - **num_counters**
///
///   `num_counters` is the number of 4-bit access counters to keep for admission and eviction.
///   Dgraph's developers have seen good performance in setting this to 10x the number of items
///   you expect to keep in the cache when full.
///
///   For example, if you expect each item to have a cost of 1 and `max_cost` is 100, set `num_counters` to 1,000.
///   Or, if you use variable cost values but expect the cache to hold around 10,000 items when full,
///   set num_counters to 100,000. The important thing is the *number of unique items* in the full cache,
///   not necessarily the `max_cost` value.
///
/// - **max_cost**
///
///   `max_cost` is how eviction decisions are made. For example, if max_cost is 100 and a new item
///   with a cost of 1 increases total cache cost to 101, 1 item will be evicted.
///
///   `max_cost` can also be used to denote the max size in bytes. For example,
///   if max_cost is 1,000,000 (1MB) and the cache is full with 1,000 1KB items,
///   a new item (that's accepted) would cause 5 1KB items to be evicted.
///
///   `max_cost` could be anything as long as it matches how you're using the cost values when calling [`insert`].
///
/// - **key_builder**
///
///   [`KeyBuilder`] is the hashing algorithm used for every key. In Stretto, the Cache will never store the real key.
///   The key will be processed by [`KeyBuilder`]. Stretto has two default built-in key builder,
///   one is [`TransparentKeyBuilder`], the other is [`DefaultKeyBuilder`]. If your key implements [`TransparentKey`] trait,
///   you can use [`TransparentKeyBuilder`] which is faster than [`DefaultKeyBuilder`]. Otherwise, you should use [`DefaultKeyBuilder`]
///   You can also write your own key builder for the Cache, by implementing [`KeyBuilder`] trait.
///
///   Note that if you want 128bit hashes you should use the full `(u64, u64)`,
///   otherwise just fill the `u64` at the `0` position, and it will behave like
///   any 64bit hash.
///
/// - **buffer_size**
///
///   `buffer_size` is the size of the insert buffers. The Dgraph's developers find that 32 * 1024 gives a good performance.
///
///   If for some reason you see insert performance decreasing with lots of contention (you shouldn't),
///   try increasing this value in increments of 32 * 1024. This is a fine-tuning mechanism
///   and you probably won't have to touch this.
///
/// - **metrics**
///
///   Metrics is true when you want real-time logging of a variety of stats.
///   The reason this is a [`AsyncCacheBuilder`] flag is because there's a 10% throughput performance overhead.
///
/// - **ignore_internal_cost**
///
///   Defaults to `true`: each insert is charged only the caller-supplied cost,
///   so `max_cost` behaves as an entry budget when you pass `1` per insert.
///   Set to `false` when `max_cost` represents a byte budget and you need each
///   stored item to also account for ~56 bytes of per-entry bookkeeping.
///
/// - **cleanup_duration**
///
///   The Cache will cleanup the expired values every 2 seconds by default.
///   Independent from the stripe drain cadence (`drain_interval`, default
///   `500ms`), since TTL sweeps are heavier than stripe drains.
///
/// - **update_validator**
///
///   By default, the Cache will always update the value if the value already exists in the cache.
///   [`UpdateValidator`] is a trait to support customized update policy (check if the value should be updated
///   if the value already exists in the cache).
///
/// - **callback**
///
///   [`CacheCallback`] is for customize some extra operations on values when related event happens..
///
/// - **coster**
///
///   [`Coster`] is a trait you can pass to the [`AsyncCacheBuilder`] in order to evaluate
///   item cost at runtime, and only for the [`insert`] calls that aren't dropped (this is
///   useful if calculating item cost is particularly expensive, and you don't want to
///   waste time on items that will be dropped anyways).
///
///   To signal to Stretto that you'd like to use this Coster trait:
///
///   1. Set the Coster field to your own Coster implementation.
///   2. When calling [`insert`] for new items or item updates, use a cost of 0.
///
/// - **hasher**
///
///     The hasher for the [`AsyncCache`], default is SipHasher.
///
/// [`AsyncCache`]: struct.AsyncCache.html
/// [`AsyncCacheBuilder`]: struct.AsyncCacheBuilder.html
/// [`TransparentKey`]: struct.TransparentKey.html
/// [`TransparentKeyBuilder`]: struct.TransparentKeyBuilder.html
/// [`DefaultKeyBuilder`]: struct.DefaultKeyBuilder.html
/// [`KeyBuilder`]: trait.KeyBuilder.html
/// [`insert`]: struct.Cache.html#method.insert
/// [`UpdateValidator`]: trait.UpdateValidator.html
/// [`CacheCallback`]: trait.CacheCallback.html
/// [`Coster`]: trait.Coster.html
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub struct AsyncCacheBuilder<
  K,
  V,
  KH = DefaultKeyBuilder<K>,
  C = DefaultCoster<V>,
  U = DefaultUpdateValidator<V>,
  CB = DefaultCacheCallback<V>,
  S = RandomState,
> {
  inner: CacheBuilderCore<K, V, KH, C, U, CB, S>,
}

impl<K: Hash + Eq, V: Send + Sync + 'static> AsyncCacheBuilder<K, V> {
  /// Create a new AsyncCacheBuilder
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn new(num_counters: usize, max_cost: i64) -> Self {
    Self {
      inner: CacheBuilderCore::new(num_counters, max_cost),
    }
  }
}

impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<Key = K>> AsyncCacheBuilder<K, V, KH> {
  /// Create a new AsyncCacheBuilder
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn new_with_key_builder(num_counters: usize, max_cost: i64, kh: KH) -> Self {
    Self {
      inner: CacheBuilderCore::new_with_key_builder(num_counters, max_cost, kh),
    }
  }
}

impl<K, V, KH, C, U, CB, S> AsyncCacheBuilder<K, V, KH, C, U, CB, S>
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>,
  C: Coster<Value = V>,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
  S: BuildHasher + Clone + 'static + Send + Sync,
{
  /// Build Cache and start all threads needed by the Cache.
  ///
  /// `R` is the async runtime to use. For example, if you use `tokio`,
  /// pass `TokioRuntime` from `agnostic-lite`.
  ///
  /// ```no_run
  /// use stretto::AsyncCacheBuilder;
  /// use agnostic_lite::tokio::TokioRuntime;
  ///
  /// AsyncCacheBuilder::<u64, u64>::new(100, 10)
  ///     .build::<TokioRuntime>()
  ///     .unwrap();
  /// ```
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn build<R: RuntimeLite>(self) -> Result<AsyncCache<K, V, R, KH, C, U, CB, S>, CacheError>
  where
    <R as RuntimeLite>::Interval: Send,
  {
    let num_counters = self.inner.num_counters;

    if num_counters == 0 {
      return Err(CacheError::InvalidNumCounters);
    }

    let max_cost = self.inner.max_cost;
    if max_cost == 0 {
      return Err(CacheError::InvalidMaxCost);
    }

    let (insert_buf_ring, buf_rx) = crate::cache::insert_stripe::InsertStripeRing::<Item<V>>::new(
      self.inner.insert_stripe_high_water,
    );
    let insert_buf_ring = Arc::new(insert_buf_ring);
    // Two stop channels: AsyncLFUPolicy still runs as an RT task and uses
    // async-channel; the cache processor now runs on a std::thread and uses
    // crossbeam_channel. Both senders live in `AsyncCacheInner` so `Drop`
    // disconnects both on shutdown.
    let (policy_stop_tx, policy_stop_rx) = crate::axync::stop_channel();
    let (stop_tx, stop_rx) = cb_bounded::<()>(1);

    let hasher = self.inner.hasher.unwrap();
    let expiration_map = ExpirationMap::with_hasher(hasher.clone());

    let store = Arc::new(ShardedMap::with_validator_and_hasher(
      expiration_map,
      self.inner.update_validator.unwrap(),
      hasher.clone(),
    ));
    let mut policy =
      AsyncLFUPolicy::with_hasher::<R>(num_counters, max_cost, hasher, policy_stop_rx)?;

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
    let clear_generation = Arc::new(AtomicU64::new(0));
    let processor = CacheProcessor::new(
      100000,
      self.inner.ignore_internal_cost,
      self.inner.cleanup_duration,
      self.inner.drain_interval,
      store.clone(),
      policy.clone(),
      buf_rx,
      insert_buf_ring.clone(),
      stop_rx,
      metrics.clone(),
      callback.clone(),
      clear_generation.clone(),
    )
    .spawn();

    let buffer_items = self.inner.buffer_items;
    let get_buf = AsyncRingStripe::new(policy.clone(), buffer_items);
    let inner = AsyncCacheInner {
      store,
      policy,
      get_buf: Arc::new(get_buf),
      insert_buf_ring,
      callback,
      key_to_hash: Arc::new(self.inner.key_to_hash),
      stop_tx: Some(stop_tx),
      policy_stop_tx: Some(policy_stop_tx),
      coster,
      metrics,
      clear_generation,
      processor: Some(processor),
      _marker: PhantomData,
      _runtime: PhantomData,
    };

    Ok(AsyncCache(Arc::new(inner)))
  }
}

pub(crate) struct CacheProcessor<V, U, CB, S> {
  insert_buf_rx: Receiver<Vec<Item<V>>>,
  insert_stripe: Arc<crate::cache::insert_stripe::InsertStripeRing<Item<V>>>,
  stop_rx: Receiver<()>,
  metrics: Arc<Metrics>,
  store: Arc<ShardedMap<V, U, S, S>>,
  policy: Arc<AsyncLFUPolicy<S>>,
  start_ts: HashMap<u64, Time, S>,
  num_to_keep: usize,
  callback: Arc<CB>,
  ignore_internal_cost: bool,
  item_size: usize,
  /// Shared counter bumped on every clear. The handler reads and advances
  /// this before wiping the store so any `Item::New` queued with the
  /// pre-bump generation is recognized as stale and skipped.
  clear_generation: Arc<AtomicU64>,
  cleanup_duration: Duration,
  /// Cadence for the dedicated stripe-drain tick. Fires independently of
  /// TTL cleanup so partial-stripe items are admitted even when the
  /// cleanup interval is long (default 2s vs drain default 500ms).
  drain_duration: Duration,
}

pub(crate) enum Item<V> {
  New {
    key: u64,
    conflict: u64,
    cost: i64,
    expiration: Time,
    version: u64,
    /// Clear-generation captured at the eager store write. Compared against
    /// the cache's current generation when the processor admits this item; a
    /// mismatch means a `clear()` intervened and the eager write has been
    /// (or must be) invalidated, so admission is skipped.
    generation: u64,
    _marker: std::marker::PhantomData<fn() -> V>,
  },
  Update {
    key: u64,
    /// Conflict hash of the row we just updated. Used by the stale-generation
    /// branch of the Update handler to version-gate the ghost-row cleanup so
    /// it can only remove a row whose (key, conflict, version) exactly match
    /// the eager write we installed. (The async eager-insert guard
    /// deliberately does not arm rollback for updates — destroying the new
    /// value on cancellation would surface as data loss — so the
    /// cancellation path does not touch this field.)
    conflict: u64,
    cost: i64,
    external_cost: i64,
    #[allow(dead_code)]
    expiration: Time,
    /// New version assigned by the store to the row we just wrote. Used by
    /// the stale-generation branch of the Update handler for version-gated
    /// ghost-row cleanup; a concurrent writer who has since landed a newer
    /// version at the same (key, conflict) is preserved. (The async
    /// eager-insert guard is not armed for updates, so cancellation does not
    /// use this field.)
    version: u64,
    /// Clear-generation captured at the eager store update. If a `clear()`
    /// slipped between the eager write and this admission the policy state
    /// was already wiped, so applying the stale cost would corrupt a
    /// post-clear admission.
    generation: u64,
  },
  Delete {
    key: u64,
    conflict: u64,
    /// Clear-generation captured at the eager `store.try_remove`. A stale
    /// pre-clear Delete must not fire against post-clear state.
    generation: u64,
    /// Version of the store entry that the eager remove actually removed.
    /// Always non-zero: `try_remove` only enqueues a Delete when the eager
    /// remove returned Some, and store versions start at 1 (0 is reserved
    /// as a "no row" sentinel). The processor's follow-up cleanup uses
    /// `try_remove_if_version` so a concurrent reinsert at the same
    /// (key, conflict) under a different version is preserved.
    version: u64,
  },
  Wait(Waiter),
  Clear(Waiter),
}

impl<V> Item<V> {
  #[cfg_attr(not(tarpaulin), inline(always))]
  fn new(key: u64, conflict: u64, cost: i64, exp: Time, version: u64, generation: u64) -> Self {
    Self::New {
      key,
      conflict,
      cost,
      expiration: exp,
      version,
      generation,
      _marker: std::marker::PhantomData,
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn update(
    key: u64,
    conflict: u64,
    cost: i64,
    external_cost: i64,
    expiration: Time,
    version: u64,
    generation: u64,
  ) -> Self {
    Self::Update {
      key,
      conflict,
      cost,
      external_cost,
      expiration,
      version,
      generation,
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn delete(key: u64, conflict: u64, generation: u64, version: u64) -> Self {
    Self::Delete {
      key,
      conflict,
      generation,
      version,
    }
  }
}

/// AsyncCache is a thread-safe async implementation of a hashmap with a TinyLFU admission
/// policy and a Sampled LFU eviction policy. You can use the same AsyncCache instance
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
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub struct AsyncCache<
  K,
  V,
  R,
  KH = DefaultKeyBuilder<K>,
  C = DefaultCoster<V>,
  U = DefaultUpdateValidator<V>,
  CB = DefaultCacheCallback<V>,
  S = RandomState,
>(pub(crate) Arc<AsyncCacheInner<K, V, R, KH, C, U, CB, S>>)
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>;

impl<K, V, R, KH, C, U, CB, S> Clone for AsyncCache<K, V, R, KH, C, U, CB, S>
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>,
{
  #[cfg_attr(not(tarpaulin), inline(always))]
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

/// Shared state behind an [`AsyncCache`]. Not part of the public API:
/// `AsyncCache` is a thin `Arc` wrapper that gives the type a cheap
/// `Clone` while keeping shared state `pub(crate)`. `Drop` runs on
/// `AsyncCacheInner`, so teardown happens exactly once when the last
/// `AsyncCache` handle is dropped — not per-clone.
pub(crate) struct AsyncCacheInner<
  K,
  V,
  R,
  KH = DefaultKeyBuilder<K>,
  C = DefaultCoster<V>,
  U = DefaultUpdateValidator<V>,
  CB = DefaultCacheCallback<V>,
  S = RandomState,
> where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>,
{
  /// store is the central concurrent hashmap where key-value items are stored.
  pub(crate) store: Arc<ShardedMap<V, U, S, S>>,

  /// policy determines what gets let in to the cache and what gets kicked out.
  pub(crate) policy: Arc<AsyncLFUPolicy<S>>,

  pub(crate) insert_buf_ring: Arc<crate::cache::insert_stripe::InsertStripeRing<Item<V>>>,

  pub(crate) get_buf: Arc<AsyncRingStripe<S>>,

  /// Held in `Option` so `Drop` can `take()` and drop it, disconnecting the
  /// processor thread's `stop_rx`. We never `send()` on this channel —
  /// disconnection alone wakes the processor's `select!` stop arm via
  /// `Err(RecvError)`, which is matched by `_`. `Drop` then joins the
  /// processor thread so the cache returns only after the drain is
  /// complete and no further callbacks can fire.
  pub(crate) stop_tx: Option<Sender<()>>,

  /// Stop sender for the `AsyncLFUPolicy` task. Separate from the cache
  /// processor's `stop_tx` because the policy still runs as an `RT` task
  /// and uses the async-channel `axync::Sender`. `Drop` drops this so the
  /// policy task observes its receiver as disconnected and exits.
  pub(crate) policy_stop_tx: Option<crate::axync::Sender<()>>,

  pub(crate) callback: Arc<CB>,

  pub(crate) key_to_hash: Arc<KH>,

  pub(crate) coster: Arc<C>,

  /// the metrics for the cache
  pub metrics: Arc<Metrics>,

  /// Clear-generation counter shared with the processor. `try_update`
  /// captures this before the eager store write so a subsequent `clear()`
  /// can invalidate the still-in-flight `Item::New` and any stale eager
  /// write it represents.
  pub(crate) clear_generation: Arc<AtomicU64>,

  /// Handle to the cache processor thread. `Drop` joins this so dropping
  /// the cache returns only after the processor has fully shut down
  /// (drain complete, no further handler work in flight). Stored as
  /// `Option` so `Drop` can `take()` the handle to call `join()` on an
  /// owned value.
  ///
  /// Panic-safe: `JoinHandle::join` returns `Err` if the processor thread
  /// panicked (e.g. from a user callback), so shutdown cannot hang on a
  /// missed handshake.
  pub(crate) processor: Option<JoinHandle<Result<(), CacheError>>>,

  pub(crate) _marker: PhantomData<fn(K)>,

  /// Type tag for the runtime that owns this cache. Carried so
  /// `try_insert_in` can call `R::yield_now()` directly instead of a
  /// custom one-shot future, keeping the cooperative yield runtime-aware
  /// without forcing the runtime bound onto read-only impls.
  pub(crate) _runtime: PhantomData<R>,
}

impl<K, V, R, KH, C, U, CB, S> AsyncCache<K, V, R, KH, C, U, CB, S>
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>,
  C: Coster<Value = V>,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
  S: BuildHasher + Clone + 'static + Send,
  R: RuntimeLite,
{
  /// clear the Cache.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub async fn clear(&self) -> Result<(), CacheError> {
    // Prelude + marker dispatch are sync calls into the crossbeam-backed
    // ring; they can briefly park the calling tokio worker if the channel
    // is full. Markers are rare (one per `clear()`/`wait()`), so this
    // tradeoff is acceptable to preserve the durable-marker invariant.
    self
      .0
      .insert_buf_ring
      .drain_all_stripes_to_channel()
      .map_err(|_| CacheError::SendError("fail to drain stripes: channel closed".to_string()))?;
    let (waiter, rx) = Waiter::new();
    self
      .0
      .insert_buf_ring
      .send_single(Item::Clear(waiter))
      .map_err(|_| {
        CacheError::SendError("fail to enqueue clear marker: channel closed".to_string())
      })?;
    let _ = rx.await;
    Ok(())
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
  pub async fn insert(&self, key: K, val: V, cost: i64) -> bool {
    self.insert_with_ttl(key, val, cost, Duration::ZERO).await
  }

  /// `try_insert` is the non-panicking version of [`insert`](#method.insert)
  pub async fn try_insert(&self, key: K, val: V, cost: i64) -> Result<bool, CacheError> {
    self
      .try_insert_with_ttl(key, val, cost, Duration::ZERO)
      .await
  }

  /// `insert_with_ttl` works like Set but adds a key-value pair to the cache that will expire
  /// after the specified TTL (time to live) has passed. A zero value means the value never
  /// expires, which is identical to calling `insert`.
  pub async fn insert_with_ttl(&self, key: K, val: V, cost: i64, ttl: Duration) -> bool {
    self
      .try_insert_in(key, val, cost, ttl, false)
      .await
      .unwrap()
  }

  /// `try_insert_with_ttl` is the non-panicking version of [`insert_with_ttl`](#method.insert_with_ttl)
  pub async fn try_insert_with_ttl(
    &self,
    key: K,
    val: V,
    cost: i64,
    ttl: Duration,
  ) -> Result<bool, CacheError> {
    self.try_insert_in(key, val, cost, ttl, false).await
  }

  /// `insert_if_present` is like `insert`, but only updates the value of an existing key. It
  /// does NOT add the key to cache if it's absent.
  pub async fn insert_if_present(&self, key: K, val: V, cost: i64) -> bool {
    self
      .try_insert_in(key, val, cost, Duration::ZERO, true)
      .await
      .unwrap()
  }

  /// `try_insert_if_present` is the non-panicking version of [`insert_if_present`](#method.insert_if_present)
  pub async fn try_insert_if_present(&self, key: K, val: V, cost: i64) -> Result<bool, CacheError> {
    self
      .try_insert_in(key, val, cost, Duration::ZERO, true)
      .await
  }

  /// wait until the previous operations finished.
  pub async fn wait(&self) -> Result<(), CacheError> {
    // Flush stripe-buffered items BEFORE the marker, so that wait()
    // returns only after all items queued *before* the call have been
    // processed. Without the prelude, partial stripes would sit idle
    // until the next drain_interval tick and the marker would slide
    // past them.
    self
      .0
      .insert_buf_ring
      .drain_all_stripes_to_channel()
      .map_err(|_| CacheError::SendError("fail to drain stripes: channel closed".to_string()))?;
    let (waiter, rx) = Waiter::new();
    self
      .0
      .insert_buf_ring
      .send_single(Item::Wait(waiter))
      .map_err(|_| {
        CacheError::SendError("fail to enqueue wait marker: channel closed".to_string())
      })?;
    let _ = rx.await;
    Ok(())
  }

  /// remove entry from Cache by key.
  pub async fn remove(&self, k: &K) {
    self.try_remove(k).await.unwrap()
  }

  /// try to remove an entry from the Cache by key
  pub async fn try_remove(&self, k: &K) -> Result<(), CacheError> {
    let (index, conflict) = self.0.key_to_hash.build_key(k);
    // Capture the current clear generation before the eager remove. Paired
    // with the Release-ordered bump in the Clear handler, this Acquire load
    // lets the processor recognize a Delete queued before a clear as stale.
    // The store gates the remove on `row.generation <= captured_gen` so a
    // pre-clear caller that resumes after `clear()` and a racing post-clear
    // reinsert cannot destroy the fresh row.
    let captured_gen = self.0.clear_generation.load(Ordering::Acquire);
    let prev = self
      .0
      .store
      .try_remove_if_not_stale(&index, conflict, captured_gen)?;

    // Only enqueue Item::Delete if we actually removed a store row. If the
    // eager remove found nothing there is no policy/store state we own to
    // reconcile, and enqueueing a Delete with version=0 would race with a
    // concurrent insert: processor admits the new row via Item::New, then
    // our Delete unconditionally calls policy.remove and orphans the fresh
    // admission outside policy accounting (bypassing max_cost).
    let Some(prev) = prev else {
      return Ok(());
    };
    let prev_version = prev.version;

    // The version we just removed is stamped on the Item so a concurrent
    // reinsert at the same (key, conflict) under a newer version survives
    // the follow-up store cleanup. Use blocking `send_single` so the
    // Delete reaches the processor — dropping it after the eager store
    // remove would strand a policy entry with no store row. The send is
    // sync (crossbeam) and may briefly park the tokio worker under
    // contention; that is the deliberate tradeoff vs dropping a Delete.
    let send_result =
      self
        .0
        .insert_buf_ring
        .send_single(Item::delete(index, conflict, captured_gen, prev_version));

    // Fire on_exit AFTER the Delete is durably enqueued (or its failure is
    // captured) so a panicking user callback cannot mask a channel error
    // and strand a ghost cost in policy.
    self.0.callback.on_exit(Some(prev.value));

    match send_result {
      Ok(()) => Ok(()),
      Err(()) => Err(CacheError::ChannelError(
        "failed to send delete to insert buffer: channel closed".to_string(),
      )),
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  async fn try_insert_in(
    &self,
    key: K,
    val: V,
    cost: i64,
    ttl: Duration,
    only_update: bool,
  ) -> Result<bool, CacheError> {
    use crate::cache::insert_stripe::PushOutcome;

    // (1) Eager store write — unchanged.
    let (_index, item, prev_val) = match self.try_update(key, val, cost, ttl, only_update)? {
      Some(triple) => triple,
      None => return Ok(false),
    };

    // Capture whether our own item is an Update before moving it into
    // the ring. If the batch is later dropped, `rollback_batch` keeps
    // Update store rows in place (graceful leak: readers already see the
    // new value), so the caller-facing result must say `Ok(true)` for an
    // Update — `Ok(false)` would be a contract violation since the
    // user's mutation survived.
    let is_update = matches!(item, Item::Update { .. });

    // (2) Push to stripe — sync, no await. Mirrors sync's
    // drop-on-overflow contract: if the bounded ring is saturated (after
    // a 20µs send_timeout) or disconnected during shutdown, the batch is
    // rolled back inline.
    let result = match self.0.insert_buf_ring.push(item) {
      PushOutcome::Buffered | PushOutcome::Sent => {
        if let Some(v) = prev_val {
          self.0.callback.on_exit(Some(v));
        }
        Ok(true)
      }
      PushOutcome::Dropped(batch) => {
        rollback_batch(
          &self.0.store,
          &self.0.policy,
          &self.0.callback,
          &self.0.metrics,
          &batch,
        );
        if let Some(v) = prev_val {
          self.0.callback.on_exit(Some(v));
        }
        Ok(is_update)
      }
    };

    // (3) Cooperative yield. The body above is fully synchronous after the
    // crossbeam-channel migration, so a producer task in a tight
    // `c.insert(..).await` loop would otherwise hog its worker thread and
    // starve sibling tasks (concurrent `clear()`, the policy worker, etc).
    // `R::yield_now()` dispatches to the runtime's native yield primitive
    // (e.g. `tokio::task::yield_now()`), keeping the cooperative yield
    // executor-aware without forcing the runtime bound onto read-only impls.
    R::yield_now().await;

    result
  }
}

impl<K, V, R, KH, C, U, CB, S> Drop for AsyncCacheInner<K, V, R, KH, C, U, CB, S>
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>,
{
  /// Blocking shutdown when the last `AsyncCache` handle is dropped.
  /// Dropping `stop_tx` disconnects the processor's `stop_rx`, which causes
  /// its `select!` stop arm to fire on the next poll and run the drain
  /// path. Then we join the processor thread so no further `CacheCallback`
  /// or policy mutation can fire after the last `AsyncCache::drop`
  /// returns.
  ///
  /// `policy_stop_tx` is dropped too so the `AsyncLFUPolicy` RT task
  /// observes its receiver disconnected and exits.
  ///
  /// Panic-safe: `JoinHandle::join` returns `Err` if the processor thread
  /// panicked (e.g. from a user callback), so shutdown cannot hang on a
  /// missed handshake.
  ///
  /// Self-drop handling: if the last `AsyncCache` is dropped inside a
  /// `CacheCallback` running on the processor thread, joining that same
  /// thread from its own stack would deadlock (self-join). In that case
  /// we skip the join — the `JoinHandle` is dropped, which detaches the
  /// thread. The thread runs to completion normally after the callback
  /// returns, observes the disconnected `stop_rx`, and exits its drain
  /// path on its own.
  fn drop(&mut self) {
    let _ = self.stop_tx.take();
    let _ = self.policy_stop_tx.take();
    if let Some(handle) = self.processor.take() {
      if handle.thread().id() == std::thread::current().id() {
        return;
      }
      let _ = handle.join();
    }
  }
}

/// Roll back a batch dropped by the saturated/closed insert ring.
/// Identical to sync's `PushOutcome::Dropped` handler in
/// `src/cache/sync.rs`:
///
/// - **New**: `metrics::DropSets`; `try_remove_if_version`; on success,
///   `callback.on_reject`. Version-gated so a concurrent reinsert at
///   the same `(key, conflict)` under a newer version is preserved.
/// - **Update**: `metrics::DropSets` only. Graceful leak — store keeps
///   the new value (the eager `try_update` already destroyed the prior
///   value via `on_exit`); policy stays stale; TTL cleanup or a future
///   admission catches the row.
/// - **Delete/Wait/Clear**: defensive no-op. These markers are sent via
///   `send_single`, never through striped batches.
///
/// Covers the calling producer's own item AND co-producer items hashed
/// into the same stripe.
pub(crate) fn rollback_batch<V, U, CB, S>(
  store: &Arc<ShardedMap<V, U, S, S>>,
  policy: &Arc<AsyncLFUPolicy<S>>,
  callback: &Arc<CB>,
  metrics: &Arc<Metrics>,
  batch: &[Item<V>],
) where
  V: Send + Sync + 'static,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
  S: BuildHasher + Clone + 'static,
{
  for item in batch {
    match item {
      Item::New {
        key,
        conflict,
        cost,
        expiration,
        version,
        ..
      } => {
        metrics.add(MetricType::DropSets, *key, 1);
        if let Ok(Some(sitem)) = store.try_remove_if_version(key, *conflict, *version) {
          // Ghost-entry cleanup. The processor may have just handled
          // a stale Item::Delete for an older version of this key
          // whose `contains_key` gate saw OUR eager row and therefore
          // skipped `policy.remove`. Removing our row here without
          // cleaning policy would strand that older entry as a ghost:
          // policy tracks the key, store has nothing at this index,
          // and no future handler is queued to reconcile (Item::New
          // was dropped, never reaches the processor).
          //
          // conflict=0 means "any row at this index" (Policy is
          // index-keyed); leave policy alone if a different conflict
          // is sharing the entry.
          if !store.contains_key(key, 0) {
            policy.remove(key);
          }
          callback.on_reject(CrateItem {
            val: Some(sitem.value),
            index: *key,
            conflict: *conflict,
            cost: *cost,
            exp: *expiration,
          });
        }
      }
      Item::Update { key, .. } => {
        metrics.add(MetricType::DropSets, *key, 1);
      }
      Item::Delete { .. } | Item::Wait(_) | Item::Clear(_) => {}
    }
  }
}

impl<V, U, CB, S> CacheProcessor<V, U, CB, S>
where
  V: Send + Sync + 'static,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
  S: BuildHasher + Clone + 'static + Send + Sync,
{
  pub(crate) fn new(
    num_to_keep: usize,
    ignore_internal_cost: bool,
    cleanup_duration: Duration,
    drain_duration: Duration,
    store: Arc<ShardedMap<V, U, S, S>>,
    policy: Arc<AsyncLFUPolicy<S>>,
    insert_buf_rx: Receiver<Vec<Item<V>>>,
    insert_stripe: Arc<crate::cache::insert_stripe::InsertStripeRing<Item<V>>>,
    stop_rx: Receiver<()>,
    metrics: Arc<Metrics>,
    callback: Arc<CB>,
    clear_generation: Arc<AtomicU64>,
  ) -> Self {
    let item_size = store.item_size();
    let hasher = store.hasher();
    Self {
      insert_buf_rx,
      insert_stripe,
      stop_rx,
      metrics,
      store,
      policy,
      start_ts: HashMap::with_hasher(hasher),
      num_to_keep,
      callback,
      ignore_internal_cost,
      item_size,
      cleanup_duration,
      drain_duration,
      clear_generation,
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn spawn(mut self) -> JoinHandle<Result<(), CacheError>> {
    // Two timers, deliberately separate: sync uses one ticker at
    // drain_interval and runs cleanup on every tick, but TTL sweeps
    // touch every expiration bucket and are heavier than stripe drains.
    // Splitting lets drain stay aggressive (default 500ms) while cleanup
    // runs at its own cadence (default 2s), matching the prior async
    // behavior on the cleanup side.
    let drain_ticker = tick(self.drain_duration);
    let cleanup_ticker = tick(self.cleanup_duration);
    let stripe = self.insert_stripe.clone();
    thread_spawn(move || {
      loop {
        cb_select! {
          recv(self.insert_buf_rx) -> res => {
            match res {
              Ok(batch) => {
                if let Err(e) = self.handle_insert_batch(batch) {
                  tracing::error!("fail to handle insert batch: {}", e);
                }
              }
              Err(e) => {
                // Channel disconnected (cache shutting down). Same as
                // the stop arm: do final inline drain, exit.
                stripe.drain_all_stripes_inline(|batch| {
                  for item in batch {
                    let _ = self.handle_item(item);
                  }
                });
                tracing::debug!("insert receiver disconnected: {}", e);
                return Ok(());
              }
            }
          },
          recv(drain_ticker) -> _ => {
            // Drain partial stripes inline on every tick — bounds
            // admission latency to one drain_duration period regardless
            // of whether a threshold flush fired.
            stripe.drain_all_stripes_inline(|batch| {
              for item in batch {
                let _ = self.handle_item(item);
              }
            });
          },
          recv(cleanup_ticker) -> _ => {
            if let Err(e) = self.handle_cleanup_event() {
              tracing::error!("fail to handle cleanup event: {}", e);
            }
          },
          recv(self.stop_rx) -> _ => {
            // Final drain: any in-flight stripe contents AND any queued
            // batches in the bounded receiver are processed before exit
            // so no item is silently lost on shutdown. Wait/Clear
            // markers are signaled so callers parked on them wake up.
            stripe.drain_all_stripes_inline(|batch| {
              for item in batch {
                let _ = self.handle_item(item);
              }
            });
            while let Ok(batch) = self.insert_buf_rx.try_recv() {
              for item in batch {
                match item {
                  Item::Wait(wg) | Item::Clear(wg) => wg.done(),
                  Item::New { .. } | Item::Update { .. } | Item::Delete { .. } => {
                    let _ = self.handle_item(item);
                  }
                }
              }
            }
            return Ok(());
          },
        }
      }
    })
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn handle_cleanup_event(&mut self) -> Result<(), CacheError> {
    self
      .store
      .try_cleanup_async(self.policy.clone())?
      .into_iter()
      .for_each(|victim| {
        self.prepare_evict(&victim);
        self.callback.on_evict(victim);
      });
    Ok(())
  }
}

impl_builder!(AsyncCacheBuilder);
impl_cache_processor!(CacheProcessor, Item);

impl<K, V, R, KH, C, U, CB, S> AsyncCache<K, V, R, KH, C, U, CB, S>
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>,
  C: Coster<Value = V>,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
  S: BuildHasher + Clone + 'static + Send,
{
  /// `get` returns a `Option<ValueRef<V>>` (if any) representing whether the
  /// value was found or not.
  pub async fn get<Q>(&self, key: &Q) -> Option<ValueRef<'_, V>>
  where
    K: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    let (index, conflict) = self.0.key_to_hash.build_key(key);

    self.0.get_buf.push(index);

    match self.0.store.get(&index, conflict) {
      None => {
        self.0.metrics.add(MetricType::Miss, index, 1);
        None
      }
      Some(v) => {
        self.0.metrics.add(MetricType::Hit, index, 1);
        Some(v)
      }
    }
  }

  /// `get_mut` returns a `Option<ValueRefMut<V>>` (if any) representing whether the
  /// value was found or not.
  pub async fn get_mut<Q>(&self, key: &Q) -> Option<ValueRefMut<'_, V>>
  where
    K: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    let (index, conflict) = self.0.key_to_hash.build_key(key);

    self.0.get_buf.push(index);

    match self.0.store.get_mut(&index, conflict) {
      None => {
        self.0.metrics.add(MetricType::Miss, index, 1);
        None
      }
      Some(v) => {
        self.0.metrics.add(MetricType::Hit, index, 1);
        Some(v)
      }
    }
  }

  /// Returns the TTL for the specified key if the
  /// item was found and is not expired.
  pub fn get_ttl<Q>(&self, key: &Q) -> Option<Duration>
  where
    K: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    let (index, conflict) = self.0.key_to_hash.build_key(key);
    self
      .0
      .store
      .get(&index, conflict)
      .and_then(|_| self.0.store.expiration(&index).map(|time| time.get_ttl()))
  }

  /// `max_cost` returns the max cost of the cache.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn max_cost(&self) -> i64 {
    self.0.policy.max_cost()
  }

  /// `update_max_cost` updates the maxCost of an existing cache.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn update_max_cost(&self, max_cost: i64) {
    self.0.policy.update_max_cost(max_cost)
  }

  /// Returns the number of items in the Cache
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn len(&self) -> usize {
    self.0.store.len()
  }

  /// Returns true if the cache is empty
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn is_empty(&self) -> bool {
    self.0.store.len() == 0
  }

  /// Eager store write. Returns the new `Item` to enqueue along with the
  /// prior value (for Update). The caller is responsible for firing
  /// `CacheCallback::on_exit(prev)` after the channel send, so a
  /// re-entrant user callback never runs while we still hold the eager
  /// write open.
  #[cfg_attr(not(tarpaulin), inline(always))]
  fn try_update(
    &self,
    key: K,
    val: V,
    cost: i64,
    ttl: Duration,
    only_update: bool,
  ) -> Result<Option<(u64, Item<V>, Option<V>)>, CacheError> {
    let expiration = if ttl.is_zero() {
      Time::now()
    } else {
      Time::now_with_expiration(ttl)
    };

    let (index, conflict) = self.0.key_to_hash.build_key(&key);

    // cost is eventually updated. The expiration must also be immediately updated
    // to prevent items from being prematurely removed from the map.
    let external_cost = if cost == 0 {
      self.0.coster.cost(&val)
    } else {
      0
    };
    // Capture the clear generation BEFORE the eager store write. If a
    // clear() observes a higher generation by the time the processor
    // admits this item, the processor treats it as stale and removes the
    // store entry (if any) instead of admitting to policy. Acquire
    // ordering pairs with the Release-ordered fetch_add in the Clear
    // handler so a captured "pre-clear" generation is guaranteed to be
    // less than the post-clear value.
    let captured_gen = self
      .0
      .clear_generation
      .load(std::sync::atomic::Ordering::Acquire);
    match self
      .0
      .store
      .try_update(index, val, conflict, expiration, captured_gen)?
    {
      UpdateResult::NotExist(v) => {
        if only_update {
          Ok(None)
        } else {
          // Insert into store immediately so reads after write see the value.
          // The background processor still runs policy admission; if rejected
          // it removes the item from the store.
          // try_insert returns None when a concurrent insert beat us and the
          // validator/conflict blocked the write (or when a post-clear row
          // already occupies this key and our captured generation is
          // stale — the store refuses in that case); skip policy.
          match self
            .0
            .store
            .try_insert(index, v, conflict, expiration, captured_gen)?
          {
            Some(version) => Ok(Some((
              index,
              Item::new(
                index,
                conflict,
                cost + external_cost,
                expiration,
                version,
                captured_gen,
              ),
              None,
            ))),
            None => Ok(None),
          }
        }
      }
      // Key already exists but the validator or conflict hash blocked the update.
      // The store is unchanged; no New item should be queued.
      UpdateResult::Reject(_) | UpdateResult::Conflict(_) => Ok(None),
      // A clear raced with this caller: the existing row was written
      // under a later generation than we captured, and the store refused
      // to clobber it. The post-clear row belongs to a different writer
      // who will admit it through their own Item — we have nothing to
      // enqueue and must not fire `on_exit`.
      UpdateResult::Stale(_) => Ok(None),
      UpdateResult::Update(v, version) => {
        // `on_exit(Some(v))` is intentionally NOT fired here — caller
        // fires it after the channel send completes.
        Ok(Some((
          index,
          Item::update(
            index,
            conflict,
            cost,
            external_cost,
            expiration,
            version,
            captured_gen,
          ),
          Some(v),
        )))
      }
    }
  }
}

impl<K, V, R, KH, C, U, CB, S> AsRef<AsyncCache<K, V, R, KH, C, U, CB, S>>
  for AsyncCache<K, V, R, KH, C, U, CB, S>
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>,
  C: Coster<Value = V>,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
  S: BuildHasher + Clone + 'static,
{
  fn as_ref(&self) -> &AsyncCache<K, V, R, KH, C, U, CB, S> {
    self
  }
}
