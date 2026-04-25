use crate::{
  CacheCallback, CacheError, Coster, DefaultCacheCallback, DefaultCoster, DefaultKeyBuilder,
  DefaultUpdateValidator, Item as CrateItem, KeyBuilder, Metrics, UpdateValidator,
  cache::builder::CacheBuilderCore,
  metrics::MetricType,
  policy::{AddOutcome, LFUPolicy},
  ring::RingStripe,
  store::ShardedMap,
  sync::{Instant, JoinHandle, Receiver, Sender, Signal, WaitGroup, select, spawn, stop_channel},
  ttl::{ExpirationMap, Time},
};
use crossbeam_channel::{RecvError, tick};
use std::{
  cell::Cell,
  collections::{HashMap, hash_map::RandomState},
  hash::{BuildHasher, Hash},
  marker::PhantomData,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

thread_local! {
  /// True while the current thread is the sync cache processor. Used to
  /// detect re-entrant calls from `CacheCallback` methods (on_evict/on_reject/
  /// on_exit). The `clear()` and `wait()` methods reject re-entry with an error
  /// because their control markers must be consumed by the processor, and a
  /// re-entrant call would self-deadlock. The `try_remove()` method detects
  /// re-entry and runs an inline Delete reconciliation on the processor thread
  /// instead of enqueueing, avoiding a self-blocking channel send.
  static ON_PROCESSOR_THREAD: Cell<bool> = const { Cell::new(false) };
}

#[inline]
fn on_processor_thread() -> bool {
  ON_PROCESSOR_THREAD.with(|c| c.get())
}

/// The `CacheBuilder` struct is used when creating [`Cache`] instances if you want to customize the [`Cache`] settings.
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
///   The reason this is a [`CacheBuilder`] flag is because there's a 10% throughput performance overhead.
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
///   The [`Cache`] will cleanup the expired values every 500ms by default.
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
///   [`Coster`] is a trait you can pass to the [`CacheBuilder`] in order to evaluate
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
///   The hasher for the [`Cache`], default is SipHasher.
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

impl<K: Hash + Eq, V: Send + Sync + 'static> CacheBuilder<K, V> {
  /// Create a new Builder
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn new(num_counters: usize, max_cost: i64) -> Self {
    Self {
      inner: CacheBuilderCore::new(num_counters, max_cost),
    }
  }
}

impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<Key = K>> CacheBuilder<K, V, KH> {
  /// Create a new AsyncCacheBuilder
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn new_with_key_builder(num_counters: usize, max_cost: i64, kh: KH) -> Self {
    Self {
      inner: CacheBuilderCore::new_with_key_builder(num_counters, max_cost, kh),
    }
  }
}

impl<K, V, KH, C, U, CB, S> CacheBuilder<K, V, KH, C, U, CB, S>
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>,
  C: Coster<Value = V>,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
  S: BuildHasher + Send + Clone + 'static + Sync,
{
  /// Build Cache and start all threads needed by the Cache.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn finalize(self) -> Result<Cache<K, V, KH, C, U, CB, S>, CacheError> {
    let num_counters = self.inner.num_counters;

    if num_counters == 0 {
      return Err(CacheError::InvalidNumCounters);
    }

    let max_cost = self.inner.max_cost;
    if max_cost == 0 {
      return Err(CacheError::InvalidMaxCost);
    }

    let high_water = self.inner.insert_stripe_high_water;
    if high_water == 0 {
      return Err(CacheError::InvalidBufferSize);
    }
    let (insert_buf, buf_rx) =
      crate::cache::insert_stripe::InsertStripeRing::<Item<V>>::new(high_water);
    let insert_buf = Arc::new(insert_buf);
    let (stop_tx, stop_rx) = stop_channel();

    let hasher = self.inner.hasher.unwrap();
    let expiration_map = ExpirationMap::with_hasher(hasher.clone());

    let store = Arc::new(ShardedMap::with_validator_and_hasher(
      expiration_map,
      self.inner.update_validator.unwrap(),
      hasher.clone(),
    ));

    let buffer_items = self.inner.buffer_items;
    let mut policy = LFUPolicy::with_hasher(num_counters, max_cost, hasher, stop_rx.clone())?;

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
      self.inner.drain_interval,
      store.clone(),
      policy.clone(),
      buf_rx,
      insert_buf.clone(),
      stop_rx,
      metrics.clone(),
      callback.clone(),
      clear_generation.clone(),
    )
    .spawn();

    let get_buf = RingStripe::new(policy.clone(), buffer_items);
    let inner = CacheInner {
      store,
      policy,
      get_buf: Arc::new(get_buf),
      insert_buf,
      callback,
      key_to_hash: Arc::new(self.inner.key_to_hash),
      stop_tx: Some(stop_tx),
      coster,
      metrics,
      clear_generation,
      processor: Some(processor),
      _marker: Default::default(),
    };

    Ok(Cache(Arc::new(inner)))
  }
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
    /// the eager write we installed.
    conflict: u64,
    cost: i64,
    external_cost: i64,
    #[allow(dead_code)]
    expiration: Time,
    /// New version assigned by the store to the row we just wrote. Used by
    /// the stale-generation branch of the Update handler for version-gated
    /// ghost-row cleanup; a concurrent writer who has since landed a newer
    /// version at the same (key, conflict) is preserved.
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
  Wait(Signal),
  Clear(Signal),
}

impl<V> Item<V> {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn new(
    key: u64,
    conflict: u64,
    cost: i64,
    exp: Time,
    version: u64,
    generation: u64,
  ) -> Self {
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
  pub(crate) fn delete(key: u64, conflict: u64, generation: u64, version: u64) -> Self {
    Self::Delete {
      key,
      conflict,
      generation,
      version,
    }
  }
}

pub(crate) struct CacheProcessor<V, U, CB, S> {
  pub(crate) insert_buf_rx: Receiver<Vec<Item<V>>>,
  pub(crate) stop_rx: Receiver<()>,
  pub(crate) metrics: Arc<Metrics>,
  pub(crate) store: Arc<ShardedMap<V, U, S, S>>,
  pub(crate) policy: Arc<LFUPolicy<S>>,
  pub(crate) start_ts: HashMap<u64, Time, S>,
  pub(crate) num_to_keep: usize,
  pub(crate) callback: Arc<CB>,
  pub(crate) ignore_internal_cost: bool,
  pub(crate) item_size: usize,
  /// Tick cadence for the processor's drain-and-cleanup arm.
  pub(crate) drain_interval: Duration,
  /// Owned reference to the producer-side ring so the processor can
  /// inline-drain it from the tick arm and the stop arm.
  pub(crate) insert_stripe: Arc<crate::cache::insert_stripe::InsertStripeRing<Item<V>>>,
  /// Shared counter bumped on every clear. The handler reads and advances
  /// this before wiping the store so any `Item::New` queued with the
  /// pre-bump generation is recognized as stale and skipped.
  pub(crate) clear_generation: Arc<AtomicU64>,
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
  KH = DefaultKeyBuilder<K>,
  C = DefaultCoster<V>,
  U = DefaultUpdateValidator<V>,
  CB = DefaultCacheCallback<V>,
  S = RandomState,
>(pub(crate) Arc<CacheInner<K, V, KH, C, U, CB, S>>);

impl<K, V, KH, C, U, CB, S> Clone for Cache<K, V, KH, C, U, CB, S> {
  #[cfg_attr(not(tarpaulin), inline(always))]
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

/// Private shared state behind [`Cache`]. Every `Cache` handle clones the
/// same `Arc<CacheInner>`; `Drop` on `CacheInner` fires exactly once, when
/// the last handle is dropped. Not part of the public API — all user-facing
/// methods live on [`Cache`] and reach this via `self.0`.
pub(crate) struct CacheInner<
  K,
  V,
  KH = DefaultKeyBuilder<K>,
  C = DefaultCoster<V>,
  U = DefaultUpdateValidator<V>,
  CB = DefaultCacheCallback<V>,
  S = RandomState,
> {
  /// store is the central concurrent hashmap where key-value items are stored.
  pub(crate) store: Arc<ShardedMap<V, U, S, S>>,

  /// policy determines what gets let in to the cache and what gets kicked out.
  pub(crate) policy: Arc<LFUPolicy<S>>,

  pub(crate) get_buf: Arc<RingStripe<S>>,

  /// Striped per-thread insert buffer. Producers (`try_insert_in`,
  /// `try_remove`, `wait`, `clear`) push here; the processor's
  /// `select!` loop receives `Vec<Item<V>>` batches via the ring's
  /// owned `Sender<Vec<Item<V>>>`. Spec:
  /// docs/superpowers/specs/2026-04-26-striped-insert-buffer-design.md
  pub(crate) insert_buf: Arc<crate::cache::insert_stripe::InsertStripeRing<Item<V>>>,

  /// Held in `Option` so `Drop` can `take()` and drop it, disconnecting the
  /// processor's `stop_rx`. This is how shutdown is signaled — we never
  /// `send()` on this channel. Disconnection wakes the processor's
  /// `select!` stop arm via `Err(RecvError)`, which is matched by `_`.
  pub(crate) stop_tx: Option<Sender<()>>,

  pub(crate) callback: Arc<CB>,

  pub(crate) key_to_hash: Arc<KH>,

  pub(crate) coster: Arc<C>,

  /// the metrics for the cache
  pub(crate) metrics: Arc<Metrics>,

  /// Clear-generation counter shared with the processor. `try_update`
  /// captures this before the eager store write so a subsequent `clear()`
  /// can invalidate the still-in-flight `Item::New` and any stale eager
  /// write it represents.
  pub(crate) clear_generation: Arc<AtomicU64>,

  /// Handle to the processor thread. `Drop` joins this so dropping the
  /// cache returns only after the processor has fully shut down (drain
  /// complete, no further handler work in flight). Stored as `Option` so
  /// `Drop` can `take()` the handle to call `join()` on an owned value.
  ///
  /// Using `JoinHandle` instead of a handshake WaitGroup makes shutdown
  /// panic-safe: a panicking user callback (`CacheCallback::on_evict` etc.)
  /// unwinds the processor thread past its normal drain/signal path, but
  /// `join()` still returns once the thread's stack has unwound. With the
  /// old WaitGroup design, a callback panic skipped the `done()` call and
  /// left `Drop` waiting forever.
  pub(crate) processor: Option<JoinHandle<Result<(), CacheError>>>,

  pub(crate) _marker: PhantomData<fn(K)>,
}

impl<K, V, KH, C, U, CB, S> Drop for CacheInner<K, V, KH, C, U, CB, S> {
  /// Blocking shutdown when the last `Cache` handle is dropped. Dropping
  /// `stop_tx` disconnects the processor's `stop_rx`, which causes its
  /// `select!` stop arm to fire on the next poll and run the drain path.
  /// Then we join the processor thread so no further `CacheCallback` or
  /// policy mutation can fire after the last `Cache::drop` returns.
  ///
  /// Panic-safe: `JoinHandle::join` returns `Err` if the processor thread
  /// panicked (e.g. from a user callback), so shutdown cannot hang on a
  /// missed handshake. The join result is otherwise discarded — any
  /// internal error was already logged by the processor.
  ///
  /// Self-drop handling: if the last `Cache` is dropped inside a
  /// `CacheCallback` running on the processor thread, joining that same
  /// thread from its own stack would deadlock (self-join). In that case
  /// we skip the join — the `JoinHandle` is dropped, which detaches the
  /// thread. The thread runs to completion normally after the callback
  /// returns, observes the disconnected `stop_rx`, and exits its drain
  /// path on its own.
  fn drop(&mut self) {
    let _ = self.stop_tx.take();
    if let Some(handle) = self.processor.take() {
      if handle.thread().id() == std::thread::current().id() {
        return;
      }
      let _ = handle.join();
    }
  }
}

impl<K: Hash + Eq, V: Send + Sync + 'static> Cache<K, V> {
  /// Returns a Cache instance with default configruations.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn new(num_counters: usize, max_cost: i64) -> Result<Self, CacheError> {
    CacheBuilder::new(num_counters, max_cost).finalize()
  }

  /// Returns a Builder.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn builder(
    num_counters: usize,
    max_cost: i64,
  ) -> CacheBuilder<
    K,
    V,
    DefaultKeyBuilder<K>,
    DefaultCoster<V>,
    DefaultUpdateValidator<V>,
    DefaultCacheCallback<V>,
    RandomState,
  > {
    CacheBuilder::new(num_counters, max_cost)
  }
}

impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<Key = K>> Cache<K, V, KH> {
  /// Returns a Cache instance with default configruations.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn new_with_key_builder(
    num_counters: usize,
    max_cost: i64,
    index: KH,
  ) -> Result<Self, CacheError> {
    CacheBuilder::new_with_key_builder(num_counters, max_cost, index).finalize()
  }
}

impl<K, V, KH, C, U, CB, S> Cache<K, V, KH, C, U, CB, S>
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>,
  C: Coster<Value = V>,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
  S: BuildHasher + Clone + 'static + Send + Sync,
{
  /// clear the Cache.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn clear(&self) -> Result<(), CacheError> {
    // Reject processor-thread re-entry. `clear()` enqueues `Item::Clear`
    // and then blocks on the marker's WaitGroup; the marker is only
    // signaled by the processor. A callback running on the processor that
    // calls `clear()` would be waiting for itself — permanent deadlock.
    if on_processor_thread() {
      return Err(CacheError::ChannelError(
        "clear() cannot be called from a CacheCallback: \
         it would wait on the processor thread that is currently running \
         the callback"
          .to_string(),
      ));
    }

    let wg = WaitGroup::new();
    let wait = Signal::new(wg.add(1));
    // Prelude: flush every stripe to the channel BEFORE the Clear
    // marker so all pre-clear inserts are processed against the old
    // generation before the wipe.
    if self.0.insert_buf.drain_all_stripes_to_channel().is_err() {
      return Err(CacheError::SendError(
        "cache insert buffer: channel closed during clear prelude".to_string(),
      ));
    }
    match self.0.insert_buf.send_single(Item::Clear(wait)) {
      Ok(()) => {
        wg.wait();
        Ok(())
      }
      Err(()) => Err(CacheError::SendError(
        "fail to enqueue clear marker: channel closed".to_string(),
      )),
    }
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
    self.try_insert(key, val, cost).unwrap()
  }

  /// `try_insert` is the non-panicking version of [`insert`](#method.insert)
  pub fn try_insert(&self, key: K, val: V, cost: i64) -> Result<bool, CacheError> {
    self.try_insert_with_ttl(key, val, cost, Duration::ZERO)
  }

  /// `insert_with_ttl` works like Set but adds a key-value pair to the cache that will expire
  /// after the specified TTL (time to live) has passed. A zero value means the value never
  /// expires, which is identical to calling `insert`.
  pub fn insert_with_ttl(&self, key: K, val: V, cost: i64, ttl: Duration) -> bool {
    self.try_insert_with_ttl(key, val, cost, ttl).unwrap()
  }

  /// `try_insert_with_ttl` is the non-panicking version of [`insert_with_ttl`](#method.insert_with_ttl)
  pub fn try_insert_with_ttl(
    &self,
    key: K,
    val: V,
    cost: i64,
    ttl: Duration,
  ) -> Result<bool, CacheError> {
    self.try_insert_in(key, val, cost, ttl, false)
  }

  /// `insert_if_present` is like `insert`, but only updates the value of an existing key. It
  /// does NOT add the key to cache if it's absent.
  pub fn insert_if_present(&self, key: K, val: V, cost: i64) -> bool {
    self.try_insert_if_present(key, val, cost).unwrap()
  }

  /// `try_insert_if_present` is the non-panicking version of [`insert_if_present`](#method.insert_if_present)
  pub fn try_insert_if_present(&self, key: K, val: V, cost: i64) -> Result<bool, CacheError> {
    self.try_insert_in(key, val, cost, Duration::ZERO, true)
  }

  /// wait until all the previous operations finished.
  pub fn wait(&self) -> Result<(), CacheError> {
    // Reject processor-thread re-entry. Same rationale as `clear()`: the
    // Wait marker is signaled by the processor, and a callback running on
    // the processor cannot wait for itself.
    if on_processor_thread() {
      return Err(CacheError::ChannelError(
        "wait() cannot be called from a CacheCallback: \
         it would wait on the processor thread that is currently running \
         the callback"
          .to_string(),
      ));
    }

    let wg = WaitGroup::new();
    let wait_item = Item::Wait(Signal::new(wg.add(1)));
    // Prelude: flush every stripe to the channel BEFORE the marker so
    // pre-wait inserts are observed by the processor before the Wait
    // barrier passes them.
    if self.0.insert_buf.drain_all_stripes_to_channel().is_err() {
      return Err(CacheError::SendError(
        "cache insert buffer: channel closed during wait prelude".to_string(),
      ));
    }
    match self.0.insert_buf.send_single(wait_item) {
      Ok(()) => {
        wg.wait();
        Ok(())
      }
      Err(()) => Err(CacheError::SendError(
        "cache insert buffer: channel closed".to_string(),
      )),
    }
  }

  /// remove an entry from Cache by key.
  pub fn remove(&self, k: &K) {
    self.try_remove(k).unwrap();
  }

  /// try to remove an entry from Cache by key.
  pub fn try_remove(&self, k: &K) -> Result<(), CacheError> {
    let (index, conflict) = self.0.key_to_hash.build_key(k);

    // Callback re-entry: if this `try_remove` was called from inside a
    // user `CacheCallback` running on the processor thread, we cannot
    // enqueue an `Item::Delete` — the sole consumer IS us, and we can't
    // drain while we're executing a callback. Apply the Delete handler
    // logic inline instead, mirroring src/cache.rs's Delete arm:
    // generation-gate against a racing `clear()`, and only wipe policy
    // if no concurrent insert has since reoccupied this index. The
    // processor's follow-up `try_remove_if_version` would be a no-op
    // (we already did the eager remove here) and is skipped.
    if on_processor_thread() {
      let captured_gen = self.0.clear_generation.load(Ordering::Acquire);
      let prev = self
        .0
        .store
        .try_remove_if_not_stale(&index, conflict, captured_gen)?;
      if let Some(prev) = prev {
        // Reconcile policy BEFORE firing on_exit so an on_exit panic
        // cannot strand a ghost cost: store and policy are already
        // consistent at the panic point.
        let current_gen = self.0.clear_generation.load(Ordering::Acquire);
        if captured_gen == current_gen && !self.0.store.contains_key(&index, 0) {
          self.0.policy.remove(&index);
        }
        self.0.callback.on_exit(Some(prev.value));
      }
      return Ok(());
    }

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
    // concurrent insert.
    let Some(prev) = prev else {
      return Ok(());
    };
    let prev_version = prev.version;

    // Blocking send so policy reconciliation is guaranteed: the eager
    // remove already wiped the store row, so dropping the Delete would
    // strand a ghost cost in policy. The bounded channel will drain.
    let send_result =
      self
        .0
        .insert_buf
        .send_single(Item::delete(index, conflict, captured_gen, prev_version));

    // Fire on_exit AFTER the Delete is durably enqueued (success path) so
    // a panicking user callback cannot strand a ghost cost in policy: the
    // in-flight Delete reconciles policy/store regardless.
    self.0.callback.on_exit(Some(prev.value));

    match send_result {
      Ok(()) => Ok(()),
      Err(()) => Err(CacheError::ChannelError(
        "failed to send delete to insert buffer: channel closed".to_string(),
      )),
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn try_insert_in(
    &self,
    key: K,
    val: V,
    cost: i64,
    ttl: Duration,
    only_update: bool,
  ) -> Result<bool, CacheError> {
    let (_index, item, prev_val) = match self.try_update(key, val, cost, ttl, only_update)? {
      Some(triple) => triple,
      None => return Ok(false),
    };

    use crate::cache::insert_stripe::PushOutcome;

    // Push into the striped insert buffer. The ring buffers up to
    // `high_water` items per stripe before swapping a full Vec out and
    // sending it through the bounded `Sender<Vec<Item<V>>>` to the
    // processor. Re-entrant callers (callbacks running on the processor
    // thread) push the same way — there is no longer a blocking-vs-
    // try-send distinction; the ring NEVER blocks the producer (worst
    // case: 20µs send_timeout on overflow before the batch is dropped).
    //
    // `Disconnected` semantics fold into `Dropped`: the ring's `push`
    // returns `Dropped` for both `Full` (after timeout) and
    // `Disconnected`. While the cache is live the receiver always
    // exists, so `Disconnected` is unreachable in practice.
    let outcome = self.0.insert_buf.push(item);
    match outcome {
      PushOutcome::Buffered | PushOutcome::Sent => {
        if let Some(v) = prev_val {
          self.0.callback.on_exit(Some(v));
        }
        Ok(true)
      }
      PushOutcome::Dropped(batch) => {
        // The batch overflowed and was dropped. The caller's own item
        // is somewhere in `batch`; items from other producer threads
        // hashed to the same stripe are also there. Honor the per-item
        // contract: New → reap eager store row + on_reject; Update →
        // graceful leak (store keeps new value, policy stale,
        // metrics::DropSets accounts for the loss); Delete → not
        // reachable here (Delete uses send_single and never goes
        // through stripes).
        for dropped in batch {
          match dropped {
            Item::New {
              key,
              conflict,
              cost,
              expiration,
              version,
              ..
            } => {
              self.0.metrics.add(MetricType::DropSets, key, 1);
              if let Ok(Some(sitem)) = self.0.store.try_remove_if_version(&key, conflict, version) {
                self.0.callback.on_reject(CrateItem {
                  val: Some(sitem.value),
                  index: key,
                  conflict,
                  cost,
                  exp: expiration,
                });
              }
            }
            Item::Update { key, .. } => {
              self.0.metrics.add(MetricType::DropSets, key, 1);
              // Graceful leak: the eager store row already holds the
              // new value; rolling it back here would surprise readers
              // mid-flight. Policy stays stale; eventual eviction
              // through TTL cleanup or a future admission will catch
              // this row.
            }
            // Delete/Wait/Clear are sent via `send_single`, never
            // through stripes. Defensive no-op for completeness.
            Item::Delete { .. } | Item::Wait(_) | Item::Clear(_) => {}
          }
        }
        if let Some(v) = prev_val {
          self.0.callback.on_exit(Some(v));
        }
        Ok(false)
      }
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
    drain_interval: Duration,
    store: Arc<ShardedMap<V, U, S, S>>,
    policy: Arc<LFUPolicy<S>>,
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
      stop_rx,
      metrics,
      store,
      policy,
      start_ts: HashMap::with_hasher(hasher),
      num_to_keep,
      callback,
      ignore_internal_cost,
      item_size,
      drain_interval,
      insert_stripe,
      clear_generation,
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn spawn(mut self) -> JoinHandle<Result<(), CacheError>> {
    let ticker = tick(self.drain_interval);
    let stripe = self.insert_stripe.clone();
    spawn(move || {
      ON_PROCESSOR_THREAD.with(|c| c.set(true));
      loop {
        select! {
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
          recv(ticker) -> msg => {
            // Drain every stripe inline first — bypasses the channel
            // entirely so backlog can clear regardless of channel
            // capacity. Then run TTL cleanup.
            stripe.drain_all_stripes_inline(|batch| {
              for item in batch {
                let _ = self.handle_item(item);
              }
            });
            if let Err(e) = self.handle_cleanup_event(msg) {
              tracing::error!("fail to handle cleanup event: {}", e);
            }
          },
          recv(self.stop_rx) -> _ => {
            // Final drain: any in-flight stripe contents AND any
            // queued batches in the bounded receiver are processed
            // before exit so no item is silently lost on shutdown.
            stripe.drain_all_stripes_inline(|batch| {
              for item in batch {
                let _ = self.handle_item(item);
              }
            });
            while let Ok(batch) = self.insert_buf_rx.try_recv() {
              for item in batch {
                match item {
                  Item::Wait(wg) | Item::Clear(wg) => wg.done(),
                  Item::New { key, conflict, version, .. }
                  | Item::Update { key, conflict, version, .. } => {
                    let _ = self.store.try_remove_if_version(&key, conflict, version);
                  }
                  Item::Delete { .. } => {}
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
  pub(crate) fn handle_cleanup_event(
    &mut self,
    res: Result<Instant, RecvError>,
  ) -> Result<(), CacheError> {
    res
      .map_err(|e| CacheError::RecvError(format!("fail to receive msg from ticker: {}", e)))
      .and_then(|_| {
        self.store.try_cleanup(self.policy.clone()).map(|items| {
          items.into_iter().for_each(|victim| {
            self.prepare_evict(&victim);
            self.callback.on_evict(victim);
          });
        })
      })
  }
}

impl_builder!(CacheBuilder);
impl_cache_processor!(CacheProcessor, Item);

use crate::{ValueRef, ValueRefMut, store::UpdateResult};

impl<K, V, KH, C, U, CB, S> Cache<K, V, KH, C, U, CB, S>
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
  pub fn get<Q>(&self, key: &Q) -> Option<ValueRef<'_, V>>
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
  pub fn get_mut<Q>(&self, key: &Q) -> Option<ValueRefMut<'_, V>>
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
  /// `CacheCallback::on_exit(prev)`.
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
        // `on_exit(Some(v))` is intentionally NOT fired here. The
        // caller fires it after the item has been enqueued (or after
        // the rollback path runs). See `try_insert_in` for the firing
        // points.
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

impl<K, V, KH, C, U, CB, S> AsRef<Cache<K, V, KH, C, U, CB, S>> for Cache<K, V, KH, C, U, CB, S>
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>,
  C: Coster<Value = V>,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
  S: BuildHasher + Clone + 'static,
{
  fn as_ref(&self) -> &Cache<K, V, KH, C, U, CB, S> {
    self
  }
}
