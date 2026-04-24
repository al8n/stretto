use crate::{
  CacheCallback, CacheError, Coster, DefaultCacheCallback, DefaultCoster, DefaultKeyBuilder,
  DefaultUpdateValidator, Item as CrateItem, KeyBuilder, Metrics, UpdateValidator,
  axync::{
    AsyncWaitGroup, Receiver, RecvError, Sender, TrySendError, Waiter, bounded, select,
    stop_channel,
  },
  cache::builder::CacheBuilderCore,
  metrics::MetricType,
  policy::{AddOutcome, AsyncLFUPolicy},
  ring::AsyncRingStripe,
  semaphore::AsyncSemaphore,
  store::ShardedMap,
  ttl::{ExpirationMap, Time},
};
use agnostic_lite::RuntimeLite;
use event_listener::Event;
use futures::{future::FutureExt, stream::StreamExt};
use std::{
  collections::{HashMap, hash_map::RandomState},
  hash::{BuildHasher, Hash},
  marker::PhantomData,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
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
///   The Cache will cleanup the expired values every 500ms by default.
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
  /// `RT` is the async runtime to use. For example, if you use `tokio`,
  /// pass `TokioRuntime` from `agnostic-lite`.
  ///
  /// ```no_run
  /// use stretto::AsyncCacheBuilder;
  /// use agnostic_lite::tokio::TokioRuntime;
  ///
  /// AsyncCacheBuilder::<u64, u64>::new(100, 10)
  ///     .finalize::<TokioRuntime>()
  ///     .unwrap();
  /// ```
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn finalize<RT: RuntimeLite>(self) -> Result<AsyncCache<K, V, KH, C, U, CB, S>, CacheError>
  where
    <RT as RuntimeLite>::Interval: Send,
  {
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
    let insert_sem = Arc::new(AsyncSemaphore::new(insert_buffer_size));

    let hasher = self.inner.hasher.unwrap();
    let expiration_map = ExpirationMap::with_hasher(hasher.clone());

    let store = Arc::new(ShardedMap::with_validator_and_hasher(
      expiration_map,
      self.inner.update_validator.unwrap(),
      hasher.clone(),
    ));
    let mut policy = AsyncLFUPolicy::with_hasher::<RT>(num_counters, max_cost, hasher)?;

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
    let close_state = AsyncCloseState::new();
    CacheProcessor::new(
      100000,
      self.inner.ignore_internal_cost,
      self.inner.cleanup_duration,
      store.clone(),
      policy.clone(),
      buf_rx,
      stop_rx,
      metrics.clone(),
      callback.clone(),
      clear_generation.clone(),
      insert_sem.clone(),
      close_state.processor_barrier(),
    )
    .spawn::<RT>();

    let buffer_items = self.inner.buffer_items;
    let get_buf = AsyncRingStripe::new(policy.clone(), buffer_items);
    let inner = AsyncCacheInner {
      store,
      policy,
      get_buf: Arc::new(get_buf),
      insert_buf_tx: buf_tx,
      insert_sem,
      callback,
      key_to_hash: Arc::new(self.inner.key_to_hash),
      stop_tx: Some(stop_tx),
      close_state,
      coster,
      metrics,
      clear_generation,
      ignore_internal_cost: self.inner.ignore_internal_cost,
      _marker: Default::default(),
    };

    Ok(AsyncCache(Arc::new(inner)))
  }
}

pub(crate) struct CacheProcessor<V, U, CB, S> {
  insert_buf_rx: Receiver<Item<V>>,
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
  /// Same permit pool as `AsyncCache::insert_sem`. The processor releases
  /// one permit for every item it consumes (main recv loop and close-drain)
  /// so blocked senders on the input side make progress.
  insert_sem: Arc<AsyncSemaphore>,
  /// Shutdown barrier. Counter starts at 1; the processor future wraps its
  /// loop in a RAII guard that calls `done()` on exit (normal return *or*
  /// panic-unwind), which unblocks every `close().await` caller. Cloned
  /// from `AsyncCache::close_wg`.
  close_wg: AsyncWaitGroup,
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
  /// In-band shutdown sentinel. When the processor pops this from the MPSC it
  /// runs the same barrier work as the legacy stop path, then returns; the
  /// processor wrapper fires `close_wg.done()` on exit so `close()` callers
  /// unblock. No per-caller waiter: the waitgroup is the barrier.
  Close,
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

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn is_update(&self) -> bool {
    matches!(self, Item::Update { .. })
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
  KH = DefaultKeyBuilder<K>,
  C = DefaultCoster<V>,
  U = DefaultUpdateValidator<V>,
  CB = DefaultCacheCallback<V>,
  S = RandomState,
>(pub(crate) Arc<AsyncCacheInner<K, V, KH, C, U, CB, S>>)
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>;

impl<K, V, KH, C, U, CB, S> Clone for AsyncCache<K, V, KH, C, U, CB, S>
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

  /// insert_buf is a buffer allowing us to batch/drop Sets during times of high
  /// contention.
  pub(crate) insert_buf_tx: Sender<Item<V>>,

  /// Bounded permit pool sized to `insert_buffer_size`. Every send into
  /// `insert_buf_tx` (New/Update/Delete/Wait/Clear) is gated on acquiring
  /// a permit. The processor releases one permit per recv (including
  /// close-drain). Acquiring BEFORE the eager store write moves
  /// backpressure to the input side so pre-admission store rows cannot
  /// accumulate beyond `insert_buffer_size` under contention. Without
  /// this, awaited `insert_buf_tx.send` calls would leave eager
  /// `try_update` rows live and uncharged, so N parked futures could push
  /// `store.len()` past `max_cost` until the processor catches up.
  pub(crate) insert_sem: Arc<AsyncSemaphore>,

  pub(crate) get_buf: Arc<AsyncRingStripe<S>>,

  /// Dropping this `Sender` disconnects the processor task's `stop_rx`,
  /// which causes its `select!` stop arm to fire and the task to run its
  /// drain and exit. `Drop` is fire-and-forget: it does not wait for the
  /// task to finish, since an async drop can't safely block the executor.
  /// Callers that need a shutdown barrier call [`Self::close`].
  /// Held in `Option` so `Drop` can `take()` and drop it, disconnecting the
  /// processor task's `stop_rx`. We never `send()` on this channel —
  /// disconnection alone wakes the processor's `select!` stop arm via
  /// `Err(RecvError)`, which is matched by `_`.
  pub(crate) stop_tx: Option<Sender<()>>,

  /// Groups every piece of state the close protocol touches: the
  /// entry-race flag, the commit flag, the loser wakeup event, and the
  /// processor barrier. See [`AsyncCloseState`] for the full protocol.
  pub(crate) close_state: AsyncCloseState,

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

  /// Mirrors the processor's `ignore_internal_cost` flag. Needed here so
  /// `try_insert_in` can pre-compute an `Item::Update`'s total effective
  /// cost before the cancellable `.send(...).await`. If the caller's future
  /// is dropped while awaiting the send, `EagerInsertGuard` applies the
  /// precomputed cost to policy on the caller thread — the async processor
  /// never sees the Item::Update, so without this reconciliation policy
  /// would keep stale cost accounting while the store serves the new
  /// (potentially much larger) value, silently bypassing `max_cost`.
  pub(crate) ignore_internal_cost: bool,

  pub(crate) _marker: PhantomData<fn(K)>,
}

impl<K: Hash + Eq, V: Send + Sync + 'static> AsyncCache<K, V> {
  /// Returns a Cache instance with default configurations.
  ///
  /// `RT` is the async runtime to use. For example:
  ///
  /// ```no_run
  /// use stretto::AsyncCache;
  /// use agnostic_lite::tokio::TokioRuntime;
  ///
  /// AsyncCache::<u64, u64>::new::<TokioRuntime>(100, 10).unwrap();
  /// ```
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn new<RT: RuntimeLite>(num_counters: usize, max_cost: i64) -> Result<Self, CacheError>
  where
    <RT as RuntimeLite>::Interval: Send,
  {
    AsyncCacheBuilder::new(num_counters, max_cost).finalize::<RT>()
  }

  /// Returns a Builder.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn builder(
    num_counters: usize,
    max_cost: i64,
  ) -> AsyncCacheBuilder<
    K,
    V,
    DefaultKeyBuilder<K>,
    DefaultCoster<V>,
    DefaultUpdateValidator<V>,
    DefaultCacheCallback<V>,
    RandomState,
  > {
    AsyncCacheBuilder::new(num_counters, max_cost)
  }
}

impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<Key = K>> AsyncCache<K, V, KH> {
  /// Returns a Cache instance with a custom key builder.
  ///
  /// ```no_run
  /// use stretto::{AsyncCache, TransparentKeyBuilder};
  /// use agnostic_lite::tokio::TokioRuntime;
  ///
  /// AsyncCache::<u64, u64, TransparentKeyBuilder<_>>::new_with_key_builder::<TokioRuntime>(
  ///     100, 10, TransparentKeyBuilder::<u64>::default(),
  /// ).unwrap();
  /// ```
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn new_with_key_builder<RT: RuntimeLite>(
    num_counters: usize,
    max_cost: i64,
    index: KH,
  ) -> Result<Self, CacheError>
  where
    <RT as RuntimeLite>::Interval: Send,
  {
    AsyncCacheBuilder::new_with_key_builder(num_counters, max_cost, index).finalize::<RT>()
  }
}

impl<K, V, KH, C, U, CB, S> AsyncCache<K, V, KH, C, U, CB, S>
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>,
  C: Coster<Value = V>,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
  S: BuildHasher + Clone + 'static + Send,
{
  /// clear the Cache.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub async fn clear(&self) -> Result<(), CacheError> {
    // Every send into the channel is gated on acquiring a permit. The
    // processor releases one per recv (including Clear). Acquire is
    // cancellation-safe: if our future is dropped here, no state is
    // mutated.
    if self.0.insert_sem.acquire().await.is_err() {
      // Unreachable while the cache is live: the semaphore is only closed
      // when `Drop` tears the processor down, which cannot happen while
      // the caller holds a reference.
      return Ok(());
    }
    let mut permit = AsyncPermitGuard {
      sem: &self.0.insert_sem,
      held: true,
    };

    let (waiter, rx) = Waiter::new();
    match self.0.insert_buf_tx.try_send(Item::Clear(waiter)) {
      Ok(()) => {
        // Permit transfers to the processor, which releases it on recv.
        permit.transfer();
        // `Err(Canceled)` means the processor dropped the waiter without
        // signaling (processor exit path). Treat it as "done" — the clear
        // semantics the caller cares about are already unrecoverable.
        let _ = rx.await;
        Ok(())
      }
      Err(_) => {
        // Unreachable under the permit invariant while the cache is live
        // (see above). Permit released on drop.
        Err(CacheError::SendError(
          "fail to enqueue clear marker: channel closed".to_string(),
        ))
      }
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
    // Same shape as `clear()` — a `Wait` marker flushes the processor
    // pipeline, gated on the insert semaphore.
    if self.0.insert_sem.acquire().await.is_err() {
      // Unreachable while the cache is live — see `clear()`.
      return Ok(());
    }
    let mut permit = AsyncPermitGuard {
      sem: &self.0.insert_sem,
      held: true,
    };

    let (waiter, rx) = Waiter::new();
    match self.0.insert_buf_tx.try_send(Item::Wait(waiter)) {
      Ok(()) => {
        permit.transfer();
        // `Err(Canceled)` = processor dropped the waiter (exit path); the
        // flush we were waiting for can no longer happen, so just return.
        let _ = rx.await;
        Ok(())
      }
      Err(e) => Err(CacheError::SendError(format!(
        "cache set buf sender: {}",
        e
      ))),
    }
  }

  /// Stop all background workers owned by this cache and wait for them to
  /// exit.
  ///
  /// Closes the cache to new inserts/updates (post-close `insert*` calls
  /// return `false`), enqueues an in-band `Item::Close` sentinel so every
  /// item queued *before* the sentinel is drained, awaits the cache
  /// processor's exit, then shuts down the policy's background LFU worker.
  ///
  /// Idempotent and concurrent-safe: the first caller to observe `closed`
  /// transition from `false` → `true` drives shutdown; every other caller
  /// simply awaits the same exit signal. Late callers (after the processor
  /// has already exited) return immediately.
  ///
  /// # Scope of the barrier
  ///
  /// On successful return:
  ///
  /// - The main processor loop has exited (no further insert/cleanup work).
  /// - Pending buffered items are drained. `Item::New`/`Update` eager
  ///   writes that never reached policy admission are version-gated-removed
  ///   to avoid ghost uncharged rows; `Wait`/`Clear` markers are signalled.
  /// - The policy's background LFU worker is stopped; `policy.push` calls
  ///   from post-close `get()` paths become clean no-ops.
  ///
  /// `close()` does **not** clear cached data. Entries admitted before the
  /// sentinel survive: post-close `get`/`len` still observe them, because
  /// the close contract is "stop background work" — not "wipe state."
  /// Callers who want empty state should call [`Self::clear`] separately
  /// (before or after close). Keeping the two operations distinct avoids a
  /// class of races between racing writers that slipped through the close
  /// entry gate and a would-be post-clear post-condition; see the
  /// [`AsyncCloseState`] module docs for the protocol.
  ///
  /// This does **not** join the spawned runtime task — `agnostic-lite` does
  /// not expose that handle — so the futures may still be in the process
  /// of being reaped by their executor. Background work is quiesced.
  ///
  /// # Cancellation
  ///
  /// Cancellation-safe against dropping any `close()` future at any
  /// `.await` point. The entry-race winner holds a [`CloseRoleGuard`];
  /// if its future is dropped before the `Item::Close` sentinel is in
  /// the channel, the guard resets `closed` and wakes every parked
  /// loser, so one of them (or a later caller) can take over the
  /// handshake. Once the sentinel is committed, processor exit is
  /// inevitable — every caller simply awaits the processor barrier.
  pub async fn close(&self) -> Result<(), CacheError> {
    loop {
      match self.0.close_state.begin_close() {
        CloseRole::Winner(mut guard) => {
          // Acquire a permit before enqueuing the sentinel, matching the
          // backpressure contract every other sender observes. Permit
          // failure means the semaphore is closed — unreachable while the
          // cache is live. If we are cancelled *during* this await, the
          // guard's Drop resets `closed` and wakes losers so they retry.
          if self.0.insert_sem.acquire().await.is_err() {
            // Channel torn down underneath us; treat as already-closed.
            // Commit so losers see the terminal state; wait on the wg in
            // case the processor guard hasn't fired yet.
            guard.commit();
            self.0.close_state.wait_complete().await;
            return Ok(());
          }
          let mut permit = AsyncPermitGuard {
            sem: &self.0.insert_sem,
            held: true,
          };

          match self.0.insert_buf_tx.try_send(Item::Close) {
            Ok(()) => {
              // Sentinel is queued. The processor will drain items ahead
              // of it, process Close, and exit — `CloseSignal` drops and
              // releases the barrier. Commit wakes any parked losers so
              // they fall through to the barrier wait too.
              permit.transfer();
              guard.commit();
              self.0.close_state.wait_complete().await;
              return Ok(());
            }
            Err(_) => {
              // Channel closed (e.g. processor already torn down). Treat
              // as already-closed; permit is released on drop.
              guard.commit();
              self.0.close_state.wait_complete().await;
              return Ok(());
            }
          }
        }
        CloseRole::Loser => match self.0.close_state.wait_for_outcome().await {
          LoserOutcome::Committed => {
            self.0.close_state.wait_complete().await;
            return Ok(());
          }
          LoserOutcome::Retry => {
            // Winner rolled back. Race to become the winner ourselves.
            continue;
          }
        },
      }
    }
  }

  /// remove entry from Cache by key.
  pub async fn remove(&self, k: &K) {
    self.try_remove(k).await.unwrap()
  }

  /// try to remove an entry from the Cache by key
  pub async fn try_remove(&self, k: &K) -> Result<(), CacheError> {
    // Acquire the insert permit BEFORE the eager `store.try_remove`. The
    // only `.await` in this function is this acquire; once it completes
    // there is no further cancellation point between the store mutation
    // and the Delete enqueue. If the caller cancels/times out the future
    // while it is parked on `acquire`, the store has not been touched yet
    // and policy accounting stays consistent.
    //
    // Pre-fix: the eager remove ran first, then the acquire was awaited.
    // A cancellation at the acquire stranded the policy entry for a key
    // whose store row was already gone — phantom cost charged until a
    // later clear or same-key admission repaired it.
    if self.0.insert_sem.acquire().await.is_err() {
      // Unreachable while the cache is live — semaphore only closes when
      // `Drop` runs. Nothing has been mutated; bail.
      return Ok(());
    }
    let mut permit = AsyncPermitGuard {
      sem: &self.0.insert_sem,
      held: true,
    };

    let (index, conflict) = self.0.key_to_hash.build_key(k);
    // Capture the current clear generation before the eager remove. Paired
    // with the Release-ordered bump in the Clear handler, this Acquire load
    // lets the processor recognize a Delete queued before a clear as stale.
    // The store gates the remove on `row.generation <= captured_gen` so a
    // pre-clear caller that resumes after `clear()` and a racing post-clear
    // reinsert cannot destroy the fresh row.
    let captured_gen = self.0.clear_generation.load(Ordering::Acquire);
    // delete immediately (no await between here and try_send below, so
    // cancellation cannot split the store mutation from its Delete).
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
    if let Some(prev) = prev {
      let prev_version = prev.version;
      self.0.callback.on_exit(Some(prev.value.into_inner()));

      // The version we just removed is stamped on the Item so a concurrent
      // reinsert at the same (key, conflict) under a newer version survives
      // the follow-up store cleanup. `try_send` is safe under the permit
      // invariant: acquired permit ⇒ reserved channel slot.
      if self
        .0
        .insert_buf_tx
        .try_send(Item::delete(index, conflict, captured_gen, prev_version))
        .is_ok()
      {
        permit.transfer();
      }
      // On try_send failure the permit is released on drop; the processor
      // is gone so there is no one to reconcile anyway.
    }
    // If nothing was removed the permit is released on drop.

    Ok(())
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
    // Fast-path closed gate: avoid a permit acquire + store write for
    // inserts that arrive after `close()` has been initiated. The Acquire
    // load inside `is_closing` pairs with the AcqRel swap in `close()`.
    if self.0.close_state.is_closing() {
      return Ok(false);
    }

    // Acquire a permit BEFORE the eager store write. The permit pool is
    // sized to `insert_buffer_size`, so at most that many pre-admission
    // eager writes can exist at once. Without this, an awaited send would
    // let multiple cancelled or parked callers each leave a live uncharged
    // row in the store, letting `store.len()` drift past `max_cost` under
    // contention.
    //
    // `AsyncSemaphore::acquire` is cancellation-safe: if our future is
    // dropped before the permit is granted, no state is mutated.
    if self.0.insert_sem.acquire().await.is_err() {
      // Unreachable while the cache is live — semaphore is only closed
      // when `Drop` runs. Nothing mutated yet; bail.
      return Ok(false);
    }
    let mut permit = AsyncPermitGuard {
      sem: &self.0.insert_sem,
      held: true,
    };

    // Re-check `closed` AFTER acquiring the permit. `close()` also goes
    // through the semaphore and sets `closed` (via AcqRel swap) before
    // it tries to send `Item::Close`, so by the time we hold a permit,
    // either: (a) we won the race and `close()` hasn't set the flag yet,
    // in which case we proceed normally; or (b) we lost the race and
    // `close()` has set the flag, in which case we abort. The permit is
    // released on guard drop. Without this check, a caller that passed
    // the fast-path gate could still eager-write the store after close
    // initiated, producing a ghost row that the processor's close-drain
    // would have to reap — the drain is a safety net, but avoiding the
    // ghost write keeps post-close semantics clean.
    if self.0.close_state.is_closing() {
      return Ok(false);
    }

    // From this point on there are no `.await` points until we either
    // `try_send` the item (transfer permit) or return with the guard
    // dropped (release permit). That removes the cancellation window
    // between the eager store write and the channel send entirely — the
    // permit-invariant guarantees `try_send` cannot return `Full`, so we
    // never need to await the send itself.

    // `try_update` no longer fires `CacheCallback::on_exit` for the prior
    // value itself — the firing happens below, AFTER the insert permit is
    // released (failure paths) or transferred to the processor via a
    // successful `try_send` (happy path). This keeps user callbacks out
    // of the permit-held window, so a callback re-entering the cache via
    // `insert`, `clear`, or `wait` cannot deadlock on a permit the outer
    // call still owns (sync parity; see
    // `test_sync_on_exit_reenters_cache_does_not_deadlock`).
    if let Some((index, item, prev_val)) = self.try_update(key, val, cost, ttl, only_update)? {
      let is_update = item.is_update();
      // Extract per-item fields needed by the two guard modes:
      //   - New: `cost` is the raw callback cost used on rollback via `on_reject`.
      //   - Update: `reconcile_cost` is the fully-adjusted policy cost applied
      //     on drop if the `.send(...).await` below is cancelled.
      // Both arms also capture `generation` and `version` so the Drop handler
      // can gate its reconciliation on "clear hasn't run since our eager
      // write" and "our version is still the live one at this (key, conflict)".
      let (item_conflict, item_cost, item_reconcile_cost, item_exp, item_version, item_generation) =
        match &item {
          Item::New {
            conflict,
            cost,
            expiration,
            version,
            generation,
            ..
          } => (*conflict, *cost, 0i64, *expiration, *version, *generation),
          Item::Update {
            conflict,
            cost,
            external_cost,
            expiration,
            version,
            generation,
            ..
          } => (
            *conflict,
            *cost,
            self.calculate_internal_cost(*cost) + *external_cost,
            *expiration,
            *version,
            *generation,
          ),
          _ => (0, 0, 0, Time::now(), 0, 0),
        };

      // Two guard modes:
      //   - New rollback (`armed`): on cancel, version-gated remove of the
      //     eager store write + on_reject callback. Restores pre-insert state.
      //   - Update reconcile (`update_armed`): on cancel, apply the precomputed
      //     `reconcile_cost` to policy directly on the caller thread so
      //     policy accounting stays in sync with the committed store value.
      //
      // Why updates need a reconcile path (not just "no rollback"): the
      // `.send(...).await` below is the function's only cancellation point
      // between the eager `store.try_update` and the policy accounting. If
      // the caller's future is dropped while that send is pending, the new
      // value is live in the store but the `Item::Update` never reaches the
      // processor — policy keeps the old cost, so a cost-1 → cost-1_000_000
      // update leaves policy accounting cost 1 for a 1M-cost row. Repeated
      // under cancellation, this silently bypasses `max_cost`. The Drop
      // handler closes that hole by invoking `policy.update` with the
      // precomputed total cost when the send does not complete.
      //
      // Why updates still must not be *rolled back*: `store.try_update`
      // already destroyed the old value via `on_exit` and committed the new
      // one under a fresh version. Removing the new row would turn an
      // otherwise successful update into observable data loss on a key that
      // was live moments ago. The reconcile path preserves data and repairs
      // accounting; it is strictly additive.
      let mut guard = EagerInsertGuard {
        store: &self.0.store,
        callback: &self.0.callback,
        metrics: &self.0.metrics,
        policy: &self.0.policy,
        clear_generation: &self.0.clear_generation,
        index,
        conflict: item_conflict,
        cost: item_cost,
        reconcile_cost: item_reconcile_cost,
        exp: item_exp,
        version: item_version,
        captured_gen: item_generation,
        armed: !is_update && item_version != 0,
        update_armed: is_update && item_version != 0,
      };

      // `try_send` is safe: the permit invariant (permit count == channel
      // capacity) guarantees that every held permit corresponds to a
      // reserved channel slot, so `Full` is unreachable under correct
      // operation. The only possible failure is `Closed` during shutdown
      // teardown. Permit ownership transfers to the processor on Ok; the
      // processor's recv loop releases it.
      match self.0.insert_buf_tx.try_send(item) {
        Ok(()) => {
          guard.armed = false;
          guard.update_armed = false;
          permit.transfer();
          // Permit ownership just transferred to the processor. Fire
          // `on_exit` for the prior value (Update only) now, OUTSIDE the
          // permit-held window, so user callbacks that re-enter the
          // cache cannot self-deadlock on a second permit acquire.
          if let Some(v) = prev_val {
            self.0.callback.on_exit(Some(v));
          }
          Ok(true)
        }
        Err(TrySendError::Full(_)) => {
          // Unreachable under the permit invariant. Be defensive: drop the
          // guard (runs New rollback if armed), and for updates reap the
          // eager write directly so the store stays consistent if the
          // invariant were ever violated.
          self.0.metrics.add(MetricType::DropSets, index, 1);
          if is_update {
            guard.update_armed = false;
            if let Ok(Some(sitem)) =
              self
                .0
                .store
                .try_remove_if_version(&index, item_conflict, item_version)
            {
              self.0.callback.on_reject(CrateItem {
                val: Some(sitem.value.into_inner()),
                index,
                conflict: item_conflict,
                cost: item_cost,
                exp: item_exp,
              });
            }
          }
          // permit released on guard drop
          drop(guard);
          drop(permit);
          // Fire `on_exit` for the prior value AFTER the permit release.
          if let Some(v) = prev_val {
            self.0.callback.on_exit(Some(v));
          }
          Ok(false)
        }
        Err(TrySendError::Closed(_)) => {
          // Unreachable while the cache is live — the processor holds the
          // sole receiver and only exits after `Drop` disconnects
          // `stop_tx`, which cannot happen while this caller holds a
          // reference. For `New`, let the rollback guard fire on drop; for
          // `Update`, the eager write is live data the caller committed
          // and we disarm the reconcile guard because policy is about to
          // be torn down regardless.
          guard.update_armed = false;
          drop(guard);
          drop(permit);
          if let Some(v) = prev_val {
            self.0.callback.on_exit(Some(v));
          }
          Ok(is_update)
        }
      }
    } else {
      // try_update returned None — no eager write happened, permit is
      // released on drop of `permit`.
      Ok(false)
    }
  }

  /// Mirrors `CacheProcessor::calculate_internal_cost`. Used by
  /// `try_insert_in` to precompute an update's total policy cost before the
  /// cancellable `.send(...).await` so the Drop-based reconcile path can
  /// apply it without re-deriving it from the processor's config.
  #[cfg_attr(not(tarpaulin), inline(always))]
  fn calculate_internal_cost(&self, cost: i64) -> i64 {
    if !self.0.ignore_internal_cost {
      cost + (self.0.store.item_size() as i64)
    } else {
      cost
    }
  }
}

impl<K, V, KH, C, U, CB, S> Drop for AsyncCacheInner<K, V, KH, C, U, CB, S>
where
  K: Hash + Eq,
  V: Send + Sync + 'static,
  KH: KeyBuilder<Key = K>,
{
  /// Fire-and-forget shutdown. Runs when the LAST `AsyncCache` handle is
  /// dropped (Arc refcount reaches zero). Dropping `stop_tx` disconnects
  /// the processor task's `stop_rx`, which wakes its `select!` stop arm
  /// and runs the drain path.
  ///
  /// Async `Drop` cannot await, so this does not block on the processor
  /// finishing its drain. Callers that need a barrier before shutdown
  /// should call [`AsyncCache::close`] first.
  fn drop(&mut self) {
    let _ = self.stop_tx.take();
  }
}

/// RAII guard for a permit acquired from `AsyncSemaphore`. On drop it
/// releases the permit unless `held` has been cleared — callers clear it by
/// either calling `transfer()` (permit handed off to the processor via a
/// successful send) or `release_now()` (explicit early release).
///
/// The sync cache uses the equivalent `SyncPermitGuard` for the same reason —
/// a panic or `Err` from user-controlled code (`Coster`, `UpdateValidator`)
/// between acquire and send must not leak the permit. Async additionally
/// relies on the guard for cancellation safety: any `.await` between the
/// acquire and the send is a cancellation point, and a dropped future there
/// would otherwise leak a permit.
struct AsyncPermitGuard<'a> {
  sem: &'a Arc<AsyncSemaphore>,
  held: bool,
}

impl AsyncPermitGuard<'_> {
  #[inline]
  fn transfer(&mut self) {
    self.held = false;
  }
}

impl Drop for AsyncPermitGuard<'_> {
  fn drop(&mut self) {
    if self.held {
      self.sem.release();
    }
  }
}

/// All shutdown synchronization state for an [`AsyncCache`], grouped in one
/// place so the close protocol is easier to reason about.
///
/// The protocol has three phases:
///
/// 1. **Entry race.** A swap on `closed` picks exactly one winner. Losers
///    park on `event` until the winner either commits or rolls back.
/// 2. **Commit point.** The winner acquires an insert permit and
///    `try_send`s `Item::Close`. If that send succeeds, the winner flips
///    `sentinel_committed` and wakes all losers. From here on processor
///    exit is inevitable: `wg` is the only thing anyone waits on.
/// 3. **Rollback.** If the winner's future is dropped (or hits an
///    unrecoverable error) before the sentinel is queued, its RAII guard
///    resets `closed` to `false` and wakes all losers so one of them can
///    retry becoming the winner. Without this, cancellation of the winner
///    would permanently wedge every future `close()` caller.
pub(crate) struct AsyncCloseState {
  /// Entry-race gate. `swap(true, AcqRel)` = `false` identifies the
  /// winner; every other caller becomes a loser. Reset to `false` by the
  /// winner's RAII guard on rollback.
  closed: AtomicBool,
  /// Commit flag. Flipped once the `Item::Close` sentinel is safely in the
  /// channel. Loser wakeups check this to decide "wait on `wg`" vs.
  /// "retry the race". Monotonic: once `true`, never goes back to `false`.
  sentinel_committed: AtomicBool,
  /// Wakeup channel for losers. The winner notifies on this event at two
  /// moments: (a) after flipping `sentinel_committed` on commit, and
  /// (b) from the rollback branch of its RAII guard after resetting
  /// `closed`. Losers register a listener *before* re-checking the flags
  /// to avoid missed wakeups.
  event: Event,
  /// Shutdown barrier. Counter starts at `1` and is `done()`'d exactly
  /// once by the processor's [`CloseSignal`] drop guard (on any exit path
  /// — normal, early return, or panic-unwind). All `close()` callers
  /// terminate by awaiting `wg.wait()`.
  wg: AsyncWaitGroup,
}

impl AsyncCloseState {
  pub(crate) fn new() -> Self {
    Self {
      closed: AtomicBool::new(false),
      sentinel_committed: AtomicBool::new(false),
      event: Event::new(),
      wg: AsyncWaitGroup::from(1),
    }
  }

  /// Fast-path shutdown check for insert/remove paths. Returns `true` as
  /// soon as any caller has entered the close race, so admission is
  /// rejected even if the winner is still mid-handshake.
  #[inline]
  pub(crate) fn is_closing(&self) -> bool {
    self.closed.load(Ordering::Acquire)
  }

  /// Barrier handle for the processor task. The processor owns this clone
  /// and wraps it in a [`CloseSignal`] drop guard so that `wg.done()`
  /// fires on any exit path.
  pub(crate) fn processor_barrier(&self) -> AsyncWaitGroup {
    self.wg.clone()
  }

  /// Enter the close race. The caller that flips `closed` from `false` to
  /// `true` becomes the winner and owns the [`CloseRoleGuard`]. Every
  /// other caller is a loser and must call [`Self::wait_for_outcome`].
  fn begin_close(&self) -> CloseRole<'_> {
    if self.closed.swap(true, Ordering::AcqRel) {
      CloseRole::Loser
    } else {
      CloseRole::Winner(CloseRoleGuard {
        state: self,
        committed: false,
      })
    }
  }

  /// Await the winner's decision. Returns [`LoserOutcome::Committed`] once
  /// the sentinel is in the channel (caller should `wait_complete()`), or
  /// [`LoserOutcome::Retry`] if the winner rolled back (caller should loop
  /// and try to become the winner themselves).
  ///
  /// Cancellation-safe: listeners register before every flag re-check, so
  /// a dropped future simply unregisters.
  async fn wait_for_outcome(&self) -> LoserOutcome {
    loop {
      if self.sentinel_committed.load(Ordering::Acquire) {
        return LoserOutcome::Committed;
      }
      if !self.closed.load(Ordering::Acquire) {
        return LoserOutcome::Retry;
      }
      // Arm the listener BEFORE re-checking so a notify from `commit()` or
      // the rollback branch of `CloseRoleGuard::drop` cannot slip past us.
      let listener = self.event.listen();
      if self.sentinel_committed.load(Ordering::Acquire) {
        return LoserOutcome::Committed;
      }
      if !self.closed.load(Ordering::Acquire) {
        return LoserOutcome::Retry;
      }
      listener.await;
    }
  }

  /// Wait for the processor to finish draining. Safe to call on any
  /// thread/task, any number of times.
  async fn wait_complete(&self) {
    self.wg.wait().await;
  }
}

/// Result of the entry-race swap inside [`AsyncCloseState::begin_close`].
enum CloseRole<'a> {
  /// First caller to flip `closed`. Owns the commit/rollback guard.
  Winner(CloseRoleGuard<'a>),
  /// Everyone else. Parks on the event loop until the winner decides.
  Loser,
}

/// Result of [`AsyncCloseState::wait_for_outcome`].
enum LoserOutcome {
  /// Winner queued the `Item::Close` sentinel. Caller should wait on the
  /// processor barrier to see the drain finish.
  Committed,
  /// Winner rolled back (its future was dropped before the sentinel was
  /// queued). Caller should race to become the winner themselves.
  Retry,
}

/// RAII guard returned by [`AsyncCloseState::begin_close`] for the winner.
///
/// While `committed == false`, `Drop` resets `closed` to `false` and wakes
/// every parked loser, so the close race can be retried. Calling
/// [`Self::commit`] transitions the guard into the committed state — at
/// that point the sentinel is in the channel, processor exit is
/// inevitable, and `Drop` becomes a no-op beyond what `commit` already
/// did.
struct CloseRoleGuard<'a> {
  state: &'a AsyncCloseState,
  committed: bool,
}

impl CloseRoleGuard<'_> {
  /// Mark shutdown as committed. Must be called after the `Item::Close`
  /// sentinel has been successfully queued (or the channel is observed
  /// closed, in which case processor exit has already happened). Flipping
  /// `sentinel_committed` with Release ordering pairs with the Acquire
  /// loads in [`AsyncCloseState::wait_for_outcome`]; the follow-up
  /// `event.notify(usize::MAX)` wakes every loser currently listening, so
  /// they re-check and observe the commit.
  fn commit(&mut self) {
    self.state.sentinel_committed.store(true, Ordering::Release);
    self.state.event.notify(usize::MAX);
    self.committed = true;
  }
}

impl Drop for CloseRoleGuard<'_> {
  fn drop(&mut self) {
    if self.committed {
      return;
    }
    // Rollback path: the winner future was dropped (or bailed) before
    // queueing the sentinel. Reset `closed` so a later caller can become
    // the winner, then notify every parked loser so they re-check and
    // see `closed == false`, triggering a retry.
    self.state.closed.store(false, Ordering::Release);
    self.state.event.notify(usize::MAX);
  }
}

/// RAII guard held inside the processor future. On Drop (normal return,
/// early return, or panic-unwind) it calls `wg.done()`, unblocking every
/// `close().await` caller. Ensures a processor panic cannot leave waiters
/// stuck forever.
pub(crate) struct CloseSignal(AsyncWaitGroup);

impl Drop for CloseSignal {
  fn drop(&mut self) {
    self.0.done();
  }
}

/// RAII guard held inside the processor future. On Drop it closes the
/// insert semaphore so blocked `acquire().await` callers wake with
/// `SemaphoreClosed`. Covers the panic-unwind path where the per-item
/// `insert_sem.release()` never runs and buffered items' permits are
/// stranded.
struct SemCloser(Arc<AsyncSemaphore>);

impl Drop for SemCloser {
  fn drop(&mut self) {
    self.0.close();
  }
}

/// Cancellation / send-failure guard for the eager store write performed by
/// `try_insert_in`. Has two mutually exclusive active modes:
///
/// - **New rollback** (`armed`): for `Item::New`, the eager store write has
///   no prior state. On cancel, version-gated `try_remove_if_version` reverts
///   to the pre-insert state and `on_reject` is invoked. A concurrent writer
///   that landed a newer version at the same (key, conflict) between our
///   write and rollback is preserved.
///
/// - **Update reconcile** (`update_armed`): for `Item::Update`, the eager
///   store write already committed the new value and destroyed the previous
///   one via `on_exit`. Rolling back would erase live data. Instead, on
///   cancel, apply the precomputed `reconcile_cost` to policy on the caller
///   thread — generation-gated (skip if `clear()` has since run) and
///   version-gated (skip if a newer writer owns the row). This closes the
///   cancellation-between-store-write-and-policy-send hole that would
///   otherwise let a cost-1 → cost-N update leave policy accounting stuck at
///   cost-1 while the store serves the cost-N value, silently bypassing
///   `max_cost`.
struct EagerInsertGuard<'a, V, U, CB, S>
where
  V: Send + Sync + 'static,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
  S: BuildHasher + Clone + 'static,
{
  store: &'a Arc<ShardedMap<V, U, S, S>>,
  callback: &'a Arc<CB>,
  metrics: &'a Arc<Metrics>,
  policy: &'a Arc<AsyncLFUPolicy<S>>,
  clear_generation: &'a Arc<AtomicU64>,
  index: u64,
  conflict: u64,
  /// Raw callback cost used by the New rollback path's `on_reject`.
  cost: i64,
  /// Fully-adjusted policy cost (`calculate_internal_cost(cost) +
  /// external_cost`) used by the Update reconcile path. Zero for New.
  reconcile_cost: i64,
  exp: Time,
  version: u64,
  /// `clear_generation` captured at the eager store write. The reconcile
  /// path compares this against the current generation on drop: a mismatch
  /// means `clear()` intervened and the eager write is a post-clear ghost,
  /// so policy must not be touched.
  captured_gen: u64,
  armed: bool,
  update_armed: bool,
}

impl<V, U, CB, S> Drop for EagerInsertGuard<'_, V, U, CB, S>
where
  V: Send + Sync + 'static,
  U: UpdateValidator<Value = V>,
  CB: CacheCallback<Value = V>,
  S: BuildHasher + Clone + 'static,
{
  fn drop(&mut self) {
    if self.armed {
      // New rollback: version-gated remove + on_reject.
      self.metrics.add(MetricType::DropSets, self.index, 1);
      if let Ok(Some(sitem)) =
        self
          .store
          .try_remove_if_version(&self.index, self.conflict, self.version)
      {
        self.callback.on_reject(CrateItem {
          val: Some(sitem.value.into_inner()),
          index: self.index,
          conflict: self.conflict,
          cost: self.cost,
          exp: self.exp,
        });
      }
      return;
    }

    if self.update_armed {
      // Update reconcile: bring policy cost accounting back in sync with the
      // live store value that our cancelled `try_update` committed. Mirrors
      // the processor's `Item::Update` handler — including the `policy.add`
      // fallthrough for the case where policy doesn't yet track the key
      // (our prior `Item::New` was skipped by its `contains_version` gate
      // because we bumped the version before it was admitted). Without the
      // fallthrough a cancelled update in that race leaves a live store
      // row with no policy entry, silently bypassing `max_cost`.
      //
      // Acquire pairs with the Release-ordered fetch_add in the processor's
      // Clear handler.
      let current_gen = self.clear_generation.load(Ordering::Acquire);
      if self.captured_gen != current_gen {
        // `clear()` ran after our eager write. The Clear barrier wiped the
        // store; if anything is at our (key, conflict, version) now it is
        // our own pre-clear eager write that slipped past the wipe — a
        // ghost. Reap it version-gated so a post-clear writer is preserved.
        let _ = self
          .store
          .try_remove_if_version(&self.index, self.conflict, self.version);
        return;
      }
      // Version gate: if a newer writer has since overwritten our row, they
      // own the policy accounting for it via their own Item::Update/New.
      // Touching policy here would double-count or corrupt their cost.
      if !self
        .store
        .contains_version(&self.index, self.conflict, self.version)
      {
        return;
      }
      if self.policy.update(&self.index, self.reconcile_cost) {
        return;
      }
      let outcome = self.policy.add(self.index, self.reconcile_cost);
      let (rejected, victims) = match outcome {
        AddOutcome::Admitted { victims } => (false, victims),
        AddOutcome::UpdatedExisting => (false, Vec::new()),
        AddOutcome::RejectedByCost => (true, Vec::new()),
        AddOutcome::RejectedBySampling { victims } => (true, victims),
      };
      if rejected {
        if let Ok(Some(sitem)) =
          self
            .store
            .try_remove_if_version(&self.index, self.conflict, self.version)
        {
          self.callback.on_reject(CrateItem {
            val: Some(sitem.value.into_inner()),
            index: self.index,
            conflict: self.conflict,
            cost: self.reconcile_cost,
            exp: self.exp,
          });
          // Ghost-entry cleanup: if our rollback left the store empty at
          // this index, any residual policy entry is stranded.
          if !self.store.contains_key(&self.index, 0) {
            self.policy.remove(&self.index);
          }
        }
      }
      for victim in victims {
        if let Ok(Some(sitem)) = self.store.try_remove(&victim.key, 0) {
          self.callback.on_evict(CrateItem {
            index: victim.key,
            val: Some(sitem.value.into_inner()),
            cost: victim.cost,
            conflict: sitem.conflict,
            exp: sitem.expiration,
          });
        }
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
    cleanup_duration: Duration,
    store: Arc<ShardedMap<V, U, S, S>>,
    policy: Arc<AsyncLFUPolicy<S>>,
    insert_buf_rx: Receiver<Item<V>>,
    stop_rx: Receiver<()>,
    metrics: Arc<Metrics>,
    callback: Arc<CB>,
    clear_generation: Arc<AtomicU64>,
    insert_sem: Arc<AsyncSemaphore>,
    close_wg: AsyncWaitGroup,
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
      cleanup_duration,
      clear_generation,
      insert_sem,
      close_wg,
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn spawn<RT: RuntimeLite>(mut self)
  where
    <RT as RuntimeLite>::Interval: Send,
  {
    RT::spawn_detach(async move {
      // RAII guard: fires `close_wg.done()` on ANY exit path (normal return,
      // early return below, or panic-unwind). Every `close().await` caller
      // unblocks the moment this drops, so a processor panic cannot hang
      // waiters.
      let _close_signal = CloseSignal(self.close_wg.clone());
      // RAII guard: fires `insert_sem.close()` on ANY exit path. A panic
      // through a user callback skips the per-item `insert_sem.release()`
      // below, and buffered items get dropped with the channel when the
      // future unwinds — their permits are lost too. Without this guard,
      // a live clone's next `insert*`/`clear`/`wait` would await forever
      // on `insert_sem.acquire()`. Closing the semaphore wakes every
      // waiter with `SemaphoreClosed`, and every caller-side
      // `acquire().await.is_err()` branch turns the call into a graceful
      // no-op.
      let _sem_closer = SemCloser(self.insert_sem.clone());
      let mut cleanup_timer = RT::interval(self.cleanup_duration);

      loop {
        select! {
          item = self.insert_buf_rx.recv().fuse() => {
            // Every recv consumes a permit previously held by the sender.
            // Intercept the in-band `Item::Close` sentinel: release its
            // permit, run the shutdown barrier, and return so `_close_signal`
            // drops → `close_wg.done()` fires.
            match item {
              Ok(Item::Close) => {
                self.insert_sem.release();
                let _ = self.handle_close_event().await;
                return;
              }
              other => {
                let had_item = other.is_ok();
                if let Err(e) = self.handle_insert_event(other) {
                  tracing::error!("fail to handle insert event, error: {}", e);
                }
                if had_item {
                  self.insert_sem.release();
                }
              }
            }
          }
          _ = cleanup_timer.next().fuse() => {
            if let Err(e) = self.handle_cleanup_event() {
              tracing::error!("fail to handle cleanup event, error: {}", e);
            }
          },
          _ = self.stop_rx.recv().fuse() => {
            // Stop arm fires when `stop_rx` is disconnected. `AsyncCache::drop`
            // triggers that by dropping `stop_tx` — we never `send()` on
            // this channel. Either way, drain and exit.
            _ = self.handle_close_event().await;
            return;
          },
        }
      }
    });
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) async fn handle_close_event(&mut self) -> Result<(), CacheError> {
    // Close the channels so subsequent sends fail rather than queue up
    // behind the already-exited processor.
    self.insert_buf_rx.close();
    self.stop_rx.close();
    // Drain buffered items so held permits are released and barrier
    // markers are signalled. Under the Option B close contract, this
    // drain MUST NOT mutate store state. A drained `Item::New` or
    // `Item::Update` carries the version of an eager store write the
    // caller already committed; `try_remove_if_version` would succeed
    // against that row and destroy live data that the caller's
    // `insert(...)→true` return value promised is readable. The post-
    // permit close-check in `try_insert_in` is not a barrier — a
    // racer can commit `store.try_update` just before `close()` flips
    // `closed=true`, then lose the MPSC ordering race so its
    // `Item::Update` lands in the drain. Leaving the row alone is the
    // only correct response under "close preserves data."
    //
    // The tradeoff: for an `Item::New` that raced the sentinel, the
    // store row is readable but policy never admitted it. This is
    // harmless — policy is being closed below, so no further admissions
    // will happen and cost accounting drift cannot cascade. `Drop`
    // wipes the store wholesale later.
    //
    // `Wait`/`Clear` markers must still `wg.done()` so any caller
    // parked on them unblocks.
    while let Ok(item) = self.insert_buf_rx.try_recv() {
      match item {
        Item::Wait(wg) | Item::Clear(wg) => {
          wg.done();
        }
        Item::New { .. } | Item::Update { .. } | Item::Delete { .. } | Item::Close => {}
      }
      self.insert_sem.release();
    }
    // Stop the policy's own background worker so it doesn't outlive the
    // cache processor. `AsyncLFUPolicy::close` flips `is_closed`, so
    // post-close `policy.push` calls from `get()` paths become clean
    // no-ops and the policy task exits on its stop channel.
    let _ = self.policy.close().await;
    Ok(())
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn handle_insert_event(
    &mut self,
    res: Result<Item<V>, RecvError>,
  ) -> Result<(), CacheError> {
    res
      .map_err(|_| CacheError::RecvError("fail to receive msg from insert buffer".to_string()))
      .and_then(|item| self.handle_item(item))
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
impl_async_cache!(AsyncCache, AsyncCacheBuilder, Item);
impl_cache_processor!(CacheProcessor, Item);
