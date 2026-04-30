# Version 0.9.0 (2026/04/27)

## Breaking changes

### Sync `Cache`

- Removed `ChannelFlavor` enum and the three `set_buffer_size`,
  `set_insert_buffer_flavor`, `set_insert_buffer_send_timeout` builder
  methods. The sync cache now uses an internal striped insert buffer
  (`InsertStripeRing`) with two builder knobs (also apply to `AsyncCache`):
  - `set_insert_stripe_high_water(items: usize)` — items per stripe
    before threshold-flush. Default 64, min 1.
  - `set_drain_interval(interval: Duration)` — processor drain-tick
    cadence. Default 500ms.
- **Removed `pub fn Cache::close()`**. Shutdown is now driven entirely by
  `Drop` on the last `Cache` handle: dropping `stop_tx` disconnects the
  processor's `stop_rx`, which runs the drain path and joins the
  processor thread. Callers that previously called `cache.close()?` should
  drop the cache handle instead. `Drop` is panic-safe — if the processor
  thread panicked from a user callback, `JoinHandle::join` returns `Err`
  rather than blocking forever.

### Async `AsyncCache`

- **Removed `pub async fn AsyncCache::close()`**. Use `wait().await` to
  flush in-flight inserts before dropping. Final shutdown is via `Drop`
  on the last `AsyncCache` handle, which disconnects the processor's
  `stop_rx`; the processor's stop arm then drains stripe-buffered items
  and processes any remaining queued batches inline before the
  std::thread exits. `Drop` joins that thread (with a self-thread guard
  for `CacheCallback`s that drop the last handle from the processor
  itself) so when `Drop` returns the cache is fully shut down.

### Public guard types

- `ValueRef<V>` and `ValueRefMut<V>` now wrap
  `parking_lot::MappedRwLock{Read,Write}Guard` instead of standard
  library guards. Auto-trait inheritance follows parking_lot's
  `GuardMarker`: by default the guards are `!Send + !Sync`. Turn on
  the new `send_guard` Cargo feature (forwards to
  `parking_lot/send_guard`) when you need to move a guard across
  threads or hold it across an `.await` point.

### Migration

See `docs/superpowers/specs/2026-04-26-striped-insert-buffer-design.md`
"Migration notes for downstream users".

## Async cache

`AsyncCache` adopts the same striped insert buffer architecture as `Cache`. Aside from the removed `close()` (above), public API is unchanged — `insert`, `get`, `wait`, `clear`, `remove`, and builder methods all keep their prior signatures.

- **Striped insert buffer**: `AsyncCache` now uses a 64-stripe `parking_lot::Mutex<Vec<Item<V>>>` insert buffer with threshold-flushed batches, replacing the per-item `async_channel::bounded(N)` from prior releases.
- **Drop-on-overflow rollback**: when the bounded ring is saturated (or closed during shutdown), the entire batch is rolled back inline and the producer returns `false` immediately — mirroring sync's `PushOutcome::Dropped` contract. Each `try_insert_in` ends with a single `R::yield_now().await` (the runtime's native cooperative yield, dispatched via `agnostic_lite::RuntimeLite`) so a tight `c.insert(..).await` loop releases its worker between iterations; without this yield the buffered fast path performs no real `.await` and a producer-heavy task graph can starve sibling tasks (e.g. concurrent `clear()`).
- **Barrier preludes**: `wait()` and `clear()` now drain stripe-buffered items to the processor before sending their barrier markers, so the marker can no longer slide past partial stripes.
- **std::thread processor**: the cache processor now runs on a dedicated `std::thread` driven by `crossbeam_channel::select!` (mirroring sync), not an `RT::spawn_detach` task on the user's runtime. The policy worker still runs as an `RT` task on the user runtime; both stop-channels live in `AsyncCacheInner` so `Drop` disconnects both at once.
- **Consistent shutdown**: `Drop` disconnects `stop_tx`, the stop arm drains stripes and queued items, and `Drop` joins the processor thread (skipping the join only when called from the processor thread itself, e.g. a `CacheCallback` dropping the last handle) — so when `Drop` returns the cache is fully shut down.
- **Runtime-agnostic**: no `tokio::*` or `async_std::*` references; all runtime ops via `agnostic_lite::RuntimeLite`. Tested under both `TokioRuntime` and `SmolRuntime`.

## Reliability fixes

- **Clear/insert race**: a `clear_generation: Arc<AtomicU64>` counter is bumped on every `clear()`. Insert and update paths sample the generation when the work enters the store and skip the commit if the generation has moved, so a `clear()` that overlaps an in-flight insert can no longer resurrect the cleared key with stale data. Applies to both `Cache` and `AsyncCache`.
- **Re-entry from user callbacks**: a new `on_processor_thread()` check detects when a `CacheCallback` (`on_exit`, `on_evict`, `on_reject`) drops the last cache handle from inside the processor thread itself. In that case `Drop` skips `JoinHandle::join` to avoid a self-join deadlock; the thread is already on its return path.
- **Panicking-callback hardening**: `on_exit` now fires after the store's delete (not before), and reconciliation of internal state happens before any user callback runs, so a panicking callback cannot leave a half-removed row in the store. If a user callback panics, the processor thread exits with the panic and `Drop`'s join returns `Err` rather than blocking forever — preserving the panic-safe `Drop` contract.
- **TTL bucket leak (issue #55)**: `ExpirationMap::try_cleanup` now sweeps every bucket whose index is at or below the current cleanup bucket, not just the bucket aligned with the current tick. With a cleanup interval larger than one second (the bucket granularity), unaligned buckets used to leak forever. Covered by the new regression test `try_cleanup_sweeps_all_past_buckets`.
- **Zero-TTL skip**: items inserted with `Duration::ZERO` (no expiration) are not placed in the expiration map and cannot be touched by cleanup ticks.
- **Clear wipes TTL buckets**: `ShardedMap::clear` now clears the expiration map alongside the rows, so buckets created before a cache-level `clear()` cannot outlive their rows and later delete fresh post-clear entries when the cleanup ticker catches up.

## Performance

- Sync `Cache::insert` latency reduced ~17.7× on the OLTP cachebench
  trace (cap=2000 from 2.83s → 0.160s) while preserving the hit ratio
  (75.35% measured at cap=2000 in Task 15 acceptance benchmarks).

# Version 0.7 (2022/08/30)
- change function definition to 
  ```rust
  pub fn get<Q>(&self, key: &Q) -> Option<ValueRef<V, S>>
      where
          K: core::borrow::Borrow<Q>,
          Q: core::hash::Hash + Eq,

  pub fn get_mut<Q>(&self, key: &Q) -> Option<ValueRefMut<V, S>>
      where
          K: core::borrow::Borrow<Q>,
          Q: core::hash::Hash + Eq,

  pub fn get_ttl<Q>(&self, key: &Q) -> Option<Duration>
      where
          K: core::borrow::Borrow<Q>,
          Q: core::hash::Hash + Eq,
  ```

# Version 0.6 (2022/08)
- Use associated type for traits

# Version 0.5 (2022/07/07)
- Support runtime agnostic `AsyncCache`
