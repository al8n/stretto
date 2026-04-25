//! Striped per-thread insert buffer (sync and async `Cache`).
//!
//! Producers push `Item<V>` (or any `T` for tests) into one of `STRIPES`
//! independent stripes hashed by current thread id. When a stripe reaches
//! `high_water` items, the full `Vec` is swapped out under the stripe lock
//! and shipped to the processor through a bounded `Sender<Vec<T>>`. The
//! processor's tick arm separately calls `drain_all_stripes_inline` every
//! `drain_interval` to bound worst-case admission latency.
//!
//! Spec: docs/superpowers/specs/2026-04-27-async-stripe-buffer-design.md

#[cfg(any(feature = "sync", feature = "async"))]
use crossbeam_utils::CachePadded;
#[cfg(any(feature = "sync", feature = "async"))]
use parking_lot::Mutex;
#[cfg(any(feature = "sync", feature = "async"))]
use std::{
  cell::Cell,
  hash::{Hash, Hasher},
  mem::replace,
};

#[cfg(any(feature = "sync", feature = "async"))]
use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
#[cfg(any(feature = "sync", feature = "async"))]
use std::time::Duration;

/// Number of independent stripes that buffer inserts before they are flushed
/// to the policy processor. Power-of-two so the stripe index reduces to a
/// mask. Mirrors `RING_STRIPES` in `src/ring.rs`.
pub(crate) const STRIPES: usize = 64;

#[cfg(any(feature = "sync", feature = "async"))]
const STRIPE_MASK: u64 = (STRIPES as u64) - 1;

/// Default per-stripe high-water mark (items). Configurable per-cache via
/// `CacheBuilder::set_insert_stripe_high_water`. Total stripe-resident
/// backlog at default = 64 × 64 = 4096 items.
pub(crate) const DEFAULT_HIGH_WATER: usize = 64;

/// Bounded channel capacity in *batches*. With default `high_water = 64`,
/// total in-flight at saturation = 64 × 64 + 2048 × 64 = ~131 K items.
/// Sized for the drop-on-overflow path (after `OVERFLOW_TIMEOUT`) which
/// no longer blocks producers: the headroom margin protects admission
/// accuracy on bursty workloads. Empirically calibrated against the OLTP
/// cachebench trace at all four target capacities (256/512/1000/2000).
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) const BATCH_CAP: usize = 2048;

/// `send_timeout` floor on overflow before the batch is dropped. Bounds
/// producer tail latency.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) const OVERFLOW_TIMEOUT: Duration = Duration::from_micros(20);

#[cfg(any(feature = "sync", feature = "async"))]
thread_local! {
  /// Cached stripe index for the current thread, computed once from a
  /// hash of the thread id. Stored as `index | TLS_INIT_BIT` so that `0`
  /// means uninitialized; avoids a separate `Cell<bool>`. Same trick as
  /// `STRIPE_HINT` in `src/ring.rs`.
  static STRIPE_HINT: Cell<u64> = const { Cell::new(0) };
}

#[cfg(any(feature = "sync", feature = "async"))]
const TLS_INIT_BIT: u64 = 1 << 63;

#[cfg(any(feature = "sync", feature = "async"))]
#[inline]
fn stripe_index() -> usize {
  STRIPE_HINT.with(|c| {
    let cached = c.get();
    if cached != 0 {
      return (cached & STRIPE_MASK) as usize;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    let idx = hasher.finish() & STRIPE_MASK;
    c.set(idx | TLS_INIT_BIT);
    idx as usize
  })
}

/// Inner stripe-buffered storage shared by sync and async wrappers. Holds
/// `STRIPES` independent `parking_lot::Mutex<Vec<T>>` cells, each padded
/// to a cache line. Sync-locked because lock holds are sub-microsecond
/// even under contention.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) struct StripeStorage<T> {
  stripes: Box<[CachePadded<Mutex<Vec<T>>>]>,
  high_water: usize,
}

#[cfg(any(feature = "sync", feature = "async"))]
impl<T> StripeStorage<T> {
  /// `high_water` is both the swap threshold and the initial Vec capacity of each stripe.
  pub(crate) fn new(high_water: usize) -> Self {
    assert!(high_water >= 1, "high_water must be >= 1");
    let stripes: Box<[CachePadded<Mutex<Vec<T>>>]> = (0..STRIPES)
      .map(|_| CachePadded::new(Mutex::new(Vec::with_capacity(high_water))))
      .collect();
    Self {
      stripes,
      high_water,
    }
  }

  /// Push `item` into the caller's stripe. Returns `None` if the push
  /// stays below threshold (caller has no further work). Returns
  /// `Some(swapped_vec)` if the push reached threshold; the caller must
  /// ship the swapped vec via its channel.
  pub(crate) fn push(&self, item: T) -> Option<Vec<T>> {
    let stripe = &self.stripes[stripe_index()];
    let mut data = stripe.lock();
    data.push(item);
    if data.len() < self.high_water {
      return None;
    }
    let batch = replace(&mut *data, Vec::with_capacity(self.high_water));
    drop(data);
    Some(batch)
  }

  /// Iterate every non-empty stripe; swap a fresh empty Vec into each;
  /// pass the swapped batch to `f`. No channel send; closure is called inline.
  /// Used by tick/stop arms and by async barrier drains for the in-process portion.
  pub(crate) fn drain_all_inline<F>(&self, mut f: F)
  where
    F: FnMut(Vec<T>),
  {
    for stripe in self.stripes.iter() {
      let mut data = stripe.lock();
      if data.is_empty() {
        continue;
      }
      let batch = replace(&mut *data, Vec::with_capacity(self.high_water));
      drop(data);
      f(batch);
    }
  }

  /// Iterate every non-empty stripe; swap a fresh empty Vec into each;
  /// pass the swapped batch to `f` (which may return an `Err` to abort).
  /// Used by `InsertStripeRing::drain_all_stripes_to_channel` to ship
  /// buffered items via the channel before a barrier marker is sent.
  pub(crate) fn drain_all<F, E>(&self, mut f: F) -> Result<(), E>
  where
    F: FnMut(Vec<T>) -> Result<(), E>,
  {
    for stripe in self.stripes.iter() {
      let mut data = stripe.lock();
      if data.is_empty() {
        continue;
      }
      let batch = replace(&mut *data, Vec::with_capacity(self.high_water));
      drop(data);
      f(batch)?;
    }
    Ok(())
  }
}

/// Result of `InsertStripeRing::push`.
#[cfg(any(feature = "sync", feature = "async"))]
#[derive(Debug)]
pub(crate) enum PushOutcome<T> {
  /// Item was appended to a stripe; no batch send was triggered.
  Buffered,
  /// Item triggered a threshold-flush; the full batch was sent to the
  /// processor channel successfully.
  Sent,
  /// Item triggered a threshold-flush but the channel was full and the
  /// `OVERFLOW_TIMEOUT` send_timeout could not place the batch. The
  /// batch is returned to the caller for `on_reject`/cleanup. The
  /// caller's own item is somewhere in this `Vec` (no positional
  /// guarantee — items from other threads in the same stripe may also
  /// be present).
  Dropped(Vec<T>),
}

#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) struct InsertStripeRing<T> {
  storage: StripeStorage<T>,
  tx: Sender<Vec<T>>,
}

#[cfg(any(feature = "sync", feature = "async"))]
impl<T> InsertStripeRing<T> {
  pub(crate) fn new(high_water: usize) -> (Self, Receiver<Vec<T>>) {
    let storage = StripeStorage::new(high_water);
    let (tx, rx) = bounded(BATCH_CAP);
    (Self { storage, tx }, rx)
  }

  pub(crate) fn push(&self, item: T) -> PushOutcome<T> {
    let Some(batch) = self.storage.push(item) else {
      return PushOutcome::Buffered;
    };
    match self.tx.try_send(batch) {
      Ok(()) => PushOutcome::Sent,
      Err(TrySendError::Full(batch)) => {
        use crossbeam_channel::SendTimeoutError;
        match self.tx.send_timeout(batch, OVERFLOW_TIMEOUT) {
          Ok(()) => PushOutcome::Sent,
          Err(SendTimeoutError::Timeout(batch)) | Err(SendTimeoutError::Disconnected(batch)) => {
            PushOutcome::Dropped(batch)
          }
        }
      }
      Err(TrySendError::Disconnected(batch)) => PushOutcome::Dropped(batch),
    }
  }

  pub(crate) fn drain_all_stripes_inline<F>(&self, f: F)
  where
    F: FnMut(Vec<T>),
  {
    self.storage.drain_all_inline(f)
  }

  /// Blocking send: barrier markers must observe all stripe items. See `StripeStorage::drain_all`.
  pub(crate) fn drain_all_stripes_to_channel(&self) -> Result<(), ()> {
    self
      .storage
      .drain_all(|batch| self.tx.send(batch).map_err(|_| ()))
  }

  pub(crate) fn send_single(&self, item: T) -> Result<(), ()> {
    self.tx.send(vec![item]).map_err(|_| ())
  }
}

#[cfg(all(test, any(feature = "sync", feature = "async")))]
mod tests {
  use super::*;

  #[test]
  fn push_below_threshold_does_not_send() {
    let (ring, rx) = InsertStripeRing::<u64>::new(64);
    for i in 0..63 {
      assert!(matches!(ring.push(i), PushOutcome::Buffered));
    }
    assert!(
      rx.try_recv().is_err(),
      "no batch should be sent below threshold"
    );
  }

  #[test]
  fn push_at_threshold_sends_full_batch() {
    let (ring, rx) = InsertStripeRing::<u64>::new(64);
    // 63 below threshold — buffered, no send.
    for i in 0..63 {
      assert!(matches!(ring.push(i), PushOutcome::Buffered));
    }
    // 64th push triggers swap-and-send.
    assert!(matches!(ring.push(63), PushOutcome::Sent));
    let batch = rx.try_recv().expect("batch should be on the channel");
    assert_eq!(batch.len(), 64);
    // Items preserved in order within a single thread's pushes.
    assert_eq!(batch, (0u64..64).collect::<Vec<_>>());
  }

  #[test]
  fn multi_thread_push_distributes_across_stripes() {
    use std::{sync::Arc, thread};

    const THREADS: usize = 8;
    const PER_THREAD: u64 = 1024;
    const HIGH_WATER: usize = 32;

    let (ring, rx) = InsertStripeRing::<u64>::new(HIGH_WATER);
    let ring = Arc::new(ring);
    let mut handles = Vec::new();
    for t in 0..THREADS {
      let ring = ring.clone();
      handles.push(thread::spawn(move || {
        let base = (t as u64) * PER_THREAD;
        for i in 0..PER_THREAD {
          let _ = ring.push(base + i);
        }
      }));
    }
    for h in handles {
      h.join().unwrap();
    }
    // Count what's already on the channel — partial stripes are not
    // drained yet (drain helpers come in Tasks 4/5). The channel
    // should hold most items as full-threshold batches.
    let mut total = 0;
    while let Ok(batch) = rx.try_recv() {
      total += batch.len();
    }
    let total_pushed = (THREADS as u64) * PER_THREAD;
    assert!(
      total as u64 >= total_pushed - (STRIPES as u64) * (HIGH_WATER as u64),
      "expected most items to flow to channel via threshold-flush; got {total}"
    );
  }

  #[test]
  fn inline_drain_consumes_partial_stripes() {
    let (ring, _rx) = InsertStripeRing::<u64>::new(64);
    // Push 5 items — well below threshold, all stay in caller's stripe.
    for i in 0..5u64 {
      assert!(matches!(ring.push(i), PushOutcome::Buffered));
    }
    // Drain inline. The closure receives one batch (the partial stripe).
    let mut received: Vec<u64> = Vec::new();
    ring.drain_all_stripes_inline(|batch| received.extend(batch));
    assert_eq!(
      received.len(),
      5,
      "all 5 partial-stripe items should be drained"
    );
    // Order is preserved within a single producer thread.
    assert_eq!(received, vec![0, 1, 2, 3, 4]);
    // After drain, calling drain again yields nothing.
    let mut second: Vec<u64> = Vec::new();
    ring.drain_all_stripes_inline(|batch| second.extend(batch));
    assert!(second.is_empty(), "drain on empty stripes is a no-op");
  }

  #[test]
  fn drain_to_channel_flushes_partial_stripes() {
    let (ring, rx) = InsertStripeRing::<u64>::new(64);
    for i in 0..5u64 {
      assert!(matches!(ring.push(i), PushOutcome::Buffered));
    }
    ring
      .drain_all_stripes_to_channel()
      .expect("channel must be open");
    let batch = rx
      .try_recv()
      .expect("partial stripe should reach the channel");
    assert_eq!(batch, vec![0, 1, 2, 3, 4]);
    // After draining, no further batches.
    assert!(
      rx.try_recv().is_err(),
      "channel should be empty after one batch"
    );
  }

  #[test]
  fn drain_to_channel_on_empty_ring_is_noop() {
    let (ring, rx) = InsertStripeRing::<u64>::new(64);
    ring
      .drain_all_stripes_to_channel()
      .expect("noop drain succeeds");
    assert!(rx.try_recv().is_err());
  }

  #[test]
  fn send_single_delivers_one_batch() {
    let (ring, rx) = InsertStripeRing::<u64>::new(64);
    ring.send_single(99).expect("channel open");
    let batch = rx.try_recv().expect("single-item batch");
    assert_eq!(batch, vec![99]);
  }

  #[test]
  fn overflow_drops_after_timeout() {
    // BATCH_CAP channel slots. We fill the channel, then push
    // enough items to trigger another threshold-flush; the send
    // should fail Full + send_timeout(20µs) + return Dropped.
    const HIGH_WATER: usize = 8;
    let (ring, rx) = InsertStripeRing::<u64>::new(HIGH_WATER);
    // Pre-fill the channel to its BATCH_CAP capacity by repeatedly
    // pushing HIGH_WATER items into the SAME stripe (this thread's
    // stripe). Each HIGH_WATER pushes triggers one batch send.
    // We push BATCH_CAP*HIGH_WATER items, all of which land as Sent.
    for i in 0..(BATCH_CAP as u64) * (HIGH_WATER as u64) {
      match ring.push(i) {
        PushOutcome::Buffered | PushOutcome::Sent => {}
        PushOutcome::Dropped(_) => {
          panic!("channel should not overflow during pre-fill at item {i}");
        }
      }
    }
    // Drop the receiver's slack so the channel is truly full.
    // (BATCH_CAP * HIGH_WATER pushes → BATCH_CAP batches sent →
    // channel at capacity.)
    // Now push another HIGH_WATER items into the same stripe to
    // trigger a flush. The first HIGH_WATER-1 pushes are Buffered;
    // the HIGH_WATER-th push triggers the swap-and-send, the channel
    // is full, send_timeout(20µs) fails → Dropped.
    for i in 0..(HIGH_WATER - 1) as u64 {
      assert!(matches!(ring.push(1_000_000 + i), PushOutcome::Buffered));
    }
    let outcome = ring.push(2_000_000);
    match outcome {
      PushOutcome::Dropped(batch) => {
        assert_eq!(
          batch.len(),
          HIGH_WATER,
          "dropped batch should be full HIGH_WATER size"
        );
        // The caller's own item (2_000_000) is in the batch.
        assert!(
          batch.contains(&2_000_000),
          "caller's item must be in the dropped batch"
        );
      }
      other => panic!("expected Dropped, got {other:?}"),
    }
    // Sanity: the receiver still has BATCH_CAP batches available.
    let mut count = 0;
    while rx.try_recv().is_ok() {
      count += 1;
    }
    assert_eq!(
      count, BATCH_CAP,
      "receiver should have exactly BATCH_CAP queued batches"
    );
  }
}
