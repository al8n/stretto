//! Counting semaphores used by the cache as a bounded permit pool.
//!
//! The cache uses a semaphore to bound the number of inserts in flight
//! between the caller-side eager store write and the processor-side policy
//! admission. Without it, blocked senders can accumulate pre-admission store
//! rows beyond `max_cost`, since the eager write happens before the caller
//! waits on a full insert buffer. Acquiring a permit BEFORE the eager write
//! moves that backpressure to the input side and keeps the store bounded by
//! the permit count.
//!
//! Both variants expose the same shape:
//! - `new(n)`: create with `n` permits.
//! - `acquire()`: block until a permit is available, or return `Err` if the
//!   semaphore is closed.
//! - `try_acquire()`: non-blocking.
//! - `release()`: return a permit to the pool.
//! - `close()`: wake all waiters with `Err` and reject future acquires.
//!
//! Permits transfer across thread/task boundaries by convention — the
//! caller acquires, enqueues work, and the processor releases after
//! consuming the work. There is no RAII guard because the permit's
//! lifetime crosses that boundary.

#[cfg(feature = "async")]
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::{AtomicBool, Ordering};

/// Error returned by `acquire` when the semaphore has been closed. Callers
/// should treat this as a shutdown signal and unwind without performing the
/// work the permit was being acquired for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SemaphoreClosed;

/// Synchronous counting semaphore backed by `parking_lot::{Mutex, Condvar}`.
/// Used by the sync cache to bound in-flight inserts.
#[cfg(feature = "sync")]
pub(crate) struct SyncSemaphore {
  // Available permit count. Guarded by the mutex so Condvar::wait can check
  // it under the same lock that `release` updates.
  permits: parking_lot::Mutex<usize>,
  cv: parking_lot::Condvar,
  closed: AtomicBool,
}

#[cfg(feature = "sync")]
impl SyncSemaphore {
  pub(crate) fn new(n: usize) -> Self {
    Self {
      permits: parking_lot::Mutex::new(n),
      cv: parking_lot::Condvar::new(),
      closed: AtomicBool::new(false),
    }
  }

  /// Block until a permit is available. Returns `Err(SemaphoreClosed)` if
  /// the semaphore has been or becomes closed while waiting.
  pub(crate) fn acquire(&self) -> Result<(), SemaphoreClosed> {
    let mut guard = self.permits.lock();
    loop {
      if self.closed.load(Ordering::Acquire) {
        return Err(SemaphoreClosed);
      }
      if *guard > 0 {
        *guard -= 1;
        return Ok(());
      }
      self.cv.wait(&mut guard);
    }
  }

  /// Non-blocking acquire. Returns `true` on success.
  ///
  /// `closed` is read AFTER taking the lock. Combined with `close` taking
  /// the same lock around its `closed.store`, this means a close that
  /// races try_acquire either lands fully before our lock acquisition (we
  /// observe closed=true and reject) or fully after we drop the lock (we
  /// took the permit before the close point — semantically a "prior"
  /// acquire, which the documented "reject future acquires" contract
  /// allows).
  pub(crate) fn try_acquire(&self) -> bool {
    let mut guard = self.permits.lock();
    if self.closed.load(Ordering::Acquire) {
      return false;
    }
    if *guard > 0 {
      *guard -= 1;
      true
    } else {
      false
    }
  }

  /// Return one permit to the pool and wake at most one waiter.
  pub(crate) fn release(&self) {
    let mut guard = self.permits.lock();
    *guard += 1;
    // Drop the lock before notifying to avoid waking a waiter just to have
    // it block on the same mutex we hold.
    drop(guard);
    self.cv.notify_one();
  }

  /// Wake all waiters and reject future acquires. Idempotent.
  ///
  /// Called by the sync processor's RAII drop-guard on thread exit
  /// (including panic-unwind). A panicking user callback strands permits
  /// that would otherwise never be released; closing the semaphore wakes
  /// every blocked acquirer with `SemaphoreClosed` so callers can fall
  /// through their `acquire().is_err()` branch rather than hang.
  ///
  /// The `closed` store happens UNDER the permits lock so that
  /// `try_acquire`/`acquire` (which read `closed` after locking) cannot
  /// witness an old `closed=false` while their lock acquisition
  /// happens-after this close — without that, a try_acquire could grab
  /// the lock, see `closed=false` (because the store has not been
  /// published through the mutex), and consume a permit after the
  /// documented "reject future acquires" point. Holding the lock around
  /// the store also keeps the prior "waiters parked before notify"
  /// guarantee for the Condvar wake.
  pub(crate) fn close(&self) {
    let guard = self.permits.lock();
    self.closed.store(true, Ordering::Release);
    drop(guard);
    self.cv.notify_all();
  }
}

/// Asynchronous counting semaphore backed by `event_listener::Event`. Used
/// by the async cache to bound in-flight inserts across tasks.
///
/// Runtime-agnostic: it does not depend on tokio or smol. `event-listener`
/// is the same primitive `async-channel` is built on, so it is already in
/// the dependency graph for the `async` feature.
#[cfg(feature = "async")]
pub(crate) struct AsyncSemaphore {
  permits: AtomicUsize,
  event: event_listener::Event,
  closed: AtomicBool,
}

#[cfg(feature = "async")]
impl AsyncSemaphore {
  pub(crate) fn new(n: usize) -> Self {
    Self {
      permits: AtomicUsize::new(n),
      event: event_listener::Event::new(),
      closed: AtomicBool::new(false),
    }
  }

  /// Acquire a permit, awaiting if the pool is empty. Returns
  /// `Err(SemaphoreClosed)` if the semaphore has been closed.
  ///
  /// Cancellation-safe: if the future is dropped before the permit is
  /// obtained, no state is mutated — the semaphore count remains unchanged
  /// and the dropped `EventListener` simply unregisters itself.
  pub(crate) async fn acquire(&self) -> Result<(), SemaphoreClosed> {
    loop {
      if self.closed.load(Ordering::Acquire) {
        return Err(SemaphoreClosed);
      }
      // Optimistic fast path: try to claim a permit without listening.
      // `claim_unless_closed` re-checks `closed` after a successful CAS,
      // so a close racing the claim cannot smuggle a permit out.
      if self.claim_unless_closed() {
        return Ok(());
      }
      // Register for notification BEFORE re-checking, so a concurrent
      // `release` or `close` between the check and the await cannot
      // silently drop the wakeup.
      let listener = self.event.listen();
      if self.closed.load(Ordering::Acquire) {
        return Err(SemaphoreClosed);
      }
      if self.claim_unless_closed() {
        return Ok(());
      }
      listener.await;
    }
  }

  /// Non-blocking acquire. Returns `true` on success.
  ///
  /// Parity with `SyncSemaphore::try_acquire`; kept for future uses (e.g.
  /// a processor-task re-entry check analogous to the sync one). Currently
  /// the async cache has no re-entry deadlock risk — `CacheCallback`
  /// methods are synchronous so they cannot `.await` an insert.
  #[cfg_attr(not(test), allow(dead_code))]
  pub(crate) fn try_acquire(&self) -> bool {
    if self.closed.load(Ordering::Acquire) {
      return false;
    }
    self.claim_unless_closed()
  }

  fn try_claim(&self) -> bool {
    // CAS loop: decrement permits if > 0. Acquire ordering pairs with the
    // Release fetch_add in `release` so a permit released by the processor
    // is observed along with the store writes the processor performed
    // before releasing.
    let mut cur = self.permits.load(Ordering::Acquire);
    while cur > 0 {
      match self
        .permits
        .compare_exchange_weak(cur, cur - 1, Ordering::AcqRel, Ordering::Acquire)
      {
        Ok(_) => return true,
        Err(observed) => cur = observed,
      }
    }
    false
  }

  /// `try_claim` plus a post-claim re-check of `closed`. If a concurrent
  /// `close` happens to land between an outer pre-check and the CAS, this
  /// rolls the permit back via `release` so we honor the documented
  /// "reject future acquires" contract instead of silently consuming a
  /// permit on a closed semaphore. The released permit is harmless on a
  /// closed semaphore — every subsequent `acquire`/`try_acquire` reads
  /// `closed=true` first and returns `Err`/`false` without ever
  /// inspecting the permit count.
  fn claim_unless_closed(&self) -> bool {
    if !self.try_claim() {
      return false;
    }
    if self.closed.load(Ordering::Acquire) {
      self.release();
      return false;
    }
    true
  }

  /// Return one permit to the pool and notify at most one waiter.
  pub(crate) fn release(&self) {
    self.permits.fetch_add(1, Ordering::Release);
    self.event.notify(1usize);
  }

  /// Wake all waiters and reject future acquires. Idempotent.
  ///
  /// Called by the async processor's RAII drop-guard on exit, including
  /// panic-unwind. See `SyncSemaphore::close` for the same reasoning.
  ///
  /// Acquirers serialize the `closed` check with the CAS via
  /// `claim_unless_closed`: if a concurrent claim wins the CAS just before
  /// we set `closed=true`, that caller observes the post-claim
  /// `closed=true` re-check and rolls the permit back. So at most one
  /// claim slipping through this race is contained.
  pub(crate) fn close(&self) {
    self.closed.store(true, Ordering::Release);
    // Notify every listener so each one re-enters `acquire` and observes
    // `closed = true`.
    self.event.notify(usize::MAX);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[cfg(feature = "sync")]
  mod sync_tests {
    use super::*;
    use std::{sync::Arc, thread, time::Duration};

    #[test]
    fn acquire_decrements_and_release_increments() {
      let s = SyncSemaphore::new(2);
      assert!(s.try_acquire());
      assert!(s.try_acquire());
      assert!(!s.try_acquire());
      s.release();
      assert!(s.try_acquire());
    }

    #[test]
    fn acquire_blocks_until_release() {
      let s = Arc::new(SyncSemaphore::new(1));
      s.acquire().unwrap();
      let s2 = s.clone();
      let handle = thread::spawn(move || s2.acquire().unwrap());
      thread::sleep(Duration::from_millis(50));
      s.release();
      handle.join().unwrap();
    }

    #[test]
    fn close_wakes_waiters_with_err() {
      let s = Arc::new(SyncSemaphore::new(0));
      let s2 = s.clone();
      let handle = thread::spawn(move || s2.acquire());
      thread::sleep(Duration::from_millis(50));
      s.close();
      let result = handle.join().unwrap();
      assert_eq!(result, Err(SemaphoreClosed));
    }

    #[test]
    fn acquire_after_close_fails_immediately() {
      let s = SyncSemaphore::new(5);
      s.close();
      assert_eq!(s.acquire(), Err(SemaphoreClosed));
      assert!(!s.try_acquire());
    }
  }

  #[cfg(feature = "async")]
  mod async_tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn async_acquire_decrements_and_release_increments() {
      let s = AsyncSemaphore::new(2);
      s.acquire().await.unwrap();
      s.acquire().await.unwrap();
      assert!(!s.try_acquire());
      s.release();
      s.acquire().await.unwrap();
    }

    #[tokio::test]
    async fn async_acquire_awaits_release() {
      let s = Arc::new(AsyncSemaphore::new(1));
      s.acquire().await.unwrap();
      let s2 = s.clone();
      let handle = tokio::spawn(async move { s2.acquire().await.unwrap() });
      tokio::time::sleep(std::time::Duration::from_millis(50)).await;
      s.release();
      handle.await.unwrap();
    }

    #[tokio::test]
    async fn async_close_wakes_waiters_with_err() {
      let s = Arc::new(AsyncSemaphore::new(0));
      let s2 = s.clone();
      let handle = tokio::spawn(async move { s2.acquire().await });
      tokio::time::sleep(std::time::Duration::from_millis(50)).await;
      s.close();
      assert_eq!(handle.await.unwrap(), Err(SemaphoreClosed));
    }

    #[tokio::test]
    async fn async_dropped_acquire_does_not_consume() {
      // Cancellation-safety check: dropping the future before it resolves
      // must not leak a permit.
      let s = Arc::new(AsyncSemaphore::new(1));
      s.acquire().await.unwrap();
      // No permits left now; this acquire will pend.
      let s2 = s.clone();
      let pending = tokio::spawn(async move { s2.acquire().await });
      tokio::time::sleep(std::time::Duration::from_millis(20)).await;
      pending.abort();
      let _ = pending.await;
      // The aborted acquire should not have consumed a permit. Releasing
      // once must make exactly one further acquire succeed.
      s.release();
      assert!(s.try_acquire());
    }
  }
}
