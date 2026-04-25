use std::{cell::Cell, hash::BuildHasher, mem::replace, sync::Arc};

use parking_lot::Mutex;

#[cfg(feature = "async")]
use crate::policy::AsyncLFUPolicy;
#[cfg(feature = "sync")]
use crate::policy::LFUPolicy;

/// Number of independent stripes that buffer get-frequency keys before they
/// are flushed to the policy processor. Power of two so the index reduces to
/// a mask. 64 stripes give effectively zero contention for typical 16–48
/// thread workloads while keeping the per-cache allocation small.
const RING_STRIPES: usize = 64;
const RING_STRIPE_MASK: u64 = (RING_STRIPES as u64) - 1;

thread_local! {
  /// Cached stripe index for the current thread, computed once from a hash
  /// of the thread id. The stored value is `index | TLS_INIT_BIT` so that
  /// `0` means uninitialized; this avoids a separate `Cell<bool>`.
  static STRIPE_HINT: Cell<u64> = const { Cell::new(0) };
}

const TLS_INIT_BIT: u64 = 1 << 63;

#[inline]
fn stripe_index() -> usize {
  STRIPE_HINT.with(|c| {
    let cached = c.get();
    if cached != 0 {
      return (cached & RING_STRIPE_MASK) as usize;
    }
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    let idx = hasher.finish() & RING_STRIPE_MASK;
    c.set(idx | TLS_INIT_BIT);
    idx as usize
  })
}

#[cfg(feature = "sync")]
pub struct RingStripe<S> {
  cons: Arc<LFUPolicy<S>>,
  stripes: Box<[Mutex<Vec<u64>>]>,
  capa: usize,
}

#[cfg(feature = "sync")]
impl<S> RingStripe<S>
where
  S: BuildHasher + Clone + 'static + Send,
{
  pub(crate) fn new(cons: Arc<LFUPolicy<S>>, capa: usize) -> RingStripe<S> {
    let stripes: Box<[Mutex<Vec<u64>>]> = (0..RING_STRIPES)
      .map(|_| Mutex::new(Vec::with_capacity(capa)))
      .collect();
    RingStripe {
      cons,
      stripes,
      capa,
    }
  }

  pub fn push(&self, item: u64) {
    let stripe = &self.stripes[stripe_index()];
    let mut data = stripe.lock();
    data.push(item);
    if data.len() >= self.capa {
      // Swap a fresh vec in under the lock so the stripe is immediately
      // available to the next pusher. Release the lock before calling
      // `cons.push` so the policy processor's channel send doesn't block
      // any thread hashed to this stripe.
      let v = replace(&mut *data, Vec::with_capacity(self.capa));
      drop(data);
      let _ = self.cons.push(v);
    }
  }
}

#[cfg(feature = "async")]
pub struct AsyncRingStripe<S> {
  cons: Arc<AsyncLFUPolicy<S>>,
  stripes: Box<[Mutex<Vec<u64>>]>,
  capa: usize,
}

#[cfg(feature = "async")]
impl<S> AsyncRingStripe<S>
where
  S: BuildHasher + Clone + 'static + Send,
{
  pub(crate) fn new(cons: Arc<AsyncLFUPolicy<S>>, capa: usize) -> AsyncRingStripe<S> {
    let stripes: Box<[Mutex<Vec<u64>>]> = (0..RING_STRIPES)
      .map(|_| Mutex::new(Vec::with_capacity(capa)))
      .collect();
    AsyncRingStripe {
      cons,
      stripes,
      capa,
    }
  }

  pub fn push(&self, item: u64) {
    let stripe = &self.stripes[stripe_index()];
    let mut data = stripe.lock();
    data.push(item);
    if data.len() >= self.capa {
      let v = replace(&mut *data, Vec::with_capacity(self.capa));
      drop(data);
      let _ = self.cons.push(v);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn stripe_index_stable_within_thread() {
    let a = stripe_index();
    let b = stripe_index();
    assert_eq!(a, b);
    assert!(a < RING_STRIPES);
  }

  #[test]
  fn stripe_index_differs_across_threads() {
    let mut indices = std::collections::HashSet::new();
    indices.insert(stripe_index());
    for _ in 0..32 {
      let idx = std::thread::spawn(stripe_index).join().unwrap();
      indices.insert(idx);
    }
    // Each thread id hashes to a stripe; with 33 threads and 64 stripes we
    // should see at least 4 distinct buckets (collision is fine, total
    // single-bucketing is what we're guarding against).
    assert!(indices.len() >= 4, "got {} distinct stripes", indices.len());
  }
}
