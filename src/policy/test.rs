use crate::{
  metrics::Metrics,
  policy::{SampledLFU, TinyLFU},
};
use std::collections::hash_map::RandomState;

#[cfg(feature = "sync")]
mod sync_test {
  use super::*;
  use crate::policy::{AddOutcome, LFUPolicy};
  use std::sync::Arc;

  fn make_policy(ctrs: usize, max_cost: i64) -> LFUPolicy {
    LFUPolicy::new(ctrs, max_cost).unwrap()
  }

  #[test]
  fn test_policy() {
    let _ = make_policy(100, 10);
  }

  #[test]
  fn test_policy_metrics() {
    let mut p = make_policy(100, 10);
    p.collect_metrics(Arc::new(Metrics::new_op()));
    assert!(p.metrics.is_op());
    assert!(p.inner.lock().costs.metrics.is_op());
  }

  #[test]
  fn test_policy_increment() {
    // Direct admit.increment is the only path now (no channel/processor).
    let p = make_policy(100, 10);
    p.admit.increment(1);
    p.admit.increments(vec![2, 2]);
    assert_eq!(p.admit.estimate(2), 2);
    assert_eq!(p.admit.estimate(1), 1);
  }

  #[test]
  fn test_policy_add() {
    let p = make_policy(1000, 100);
    assert!(matches!(p.add(1, 101), AddOutcome::RejectedByCost));

    let mut inner = p.inner.lock();
    inner.costs.increment(1, 1);
    drop(inner);
    p.admit.increment(1);
    p.admit.increment(2);
    p.admit.increment(3);

    assert!(matches!(p.add(1, 1), AddOutcome::UpdatedExisting));

    assert!(matches!(
      p.add(2, 20),
      AddOutcome::Admitted { ref victims } if victims.is_empty()
    ));

    assert!(matches!(
      p.add(3, 90),
      AddOutcome::Admitted { ref victims } if !victims.is_empty()
    ));

    assert!(matches!(
      p.add(4, 20),
      AddOutcome::RejectedBySampling { .. }
    ));
  }

  #[test]
  fn test_policy_has() {
    let p = make_policy(100, 10);
    p.add(1, 1);
    assert!(p.contains(&1));
    assert!(!p.contains(&2));
  }

  #[test]
  fn test_policy_del() {
    let p = make_policy(100, 10);
    p.add(1, 1);
    p.remove(&1);
    p.remove(&2);
    assert!(!p.contains(&1));
    assert!(!p.contains(&2));
  }

  #[test]
  fn test_policy_cap() {
    let p = make_policy(100, 10);
    p.add(1, 1);
    assert_eq!(p.cap(), 9);
  }

  #[test]
  fn test_policy_update() {
    let p = make_policy(100, 10);
    p.add(1, 1);
    p.update(&1, 2);
    let inner = p.inner.lock();
    assert_eq!(inner.costs.key_costs.get(&1).unwrap(), &2);
  }

  #[test]
  fn test_policy_cost() {
    let p = make_policy(100, 10);
    p.add(1, 2);
    assert_eq!(p.cost(&1), 2);
    assert_eq!(p.cost(&2), -1);
  }

  #[test]
  fn test_policy_clear() {
    let p = make_policy(100, 10);
    p.add(1, 1);
    p.add(2, 2);
    p.add(3, 3);
    p.clear();

    assert_eq!(p.cap(), 10);
    assert!(!p.contains(&2));
    assert!(!p.contains(&2));
    assert!(!p.contains(&3));
  }
}

#[cfg(feature = "async")]
mod async_test {
  use super::*;
  use crate::policy::{AddOutcome, AsyncLFUPolicy};
  use std::sync::Arc;

  fn make_policy(ctrs: usize, max_cost: i64) -> AsyncLFUPolicy {
    AsyncLFUPolicy::new(ctrs, max_cost).unwrap()
  }

  #[tokio::test]
  async fn test_policy() {
    let _ = make_policy(100, 10);
  }

  #[tokio::test]
  async fn test_policy_metrics() {
    let mut p = make_policy(100, 10);
    p.collect_metrics(Arc::new(Metrics::new_op()));
    assert!(p.metrics.is_op());
    assert!(p.inner.lock().costs.metrics.is_op());
  }

  #[tokio::test]
  async fn test_policy_increment() {
    let p = make_policy(100, 10);
    p.admit.increment(1);
    p.admit.increments(vec![2, 2]);
    assert_eq!(p.admit.estimate(2), 2);
    assert_eq!(p.admit.estimate(1), 1);
  }

  #[tokio::test]
  async fn test_policy_add() {
    let p = make_policy(1000, 100);
    assert!(matches!(p.add(1, 101), AddOutcome::RejectedByCost));

    let mut inner = p.inner.lock();
    inner.costs.increment(1, 1);
    drop(inner);
    p.admit.increment(1);
    p.admit.increment(2);
    p.admit.increment(3);

    assert!(matches!(p.add(1, 1), AddOutcome::UpdatedExisting));

    assert!(matches!(
      p.add(2, 20),
      AddOutcome::Admitted { ref victims } if victims.is_empty()
    ));

    assert!(matches!(
      p.add(3, 90),
      AddOutcome::Admitted { ref victims } if !victims.is_empty()
    ));

    assert!(matches!(
      p.add(4, 20),
      AddOutcome::RejectedBySampling { .. }
    ));
  }

  #[tokio::test]
  async fn test_policy_has() {
    let p = make_policy(100, 10);
    p.add(1, 1);
    assert!(p.contains(&1));
    assert!(!p.contains(&2));
  }

  #[tokio::test]
  async fn test_policy_del() {
    let p = make_policy(100, 10);
    p.add(1, 1);
    p.remove(&1);
    p.remove(&2);
    assert!(!p.contains(&1));
    assert!(!p.contains(&2));
  }

  #[tokio::test]
  async fn test_policy_cap() {
    let p = make_policy(100, 10);
    p.add(1, 1);
    assert_eq!(p.cap(), 9);
  }

  #[tokio::test]
  async fn test_policy_update() {
    let p = make_policy(100, 10);
    p.add(1, 1);
    p.update(&1, 2);
    let inner = p.inner.lock();
    assert_eq!(inner.costs.key_costs.get(&1).unwrap(), &2);
  }

  #[tokio::test]
  async fn test_policy_cost() {
    let p = make_policy(100, 10);
    p.add(1, 2);
    assert_eq!(p.cost(&1), 2);
    assert_eq!(p.cost(&2), -1);
  }

  #[tokio::test]
  async fn test_policy_clear() {
    let p = make_policy(100, 10);
    p.add(1, 1);
    p.add(2, 2);
    p.add(3, 3);
    p.clear();

    assert_eq!(p.cap(), 10);
    assert!(!p.contains(&2));
    assert!(!p.contains(&2));
    assert!(!p.contains(&3));
  }
}

#[test]
fn test_sampled_lfu_constructor() {
  let _ = SampledLFU::with_samples_and_hasher(100, 100, RandomState::new());
  let _ = SampledLFU::with_samples(100, 100);
}

#[test]
fn test_sampled_lfu_remove() {
  let mut lfu = SampledLFU::new(4);
  lfu.increment(1, 1);
  lfu.increment(2, 2);
  assert_eq!(lfu.remove(&2), Some(2));
  assert_eq!(lfu.used, 1);
  assert_eq!(lfu.key_costs.get(&2), None);
  assert_eq!(lfu.remove(&4), None);
}

#[test]
fn test_sampled_lfu_room() {
  let mut l = SampledLFU::new(16);
  l.increment(1, 1);
  l.increment(2, 2);
  l.increment(3, 3);
  assert_eq!(6, l.room_left(4));
}

#[test]
fn test_sampled_lfu_clear() {
  let mut l = SampledLFU::new(4);
  l.increment(1, 1);
  l.increment(2, 2);
  l.increment(3, 3);
  l.clear();
  assert_eq!(0, l.key_costs.len());
  assert_eq!(0, l.used);
}

#[test]
fn test_sampled_lfu_update() {
  let mut l = SampledLFU::new(5);
  l.increment(1, 1);
  l.increment(2, 2);
  assert!(l.update(&1, 2));
  assert_eq!(4, l.used);
  assert!(l.update(&2, 3));
  assert_eq!(5, l.used);
  assert!(!l.update(&3, 3));
}

#[test]
fn test_sampled_lfu_fill_sample() {
  let mut l = SampledLFU::new(16);
  l.increment(4, 4);
  l.increment(5, 5);
  let sample = l.fill_sample(vec![(1, 1).into(), (2, 2).into(), (3, 3).into()]);
  let k = sample[sample.len() - 1].key;
  assert_eq!(5, sample.len());
  assert_ne!(1, k);
  assert_ne!(2, k);
  assert_ne!(3, k);
  assert_eq!(sample.len(), l.fill_sample(sample.clone()).len());
  l.remove(&5);
  let sample = l.fill_sample(sample[0..(sample.len() - 2)].to_vec());
  assert_eq!(4, sample.len())
}

#[test]
fn test_sampled_lfu_fill_sample_is_random() {
  // Regression for https://github.com/al8n/stretto/issues/37:
  // Rust's HashMap iteration is deterministic for a given instance, so
  // the previous fill_sample always returned the same prefix of keys. With
  // reservoir sampling, the subset should vary across calls.
  const NUM_KEYS: u64 = 200;
  const SAMPLES: usize = 5;

  let mut l = SampledLFU::with_samples(NUM_KEYS as i64, SAMPLES);
  for k in 0..NUM_KEYS {
    l.increment(k, 1);
  }

  let mut seen_keys = std::collections::HashSet::new();
  for _ in 0..64 {
    let sample = l.fill_sample(Vec::new());
    assert_eq!(sample.len(), SAMPLES);
    for pair in &sample {
      seen_keys.insert(pair.key);
    }
  }

  // If iteration were deterministic, 64 calls would all return the same
  // 5 keys. Reservoir sampling should hit a large fraction of the pool.
  assert!(
    seen_keys.len() > SAMPLES * 4,
    "fill_sample looks biased: only saw {} distinct keys across 64 calls",
    seen_keys.len()
  );
}

#[test]
fn test_tinylfu_increment() {
  let l = TinyLFU::new(4).unwrap();
  l.increment(1);
  l.increment(1);
  l.increment(1);
  assert!(l.doorkeeper.contains(1));
  assert_eq!(l.ctr.estimate(1), 2);

  // The 4th increment trips `try_reset` (samples == 4), which halves
  // the count-min counters and clears the doorkeeper. After reset the
  // increment itself runs first, but the reset wipes both — so the
  // bit/counter are zero post-reset.
  l.increment(1);
  assert!(!l.doorkeeper.contains(1));
  assert_eq!(l.ctr.estimate(1), 1);
}

#[test]
fn test_tinylfu_estimate() {
  use std::sync::atomic::Ordering;
  let l = TinyLFU::new(8).unwrap();
  l.increment(1);
  l.increment(1);
  l.increment(1);

  assert_eq!(l.estimate(1), 3);
  assert_eq!(l.estimate(2), 0);
  assert_eq!(l.w.load(Ordering::Relaxed), 3);
}

#[test]
fn test_tinylfu_increments() {
  use std::sync::atomic::Ordering;
  let l = TinyLFU::new(16).unwrap();

  assert_eq!(l.samples, 16);
  l.increments([1, 2, 2, 3, 3, 3].to_vec());
  assert_eq!(l.estimate(1), 1);
  assert_eq!(l.estimate(2), 2);
  assert_eq!(l.estimate(3), 3);
  assert_eq!(6, l.w.load(Ordering::Relaxed));
}

#[test]
fn test_tinylfu_clear() {
  use std::sync::atomic::Ordering;
  let l = TinyLFU::new(16).unwrap();
  l.increments([1, 3, 3, 3].to_vec());
  l.clear();
  assert_eq!(0, l.w.load(Ordering::Relaxed));
  assert_eq!(0, l.estimate(3));
}

/// Stress test: many threads increment concurrently while the reset
/// threshold is crossed repeatedly. Verifies that:
///   1. No nibble in the count-min sketch overflows past 15
///   2. The window counter `w` doesn't drift far past `samples` (the
///      `fetch_sub(samples)` carryover keeps it bounded; without it,
///      losing-CAS callers would have their bumps erased and `w`
///      would never accumulate the right total)
///   3. No panic / data race / deadlock under concurrent reset
#[test]
fn test_tinylfu_concurrent_increment_with_reset() {
  use std::{
    sync::{Arc, atomic::Ordering},
    thread,
  };

  // Small samples=64 so resets fire often during the run; 8 threads
  // each issue 5_000 increments across 32 distinct keys → many resets.
  let l = Arc::new(TinyLFU::new(64).unwrap());
  let mut handles = Vec::with_capacity(8);
  for t in 0..8u64 {
    let l = l.clone();
    handles.push(thread::spawn(move || {
      for i in 0..5_000u64 {
        l.increment((t * 5_000 + i) % 32);
      }
    }));
  }
  for h in handles {
    h.join().unwrap();
  }

  // Estimates per key are bounded by the 4-bit counter (max 15), and
  // because reset halves the sketch each window, no key should read
  // out as anything wild. Just spot-check no panic / overflow.
  for k in 0..32u64 {
    let est = l.estimate(k);
    assert!(
      (0..=16).contains(&est),
      "estimate for key {} is out of range: {}",
      k,
      est
    );
  }

  // w may be anywhere in [0, samples) after the last reset, but should
  // never exceed `samples * concurrent_threads` (that would mean the
  // reset coordination broke and never fired).
  let final_w = l.w.load(Ordering::Relaxed);
  assert!(
    final_w < l.samples * 8,
    "w runaway: {} (samples={}, threads=8)",
    final_w,
    l.samples
  );
}
