use crate::{
  metrics::Metrics,
  policy::{SampledLFU, TinyLFU},
};
use std::collections::hash_map::RandomState;

use std::time::Duration;

static WAIT: Duration = Duration::from_millis(100);

#[cfg(feature = "sync")]
mod sync_test {
  use super::*;
  use crate::{
    policy::{AddOutcome, LFUPolicy},
    sync::{Sender, stop_channel},
  };
  use std::{sync::Arc, thread::sleep};

  fn make_policy(ctrs: usize, max_cost: i64) -> (LFUPolicy, Sender<()>) {
    let (stop_tx, stop_rx) = stop_channel();
    (LFUPolicy::new(ctrs, max_cost, stop_rx).unwrap(), stop_tx)
  }

  #[test]
  fn test_policy() {
    let _ = make_policy(100, 10);
  }

  #[test]
  fn test_policy_metrics() {
    let (mut p, _stop_tx) = make_policy(100, 10);
    p.collect_metrics(Arc::new(Metrics::new_op()));
    assert!(p.metrics.is_op());
    assert!(p.inner.lock().costs.metrics.is_op());
  }

  #[test]
  fn test_policy_process_items() {
    let (p, stop_tx) = make_policy(100, 10);
    p.items_tx.send(vec![1, 2, 2]).unwrap();
    sleep(WAIT);
    assert_eq!(p.admit.estimate(2), 2);
    assert_eq!(p.admit.estimate(1), 1);

    drop(stop_tx);
    sleep(WAIT);
    // After the processor exits, items_rx is dropped and push's try_send
    // returns Disconnected, surfaced as Some(keys).
    assert!(p.push(vec![3, 3, 3]).is_some());
    assert_eq!(p.admit.estimate(3), 0);
  }

  #[test]
  fn test_policy_push() {
    let (p, _stop_tx) = make_policy(100, 10);
    // Empty input is returned as-is — nothing to enqueue.
    assert_eq!(p.push(vec![]), Some(vec![]));

    let mut keep_count = 0;
    (0..10).for_each(|_| {
      if p.push(vec![1, 2, 3, 4, 5]).is_none() {
        keep_count += 1;
      }
    });

    assert_ne!(0, keep_count);
  }

  #[test]
  fn test_policy_add() {
    let (p, _stop_tx) = make_policy(1000, 100);
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
    let (p, _stop_tx) = make_policy(100, 10);
    p.add(1, 1);
    assert!(p.contains(&1));
    assert!(!p.contains(&2));
  }

  #[test]
  fn test_policy_del() {
    let (p, _stop_tx) = make_policy(100, 10);
    p.add(1, 1);
    p.remove(&1);
    p.remove(&2);
    assert!(!p.contains(&1));
    assert!(!p.contains(&2));
  }

  #[test]
  fn test_policy_cap() {
    let (p, _stop_tx) = make_policy(100, 10);
    p.add(1, 1);
    assert_eq!(p.cap(), 9);
  }

  #[test]
  fn test_policy_update() {
    let (p, _stop_tx) = make_policy(100, 10);
    p.add(1, 1);
    p.update(&1, 2);
    let inner = p.inner.lock();
    assert_eq!(inner.costs.key_costs.get(&1).unwrap(), &2);
  }

  #[test]
  fn test_policy_cost() {
    let (p, _stop_tx) = make_policy(100, 10);
    p.add(1, 2);
    assert_eq!(p.cost(&1), 2);
    assert_eq!(p.cost(&2), -1);
  }

  #[test]
  fn test_policy_clear() {
    let (p, _stop_tx) = make_policy(100, 10);
    p.add(1, 1);
    p.add(2, 2);
    p.add(3, 3);
    p.clear();

    assert_eq!(p.cap(), 10);
    assert!(!p.contains(&2));
    assert!(!p.contains(&2));
    assert!(!p.contains(&3));
  }

  // Dropping the stop sender tears down the processor, after which
  // direct channel sends and `push` both fail.
  #[test]
  fn test_policy_shutdown_on_stop_drop() {
    let (p, stop_tx) = make_policy(100, 10);
    p.add(1, 1);
    drop(stop_tx);
    sleep(WAIT);
    assert!(p.items_tx.send(vec![1]).is_err());
  }

  #[test]
  fn test_policy_push_after_shutdown() {
    let (p, stop_tx) = make_policy(100, 10);
    drop(stop_tx);
    sleep(WAIT);
    assert!(p.push(vec![1, 2]).is_some());
  }

  #[test]
  fn test_policy_add_after_shutdown() {
    let (p, stop_tx) = make_policy(100, 10);
    drop(stop_tx);
    // `add` operates on the inner lock and is independent of the
    // background processor — it must still succeed.
    p.add(1, 1);
  }
}

#[cfg(feature = "async")]
mod async_test {
  use super::*;
  use crate::{
    axync::{Sender, stop_channel},
    policy::{AddOutcome, AsyncLFUPolicy},
  };
  use agnostic_lite::tokio::TokioRuntime;
  use std::sync::Arc;
  use tokio::time::sleep;

  fn make_policy(ctrs: usize, max_cost: i64) -> (AsyncLFUPolicy, Sender<()>) {
    let (stop_tx, stop_rx) = stop_channel();
    (
      AsyncLFUPolicy::new::<TokioRuntime>(ctrs, max_cost, stop_rx).unwrap(),
      stop_tx,
    )
  }

  #[tokio::test]
  async fn test_policy() {
    let _ = make_policy(100, 10);
  }

  #[tokio::test]
  async fn test_policy_metrics() {
    let (mut p, _stop_tx) = make_policy(100, 10);
    p.collect_metrics(Arc::new(Metrics::new_op()));
    assert!(p.metrics.is_op());
    assert!(p.inner.lock().costs.metrics.is_op());
  }

  #[tokio::test]
  async fn test_policy_process_items() {
    let (p, stop_tx) = make_policy(100, 10);

    assert!(p.push(vec![1, 2, 2]).is_none());
    sleep(WAIT).await;

    assert_eq!(p.admit.estimate(2), 2);
    assert_eq!(p.admit.estimate(1), 1);

    drop(stop_tx);
    sleep(WAIT).await;
    assert!(p.push(vec![3, 3, 3]).is_some());
    assert_eq!(p.admit.estimate(3), 0);
  }

  #[tokio::test]
  async fn test_policy_push() {
    let (p, _stop_tx) = make_policy(100, 10);
    assert_eq!(p.push(vec![]), Some(vec![]));

    let mut keep_count = 0;
    for _ in 0..10 {
      if p.push(vec![1, 2, 3, 4, 5]).is_none() {
        keep_count += 1;
      }
    }

    assert_ne!(0, keep_count);
  }

  #[tokio::test]
  async fn test_policy_add() {
    let (p, _stop_tx) = make_policy(1000, 100);
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
    let (p, _stop_tx) = make_policy(100, 10);
    p.add(1, 1);
    assert!(p.contains(&1));
    assert!(!p.contains(&2));
  }

  #[tokio::test]
  async fn test_policy_del() {
    let (p, _stop_tx) = make_policy(100, 10);
    p.add(1, 1);
    p.remove(&1);
    p.remove(&2);
    assert!(!p.contains(&1));
    assert!(!p.contains(&2));
  }

  #[tokio::test]
  async fn test_policy_cap() {
    let (p, _stop_tx) = make_policy(100, 10);
    p.add(1, 1);
    assert_eq!(p.cap(), 9);
  }

  #[tokio::test]
  async fn test_policy_update() {
    let (p, _stop_tx) = make_policy(100, 10);
    p.add(1, 1);
    p.update(&1, 2);
    let inner = p.inner.lock();
    assert_eq!(inner.costs.key_costs.get(&1).unwrap(), &2);
  }

  #[tokio::test]
  async fn test_policy_cost() {
    let (p, _stop_tx) = make_policy(100, 10);
    p.add(1, 2);
    assert_eq!(p.cost(&1), 2);
    assert_eq!(p.cost(&2), -1);
  }

  #[tokio::test]
  async fn test_policy_clear() {
    let (p, _stop_tx) = make_policy(100, 10);
    p.add(1, 1);
    p.add(2, 2);
    p.add(3, 3);
    p.clear();

    assert_eq!(p.cap(), 10);
    assert!(!p.contains(&2));
    assert!(!p.contains(&2));
    assert!(!p.contains(&3));
  }

  #[tokio::test]
  async fn test_policy_shutdown_on_stop_drop() {
    let (p, stop_tx) = make_policy(100, 10);
    p.add(1, 1);
    drop(stop_tx);
    sleep(WAIT).await;
    assert!(p.items_tx.send(vec![1]).await.is_err());
  }

  #[tokio::test]
  async fn test_policy_push_after_shutdown() {
    let (p, stop_tx) = make_policy(100, 10);
    drop(stop_tx);
    sleep(WAIT).await;
    assert!(p.push(vec![1, 2]).is_some());
  }

  #[tokio::test]
  async fn test_policy_add_after_shutdown() {
    let (p, stop_tx) = make_policy(100, 10);
    drop(stop_tx);
    p.add(1, 1);
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
