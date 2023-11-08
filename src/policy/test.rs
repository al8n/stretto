use crate::metrics::Metrics;
use crate::policy::{SampledLFU, TinyLFU};
use std::collections::hash_map::RandomState;

use std::time::Duration;

static WAIT: Duration = Duration::from_millis(100);

#[cfg(feature = "sync")]
mod sync_test {
    use super::*;
    use crate::policy::LFUPolicy;
    use std::sync::Arc;
    use std::thread::sleep;

    #[test]
    fn test_policy() {
        let _ = LFUPolicy::new(100, 10);
    }

    #[test]
    fn test_policy_metrics() {
        let mut p = LFUPolicy::new(100, 10).unwrap();
        p.collect_metrics(Arc::new(Metrics::new_op()));
        assert!(p.metrics.is_op());
        assert!(p.inner.lock().costs.metrics.is_op());
    }

    #[test]
    fn test_policy_process_items() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.items_tx.send(vec![1, 2, 2]).unwrap();
        sleep(WAIT);
        let inner = p.inner.lock();
        assert_eq!(inner.admit.estimate(2), 2);
        assert_eq!(inner.admit.estimate(1), 1);
        drop(inner);

        p.stop_tx.send(()).unwrap();
        sleep(WAIT);
        assert!(p.push(vec![3, 3, 3]).is_err());
        let inner = p.inner.lock();
        assert_eq!(inner.admit.estimate(3), 0);
    }

    #[test]
    fn test_policy_push() {
        let p = LFUPolicy::new(100, 10).unwrap();
        assert!(p.push(vec![]).unwrap());

        let mut keep_count = 0;
        (0..10).for_each(|_| {
            if p.push(vec![1, 2, 3, 4, 5]).unwrap() {
                keep_count += 1;
            }
        });

        assert_ne!(0, keep_count);
    }

    #[test]
    fn test_policy_add() {
        let p = LFUPolicy::new(1000, 100).unwrap();
        let (victims, added) = p.add(1, 101);
        assert!(victims.is_none());
        assert!(!added);

        let mut inner = p.inner.lock();
        inner.costs.increment(1, 1);
        inner.admit.increment(1);
        inner.admit.increment(2);
        inner.admit.increment(3);
        drop(inner);

        let (victims, added) = p.add(1, 1);
        assert!(victims.is_none());
        assert!(!added);

        let (victims, added) = p.add(2, 20);
        assert!(victims.is_none());
        assert!(added);

        let (victims, added) = p.add(3, 90);
        assert!(victims.is_some());
        assert!(added);

        let (victims, added) = p.add(4, 20);
        assert!(victims.is_some());
        assert!(!added);
    }

    #[test]
    fn test_policy_has() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        assert!(p.contains(&1));
        assert!(!p.contains(&2));
    }

    #[test]
    fn test_policy_del() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        p.remove(&1);
        p.remove(&2);
        assert!(!p.contains(&1));
        assert!(!p.contains(&2));
    }

    #[test]
    fn test_policy_cap() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        assert_eq!(p.cap(), 9);
    }

    #[test]
    fn test_policy_update() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        p.update(&1, 2);
        let inner = p.inner.lock();
        assert_eq!(inner.costs.key_costs.get(&1).unwrap(), &2);
    }

    #[test]
    fn test_policy_cost() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 2);
        assert_eq!(p.cost(&1), 2);
        assert_eq!(p.cost(&2), -1);
    }

    #[test]
    fn test_policy_clear() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        p.add(2, 2);
        p.add(3, 3);
        p.clear();

        assert_eq!(p.cap(), 10);
        assert!(!p.contains(&2));
        assert!(!p.contains(&2));
        assert!(!p.contains(&3));
    }

    #[test]
    fn test_policy_close() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        let _ = p.close();
        sleep(WAIT);
        assert!(p.items_tx.send(vec![1]).is_err())
    }

    #[test]
    fn test_policy_push_after_close() {
        let p = LFUPolicy::new(100, 10).unwrap();
        let _ = p.close();
        assert!(!p.push(vec![1, 2]).unwrap());
    }

    #[test]
    fn test_policy_add_after_close() {
        let p = LFUPolicy::new(100, 10).unwrap();
        let _ = p.close();
        p.add(1, 1);
    }
}

#[cfg(feature = "async")]
mod async_test {
    use super::*;
    use crate::policy::AsyncLFUPolicy;
    use std::sync::Arc;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_policy() {
        let _ = AsyncLFUPolicy::new(100, 10, tokio::spawn);
    }

    #[tokio::test]
    async fn test_policy_metrics() {
        let mut p = AsyncLFUPolicy::new(100, 10, tokio::spawn).unwrap();
        p.collect_metrics(Arc::new(Metrics::new_op()));
        assert!(p.metrics.is_op());
        assert!(p.inner.lock().costs.metrics.is_op());
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_policy_process_items() {
        let p = AsyncLFUPolicy::new(100, 10, tokio::spawn).unwrap();

        p.push(vec![1, 2, 2]).await.unwrap();
        sleep(WAIT).await;

        let inner = p.inner.lock();
        assert_eq!(inner.admit.estimate(2), 2);
        assert_eq!(inner.admit.estimate(1), 1);
        drop(inner);

        p.stop_tx.send(()).await.unwrap();
        sleep(WAIT).await;
        assert!(p.push(vec![3, 3, 3]).await.is_err());
        let inner = p.inner.lock();
        assert_eq!(inner.admit.estimate(3), 0);
    }

    #[tokio::test]
    async fn test_policy_push() {
        let p = AsyncLFUPolicy::new(100, 10, tokio::spawn).unwrap();
        assert!(p.push(vec![]).await.unwrap());

        let mut keep_count = 0;
        for _ in 0..10 {
            if p.push(vec![1, 2, 3, 4, 5]).await.unwrap() {
                keep_count += 1;
            }
        }

        assert_ne!(0, keep_count);
    }

    #[tokio::test]
    async fn test_policy_add() {
        let p = AsyncLFUPolicy::new(1000, 100, tokio::spawn).unwrap();
        let (victims, added) = p.add(1, 101);
        assert!(victims.is_none());
        assert!(!added);

        let mut inner = p.inner.lock();
        inner.costs.increment(1, 1);
        inner.admit.increment(1);
        inner.admit.increment(2);
        inner.admit.increment(3);
        drop(inner);

        let (victims, added) = p.add(1, 1);
        assert!(victims.is_none());
        assert!(!added);

        let (victims, added) = p.add(2, 20);
        assert!(victims.is_none());
        assert!(added);

        let (victims, added) = p.add(3, 90);
        assert!(victims.is_some());
        assert!(added);

        let (victims, added) = p.add(4, 20);
        assert!(victims.is_some());
        assert!(!added);
    }

    #[tokio::test]
    async fn test_policy_has() {
        let p = AsyncLFUPolicy::new(100, 10, tokio::spawn).unwrap();
        p.add(1, 1);
        assert!(p.contains(&1));
        assert!(!p.contains(&2));
    }

    #[tokio::test]
    async fn test_policy_del() {
        let p = AsyncLFUPolicy::new(100, 10, tokio::spawn).unwrap();
        p.add(1, 1);
        p.remove(&1);
        p.remove(&2);
        assert!(!p.contains(&1));
        assert!(!p.contains(&2));
    }

    #[tokio::test]
    async fn test_policy_cap() {
        let p = AsyncLFUPolicy::new(100, 10, tokio::spawn).unwrap();
        p.add(1, 1);
        assert_eq!(p.cap(), 9);
    }

    #[tokio::test]
    async fn test_policy_update() {
        let p = AsyncLFUPolicy::new(100, 10, tokio::spawn).unwrap();
        p.add(1, 1);
        p.update(&1, 2);
        let inner = p.inner.lock();
        assert_eq!(inner.costs.key_costs.get(&1).unwrap(), &2);
    }

    #[tokio::test]
    async fn test_policy_cost() {
        let p = AsyncLFUPolicy::new(100, 10, tokio::spawn).unwrap();
        p.add(1, 2);
        assert_eq!(p.cost(&1), 2);
        assert_eq!(p.cost(&2), -1);
    }

    #[tokio::test]
    async fn test_policy_clear() {
        let p = AsyncLFUPolicy::new(100, 10, tokio::spawn).unwrap();
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
    async fn test_policy_close() {
        let p = AsyncLFUPolicy::new(100, 10, tokio::spawn).unwrap();
        p.add(1, 1);
        p.close().await.unwrap();
        sleep(WAIT).await;
        assert!(p.items_tx.send(vec![1]).await.is_err())
    }

    #[tokio::test]
    async fn test_policy_push_after_close() {
        let p = AsyncLFUPolicy::new(100, 10, tokio::spawn).unwrap();
        p.close().await.unwrap();
        assert!(!p.push(vec![1, 2]).await.unwrap());
    }

    #[tokio::test]
    async fn test_policy_add_after_close() {
        let p = AsyncLFUPolicy::new(100, 10, tokio::spawn).unwrap();
        p.close().await.unwrap();
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
fn test_tinylfu_increment() {
    let mut l = TinyLFU::new(4).unwrap();
    l.increment(1);
    l.increment(1);
    l.increment(1);
    assert!(l.doorkeeper.contains(1));
    assert_eq!(l.ctr.estimate(1), 2);

    l.increment(1);
    assert!(!l.doorkeeper.contains(1));
    assert_eq!(l.ctr.estimate(1), 1);
}

#[test]
fn test_tinylfu_estimate() {
    let mut l = TinyLFU::new(8).unwrap();
    l.increment(1);
    l.increment(1);
    l.increment(1);

    assert_eq!(l.estimate(1), 3);
    assert_eq!(l.estimate(2), 0);
    assert_eq!(l.w, 3);
}

#[test]
fn test_tinylfu_increments() {
    let mut l = TinyLFU::new(16).unwrap();

    assert_eq!(l.samples, 16);
    l.increments([1, 2, 2, 3, 3, 3].to_vec());
    assert_eq!(l.estimate(1), 1);
    assert_eq!(l.estimate(2), 2);
    assert_eq!(l.estimate(3), 3);
    assert_eq!(6, l.w);
}

#[test]
fn test_tinylfu_clear() {
    let mut l = TinyLFU::new(16).unwrap();
    l.increments([1, 3, 3, 3].to_vec());
    l.clear();
    assert_eq!(0, l.w);
    assert_eq!(0, l.estimate(3));
}
