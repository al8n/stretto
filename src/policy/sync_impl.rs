use crate::metrics::{MetricType, Metrics};
use crate::policy::PolicyInner;
use crate::CacheError;
use crossbeam_channel::{bounded, unbounded, Receiver, RecvError, Sender};
use parking_lot::Mutex;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{spawn, JoinHandle};

pub struct LFUPolicy<S = RandomState> {
    pub(crate) inner: Arc<Mutex<PolicyInner<S>>>,
    pub(crate) processor_handle: JoinHandle<()>,
    pub(crate) items_tx: Sender<Vec<u64>>,
    pub(crate) stop_tx: Sender<()>,
    pub(crate) is_closed: AtomicBool,
    pub(crate) metrics: Arc<Metrics>,
}

impl LFUPolicy {
    pub(crate) fn new(ctrs: usize, max_cost: i64) -> Result<Self, CacheError> {
        Self::with_hasher(ctrs, max_cost, RandomState::new())
    }
}

impl<S: BuildHasher + Clone + 'static> LFUPolicy<S> {
    pub fn with_hasher(ctrs: usize, max_cost: i64, hasher: S) -> Result<Self, CacheError> {
        let inner = PolicyInner::with_hasher(ctrs, max_cost, hasher)?;

        let (items_tx, items_rx) = unbounded();
        let (stop_tx, stop_rx) = bounded(0);

        let handle = PolicyProcessor::spawn(inner.clone(), items_rx, stop_rx);

        let this = Self {
            inner: inner.clone(),
            processor_handle: handle,
            items_tx,
            stop_tx,
            is_closed: AtomicBool::new(false),
            metrics: Arc::new(Metrics::new()),
        };

        Ok(this)
    }

    pub fn push(&self, keys: Vec<u64>) -> Result<bool, CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let num_of_keys = keys.len() as u64;
        if num_of_keys == 0 {
            return Ok(true);
        }
        let first = keys[0];
        select! {
            send(self.items_tx, keys) -> res =>
                res
                .map(|_| {
                    self.metrics.add(MetricType::KeepGets, first, num_of_keys);
                    true
                })
                .map_err(|e| {
                    self.metrics.add(MetricType::DropGets, first, num_of_keys);
                    CacheError::SendError(format!("sending on a disconnected channel, msg: {:?}", e.0))
                }),
            default => {
                self.metrics.add(MetricType::DropGets, first, num_of_keys);
                return Ok(false);
            }
        }
    }

    pub fn close(&self) -> Result<(), CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        // block until the Processor thread returns.
        self.stop_tx
            .send(())
            .map_err(|e| CacheError::SendError(format!("{}", e)))?;
        self.is_closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct PolicyProcessor<S: BuildHasher + Clone + 'static> {
    inner: Arc<Mutex<PolicyInner<S>>>,
    items_rx: Receiver<Vec<u64>>,
    stop_rx: Receiver<()>,
}

impl<S: BuildHasher + Clone + 'static> PolicyProcessor<S> {
    fn spawn(
        inner: Arc<Mutex<PolicyInner<S>>>,
        items_rx: Receiver<Vec<u64>>,
        stop_rx: Receiver<()>,
    ) -> JoinHandle<()> {
        let this = Self {
            inner,
            items_rx,
            stop_rx,
        };

        spawn(move || loop {
            select! {
                recv(this.items_rx) -> items => this.handle_items(items),
                recv(this.stop_rx) -> _ => {
                    drop(this);
                    return;
                },
            }
        })
    }

    fn handle_items(&self, items: Result<Vec<u64>, RecvError>) {
        match items {
            Ok(items) => {
                let mut inner = self.inner.lock();
                inner.admit.increments(items);
            }
            Err(e) => error!("policy processor error: {}", e),
        }
    }
}

unsafe impl<S: BuildHasher + Clone + 'static> Send for PolicyProcessor<S> {}
unsafe impl<S: BuildHasher + Clone + 'static> Sync for PolicyProcessor<S> {}

#[cfg(test)]
mod test {
    use crate::metrics::Metrics;
    use crate::policy::LFUPolicy;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    static WAIT: Duration = Duration::from_millis(100);

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
