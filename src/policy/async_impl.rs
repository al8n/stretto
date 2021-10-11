use crate::metrics::{MetricType, Metrics};
use crate::policy::PolicyInner;
use crate::CacheError;
use parking_lot::Mutex;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use tokio::{select, spawn, task::JoinHandle};

pub struct LFUPolicy<S = RandomState> {
    pub(crate) inner: Arc<Mutex<PolicyInner<S>>>,
    pub(crate) processor_handle: JoinHandle<()>,
    pub(crate) items_tx: UnboundedSender<Vec<u64>>,
    pub(crate) stop_tx: Sender<()>,
    pub(crate) is_closed: AtomicBool,
    pub(crate) metrics: Arc<Metrics>,
}

impl LFUPolicy {
    pub(crate) fn new(ctrs: usize, max_cost: i64) -> Result<Self, CacheError> {
        Self::with_hasher(ctrs, max_cost, RandomState::default())
    }
}

impl<S: BuildHasher + Clone + 'static> LFUPolicy<S> {
    pub fn with_hasher(ctrs: usize, max_cost: i64, hasher: S) -> Result<Self, CacheError> {
        let inner = PolicyInner::with_hasher(ctrs, max_cost, hasher)?;

        let (items_tx, items_rx) = unbounded_channel();
        let (stop_tx, stop_rx) = channel(1);

        let handle = PolicyProcessor::spawn(inner.clone(), items_rx, stop_rx);

        let this = Self {
            inner: inner.clone(),
            processor_handle: handle,
            items_tx,
            stop_tx,
            is_closed: AtomicBool::new(false),
            metrics: Arc::new(Metrics::Noop),
        };

        Ok(this)
    }

    pub async fn push(&self, keys: Vec<u64>) -> Result<bool, CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let num_of_keys = keys.len() as u64;
        if num_of_keys == 0 {
            return Ok(true);
        }
        let first = keys[0];

        tokio::select! {
            rst = async { self.items_tx.send(keys) } => rst.map(|_| {
                self.metrics.add(MetricType::KeepGets, first, num_of_keys);
                true
            })
            .map_err(|e| {
                self.metrics.add(MetricType::DropGets, first, num_of_keys);
                CacheError::SendError(format!("sending on a disconnected channel, msg: {:?}", e))
            }),
            else => {
                self.metrics.add(MetricType::DropGets, first, num_of_keys);
                return Ok(false);
            }
        }
    }

    pub async fn close(&self) -> Result<(), CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        // block until the Processor thread returns.
        self.stop_tx
            .send(())
            .await
            .map_err(|e| CacheError::SendError(format!("{}", e)))?;
        self.is_closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct PolicyProcessor<S: BuildHasher + Clone + 'static> {
    inner: Arc<Mutex<PolicyInner<S>>>,
    items_rx: UnboundedReceiver<Vec<u64>>,
    stop_rx: Receiver<()>,
}

impl<S: BuildHasher + Clone + 'static> PolicyProcessor<S> {
    fn spawn(
        inner: Arc<Mutex<PolicyInner<S>>>,
        items_rx: UnboundedReceiver<Vec<u64>>,
        stop_rx: Receiver<()>,
    ) -> JoinHandle<()> {
        let mut this = Self {
            inner,
            items_rx,
            stop_rx,
        };

        spawn(async {
            loop {
                select! {
                    items = this.items_rx.recv() => this.handle_items(items),
                    _ = this.stop_rx.recv() => {
                        drop(this);
                        return;
                    },
                }
            }
        })
    }

    // TODO: None handle
    fn handle_items(&self, items: Option<Vec<u64>>) {
        match items {
            Some(items) => {
                let mut inner = self.inner.lock();
                inner.admit.increments(items);
            }
            None => {
                error!("policy processor error")
            }
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
    use std::time::Duration;
    use tokio::time::sleep;

    static WAIT: Duration = Duration::from_millis(100);

    #[tokio::test]
    async fn test_policy() {
        let _ = LFUPolicy::new(100, 10);
    }

    #[tokio::test]
    async fn test_policy_metrics() {
        let mut p = LFUPolicy::new(100, 10).unwrap();
        p.collect_metrics(Arc::new(Metrics::new_op()));
        assert!(p.metrics.is_op());
        assert!(p.inner.lock().costs.metrics.is_op());
    }

    #[tokio::test]
    async fn test_policy_process_items() {
        let p = LFUPolicy::new(100, 10).unwrap();

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
        let p = LFUPolicy::new(100, 10).unwrap();
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

    #[tokio::test]
    async fn test_policy_has() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        assert!(p.contains(&1));
        assert!(!p.contains(&2));
    }

    #[tokio::test]
    async fn test_policy_del() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        p.remove(&1);
        p.remove(&2);
        assert!(!p.contains(&1));
        assert!(!p.contains(&2));
    }

    #[tokio::test]
    async fn test_policy_cap() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        assert_eq!(p.cap(), 9);
    }

    #[tokio::test]
    async fn test_policy_update() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        p.update(&1, 2);
        let inner = p.inner.lock();
        assert_eq!(inner.costs.key_costs.get(&1).unwrap(), &2);
    }

    #[tokio::test]
    async fn test_policy_cost() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 2);
        assert_eq!(p.cost(&1), 2);
        assert_eq!(p.cost(&2), -1);
    }

    #[tokio::test]
    async fn test_policy_clear() {
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

    #[tokio::test]
    async fn test_policy_close() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        p.close().await.unwrap();
        sleep(WAIT).await;
        assert!(p.items_tx.send(vec![1]).is_err())
    }

    #[tokio::test]
    async fn test_policy_push_after_close() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.close().await.unwrap();
        assert!(!p.push(vec![1, 2]).await.unwrap());
    }

    #[tokio::test]
    async fn test_policy_add_after_close() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.close().await.unwrap();
        p.add(1, 1);
    }
}
