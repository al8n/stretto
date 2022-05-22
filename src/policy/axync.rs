use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use parking_lot::Mutex;
use crate::axync::{Sender, stop_channel, unbounded, UnboundedSender, select, UnboundedReceiver, Receiver, JoinHandle, spawn};
use crate::{CacheError, Metrics, MetricType};
use crate::policy::PolicyInner;

pub(crate) struct AsyncLFUPolicy<S = RandomState> {
    pub(crate) inner: Arc<Mutex<PolicyInner<S>>>,
    pub(crate) items_tx: UnboundedSender<Vec<u64>>,
    pub(crate) stop_tx: Sender<()>,
    pub(crate) is_closed: AtomicBool,
    pub(crate) metrics: Arc<Metrics>,
}

impl<S: BuildHasher + Clone + 'static> AsyncLFUPolicy<S> {
    #[inline]
    pub fn with_hasher(ctrs: usize, max_cost: i64, hasher: S) -> Result<Self, CacheError> {
        let inner = PolicyInner::with_hasher(ctrs, max_cost, hasher)?;

        let (items_tx, items_rx) = unbounded();
        let (stop_tx, stop_rx) = stop_channel();

        PolicyProcessor::new(inner.clone(), items_rx, stop_rx).spawn();

        let this = Self {
            inner: inner.clone(),
            items_tx,
            stop_tx,
            is_closed: AtomicBool::new(false),
            metrics: Arc::new(Metrics::new()),
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

        select! {
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

    #[inline]
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

pub(crate) struct PolicyProcessor<S> {
    inner: Arc<Mutex<PolicyInner<S>>>,
    items_rx: UnboundedReceiver<Vec<u64>>,
    stop_rx: Receiver<()>,
}

impl<S: BuildHasher + Clone + 'static> PolicyProcessor<S> {
    #[inline]
    fn new(
        inner: Arc<Mutex<PolicyInner<S>>>,
        items_rx: UnboundedReceiver<Vec<u64>>,
        stop_rx: Receiver<()>,
    ) -> Self {
        Self {
            inner,
            items_rx,
            stop_rx,
        }
    }


        #[inline]
        fn spawn(mut self) -> JoinHandle<()> {
            spawn(async {
                loop {
                    select! {
                        items = self.items_rx.recv() => self.handle_items(items),
                        _ = self.stop_rx.recv() => {
                            drop(self);
                            return;
                        },
                    }
                }
            })
        }

        // TODO: None handle
        #[inline]
        fn handle_items(&self, items: Option<Vec<u64>>) {
            match items {
                Some(items) => {
                    let mut inner = self.inner.lock();
                    inner.admit.increments(items);
                }
                None => {
                    // error!("policy processor error")
                }
            }
        }

}

unsafe impl<S: BuildHasher + Clone + 'static> Send for PolicyProcessor<S> {}
unsafe impl<S: BuildHasher + Clone + 'static> Sync for PolicyProcessor<S> {}


impl_policy!(AsyncLFUPolicy);