use crate::axync::{select, stop_channel, unbounded, Receiver, RecvError, Sender};
use crate::policy::PolicyInner;
use crate::{CacheError, MetricType, Metrics};
use futures::future::{BoxFuture, FutureExt};
use parking_lot::Mutex;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(crate) struct AsyncLFUPolicy<S = RandomState> {
    pub(crate) inner: Arc<Mutex<PolicyInner<S>>>,
    pub(crate) items_tx: Sender<Vec<u64>>,
    pub(crate) stop_tx: Sender<()>,
    pub(crate) is_closed: AtomicBool,
    pub(crate) metrics: Arc<Metrics>,
}

impl AsyncLFUPolicy {
    #[inline]
    pub(crate) fn new<SP, R>(ctrs: usize, max_cost: i64, spawner: SP) -> Result<Self, CacheError>
    where
        SP: Fn(BoxFuture<'static, ()>) -> R + Send + Sync + 'static,
    {
        Self::with_hasher(ctrs, max_cost, RandomState::new(), spawner)
    }
}

impl<S: BuildHasher + Clone + 'static + Send> AsyncLFUPolicy<S> {
    #[inline]
    pub fn with_hasher<SP, R>(
        ctrs: usize,
        max_cost: i64,
        hasher: S,
        spawner: SP,
    ) -> Result<Self, CacheError>
    where
        SP: Fn(BoxFuture<'static, ()>) -> R + Send + Sync + 'static,
    {
        let inner = PolicyInner::with_hasher(ctrs, max_cost, hasher)?;

        let (items_tx, items_rx) = unbounded();
        let (stop_tx, stop_rx) = stop_channel();

        PolicyProcessor::new(inner.clone(), items_rx, stop_rx).spawn(Box::new(move |fut| {
            spawner(fut);
        }));

        let this = Self {
            inner,
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
            rst =  self.items_tx.send(keys).fuse() => rst.map(|_| {
                self.metrics.add(MetricType::KeepGets, first, num_of_keys);
                true
            })
            .map_err(|e| {
                self.metrics.add(MetricType::DropGets, first, num_of_keys);
                CacheError::SendError(format!("sending on a disconnected channel, msg: {:?}", e))
            }),
            default => {
                self.metrics.add(MetricType::DropGets, first, num_of_keys);
                Ok(false)
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
    items_rx: Receiver<Vec<u64>>,
    stop_rx: Receiver<()>,
}

impl<S: BuildHasher + Clone + 'static + Send> PolicyProcessor<S> {
    #[inline]
    fn new(
        inner: Arc<Mutex<PolicyInner<S>>>,
        items_rx: Receiver<Vec<u64>>,
        stop_rx: Receiver<()>,
    ) -> Self {
        Self {
            inner,
            items_rx,
            stop_rx,
        }
    }

    #[inline]
    fn spawn(self, spawner: Box<dyn Fn(BoxFuture<'static, ()>) + Send + Sync>) {
        (spawner)(Box::pin(async move {
            loop {
                select! {
                    items = self.items_rx.recv().fuse() => self.handle_items(items),
                    _ = self.stop_rx.recv().fuse() => {
                        drop(self);
                        return;
                    },
                }
            }
        }))
    }

    // TODO: None handle
    #[inline]
    fn handle_items(&self, items: Result<Vec<u64>, RecvError>) {
        match items {
            Ok(items) => {
                let mut inner = self.inner.lock();
                inner.admit.increments(items);
            }
            Err(_) => {
                // error!("policy processor error")
            }
        }
    }
}

unsafe impl<S: BuildHasher + Clone + 'static + Send> Send for PolicyProcessor<S> {}
unsafe impl<S: BuildHasher + Clone + 'static + Send + Sync> Sync for PolicyProcessor<S> {}

impl_policy!(AsyncLFUPolicy);
