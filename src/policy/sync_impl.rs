use crate::metrics::{MetricType, Metrics};
use crate::policy::PolicyInner;
use crate::CacheError;
use crossbeam_channel::{bounded, select, unbounded, Receiver, RecvError, Sender};
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
            Err(_) => {
                // error!("policy processor error: {}", e)
            }
        }
    }
}

unsafe impl<S: BuildHasher + Clone + 'static> Send for PolicyProcessor<S> {}
unsafe impl<S: BuildHasher + Clone + 'static> Sync for PolicyProcessor<S> {}
