use crate::policy::PolicyInner;
use crate::sync::{JoinHandle, Receiver, Sender, select, spawn, stop_channel};
use crate::{CacheError, MetricType, Metrics};
use crossbeam_channel::{RecvError, bounded};
use parking_lot::Mutex;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) struct LFUPolicy<S = RandomState> {
  pub(crate) inner: Arc<Mutex<PolicyInner<S>>>,
  pub(crate) items_tx: Sender<Vec<u64>>,
  pub(crate) stop_tx: Sender<()>,
  pub(crate) is_closed: AtomicBool,
  pub(crate) close_lock: Mutex<()>,
  pub(crate) metrics: Arc<Metrics>,
}

impl LFUPolicy {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn new(ctrs: usize, max_cost: i64) -> Result<Self, CacheError> {
    Self::with_hasher(ctrs, max_cost, RandomState::new())
  }
}

impl<S: BuildHasher + Clone + 'static> LFUPolicy<S> {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn with_hasher(ctrs: usize, max_cost: i64, hasher: S) -> Result<Self, CacheError> {
    let inner = PolicyInner::with_hasher(ctrs, max_cost, hasher)?;

    let (items_tx, items_rx) = bounded(3);
    let (stop_tx, stop_rx) = stop_channel();

    PolicyProcessor::new(inner.clone(), items_rx, stop_rx).spawn();

    let this = Self {
      inner,
      items_tx,
      stop_tx,
      is_closed: AtomicBool::new(false),
      close_lock: Mutex::new(()),
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
            Ok(false)
        }
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn close(&self) -> Result<(), CacheError> {
    if self.is_closed.load(Ordering::Acquire) {
      return Ok(());
    }

    let _guard = self.close_lock.lock();
    if self.is_closed.load(Ordering::Acquire) {
      return Ok(());
    }

    // block until the Processor thread returns.
    self
      .stop_tx
      .send(())
      .map_err(|e| CacheError::SendError(format!("{}", e)))?;

    self.is_closed.store(true, Ordering::Release);
    Ok(())
  }
}

pub(crate) struct PolicyProcessor<S> {
  inner: Arc<Mutex<PolicyInner<S>>>,
  items_rx: Receiver<Vec<u64>>,
  stop_rx: Receiver<()>,
}

impl<S: BuildHasher + Clone + 'static> PolicyProcessor<S> {
  #[cfg_attr(not(tarpaulin), inline(always))]
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

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn spawn(self) -> JoinHandle<()> {
    spawn(move || {
      loop {
        select! {
            recv(self.items_rx) -> items => self.handle_items(items),
            recv(self.stop_rx) -> _ => {
                drop(self);
                return;
            },
        }
      }
    })
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn handle_items(&self, items: Result<Vec<u64>, RecvError>) {
    match items {
      Ok(items) => {
        let mut inner = self.inner.lock();
        inner.admit.increments(items);
      }
      Err(e) => {
        tracing::error!("policy processor error: {}", e);
      }
    }
  }
}

unsafe impl<S: BuildHasher + Clone + 'static> Send for PolicyProcessor<S> {}
unsafe impl<S: BuildHasher + Clone + 'static> Sync for PolicyProcessor<S> {}

impl_policy!(LFUPolicy);
