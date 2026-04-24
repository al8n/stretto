use crate::{
  CacheError, MetricType, Metrics,
  axync::{Receiver, RecvError, Sender, select, stop_channel, unbounded},
  policy::PolicyInner,
};
use agnostic_lite::RuntimeLite;
use futures::{future::FutureExt, lock::Mutex as AsyncMutex};
use parking_lot::Mutex;
use std::{
  collections::hash_map::RandomState,
  hash::BuildHasher,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

pub(crate) struct AsyncLFUPolicy<S = RandomState> {
  pub(crate) inner: Arc<Mutex<PolicyInner<S>>>,
  pub(crate) items_tx: Sender<Vec<u64>>,
  pub(crate) stop_tx: Sender<()>,
  pub(crate) is_closed: AtomicBool,
  pub(crate) close_lock: AsyncMutex<()>,
  pub(crate) metrics: Arc<Metrics>,
}

impl AsyncLFUPolicy {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn new<RT: RuntimeLite>(ctrs: usize, max_cost: i64) -> Result<Self, CacheError> {
    Self::with_hasher::<RT>(ctrs, max_cost, RandomState::new())
  }
}

impl<S: BuildHasher + Clone + 'static + Send> AsyncLFUPolicy<S> {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn with_hasher<RT: RuntimeLite>(
    ctrs: usize,
    max_cost: i64,
    hasher: S,
  ) -> Result<Self, CacheError> {
    let inner = PolicyInner::with_hasher(ctrs, max_cost, hasher)?;

    let (items_tx, items_rx) = unbounded();
    let (stop_tx, stop_rx) = stop_channel();

    PolicyProcessor::new(inner.clone(), items_rx, stop_rx).spawn::<RT>();

    let this = Self {
      inner,
      items_tx,
      stop_tx,
      is_closed: AtomicBool::new(false),
      close_lock: AsyncMutex::new(()),
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
        rst = self.items_tx.send(keys).fuse() => rst.map(|_| {
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

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub async fn close(&self) -> Result<(), CacheError> {
    if self.is_closed.load(Ordering::Acquire) {
      return Ok(());
    }

    let _guard = self.close_lock.lock().await;
    if self.is_closed.load(Ordering::Acquire) {
      return Ok(());
    }

    self
      .stop_tx
      .send(())
      .await
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

impl<S: BuildHasher + Clone + 'static + Send> PolicyProcessor<S> {
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
  fn spawn<RT: RuntimeLite>(self) {
    RT::spawn_detach(async move {
      loop {
        select! {
            items = self.items_rx.recv().fuse() => self.handle_items(items),
            _ = self.stop_rx.recv().fuse() => {
                drop(self);
                return;
            },
        }
      }
    });
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn handle_items(&self, items: Result<Vec<u64>, RecvError>) {
    if let Ok(items) = items {
      let mut inner = self.inner.lock();
      inner.admit.increments(items);
    }
  }
}

unsafe impl<S: BuildHasher + Clone + 'static + Send> Send for PolicyProcessor<S> {}
unsafe impl<S: BuildHasher + Clone + 'static + Send + Sync> Sync for PolicyProcessor<S> {}

impl_policy!(AsyncLFUPolicy);
