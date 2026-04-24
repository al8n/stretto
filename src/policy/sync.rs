use crate::{
  CacheError, MetricType, Metrics,
  policy::PolicyInner,
  sync::{JoinHandle, Receiver, Sender, select, spawn},
};
use crossbeam_channel::bounded;
use parking_lot::Mutex;
use std::{collections::hash_map::RandomState, hash::BuildHasher, sync::Arc};

pub(crate) struct LFUPolicy<S = RandomState> {
  pub(crate) inner: Arc<Mutex<PolicyInner<S>>>,
  pub(crate) items_tx: Sender<Vec<u64>>,
  pub(crate) metrics: Arc<Metrics>,
}

impl LFUPolicy {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn new(ctrs: usize, max_cost: i64, stop_rx: Receiver<()>) -> Result<Self, CacheError> {
    Self::with_hasher(ctrs, max_cost, RandomState::new(), stop_rx)
  }
}

impl<S: BuildHasher + Clone + 'static> LFUPolicy<S> {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn with_hasher(
    ctrs: usize,
    max_cost: i64,
    hasher: S,
    stop_rx: Receiver<()>,
  ) -> Result<Self, CacheError> {
    let inner = PolicyInner::with_hasher(ctrs, max_cost, hasher)?;

    let (items_tx, items_rx) = bounded(3);

    PolicyProcessor::new(inner.clone(), items_rx, stop_rx).spawn();

    let this = Self {
      inner,
      items_tx,
      metrics: Arc::new(Metrics::new()),
    };

    Ok(this)
  }

  pub fn push(&self, keys: Vec<u64>) -> Option<Vec<u64>> {
    let num_of_keys = keys.len() as u64;
    if num_of_keys == 0 {
      return Some(keys);
    }

    let first = keys[0];
    match self.items_tx.try_send(keys) {
      Ok(_) => {
        self.metrics.add(MetricType::KeepGets, first, num_of_keys);
        None
      }
      Err(err) => {
        self.metrics.add(MetricType::DropGets, first, num_of_keys);
        Some(err.into_inner())
      }
    }
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
          recv(self.items_rx) -> items => {
            if let Ok(items) = items {
              self.handle_items(items);
            } else {
              tracing::info!("stretto: items channel closed, policy processor exiting.");
              return;
            }
          },
          recv(self.stop_rx) -> _ => return,
        }
      }
    })
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn handle_items(&self, items: Vec<u64>) {
    let mut inner = self.inner.lock();
    inner.admit.increments(items);
  }
}

unsafe impl<S: BuildHasher + Clone + 'static> Send for PolicyProcessor<S> {}
unsafe impl<S: BuildHasher + Clone + 'static> Sync for PolicyProcessor<S> {}

impl_policy!(LFUPolicy);
