use crate::{
  CacheError, MetricType, Metrics,
  axync::{Receiver, Sender, select, unbounded},
  policy::{PolicyInner, TinyLFU},
};
use agnostic_lite::RuntimeLite;
use futures::future::FutureExt;
use parking_lot::Mutex;
use std::{collections::hash_map::RandomState, hash::BuildHasher, sync::Arc};

pub(crate) struct AsyncLFUPolicy<S = RandomState> {
  pub(crate) inner: Arc<Mutex<PolicyInner<S>>>,
  pub(crate) admit: Arc<TinyLFU>,
  pub(crate) items_tx: Sender<Vec<u64>>,
  pub(crate) metrics: Arc<Metrics>,
}

impl AsyncLFUPolicy {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn new<RT: RuntimeLite>(
    ctrs: usize,
    max_cost: i64,
    stop_rx: Receiver<()>,
  ) -> Result<Self, CacheError> {
    Self::with_hasher::<RT>(ctrs, max_cost, RandomState::new(), stop_rx)
  }
}

impl<S: BuildHasher + Clone + 'static + Send> AsyncLFUPolicy<S> {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn with_hasher<RT: RuntimeLite>(
    ctrs: usize,
    max_cost: i64,
    hasher: S,
    stop_rx: Receiver<()>,
  ) -> Result<Self, CacheError> {
    let inner = PolicyInner::with_hasher(max_cost, hasher);
    let admit = Arc::new(TinyLFU::new(ctrs)?);

    let (items_tx, items_rx) = unbounded();

    PolicyProcessor::new(admit.clone(), items_rx, stop_rx).spawn::<RT>();

    let this = Self {
      inner,
      admit,
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

pub(crate) struct PolicyProcessor {
  admit: Arc<TinyLFU>,
  items_rx: Receiver<Vec<u64>>,
  stop_rx: Receiver<()>,
}

impl PolicyProcessor {
  #[cfg_attr(not(tarpaulin), inline(always))]
  fn new(admit: Arc<TinyLFU>, items_rx: Receiver<Vec<u64>>, stop_rx: Receiver<()>) -> Self {
    Self {
      admit,
      items_rx,
      stop_rx,
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn spawn<RT: RuntimeLite>(self) {
    RT::spawn_detach(async move {
      loop {
        select! {
          items = self.items_rx.recv().fuse() => {
            if let Ok(items) = items {
              self.handle_items(items);
            } else {
              // Channel closed, so no more items will be received. Exit the loop.
              return;
            }
          },
          _ = self.stop_rx.recv().fuse() => return,
        }
      }
    });
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn handle_items(&self, items: Vec<u64>) {
    self.admit.increments(items);
  }
}

impl_policy!(AsyncLFUPolicy);
