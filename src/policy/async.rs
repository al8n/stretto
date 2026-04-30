use crate::{
  CacheError, MetricType, Metrics,
  policy::{PolicyInner, TinyLFU},
};
use parking_lot::Mutex;
use std::{collections::hash_map::RandomState, hash::BuildHasher, sync::Arc};

pub(crate) struct AsyncLFUPolicy<S = RandomState> {
  pub(crate) inner: Arc<Mutex<PolicyInner<S>>>,
  pub(crate) admit: Arc<TinyLFU>,
  pub(crate) metrics: Arc<Metrics>,
}

impl AsyncLFUPolicy {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn new(ctrs: usize, max_cost: i64) -> Result<Self, CacheError> {
    Self::with_hasher(ctrs, max_cost, RandomState::new())
  }
}

impl<S: BuildHasher + Clone + 'static + Send> AsyncLFUPolicy<S> {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn with_hasher(ctrs: usize, max_cost: i64, hasher: S) -> Result<Self, CacheError> {
    let inner = PolicyInner::with_hasher(max_cost, hasher);
    let admit = Arc::new(TinyLFU::new(ctrs)?);

    Ok(Self {
      inner,
      admit,
      metrics: Arc::new(Metrics::new()),
    })
  }
}

impl_policy!(AsyncLFUPolicy);
