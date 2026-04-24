use std::{hash::BuildHasher, mem::take, sync::Arc};

use parking_lot::Mutex;

#[cfg(feature = "async")]
use crate::policy::AsyncLFUPolicy;
#[cfg(feature = "sync")]
use crate::policy::LFUPolicy;

#[cfg(feature = "sync")]
pub struct RingStripe<S> {
  cons: Arc<LFUPolicy<S>>,
  data: Mutex<Vec<u64>>,
  capa: usize,
}

#[cfg(feature = "sync")]
impl<S> RingStripe<S>
where
  S: BuildHasher + Clone + 'static,
{
  pub(crate) fn new(cons: Arc<LFUPolicy<S>>, capa: usize) -> RingStripe<S> {
    RingStripe {
      cons,
      data: Mutex::new(Vec::with_capacity(capa)),
      capa,
    }
  }

  pub fn push(&self, item: u64) {
    let mut data = self.data.lock();
    data.push(item);
    if data.len() >= self.capa {
      let v = take(&mut *data);
      match self.cons.push(v) {
        Some(mut ret) => {
          ret.clear();
          *data = ret;
        }
        None => *data = Vec::with_capacity(self.capa),
      }
    }
  }
}

#[cfg(feature = "async")]
pub struct AsyncRingStripe<S> {
  cons: Arc<AsyncLFUPolicy<S>>,
  data: Mutex<Vec<u64>>,
  capa: usize,
}

#[cfg(feature = "async")]
impl<S> AsyncRingStripe<S>
where
  S: BuildHasher + Clone + 'static + Send,
{
  pub(crate) fn new(cons: Arc<AsyncLFUPolicy<S>>, capa: usize) -> AsyncRingStripe<S> {
    AsyncRingStripe {
      cons,
      data: Mutex::new(Vec::with_capacity(capa)),
      capa,
    }
  }

  pub fn push(&self, item: u64) {
    let mut data = self.data.lock();
    data.push(item);
    if data.len() >= self.capa {
      let v = take(&mut *data);
      match self.cons.push(v) {
        Some(mut ret) => {
          ret.clear();
          *data = ret;
        }
        None => *data = Vec::with_capacity(self.capa),
      }
    }
  }
}
