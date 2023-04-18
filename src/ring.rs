use std::{hash::BuildHasher, sync::Arc};

use parking_lot::Mutex;

#[cfg(feature = "async")]
use crate::policy::AsyncLFUPolicy;
use crate::policy::LFUPolicy;

pub struct RingStripe<S> {
    cons: Arc<LFUPolicy<S>>,
    data: Mutex<Vec<u64>>,
    capa: usize,
}

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
            match self.cons.push(data.clone()) {
                Ok(true) => *data = Vec::with_capacity(self.capa),
                _ => data.clear(),
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

    pub async fn push(&self, item: u64) {
        let data = {
            let mut data = self.data.lock();
            data.push(item);
            if data.len() >= self.capa {
                let ret = data.clone();
                data.clear();
                ret
            } else {
                return;
            }
        };

        _ = self.cons.push(data).await;
    }
}
