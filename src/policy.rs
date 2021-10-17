cfg_not_async!(
    use crossbeam_channel::RecvError;
);
#[cfg(test)]
mod test;

use crate::{
    bbloom::Bloom,
    error::CacheError,
    metrics::{MetricType, Metrics},
    select,
    sketch::CountMinSketch,
    spawn, stop_channel, unbounded, JoinHandle, Receiver, Sender, UnboundedReceiver,
    UnboundedSender,
};
use parking_lot::Mutex;
use std::{
    collections::{hash_map::RandomState, HashMap},
    hash::BuildHasher,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
};

/// DEFAULT_SAMPLES is the number of items to sample when looking at eviction
/// candidates. 5 seems to be the most optimal number [citation needed].
const DEFAULT_SAMPLES: usize = 5;

#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct PolicyPair {
    pub(crate) key: u64,
    pub(crate) cost: i64,
}

impl PolicyPair {
    #[inline]
    fn new(k: u64, c: i64) -> Self {
        Self { key: k, cost: c }
    }
}

impl Into<PolicyPair> for (u64, i64) {
    fn into(self) -> PolicyPair {
        PolicyPair {
            key: self.0,
            cost: self.1,
        }
    }
}

struct PolicyInner<S = RandomState> {
    admit: TinyLFU,
    costs: SampledLFU<S>,
}

pub(crate) struct LFUPolicy<S = RandomState> {
    inner: Arc<Mutex<PolicyInner<S>>>,
    items_tx: UnboundedSender<Vec<u64>>,
    stop_tx: Sender<()>,
    is_closed: AtomicBool,
    metrics: Arc<Metrics>,
}

impl LFUPolicy {
    #[inline]
    pub(crate) fn new(ctrs: usize, max_cost: i64) -> Result<Self, CacheError> {
        Self::with_hasher(ctrs, max_cost, RandomState::new())
    }
}

impl<S: BuildHasher + Clone + 'static> PolicyInner<S> {
    #[inline]
    fn set_metrics(&mut self, metrics: Arc<Metrics>) {
        self.costs.metrics = metrics;
    }

    #[inline]
    fn with_hasher(ctrs: usize, max_cost: i64, hasher: S) -> Result<Arc<Mutex<Self>>, CacheError> {
        let this = Self {
            admit: TinyLFU::new(ctrs)?,
            costs: SampledLFU::with_hasher(max_cost, hasher),
        };
        Ok(Arc::new(Mutex::new(this)))
    }
}

unsafe impl<S: BuildHasher + Clone + 'static> Send for PolicyInner<S> {}
unsafe impl<S: BuildHasher + Clone + 'static> Sync for PolicyInner<S> {}

impl<S: BuildHasher + Clone + 'static> LFUPolicy<S> {
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

    cfg_async! {
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

    cfg_not_async! {
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

        #[inline]
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

    #[inline]
    pub fn collect_metrics(&mut self, metrics: Arc<Metrics>) {
        self.metrics = metrics.clone();
        self.inner.lock().set_metrics(metrics);
    }

    pub fn add(&self, key: u64, cost: i64) -> (Option<Vec<PolicyPair>>, bool) {
        let mut inner = self.inner.lock();
        let max_cost = inner.costs.get_max_cost();

        // cannot ad an item bigger than entire cache
        if cost > max_cost {
            return (None, false);
        }

        // no need to go any further if the item is already in the cache
        if inner.costs.update(&key, cost) {
            // an update does not count as an addition, so return false.
            return (None, false);
        }

        // If the execution reaches this point, the key doesn't exist in the cache.
        // Calculate the remaining room in the cache (usually bytes).
        let mut room = inner.costs.room_left(cost);
        if room >= 0 {
            // There's enough room in the cache to store the new item without
            // overflowing. Do that now and stop here.
            inner.costs.increment(key, cost);
            self.metrics.add(MetricType::CostAdd, key, cost as u64);
            return (None, true);
        }

        // inc_hits is the hit count for the incoming item
        let inc_hits = inner.admit.estimate(key);
        // sample is the eviction candidate pool to be filled via random sampling.
        // TODO: perhaps we should use a min heap here. Right now our time
        // complexity is N for finding the min. Min heap should bring it down to
        // O(lg N).
        let mut sample = Vec::with_capacity(DEFAULT_SAMPLES);
        let mut victims = Vec::new();

        // Delete victims until there's enough space or a minKey is found that has
        // more hits than incoming item.
        while room < 0 {
            // fill up empty slots in sample
            sample = inner.costs.fill_sample(sample);

            // find minimally used item in sample
            let (mut min_key, mut min_hits, mut min_id, mut min_cost) = (0u64, i64::MAX, 0, 0i64);

            sample.iter().enumerate().for_each(|(idx, pair)| {
                // Look up hit count for sample key.
                let hits = inner.admit.estimate(pair.key);
                if hits < min_hits {
                    min_key = pair.key;
                    min_hits = hits;
                    min_id = idx;
                    min_cost = pair.cost;
                }
            });

            // If the incoming item isn't worth keeping in the policy, reject.
            if inc_hits < min_hits {
                self.metrics.add(MetricType::RejectSets, key, 1);
                return (Some(victims), false);
            }

            // Delete the victim from metadata.
            inner.costs.remove(&min_key).map(|cost| {
                self.metrics
                    .add(MetricType::CostEvict, min_key, cost as u64);
                self.metrics.add(MetricType::KeyEvict, min_key, 1);
            });

            // Delete the victim from sample.
            let new_len = sample.len() - 1;
            sample[min_id] = sample[new_len];
            sample.drain(new_len..);
            // store victim in evicted victims slice
            victims.push(PolicyPair::new(min_key, min_cost));

            room = inner.costs.room_left(cost);
        }

        inner.costs.increment(key, cost);
        self.metrics.add(MetricType::CostAdd, key, cost as u64);
        (Some(victims), true)
    }

    #[inline]
    pub fn contains(&self, k: &u64) -> bool {
        let inner = self.inner.lock();
        inner.costs.contains(k)
    }

    #[inline]
    pub fn remove(&self, k: &u64) {
        let mut inner = self.inner.lock();
        inner.costs.remove(k).map(|cost| {
            self.metrics.add(MetricType::CostEvict, *k, cost as u64);
            self.metrics.add(MetricType::KeyEvict, *k, 1);
        });
    }

    #[inline]
    pub fn cap(&self) -> i64 {
        let inner = self.inner.lock();
        inner.costs.get_max_cost() - inner.costs.used
    }

    #[inline]
    pub fn update(&self, k: &u64, cost: i64) {
        let mut inner = self.inner.lock();
        inner.costs.update(k, cost);
    }

    #[inline]
    pub fn cost(&self, k: &u64) -> i64 {
        let inner = self.inner.lock();
        inner.costs.key_costs.get(k).map_or(-1, |cost| *cost)
    }

    #[inline]
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.admit.clear();
        inner.costs.clear();
    }

    #[inline]
    pub fn max_cost(&self) -> i64 {
        let inner = self.inner.lock();
        inner.costs.get_max_cost()
    }

    #[inline]
    pub fn update_max_cost(&self, mc: i64) {
        let inner = self.inner.lock();
        inner.costs.update_max_cost(mc)
    }
}

unsafe impl<S: BuildHasher + Clone + 'static> Send for LFUPolicy<S> {}
unsafe impl<S: BuildHasher + Clone + 'static> Sync for LFUPolicy<S> {}

struct PolicyProcessor<S: BuildHasher + Clone + 'static> {
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

    cfg_async! {
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

    cfg_not_async! {
        #[inline]
        fn spawn(self) -> JoinHandle<()> {
            spawn(move || loop {
                select! {
                        recv(self.items_rx) -> items => self.handle_items(items),
                        recv(self.stop_rx) -> _ => {
                            drop(self);
                            return;
                        },
                    }
            })
        }

        #[inline]
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
}

unsafe impl<S: BuildHasher + Clone + 'static> Send for PolicyProcessor<S> {}
unsafe impl<S: BuildHasher + Clone + 'static> Sync for PolicyProcessor<S> {}

/// SampledLFU stores key-costs paris.
pub(crate) struct SampledLFU<S = RandomState> {
    samples: usize,
    max_cost: AtomicI64,
    used: i64,
    key_costs: HashMap<u64, i64, S>,
    metrics: Arc<Metrics>,
}

impl SampledLFU {
    /// Create a new SampledLFU
    pub fn new(max_cost: i64) -> Self {
        Self {
            samples: DEFAULT_SAMPLES,
            max_cost: AtomicI64::new(max_cost),
            used: 0,
            key_costs: HashMap::new(),
            metrics: Arc::new(Metrics::new()),
        }
    }

    /// Create a new SampledLFU with samples.
    #[inline]
    pub fn with_samples(max_cost: i64, samples: usize) -> Self {
        Self {
            samples,
            max_cost: AtomicI64::new(max_cost),
            used: 0,
            key_costs: HashMap::new(),
            metrics: Arc::new(Metrics::new()),
        }
    }
}

impl<S: BuildHasher + Clone + 'static> SampledLFU<S> {
    /// Create a new SampledLFU with specific hasher
    #[inline]
    pub fn with_hasher(max_cost: i64, hasher: S) -> Self {
        Self {
            samples: DEFAULT_SAMPLES,
            max_cost: AtomicI64::new(max_cost),
            used: 0,
            key_costs: HashMap::with_hasher(hasher),
            metrics: Arc::new(Metrics::Noop),
        }
    }

    /// Create a new SampledLFU with samples and hasher
    #[inline]
    pub fn with_samples_and_hasher(max_cost: i64, samples: usize, hasher: S) -> Self {
        Self {
            samples,
            max_cost: AtomicI64::new(max_cost),
            used: 0,
            key_costs: HashMap::with_hasher(hasher),
            metrics: Arc::new(Metrics::Noop),
        }
    }

    /// Update the max_cost
    #[inline]
    pub fn update_max_cost(&self, mc: i64) {
        self.max_cost.store(mc, Ordering::SeqCst);
    }

    /// get the max_cost
    #[inline]
    pub fn get_max_cost(&self) -> i64 {
        self.max_cost.load(Ordering::SeqCst)
    }

    /// get the remain space of SampledLRU
    #[inline]
    pub fn room_left(&self, cost: i64) -> i64 {
        self.get_max_cost() - (self.used + cost)
    }

    /// try to fill the SampledLFU by the given pairs.
    pub fn fill_sample(&mut self, mut pairs: Vec<PolicyPair>) -> Vec<PolicyPair> {
        if pairs.len() >= self.samples {
            pairs
        } else {
            for (k, v) in self.key_costs.iter() {
                pairs.push(PolicyPair::new(*k, *v));
                if pairs.len() >= self.samples {
                    return pairs;
                }
            }
            pairs
        }
    }

    /// Put a hashed key and cost to SampledLFU
    #[inline]
    pub fn increment(&mut self, key: u64, cost: i64) {
        self.key_costs.insert(key, cost);
        self.used += cost;
    }

    /// Remove an entry from SampledLFU by hashed key
    #[inline]
    pub fn remove(&mut self, kh: &u64) -> Option<i64> {
        self.key_costs.remove(kh).map(|cost| {
            self.used -= cost;
            cost
        })
    }

    #[inline]
    pub fn contains(&self, k: &u64) -> bool {
        self.key_costs.contains_key(k)
    }

    /// Clear the SampledLFU
    #[inline]
    pub fn clear(&mut self) {
        self.used = 0;
        self.key_costs.clear();
    }

    /// Update the cost by hashed key. If the provided key in SampledLFU, then update it and return true, otherwise false.
    #[inline]
    pub fn update(&mut self, k: &u64, cost: i64) -> bool {
        // Update the cost of an existing key, but don't worry about evicting.
        // Evictions will be handled the next time a new item is added
        match self.key_costs.get_mut(k) {
            None => false,
            Some(prev) => {
                let prev_val = *prev;
                let k = *k;
                if self.metrics.is_op() {
                    self.metrics.add(MetricType::KeyUpdate, k, 1);
                    if prev_val > cost {
                        let diff = (prev_val - cost) as u64 - 1;
                        self.metrics.add(MetricType::CostAdd, k, !diff);
                    } else if cost > prev_val {
                        let diff = (cost - prev_val) as u64;
                        self.metrics.add(MetricType::CostAdd, k, diff);
                    }
                }

                self.used += cost - prev_val;
                *prev = cost;
                true
            }
        }
    }
}

unsafe impl<S: BuildHasher + Clone + 'static> Send for SampledLFU<S> {}
unsafe impl<S: BuildHasher + Clone + 'static> Sync for SampledLFU<S> {}

/// TinyLFU is an admission helper that keeps track of access frequency using
/// tiny (4-bit) counters in the form of a count-min sketch.
pub(crate) struct TinyLFU {
    ctr: CountMinSketch,
    doorkeeper: Bloom,
    samples: usize,
    w: usize,
}

impl TinyLFU {
    /// The constructor of TinyLFU
    #[inline]
    pub fn new(num_ctrs: usize) -> Result<Self, CacheError> {
        Ok(Self {
            ctr: CountMinSketch::new(num_ctrs as u64)?,
            doorkeeper: Bloom::new(num_ctrs, 0.01),
            samples: num_ctrs,
            w: 0,
        })
    }

    /// estimates the frequency.of key hash
    ///
    /// # Details
    /// Explanation from [TinyLFU: A Highly Efficient Cache Admission Policy §3.4.2]:
    /// - When querying items, we use both the Doorkeeper and the main structures.
    /// That is, if the item is included in the Doorkeeper,
    /// TinyLFU estimates the frequency of this item as its estimation in the main structure plus 1.
    /// Otherwise, TinyLFU returns just the estimation from the main structure.
    ///
    /// [TinyLFU: A Highly Efficient Cache Admission Policy §3.4.2]: https://arxiv.org/pdf/1512.00727.pdf
    #[inline]
    pub fn estimate(&self, kh: u64) -> i64 {
        let mut hits = self.ctr.estimate(kh);
        if self.doorkeeper.contains(kh) {
            hits += 1;
        }
        hits
    }

    /// increment multiple hashed keys, for details, please see [`increment_hash`].
    ///
    /// [`increment`]: struct.TinyLFU.method.increment.html
    #[inline]
    pub fn increments(&mut self, khs: Vec<u64>) {
        khs.iter().for_each(|k| self.increment(*k))
    }

    /// See [TinyLFU: A Highly Efficient Cache Admission Policy] §3.2
    ///
    /// [TinyLFU: A Highly Efficient Cache Admission Policy]: https://arxiv.org/pdf/1512.00727.pdf
    #[inline]
    pub fn increment(&mut self, kh: u64) {
        // Flip doorkeeper bit if not already done.
        if !self.doorkeeper.contains_or_add(kh) {
            // Increment count-min counter if doorkeeper bit is already set.
            self.ctr.increment(kh);
        }

        self.try_reset();
    }

    /// See [TinyLFU: A Highly Efficient Cache Admission Policy] §3.2 and §3.3
    ///
    /// [TinyLFU: A Highly Efficient Cache Admission Policy]: https://arxiv.org/pdf/1512.00727.pdf
    #[inline]
    pub fn try_reset(&mut self) {
        self.w += 1;
        if self.w >= self.samples {
            self.reset();
        }
    }

    #[inline]
    fn reset(&mut self) {
        // zero out size
        self.w = 0;

        // zero bloom filter bits
        self.doorkeeper.reset();

        // halves count-min counters
        self.ctr.reset();
    }

    /// `clear` is an extension for the original TinyLFU.
    ///
    /// Comparing to [`reset`] halves the all the bits of count-min sketch,
    /// `clear` will set all the bits to zero of count-min sketch
    ///
    /// [`reset`]: struct.TinyLFU.method.reset.html
    #[inline]
    pub fn clear(&mut self) {
        self.w = 0;
        self.doorkeeper.clear();
        self.ctr.clear();
    }

    /// `contains` checks if bit(s) for entry hash is/are set,
    /// returns true if the hash was added to the TinyLFU.
    #[inline]
    pub fn contains(&self, kh: u64) -> bool {
        self.doorkeeper.contains(kh)
    }
}
