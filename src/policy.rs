use crate::metrics::MetricType;
use crate::{bbloom::Bloom, error::CacheError, metrics::Metrics, sketch::CountMinSketch};
use crossbeam::{
    channel::{bounded, unbounded, Receiver, RecvError, Sender},
    thread::scope,
};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{hash_map::RandomState, HashMap},
    hash::BuildHasher,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

/// DEFAULT_SAMPLES is the number of items to sample when looking at eviction
/// candidates. 5 seems to be the most optimal number [citation needed].
const DEFAULT_SAMPLES: usize = 5;

pub(crate) struct LFUPolicy {
    inner: Arc<Mutex<PolicyInner>>,
    // processor_handle: ScopedJoinHandle<>
    items_tx: Sender<Vec<u64>>,
    stop_tx: Sender<()>,
    is_closed: bool,
    metrics: Option<Arc<Metrics>>,
}

impl LFUPolicy {
    pub(crate) fn new(ctrs: usize, max_cost: i64) -> Result<Self, CacheError> {
        let inner = PolicyInner::new(ctrs, max_cost)?;

        let (items_tx, items_rx) = unbounded();
        let (stop_tx, stop_rx) = bounded(1);

        PolicyProcessor::spawn(inner.clone(), items_rx, stop_rx);

        let this = Self {
            inner: inner.clone(),
            items_tx,
            stop_tx,
            is_closed: false,
            metrics: None,
        };

        Ok(this)
    }

    pub fn collect_metrics(&mut self, metrics: Arc<Metrics>) {
        self.metrics = Some(metrics.clone());
        self.inner.get_mut().set_metrics(metrics);
    }
}

struct PolicyInner {
    admit: TinyLFU,
    costs: SampledLFU,
}

impl PolicyInner {
    fn new(ctrs: usize, max_cost: i64) -> Result<Arc<Mutex<Self>>, CacheError> {
        let this = Self {
            admit: TinyLFU::new(ctrs)?,
            costs: SampledLFU::new(max_cost),
        };
        Ok(Arc::new(Mutex::new(this)))
    }

    fn set_metrics(&mut self, metrics: Arc<Metrics>) {
        self.costs.metrics = Some(metrics);
    }
}

struct PolicyProcessor {
    inner: Arc<Mutex<PolicyInner>>,
    items_rx: Receiver<Vec<u64>>,
    stop_rx: Receiver<()>,
}

impl PolicyProcessor {
    fn spawn(inner: Arc<Mutex<PolicyInner>>, items_rx: Receiver<Vec<u64>>, stop_rx: Receiver<()>) {
        let this = Self {
            inner,
            items_rx,
            stop_rx,
        };

        let _ = scope(|s| {
            s.spawn(|_| loop {
                select! {
                    recv(this.items_rx) -> items => this.handle_items(items),
                    recv(this.stop_rx) -> _ => return,
                }
            });
        });
    }

    fn handle_items(&self, items: Result<Vec<u64>, RecvError>) {
        match items {
            Ok(items) => {
                let mut inner = self.inner.lock();
                inner.admit.increments(items);
            }
            Err(e) => error!("policy processor error: {}", e),
        }
    }
}

/// SampledLFU stores key-costs paris.
///
/// SampledLFU is thread-safe.
struct SampledLFU<S = RandomState> {
    samples: usize,
    max_cost: AtomicI64,
    used: AtomicI64,
    key_costs: RwLock<HashMap<u64, i64, S>>,
    metrics: Option<Arc<Metrics>>,
}

impl SampledLFU {
    /// Create a new SampledLFU
    pub fn new(max_cost: i64) -> Self {
        Self {
            samples: DEFAULT_SAMPLES,
            max_cost: AtomicI64::new(max_cost),
            used: AtomicI64::new(0),
            key_costs: RwLock::new(HashMap::new()),
            metrics: None,
        }
    }

    /// Create a new SampledLFU with samples.
    #[inline]
    pub fn with_samples(max_cost: i64, samples: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            samples,
            max_cost: AtomicI64::new(max_cost),
            used: AtomicI64::new(0),
            key_costs: RwLock::new(HashMap::new()),
            metrics: None,
        }
    }
}

impl<S: BuildHasher> SampledLFU<S> {
    /// Create a new SampledLFU with specific hasher
    #[inline]
    pub fn with_hasher(max_cost: i64, hasher: S, metrics: Arc<Metrics>) -> Self {
        Self {
            samples: DEFAULT_SAMPLES,
            max_cost: AtomicI64::new(max_cost),
            used: AtomicI64::new(0),
            key_costs: RwLock::new(HashMap::with_hasher(hasher)),
            metrics: None,
        }
    }

    /// Create a new SampledLFU with samples and hasher
    #[inline]
    pub fn with_samples_and_hasher(
        max_cost: i64,
        samples: usize,
        hasher: S,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            samples,
            max_cost: AtomicI64::new(max_cost),
            used: AtomicI64::new(0),
            key_costs: RwLock::new(HashMap::with_hasher(hasher)),
            metrics: None,
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
        self.get_max_cost() - (self.used.fetch_add(cost, Ordering::SeqCst) + cost)
    }

    /// try to fill the SampledLFU by the given pairs.
    pub fn fill_sample(&mut self, mut pairs: Vec<(u64, i64)>) -> Vec<(u64, i64)> {
        if pairs.len() >= self.samples {
            pairs
        } else {
            for (k, v) in self.key_costs.read().iter() {
                pairs.push((*k, *v));
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
        self.key_costs.write().insert(key, cost);
        self.used.fetch_add(cost, Ordering::SeqCst);
    }

    /// Remove an entry from SampledLFU by hashed key
    #[inline]
    pub fn remove(&mut self, kh: u64) -> Option<i64> {
        self.key_costs.write().remove(&kh).map(|cost| {
            self.used.fetch_sub(cost, Ordering::SeqCst);
            cost
        })
    }

    /// Clear the SampledLFU
    #[inline]
    pub fn clear(&mut self) {
        self.used = AtomicI64::new(0);
        self.key_costs.write().clear();
    }

    /// Update the cost by hashed key. If the provided key in SampledLFU, then update it and return true, otherwise false.
    pub fn update(&mut self, k: u64, cost: i64) -> bool {
        // Update the cost of an existing key, but don't worry about evicting.
        // Evictions will be handled the next time a new item is added
        match self.key_costs.write().get_mut(&k) {
            None => false,
            Some(prev) => {
                let prev_val = *prev;
                if let Some(ref m) = self.metrics {
                    m.add(MetricType::KeyUpdate, k, 1);
                    if prev_val > cost {
                        let diff = (prev_val - cost) as u64 - 1;
                        m.add(MetricType::CostAdd, k, !diff);
                    } else {
                        let diff = (cost - prev_val) as u64;
                        m.add(MetricType::CostAdd, k, diff);
                    }
                }

                self.used.fetch_add(cost - prev_val, Ordering::SeqCst);
                *prev = cost;
                true
            }
        }
    }
}

/// TinyLFU is an admission helper that keeps track of access frequency using
/// tiny (4-bit) counters in the form of a count-min sketch.
pub struct TinyLFU {
    ctr: CountMinSketch,
    doorkeeper: Bloom,
    samples: usize,
    w: usize,
}

impl TinyLFU {
    /// The constructor of TinyLFU
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
    pub fn estimate(&self, kh: u64) -> u64 {
        let mut hits = self.ctr.estimate(kh);
        if self.doorkeeper.contains(kh) {
            hits += 1;
        }
        hits
    }

    /// increment multiple hashed keys, for details, please see [`increment_hash`].
    ///
    /// [`increment`]: struct.TinyLFU.method.increment.html
    pub fn increments(&mut self, khs: Vec<u64>) {
        khs.iter().for_each(|k| self.increment(*k))
    }

    /// See [TinyLFU: A Highly Efficient Cache Admission Policy] §3.2
    ///
    /// [TinyLFU: A Highly Efficient Cache Admission Policy]: https://arxiv.org/pdf/1512.00727.pdf
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
    pub fn clear(&mut self) {
        self.w = 0;
        self.doorkeeper.clear();
        self.ctr.clear();
    }

    /// `contains` checks if bit(s) for entry hash is/are set,
    /// returns true if the hash was added to the TinyLFU.
    pub fn contains(&self, kh: u64) -> bool {
        self.doorkeeper.contains(kh)
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_sampled_lfu_remove() {
        println!("{} {}", !17u64, !0u64);
        let mut lfu = SampledLFU::new(4);
        lfu.increment(1, 1);
        lfu.increment(2, 2);
        assert_eq!(lfu.remove(2), Some(2));
        assert_eq!(lfu.used.load(Ordering::SeqCst), 1);
        assert_eq!(lfu.key_costs.read().get(&2), None);
        assert_eq!(lfu.remove(4), None);
    }

    #[test]
    fn test_sampled_lfu_room() {
        let mut l = SampledLFU::new(16);
        l.increment(1, 1);
        l.increment(2, 2);
        l.increment(3, 3);
        assert_eq!(6, l.room_left(4));
    }

    #[test]
    fn test_sampled_lfu_clear() {
        let mut l = SampledLFU::new(4);
        l.increment(1, 1);
        l.increment(2, 2);
        l.increment(3, 3);
        l.clear();
        assert_eq!(0, l.key_costs.read().len());
        assert_eq!(0, l.used.load(Ordering::SeqCst));
    }

    #[test]
    fn test_sampled_lfu_update() {
        let mut l = SampledLFU::new(5);
        l.increment(1, 1);
        l.increment(2, 2);
        assert!(l.update(1, 2));
        assert_eq!(4, l.used.load(Ordering::SeqCst));
        assert!(l.update(2, 3));
        assert_eq!(5, l.used.load(Ordering::SeqCst));
        assert!(!l.update(3, 3));
    }

    #[test]
    fn test_sampled_lfu_fill_sample() {
        let mut l = SampledLFU::new(16);
        l.increment(4, 4);
        l.increment(5, 5);
        let sample = l.fill_sample(vec![(1, 1), (2, 2), (3, 3)]);
        let k = sample[sample.len() - 1].0;
        assert_eq!(5, sample.len());
        assert_ne!(1, k);
        assert_ne!(2, k);
        assert_ne!(3, k);
        assert_eq!(sample.len(), l.fill_sample(sample.clone()).len());
        l.remove(5);
        let sample = l.fill_sample(sample[0..(sample.len() - 2)].to_vec());
        assert_eq!(4, sample.len())
    }

    #[test]
    fn test_tinylfu_increment() {
        let mut l = TinyLFU::new(4).unwrap();
        l.increment(1);
        l.increment(1);
        l.increment(1);
        assert!(l.doorkeeper.contains(1));
        assert_eq!(l.ctr.estimate(1), 2);

        l.increment(1);
        assert!(!l.doorkeeper.contains(1));
        assert_eq!(l.ctr.estimate(1), 1);
    }

    #[test]
    fn test_tinylfu_estimate() {
        let mut l = TinyLFU::new(8).unwrap();
        l.increment(1);
        l.increment(1);
        l.increment(1);

        assert_eq!(l.estimate(1), 3);
        assert_eq!(l.estimate(2), 0);
        assert_eq!(l.w, 3);
    }

    #[test]
    fn test_tinylfu_increments() {
        let mut l = TinyLFU::new(16).unwrap();

        assert_eq!(l.samples, 16);
        l.increments([1, 2, 2, 3, 3, 3].to_vec());
        assert_eq!(l.estimate(1), 1);
        assert_eq!(l.estimate(2), 2);
        assert_eq!(l.estimate(3), 3);
        assert_eq!(6, l.w);
    }

    #[test]
    fn test_tinylfu_clear() {
        let mut l = TinyLFU::new(16).unwrap();
        l.increments([1, 3, 3, 3].to_vec());
        l.clear();
        assert_eq!(0, l.w);
        assert_eq!(0, l.estimate(3));
    }
}
