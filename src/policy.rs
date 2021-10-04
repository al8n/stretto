use crate::{
    bbloom::Bloom,
    cache::ItemMeta,
    error::CacheError,
    metrics::{MetricType, Metrics},
    sketch::CountMinSketch,
};
use crossbeam::channel::{bounded, unbounded, Receiver, RecvError, Sender};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{spawn, JoinHandle};
use std::{
    collections::{hash_map::RandomState, HashMap},
    hash::BuildHasher,
    sync::Arc,
};

/// DEFAULT_SAMPLES is the number of items to sample when looking at eviction
/// candidates. 5 seems to be the most optimal number [citation needed].
const DEFAULT_SAMPLES: usize = 5;

#[derive(Copy, Clone, Debug, Default)]
struct PolicyPair {
    key: u64,
    cost: i64,
}

impl PolicyPair {
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

pub(crate) struct LFUPolicy {
    inner: Arc<Mutex<PolicyInner>>,
    processor_handle: JoinHandle<()>,
    items_tx: Sender<Vec<u64>>,
    stop_tx: Sender<()>,
    is_closed: AtomicBool,
    metrics: Option<Arc<Metrics>>,
}

impl LFUPolicy {
    pub(crate) fn new(ctrs: usize, max_cost: i64) -> Result<Self, CacheError> {
        let inner = PolicyInner::new(ctrs, max_cost)?;

        let (items_tx, items_rx) = unbounded();
        let (stop_tx, stop_rx) = bounded(1);

        let handle = PolicyProcessor::spawn(inner.clone(), items_rx, stop_rx);

        let this = Self {
            inner: inner.clone(),
            processor_handle: handle,
            items_tx,
            stop_tx,
            is_closed: AtomicBool::new(false),
            metrics: None,
        };

        Ok(this)
    }

    pub fn collect_metrics(&mut self, metrics: Arc<Metrics>) {
        self.metrics = Some(metrics.clone());
        self.inner.lock().set_metrics(metrics);
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
                    if let Some(ref m) = self.metrics {
                        m.add(MetricType::KeepGets, first, num_of_keys);
                    }
                    true
                })
                .map_err(|e| {
                    CacheError::SendError(format!("sending on a disconnected channel, msg: {:?}", e.0))
                }),
            default => {
                if let Some(ref m) = self.metrics {
                    m.add(MetricType::DropGets, first, num_of_keys);
                }
                return Ok(false);
            }
        }
    }

    pub fn add(&self, key: u64, cost: i64) -> (Option<Vec<ItemMeta>>, bool) {
        let mut inner = self.inner.lock();
        let max_cost = inner.costs.get_max_cost();

        // cannot ad an item bigger than entire cache
        if cost > max_cost {
            return (None, false);
        }

        // no need to go any further if the item is already in the cache
        if inner.costs.update(key, cost) {
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
            if let Some(ref m) = self.metrics {
                m.add(MetricType::CostAdd, key, cost as u64);
            }
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
                if let Some(ref m) = self.metrics {
                    m.add(MetricType::RejectSets, key, 1);
                }
                return (Some(victims), false);
            }

            // Delete the victim from metadata.
            inner.costs.remove(min_key);

            // Delete the victim from sample.
            let new_len = sample.len() - 1;
            sample[min_id] = sample[new_len];
            sample.drain(new_len..);
            // store victim in evicted victims slice
            victims.push(ItemMeta::new(min_key, min_cost, 0));

            room = inner.costs.room_left(cost);
        }

        inner.costs.increment(key, cost);
        if let Some(ref m) = self.metrics {
            m.add(MetricType::CostAdd, key, cost as u64);
        }
        (Some(victims), true)
    }

    pub fn contains(&self, k: u64) -> bool {
        let inner = self.inner.lock();
        inner.costs.contains(k)
    }

    pub fn remove(&self, k: u64) {
        let mut inner = self.inner.lock();
        inner.costs.remove(k);
    }

    pub fn cap(&self) -> i64 {
        let inner = self.inner.lock();
        inner.costs.get_max_cost() - inner.costs.used
    }

    pub fn update(&self, k: u64, cost: i64) {
        let mut inner = self.inner.lock();
        inner.costs.update(k, cost);
    }

    pub fn cost(&self, k: u64) -> i64 {
        let inner = self.inner.lock();
        inner.costs.key_costs.get(&k).map_or(-1, |cost| *cost)
    }

    pub fn clear(&mut self) {
        let mut inner = self.inner.lock();
        inner.admit.clear();
        inner.costs.clear();
    }

    pub fn close(&mut self) {
        if self.is_closed.load(Ordering::SeqCst) {
            return;
        }

        // block until the Processor thread returns.
        self.stop_tx.send(()).unwrap_or(());
        self.is_closed.store(true, Ordering::SeqCst);
    }

    pub fn max_cost(&self) -> i64 {
        let inner = self.inner.lock();
        inner.costs.get_max_cost()
    }

    pub fn update_max_cost(&self, mc: i64) {
        let mut inner = self.inner.lock();
        inner.costs.update_max_cost(mc)
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
    fn spawn(
        inner: Arc<Mutex<PolicyInner>>,
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
            Err(e) => error!("policy processor error: {}", e),
        }
    }
}

/// SampledLFU stores key-costs paris.
struct SampledLFU<S = RandomState> {
    samples: usize,
    max_cost: i64,
    used: i64,
    key_costs: HashMap<u64, i64, S>,
    metrics: Option<Arc<Metrics>>,
}

impl SampledLFU {
    /// Create a new SampledLFU
    pub fn new(max_cost: i64) -> Self {
        Self {
            samples: DEFAULT_SAMPLES,
            max_cost,
            used: 0,
            key_costs: HashMap::new(),
            metrics: None,
        }
    }

    /// Create a new SampledLFU with samples.
    #[inline]
    pub fn with_samples(max_cost: i64, samples: usize) -> Self {
        Self {
            samples,
            max_cost,
            used: 0,
            key_costs: HashMap::new(),
            metrics: None,
        }
    }
}

impl<S: BuildHasher> SampledLFU<S> {
    /// Create a new SampledLFU with specific hasher
    #[inline]
    pub fn with_hasher(max_cost: i64, hasher: S) -> Self {
        Self {
            samples: DEFAULT_SAMPLES,
            max_cost,
            used: 0,
            key_costs: HashMap::with_hasher(hasher),
            metrics: None,
        }
    }

    /// Create a new SampledLFU with samples and hasher
    #[inline]
    pub fn with_samples_and_hasher(max_cost: i64, samples: usize, hasher: S) -> Self {
        Self {
            samples,
            max_cost,
            used: 0,
            key_costs: HashMap::with_hasher(hasher),
            metrics: None,
        }
    }

    /// Update the max_cost
    #[inline]
    pub fn update_max_cost(&mut self, mc: i64) {
        self.max_cost = mc;
    }

    /// get the max_cost
    #[inline]
    pub fn get_max_cost(&self) -> i64 {
        self.max_cost
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
    pub fn remove(&mut self, kh: u64) -> Option<i64> {
        self.key_costs.remove(&kh).map(|cost| {
            self.used -= cost;
            cost
        })
    }

    #[inline]
    pub fn contains(&self, k: u64) -> bool {
        self.key_costs.contains_key(&k)
    }

    /// Clear the SampledLFU
    #[inline]
    pub fn clear(&mut self) {
        self.used = 0;
        self.key_costs.clear();
    }

    /// Update the cost by hashed key. If the provided key in SampledLFU, then update it and return true, otherwise false.
    pub fn update(&mut self, k: u64, cost: i64) -> bool {
        // Update the cost of an existing key, but don't worry about evicting.
        // Evictions will be handled the next time a new item is added
        match self.key_costs.get_mut(&k) {
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

                self.used += cost - prev_val;
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
    use std::thread::sleep;
    use std::time::Duration;

    static WAIT: Duration = Duration::from_millis(100);

    #[test]
    fn test_policy() {
        let _ = LFUPolicy::new(100, 10);
    }

    #[test]
    fn test_policy_metrics() {
        let mut p = LFUPolicy::new(100, 10).unwrap();
        p.collect_metrics(Arc::new(Metrics::new()));
        assert!(p.metrics.is_some());
        assert!(p.inner.lock().costs.metrics.is_some());
    }

    #[test]
    fn test_policy_process_items() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.items_tx.send(vec![1, 2, 2]).unwrap();
        sleep(WAIT);
        let inner = p.inner.lock();
        assert_eq!(inner.admit.estimate(2), 2);
        assert_eq!(inner.admit.estimate(1), 1);
        drop(inner);

        p.stop_tx.send(()).unwrap();
        assert!(p.items_tx.send(vec![3, 3, 3]).is_err());
        sleep(WAIT);
        let inner = p.inner.lock();
        assert_eq!(inner.admit.estimate(3), 0);
    }

    #[test]
    fn test_policy_push() {
        let p = LFUPolicy::new(100, 10).unwrap();
        assert!(p.push(vec![]).unwrap());

        let mut keep_count = 0;
        (0..10).for_each(|_| {
            if p.push(vec![1, 2, 3, 4, 5]).unwrap() {
                keep_count += 1;
            }
        });

        assert_ne!(0, keep_count);
    }

    #[test]
    fn test_policy_add() {
        let p = LFUPolicy::new(1000, 100).unwrap();
        let (victims, added) = p.add(1, 101);
        assert!(victims.is_none());
        assert!(!added);

        let mut inner = p.inner.lock();
        inner.costs.increment(1, 1);
        inner.admit.increment(1);
        inner.admit.increment(2);
        inner.admit.increment(3);
        drop(inner);

        let (victims, added) = p.add(1, 1);
        assert!(victims.is_none());
        assert!(!added);

        let (victims, added) = p.add(2, 20);
        assert!(victims.is_none());
        assert!(added);

        let (victims, added) = p.add(3, 90);
        assert!(victims.is_some());
        assert!(added);

        let (victims, added) = p.add(4, 20);
        assert!(victims.is_some());
        assert!(!added);
    }

    #[test]
    fn test_policy_has() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        assert!(p.contains(1));
        assert!(!p.contains(2));
    }

    #[test]
    fn test_policy_del() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        p.remove(1);
        p.remove(2);
        assert!(!p.contains(1));
        assert!(!p.contains(2));
    }

    #[test]
    fn test_policy_cap() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        assert_eq!(p.cap(), 9);
    }

    #[test]
    fn test_policy_update() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        p.update(1, 2);
        let inner = p.inner.lock();
        assert_eq!(inner.costs.key_costs.get(&1).unwrap(), &2);
    }

    #[test]
    fn test_policy_cost() {
        let p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 2);
        assert_eq!(p.cost(1), 2);
        assert_eq!(p.cost(2), -1);
    }

    #[test]
    fn test_policy_clear() {
        let mut p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        p.add(2, 2);
        p.add(3, 3);
        p.clear();

        assert_eq!(p.cap(), 10);
        assert!(!p.contains(2));
        assert!(!p.contains(2));
        assert!(!p.contains(3));
    }

    #[test]
    fn test_policy_close() {
        let mut p = LFUPolicy::new(100, 10).unwrap();
        p.add(1, 1);
        p.close();
        sleep(WAIT);
        assert!(p.items_tx.send(vec![1]).is_err())
    }

    #[test]
    fn test_policy_push_after_close() {
        let mut p = LFUPolicy::new(100, 10).unwrap();
        p.close();
        assert!(!p.push(vec![1, 2]).unwrap());
    }

    #[test]
    fn test_policy_add_after_close() {
        let mut p = LFUPolicy::new(100, 10).unwrap();
        p.close();
        p.add(1, 1);
    }

    #[test]
    fn test_sampled_lfu_remove() {
        let mut lfu = SampledLFU::new(4);
        lfu.increment(1, 1);
        lfu.increment(2, 2);
        assert_eq!(lfu.remove(2), Some(2));
        assert_eq!(lfu.used, 1);
        assert_eq!(lfu.key_costs.get(&2), None);
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
        assert_eq!(0, l.key_costs.len());
        assert_eq!(0, l.used);
    }

    #[test]
    fn test_sampled_lfu_update() {
        let mut l = SampledLFU::new(5);
        l.increment(1, 1);
        l.increment(2, 2);
        assert!(l.update(1, 2));
        assert_eq!(4, l.used);
        assert!(l.update(2, 3));
        assert_eq!(5, l.used);
        assert!(!l.update(3, 3));
    }

    #[test]
    fn test_sampled_lfu_fill_sample() {
        let mut l = SampledLFU::new(16);
        l.increment(4, 4);
        l.increment(5, 5);
        let sample = l.fill_sample(vec![(1, 1).into(), (2, 2).into(), (3, 3).into()]);
        let k = sample[sample.len() - 1].key;
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
