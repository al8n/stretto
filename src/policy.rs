#[cfg(test)]
mod test;

use crate::{
  bbloom::Bloom,
  error::CacheError,
  metrics::{MetricType, Metrics},
  sketch::CountMinSketch,
};
use parking_lot::Mutex;
use rand::seq::IteratorRandom;
use std::{
  collections::{HashMap, hash_map::RandomState},
  hash::BuildHasher,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
  },
};

/// DEFAULT_SAMPLES is the number of items to sample when looking at eviction
/// candidates. 5 seems to be the most optimal number [citation needed].
const DEFAULT_SAMPLES: usize = 5;

macro_rules! impl_policy {
  ($policy: ident) => {
    use crate::policy::{AddOutcome, DEFAULT_SAMPLES, PolicyPair};

    impl<S: BuildHasher + Clone + 'static> $policy<S> {
      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn collect_metrics(&mut self, metrics: Arc<Metrics>) {
        self.metrics = metrics.clone();
        self.inner.lock().set_metrics(metrics);
      }

      pub fn add(&self, key: u64, cost: i64) -> AddOutcome {
        let mut inner = self.inner.lock();
        let max_cost = inner.costs.get_max_cost();

        // Negative costs are a programming error from a custom `Coster`. They
        // would drive `SampledLFU::used` negative, permanently corrupt the
        // budget (every subsequent admission compares against a lower `used`
        // than reality), and produce `room_left` values that let the cache
        // exceed its own cost bound. Reject at the boundary.
        if cost < 0 || cost > max_cost {
          return AddOutcome::RejectedByCost;
        }

        if inner.costs.update(&key, cost) {
          return AddOutcome::UpdatedExisting;
        }

        let mut room = inner.costs.room_left(cost);
        if room >= 0 {
          inner.costs.increment(key, cost);
          self.metrics.add(MetricType::CostAdd, key, cost as u64);
          return AddOutcome::Admitted {
            victims: Vec::new(),
          };
        }

        let inc_hits = self.admit.estimate(key);
        // TODO: perhaps we should use a min heap here. Right now our time
        // complexity is N for finding the min. Min heap should bring it down to
        // O(lg N). We try to use std::collections::BinaryHeap, but it is very slower.
        // https://github.com/al8n/stretto/pull/6/commits/c3a2a549ad4b033651470774224c583e2322d08a
        let mut sample = Vec::with_capacity(DEFAULT_SAMPLES);
        let mut victims = Vec::new();

        while room < 0 {
          sample = inner.costs.fill_sample(sample);

          let (mut min_key, mut min_hits, mut min_id, mut min_cost) = (0u64, i64::MAX, 0, 0i64);

          sample.iter().enumerate().for_each(|(idx, pair)| {
            let hits = self.admit.estimate(pair.key);
            if hits < min_hits {
              min_key = pair.key;
              min_hits = hits;
              min_id = idx;
              min_cost = pair.cost;
            }
          });

          if inc_hits < min_hits {
            self.metrics.add(MetricType::RejectSets, key, 1);
            return AddOutcome::RejectedBySampling { victims };
          }

          inner.costs.remove(&min_key).map(|cost| {
            self
              .metrics
              .add(MetricType::CostEvict, min_key, cost as u64);
            self.metrics.add(MetricType::KeyEvict, min_key, 1);
          });

          let new_len = sample.len() - 1;
          sample[min_id] = sample[new_len];
          sample.drain(new_len..);
          victims.push(PolicyPair::new(min_key, min_cost));

          room = inner.costs.room_left(cost);
        }

        inner.costs.increment(key, cost);
        self.metrics.add(MetricType::CostAdd, key, cost as u64);
        AddOutcome::Admitted { victims }
      }

      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn contains(&self, k: &u64) -> bool {
        let inner = self.inner.lock();
        inner.costs.contains(k)
      }

      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn remove(&self, k: &u64) {
        let mut inner = self.inner.lock();
        inner.costs.remove(k).map(|cost| {
          self.metrics.add(MetricType::CostEvict, *k, cost as u64);
          self.metrics.add(MetricType::KeyEvict, *k, 1);
        });
      }

      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn cap(&self) -> i64 {
        let inner = self.inner.lock();
        inner.costs.get_max_cost() - inner.costs.used
      }

      /// Updates an existing key's cost. Returns `true` if the key was
      /// present in policy (and its cost was updated), `false` if the key
      /// wasn't tracked. The Update handler needs this distinction to
      /// recover from the MPSC race where our Item::New was skipped by the
      /// `contains_version` gate (a later writer bumped the version before
      /// the New was admitted), leaving our row live in the store but the
      /// key absent from policy. Returning `false` here signals that the
      /// caller must fall through to `policy.add` to admit the row.
      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn update(&self, k: &u64, cost: i64) -> bool {
        let mut inner = self.inner.lock();
        inner.costs.update(k, cost)
      }

      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn cost(&self, k: &u64) -> i64 {
        let inner = self.inner.lock();
        inner.costs.key_costs.get(k).map_or(-1, |cost| *cost)
      }

      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn clear(&self) {
        // Hold the costs lock across both clears so a concurrent `add()`
        // (which also locks costs) cannot observe a half-cleared state —
        // i.e. cleared admit + populated costs would make admission decisions
        // against zero estimates with full budget, biasing toward eviction.
        let mut inner = self.inner.lock();
        self.admit.clear();
        inner.costs.clear();
      }

      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn max_cost(&self) -> i64 {
        let inner = self.inner.lock();
        inner.costs.get_max_cost()
      }

      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn update_max_cost(&self, mc: i64) {
        let inner = self.inner.lock();
        inner.costs.update_max_cost(mc)
      }
    }
  };
}

#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
mod sync;
#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
pub(crate) use sync::LFUPolicy;

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
mod r#async;
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub(crate) use r#async::AsyncLFUPolicy;

pub(crate) struct PolicyInner<S = RandomState> {
  costs: SampledLFU<S>,
}

/// Outcome of `LFUPolicy::add`.
///
/// `policy.add` can end in four distinct states that callers need to
/// disambiguate (e.g. to decide whether to roll back an eager store write).
/// Returning them as a tagged enum avoids the previous `(Option<Vec>, bool)`
/// encoding that collapsed "key already tracked — cost updated" and "rejected"
/// into the same shape and forced callers to probe `policy.contains` to tell
/// them apart.
pub(crate) enum AddOutcome {
  /// Fresh admission. `victims` lists keys evicted from policy metadata to
  /// make room; callers must mirror these removals in the store.
  Admitted { victims: Vec<PolicyPair> },
  /// The key was already tracked; its cost was updated in place. The caller
  /// should leave its eager store write intact — `policy.add` treated it as a
  /// cost update, not a fresh admission.
  UpdatedExisting,
  /// Rejected because `cost > max_cost`. No policy or store state changed.
  RejectedByCost,
  /// Sampled eviction could not admit the new key. `victims` lists any
  /// partial evictions that already happened before the admission lost and
  /// must still be mirrored in the store.
  RejectedBySampling { victims: Vec<PolicyPair> },
}

#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct PolicyPair {
  pub(crate) key: u64,
  pub(crate) cost: i64,
}

impl PolicyPair {
  #[cfg_attr(not(tarpaulin), inline(always))]
  fn new(k: u64, c: i64) -> Self {
    Self { key: k, cost: c }
  }
}

impl From<(u64, i64)> for PolicyPair {
  fn from(pair: (u64, i64)) -> Self {
    Self {
      key: pair.0,
      cost: pair.1,
    }
  }
}

impl<S: BuildHasher + Clone + 'static> PolicyInner<S> {
  #[cfg_attr(not(tarpaulin), inline(always))]
  fn set_metrics(&mut self, metrics: Arc<Metrics>) {
    self.costs.metrics = metrics;
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn with_hasher(max_cost: i64, hasher: S) -> Arc<Mutex<Self>> {
    let this = Self {
      costs: SampledLFU::with_hasher(max_cost, hasher),
    };
    Arc::new(Mutex::new(this))
  }
}

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
  #[cfg_attr(not(tarpaulin), inline(always))]
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
  #[cfg_attr(not(tarpaulin), inline(always))]
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
  #[cfg_attr(not(tarpaulin), inline(always))]
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
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn update_max_cost(&self, mc: i64) {
    self.max_cost.store(mc, Ordering::SeqCst);
  }

  /// get the max_cost
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn get_max_cost(&self) -> i64 {
    self.max_cost.load(Ordering::SeqCst)
  }

  /// get the remain space of SampledLRU
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn room_left(&self, cost: i64) -> i64 {
    self.get_max_cost() - (self.used + cost)
  }

  /// try to fill the SampledLFU by the given pairs.
  ///
  /// Rust's `HashMap` iteration order is deterministic for a given instance,
  /// unlike Go's randomized iteration. Using the raw iterator here would bias
  /// eviction toward the same keys each call and degrade hit ratio. Use
  /// reservoir sampling to pull a uniformly random subset instead.
  pub fn fill_sample(&mut self, mut pairs: Vec<PolicyPair>) -> Vec<PolicyPair> {
    if pairs.len() >= self.samples {
      return pairs;
    }
    let need = self.samples - pairs.len();
    let mut rng = rand::rng();
    for (k, v) in self
      .key_costs
      .iter()
      .map(|(k, v)| (*k, *v))
      .sample(&mut rng, need)
    {
      pairs.push(PolicyPair::new(k, v));
    }
    pairs
  }

  /// Put a hashed key and cost to SampledLFU
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn increment(&mut self, key: u64, cost: i64) {
    self.key_costs.insert(key, cost);
    self.used += cost;
  }

  /// Remove an entry from SampledLFU by hashed key
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn remove(&mut self, kh: &u64) -> Option<i64> {
    self.key_costs.remove(kh).inspect(|&cost| {
      self.used -= cost;
    })
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn contains(&self, k: &u64) -> bool {
    self.key_costs.contains_key(k)
  }

  /// Clear the SampledLFU
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn clear(&mut self) {
    self.used = 0;
    self.key_costs.clear();
  }

  /// Update the cost by hashed key. If the provided key in SampledLFU, then update it and return true, otherwise false.
  /// Returns false for costs that exceed `max_cost`: accepting them would push
  /// `used` past the budget, so the caller must treat it as a rejection and
  /// roll the row back.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn update(&mut self, k: &u64, cost: i64) -> bool {
    // Update the cost of an existing key, but don't worry about evicting.
    // Evictions will be handled the next time a new item is added
    if cost < 0 || cost > self.get_max_cost() {
      return false;
    }
    match self.key_costs.get_mut(k) {
      None => false,
      Some(prev) => {
        let prev_val = *prev;
        let k = *k;
        if self.metrics.is_op() {
          self.metrics.add(MetricType::KeyUpdate, k, 1);
          match prev_val.cmp(&cost) {
            std::cmp::Ordering::Less => {
              let diff = (cost - prev_val) as u64;
              self.metrics.add(MetricType::CostAdd, k, diff);
            }
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => {
              let diff = (prev_val - cost) as u64;
              self.metrics.add(MetricType::CostEvict, k, diff);
            }
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
///
/// All mutating methods take `&self` and use atomic ops internally
/// (`CountMinSketch` is CAS-based, `Bloom` is `fetch_or`-based, `w` is
/// `AtomicUsize`). Concurrent callers do not need an external lock.
pub(crate) struct TinyLFU {
  ctr: CountMinSketch,
  doorkeeper: Bloom,
  samples: usize,
  w: AtomicUsize,
  /// Single-winner guard for `try_reset`. The thread that flips this from
  /// `false` to `true` runs the halving; concurrent callers that find it set
  /// bail out and let their increment leak past `samples` briefly. Without
  /// this guard, multiple threads crossing the threshold would each run the
  /// halving and double-decay the counters.
  resetting: AtomicBool,
}

impl TinyLFU {
  /// The constructor of TinyLFU
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn new(num_ctrs: usize) -> Result<Self, CacheError> {
    Ok(Self {
      ctr: CountMinSketch::new(num_ctrs as u64)?,
      doorkeeper: Bloom::new(num_ctrs, 0.01),
      samples: num_ctrs,
      w: AtomicUsize::new(0),
      resetting: AtomicBool::new(false),
    })
  }

  /// estimates the frequency.of key hash
  ///
  /// # Details
  /// Explanation from [TinyLFU: A Highly Efficient Cache Admission Policy §3.4.2]:
  /// - When querying items, we use both the Doorkeeper and the main structures.
  ///   That is, if the item is included in the Doorkeeper,
  ///   TinyLFU estimates the frequency of this item as its estimation in the main structure plus 1.
  ///   Otherwise, TinyLFU returns just the estimation from the main structure.
  ///
  /// [TinyLFU: A Highly Efficient Cache Admission Policy §3.4.2]: https://arxiv.org/pdf/1512.00727.pdf
  #[cfg_attr(not(tarpaulin), inline(always))]
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
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn increments(&self, khs: Vec<u64>) {
    khs.iter().for_each(|k| self.increment(*k))
  }

  /// See [TinyLFU: A Highly Efficient Cache Admission Policy] §3.2
  ///
  /// [TinyLFU: A Highly Efficient Cache Admission Policy]: https://arxiv.org/pdf/1512.00727.pdf
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn increment(&self, kh: u64) {
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
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn try_reset(&self) {
    if self.w.fetch_add(1, Ordering::Relaxed) + 1 < self.samples {
      return;
    }
    self.reset();
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn reset(&self) {
    // Only one thread runs the halving per crossing. Other threads that race
    // here find `resetting == true` and bail; their bumped `w` will drive a
    // subsequent reset call once we release the guard.
    if self
      .resetting
      .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
      .is_err()
    {
      return;
    }

    // zero bloom filter bits
    self.doorkeeper.reset();

    // halves count-min counters
    self.ctr.reset();

    // zero out size
    self.w.store(0, Ordering::Relaxed);

    self.resetting.store(false, Ordering::Release);
  }

  /// `clear` is an extension for the original TinyLFU.
  ///
  /// Comparing to [`reset`] halves the all the bits of count-min sketch,
  /// `clear` will set all the bits to zero of count-min sketch
  ///
  /// [`reset`]: struct.TinyLFU.method.reset.html
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn clear(&self) {
    self.w.store(0, Ordering::Relaxed);
    self.doorkeeper.clear();
    self.ctr.clear();
  }

  /// `contains` checks if bit(s) for entry hash is/are set,
  /// returns true if the hash was added to the TinyLFU.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn contains(&self, kh: u64) -> bool {
    self.doorkeeper.contains(kh)
  }
}
