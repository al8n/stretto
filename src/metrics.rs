use crate::histogram::Histogram;
use crate::utils::vec_to_array;
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const HISTOGRAM_BOUND_SIZE: usize = 16;

const NUMS_OF_METRIC_TYPE: usize = 11;
const SIZE_FOR_EACH_TYPE: usize = 256;
static METRIC_TYPES_ARRAY: [MetricType; NUMS_OF_METRIC_TYPE] = [
    MetricType::Hit,
    MetricType::Miss,
    MetricType::KeyAdd,
    MetricType::KeyUpdate,
    MetricType::KeyEvict,
    MetricType::CostAdd,
    MetricType::CostEvict,
    MetricType::DropSets,
    MetricType::RejectSets,
    MetricType::DropGets,
    MetricType::KeepGets,
];

/// The data field in a Metrics
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
#[repr(u16)]
pub enum MetricType {
    /// track hits.
    Hit,
    /// track misses
    Miss,

    /// track keys added
    KeyAdd,

    /// track keys updated
    KeyUpdate,

    /// track keys evicted
    KeyEvict,

    /// track the cost of keys added
    CostAdd,

    /// track the cost of keys evicted
    CostEvict,

    /// track how many sets were dropped
    DropSets,

    /// track how many sets were rejected
    RejectSets,

    /// track how many gets were dropped on the floor
    DropGets,

    /// track how many gets were kept on the floor
    KeepGets,

    /// This should be the final enum. Other enums should be set before this.
    #[allow(dead_code)]
    DoNotUse,
}

impl Display for MetricType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricType::Hit => write!(f, "hit"),
            MetricType::Miss => write!(f, "miss"),
            MetricType::KeyAdd => write!(f, "keys-added"),
            MetricType::KeyUpdate => write!(f, "keys-updated"),
            MetricType::KeyEvict => write!(f, "keys-evicted"),
            MetricType::CostAdd => write!(f, "cost-added"),
            MetricType::CostEvict => write!(f, "cost-evicted"),
            MetricType::DropSets => write!(f, "sets-dropped"),
            MetricType::RejectSets => write!(f, "sets-rejected"),
            MetricType::DropGets => write!(f, "gets-dropped"),
            MetricType::KeepGets => write!(f, "gets-kept"),
            MetricType::DoNotUse => write!(f, "unidentified"),
        }
    }
}

/// Metrics is a snapshot of performance statistics for the lifetime of a cache instance.
///
/// Metrics promises thread-safe.
#[derive(Clone)]
pub enum Metrics {
    /// No operation metrics
    Noop,

    /// Op metrics
    Op(MetricsInner),
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create a Noop metrics
    #[inline]
    pub const fn new() -> Self {
        Self::Noop
    }

    /// Create a new op metrics
    #[inline]
    pub fn new_op() -> Self {
        Self::Op(MetricsInner::new())
    }

    /// Return if the metrics is a Op metrics
    #[inline]
    pub fn is_op(&self) -> bool {
        match self {
            Metrics::Noop => false,
            Metrics::Op(_) => true,
        }
    }

    /// Return if the metrics is Noop
    #[inline]
    pub fn is_noop(&self) -> bool {
        match self {
            Metrics::Noop => true,
            Metrics::Op(_) => false,
        }
    }

    #[inline]
    pub(crate) fn track_eviction(&self, num_seconds: i64) {
        match self {
            Metrics::Noop => {}
            Metrics::Op(m) => m.track_eviction(num_seconds),
        }
    }

    #[inline]
    pub(crate) fn add(&self, typ: MetricType, hash: u64, delta: u64) -> bool {
        match self {
            Metrics::Noop => false,
            Metrics::Op(m) => {
                m.add(typ, hash, delta);
                true
            }
        }
    }

    /// Returns the number of Get calls where a value was found for the corresponding key.
    #[inline]
    pub fn get_hits(&self) -> Option<u64> {
        self.map(|m| m.get_hits())
    }

    /// Returns the number of Get calls where a value was not found for the corresponding key.
    #[inline]
    pub fn get_misses(&self) -> Option<u64> {
        self.map(|m| m.get_misses())
    }

    /// Returns the total number of Set calls where a new key-value item was added.
    #[inline]
    pub fn get_keys_added(&self) -> Option<u64> {
        self.map(|m| m.get_keys_added())
    }

    /// Returns the total number of Set calls where a new key-value item was updated.
    #[inline]
    pub fn get_keys_updated(&self) -> Option<u64> {
        self.map(|m| m.get_keys_updated())
    }

    /// Returns the total number of keys evicted.
    #[inline]
    pub fn get_keys_evicted(&self) -> Option<u64> {
        self.map(|m| m.get_keys_evicted())
    }

    /// Returns the sum of costs that have been added (successful Set calls).
    #[inline]
    pub fn get_cost_added(&self) -> Option<u64> {
        self.map(|m| m.get_cost_added())
    }

    /// Returns the sum of all costs that have been evicted.
    #[inline]
    pub fn get_cost_evicted(&self) -> Option<u64> {
        self.map(|m| m.get_cost_evicted())
    }

    /// Returns the number of Set calls that don't make it into internal
    /// buffers (due to contention or some other reason).
    #[inline]
    pub fn get_sets_dropped(&self) -> Option<u64> {
        self.map(|m| m.get_sets_dropped())
    }

    /// Returns the number of Set calls rejected by the policy (TinyLFU).
    #[inline]
    pub fn get_sets_rejected(&self) -> Option<u64> {
        self.map(|m| m.get_sets_rejected())
    }

    /// Returns the number of Get counter increments that are dropped
    /// internally.
    #[inline]
    pub fn get_gets_dropped(&self) -> Option<u64> {
        self.map(|m| m.get_gets_dropped())
    }

    /// Returns the number of Get counter increments that are kept.
    #[inline]
    pub fn get_gets_kept(&self) -> Option<u64> {
        self.map(|m| m.get_gets_kept())
    }

    /// Ratio is the number of Hits over all accesses (Hits + Misses). This is the
    /// percentage of successful Get calls.
    #[inline]
    pub fn ratio(&self) -> Option<f64> {
        self.map(|m| m.ratio())
    }

    /// Returns the histogram data of this metrics
    #[inline]
    pub fn life_expectancy_seconds(&self) -> Option<Histogram> {
        self.map(|m| m.life_expectancy_seconds())
    }

    /// clear resets all the metrics
    #[inline]
    pub fn clear(&self) {
        match self {
            Metrics::Noop => {}
            Metrics::Op(m) => m.clear(),
        }
    }

    #[inline]
    fn map<U, F: FnOnce(&MetricsInner) -> U>(&self, f: F) -> Option<U> {
        match self {
            Metrics::Noop => None,
            Metrics::Op(m) => Some(f(m)),
        }
    }
}

impl Display for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Metrics::Noop => write!(f, "Metrics::Noop"),
            Metrics::Op(inner) => write!(f, "{}", inner),
        }
    }
}

#[derive(Clone)]
pub struct MetricsInner {
    /// use Arc and AtomicU64 implement lock-free fearless-concurrency
    all: Arc<BTreeMap<MetricType, [AtomicU64; SIZE_FOR_EACH_TYPE]>>,

    /// tracks the life expectancy of a key
    life: Histogram,
}

impl Default for MetricsInner {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsInner {
    #[inline]
    pub fn new() -> Self {
        let h = Histogram::new(new_histogram_bound());

        let map: BTreeMap<MetricType, [AtomicU64; SIZE_FOR_EACH_TYPE]> = METRIC_TYPES_ARRAY
            .iter()
            .map(|typ| {
                let vec = vec![0; SIZE_FOR_EACH_TYPE]
                    .iter()
                    .map(|_| AtomicU64::new(0))
                    .collect();
                (*typ, vec_to_array::<AtomicU64, SIZE_FOR_EACH_TYPE>(vec))
            })
            .collect();

        Self {
            all: Arc::new(map),
            life: h,
        }
    }

    /// Returns the number of Get calls where a value was found for the corresponding key.
    #[inline]
    pub fn get_hits(&self) -> u64 {
        self.get(&MetricType::Hit)
    }

    /// Returns the number of Get calls where a value was not found for the corresponding key.
    #[inline]
    pub fn get_misses(&self) -> u64 {
        self.get(&MetricType::Miss)
    }

    /// Returns the total number of Set calls where a new key-value item was added.
    #[inline]
    pub fn get_keys_added(&self) -> u64 {
        self.get(&MetricType::KeyAdd)
    }

    /// Returns the total number of Set calls where a new key-value item was updated.
    #[inline]
    pub fn get_keys_updated(&self) -> u64 {
        self.get(&MetricType::KeyUpdate)
    }

    /// Returns the total number of keys evicted.
    #[inline]
    pub fn get_keys_evicted(&self) -> u64 {
        self.get(&MetricType::KeyEvict)
    }

    /// Returns the sum of costs that have been added (successful Set calls).
    #[inline]
    pub fn get_cost_added(&self) -> u64 {
        self.get(&MetricType::CostAdd)
    }

    /// Returns the sum of all costs that have been evicted.
    #[inline]
    pub fn get_cost_evicted(&self) -> u64 {
        self.get(&MetricType::CostEvict)
    }

    /// Returns the number of Set calls that don't make it into internal
    /// buffers (due to contention or some other reason).
    #[inline]
    pub fn get_sets_dropped(&self) -> u64 {
        self.get(&MetricType::DropSets)
    }

    /// Returns the number of Set calls rejected by the policy (TinyLFU).
    #[inline]
    pub fn get_sets_rejected(&self) -> u64 {
        self.get(&MetricType::RejectSets)
    }

    /// Returns the number of Get counter increments that are dropped
    /// internally.
    #[inline]
    pub fn get_gets_dropped(&self) -> u64 {
        self.get(&MetricType::DropGets)
    }

    /// Returns the number of Get counter increments that are kept.
    #[inline]
    pub fn get_gets_kept(&self) -> u64 {
        self.get(&MetricType::KeepGets)
    }

    /// Ratio is the number of Hits over all accesses (Hits + Misses). This is the
    /// percentage of successful Get calls.
    #[inline]
    pub fn ratio(&self) -> f64 {
        let hits = self.get_hits();
        let misses = self.get_misses();
        if hits == 0 && misses == 0 {
            0.0
        } else {
            (hits as f64) / ((hits + misses) as f64)
        }
    }

    /// Returns the histogram data of this metrics
    #[inline]
    pub fn life_expectancy_seconds(&self) -> Histogram {
        self.life.clone()
    }

    /// clear resets all the metrics
    #[inline]
    pub fn clear(&self) {
        METRIC_TYPES_ARRAY.iter().for_each(|typ| {
            if let Some(arr) = self.all.get(typ) {
                arr.iter().for_each(|val| val.store(0, Ordering::SeqCst))
            }
        });

        self.life.clear()
    }

    #[inline]
    pub(crate) fn track_eviction(&self, num_seconds: i64) {
        self.life.update(num_seconds)
    }

    #[inline]
    pub(crate) fn add(&self, typ: MetricType, hash: u64, delta: u64) {
        if let Some(val) = self.all.get(&typ) {
            let idx = ((hash % 25) * 10) as usize;
            val[idx].fetch_add(delta, Ordering::SeqCst);
        }
    }

    #[inline]
    fn get(&self, typ: &MetricType) -> u64 {
        let mut total = 0;
        if let Some(v) = self.all.get(typ) {
            v.iter()
                .for_each(|atom| total += atom.load(Ordering::SeqCst));
        }
        total
    }
}

#[cfg(feature = "serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
use serde::ser::{Error, SerializeStruct};
#[cfg(feature = "serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
use serde::{Serialize, Serializer};

#[cfg(feature = "serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
impl Serialize for MetricsInner {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("MetricsInner", 13)?;

        let types: [&'static str; 11] = [
            "hit",
            "miss",
            "keys-added",
            "keys-updated",
            "keys-evicted",
            "cost-added",
            "cost-evicted",
            "sets-dropped",
            "sets-rejected",
            "gets-dropped",
            "gets-kept",
        ];

        for (idx, typ) in METRIC_TYPES_ARRAY.iter().enumerate() {
            s.serialize_field(types[idx], &self.get(typ))?;
        }
        s.serialize_field(
            "gets-total",
            &(self.get(&MetricType::Hit) + self.get(&MetricType::Miss)),
        )?;
        s.serialize_field("hit-ratio", &self.ratio())?;
        s.end()
    }
}

#[cfg(not(feature = "serde"))]
impl Display for MetricsInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut buf = Vec::new();
        buf.extend("Metrics::Op {\n".as_bytes());
        METRIC_TYPES_ARRAY.iter().for_each(|typ| {
            buf.extend(format!("  \"{}\": {},\n", typ, self.get(typ)).as_bytes());
        });

        buf.extend(
            format!(
                "  \"gets-total\": {},\n",
                self.get(&MetricType::Hit) + self.get(&MetricType::Miss)
            )
            .as_bytes(),
        );
        buf.extend(format!("  \"hit-ratio\": {:.2}\n}}", self.ratio()).as_bytes());
        write!(f, "{}", String::from_utf8(buf).unwrap())
    }
}

#[cfg(feature = "serde")]
impl Display for MetricsInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str = serde_json::to_string_pretty(self).map_err(std::fmt::Error::custom)?;
        write!(f, "Metrics::Op {}", str)
    }
}

fn new_histogram_bound() -> Vec<f64> {
    (1..=HISTOGRAM_BOUND_SIZE as u64)
        .map(|idx| (1 << idx) as f64)
        .collect()
}

#[cfg(test)]
mod test {
    use crate::metrics::MetricsInner;
    use crate::Metrics;

    #[test]
    fn test_metrics() {
        let m = Metrics::new();
        println!("{}", m);
        assert!(m.get_hits().is_none());
        assert!(m.get_misses().is_none());
        assert!(m.get_keys_added().is_none());
        assert!(m.get_keys_updated().is_none());
        assert!(m.get_keys_evicted().is_none());
        assert!(m.get_sets_dropped().is_none());
        assert!(m.get_gets_dropped().is_none());
        assert!(m.get_gets_kept().is_none());
        assert!(m.get_sets_dropped().is_none());
        assert!(m.get_sets_rejected().is_none());
        assert!(m.get_cost_evicted().is_none());
        assert!(m.get_cost_added().is_none());
        assert!(m.life_expectancy_seconds().is_none());
        m.track_eviction(10);
        assert!(m.is_noop());

        let m = Metrics::new_op();
        m.get_hits().unwrap();
        m.get_misses().unwrap();
        m.get_keys_added().unwrap();
        m.get_keys_updated().unwrap();
        m.get_keys_evicted().unwrap();
        m.get_sets_dropped().unwrap();
        m.get_gets_dropped().unwrap();
        m.get_gets_kept().unwrap();
        m.get_sets_dropped().unwrap();
        m.get_sets_rejected().unwrap();
        m.get_cost_evicted().unwrap();
        m.get_cost_added().unwrap();
        m.life_expectancy_seconds().unwrap();
        m.track_eviction(10);
        assert!(m.is_op());
        println!("{}", m);
    }

    #[test]
    #[cfg(all(feature = "serde", feature = "serde_json"))]
    fn test_display() {
        let m = MetricsInner::new();
        let ms = serde_json::to_string_pretty(&m).unwrap();
        assert_eq!(format!("{}", m), format!("Metrics::Op {}", ms));
    }

    #[test]
    #[cfg(not(all(feature = "serde", feature = "serde_json")))]
    fn test_display() {
        let m = MetricsInner::new();
        let exp = "Metrics::Op {
  \"hit\": 0,
  \"miss\": 0,
  \"keys-added\": 0,
  \"keys-updated\": 0,
  \"keys-evicted\": 0,
  \"cost-added\": 0,
  \"cost-evicted\": 0,
  \"sets-dropped\": 0,
  \"sets-rejected\": 0,
  \"gets-dropped\": 0,
  \"gets-kept\": 0,
  \"gets-total\": 0,
  \"hit-ratio\": 0.00
}"
        .to_string();
        assert_eq!(format!("{}", m), exp)
    }
}
