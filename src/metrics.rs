use crate::{cfg_not_serde, cfg_serde, histogram::Histogram, utils::vec_to_array};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const HISTOGRAM_BOUND_SIZE: usize = 16;
const HISTOGRAM_COUNT_PER_BUCKET_SIZE: usize = HISTOGRAM_BOUND_SIZE + 1;

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

#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
#[repr(u16)]
pub(crate) enum MetricType {
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
pub enum Metrics {
    Noop,
    Op(MetricsInner),
}

impl Metrics {
    pub fn new() -> Self {
        Self::Noop
    }

    pub fn new_op() -> Self {
        Self::Op(MetricsInner::new())
    }

    pub fn is_op(&self) -> bool {
        match self {
            Metrics::Noop => false,
            Metrics::Op(_) => true,
        }
    }

    pub fn is_noop(&self) -> bool {
        match self {
            Metrics::Noop => true,
            Metrics::Op(_) => false,
        }
    }

    pub(crate) fn track_eviction(&self, num_seconds: i64) {
        match self {
            Metrics::Noop => return,
            Metrics::Op(m) => m.life.update(num_seconds),
        }
    }

    pub(crate) fn add(&self, typ: MetricType, hash: u64, delta: u64) -> bool {
        match self {
            Metrics::Noop => false,
            Metrics::Op(m) => {
                m.add(typ, hash, delta);
                true
            }
        }
    }

    pub(crate) fn sub(&self, typ: MetricType, hash: u64, delta: u64) -> bool {
        match self {
            Metrics::Noop => false,
            Metrics::Op(m) => {
                m.sub(typ, hash, delta);
                true
            }
        }
    }

    /// Returns the number of Get calls where a value was found for the corresponding key.
    pub fn get_hits(&self) -> Option<u64> {
        self.map(|ref m| m.get(&MetricType::Hit))
    }

    /// Returns the number of Get calls where a value was not found for the corresponding key.
    pub fn get_misses(&self) -> Option<u64> {
        self.map(|ref m| m.get(&MetricType::Miss))
    }

    /// Returns the total number of Set calls where a new key-value item was added.
    pub fn get_keys_added(&self) -> Option<u64> {
        self.map(|ref m| m.get(&MetricType::KeyAdd))
    }

    /// Returns the total number of Set calls where a new key-value item was updated.
    pub fn get_keys_updated(&self) -> Option<u64> {
        self.map(|ref m| m.get(&MetricType::KeyUpdate))
    }

    /// Returns the total number of keys evicted.
    pub fn get_keys_evicted(&self) -> Option<u64> {
        self.map(|ref m| m.get(&MetricType::KeyEvict))
    }

    /// Returns the sum of costs that have been added (successful Set calls).
    pub fn get_cost_added(&self) -> Option<u64> {
        self.map(|ref m| m.get(&MetricType::CostAdd))
    }

    /// Returns the sum of all costs that have been evicted.
    pub fn get_cost_evicted(&self) -> Option<u64> {
        self.map(|ref m| m.get(&MetricType::CostEvict))
    }

    /// Returns the number of Set calls that don't make it into internal
    /// buffers (due to contention or some other reason).
    pub fn get_sets_dropped(&self) -> Option<u64> {
        self.map(|ref m| m.get(&MetricType::DropSets))
    }

    /// Returns the number of Set calls rejected by the policy (TinyLFU).
    pub fn get_sets_rejected(&self) -> Option<u64> {
        self.map(|ref m| m.get(&MetricType::RejectSets))
    }

    /// Returns the number of Get counter increments that are dropped
    /// internally.
    pub fn get_gets_dropped(&self) -> Option<u64> {
        self.map(|ref m| m.get(&MetricType::DropGets))
    }

    /// Returns the number of Get counter increments that are kept.
    pub fn get_gets_kept(&self) -> Option<u64> {
        self.map(|ref m| m.get(&MetricType::KeepGets))
    }

    /// Ratio is the number of Hits over all accesses (Hits + Misses). This is the
    /// percentage of successful Get calls.
    pub fn ratio(&self) -> Option<f64> {
        self.map(|ref m| m.ratio())
    }

    /// Returns the histogram data of this metrics
    pub fn life_expectancy_seconds(
        &self,
    ) -> Option<Histogram<HISTOGRAM_BOUND_SIZE, HISTOGRAM_COUNT_PER_BUCKET_SIZE>> {
        self.map(|ref m| m.life.clone())
    }

    /// clear resets all the metrics
    pub fn clear(&self) {
        match self {
            Metrics::Noop => {}
            Metrics::Op(m) => m.clear(),
        }
    }

    fn map<U, F: FnOnce(&MetricsInner) -> U>(&self, f: F) -> Option<U> {
        match self {
            Metrics::Noop => None,
            Metrics::Op(m) => Some(f(m)),
        }
    }
}

#[derive(Clone)]
pub struct MetricsInner {
    /// use Arc and AtomicU64 implement lock-free fearless-concurrency
    all: Arc<BTreeMap<MetricType, [AtomicU64; SIZE_FOR_EACH_TYPE]>>,

    /// tracks the life expectancy of a key
    life: Histogram<HISTOGRAM_BOUND_SIZE, HISTOGRAM_COUNT_PER_BUCKET_SIZE>,
}

impl MetricsInner {
    pub fn new() -> Self {
        let h = Histogram::<HISTOGRAM_BOUND_SIZE, HISTOGRAM_COUNT_PER_BUCKET_SIZE>::new(
            new_histogram_bound(),
        );

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

        let this = Self {
            all: Arc::new(map),
            life: h,
        };

        this
    }

    /// Returns the number of Get calls where a value was found for the corresponding key.
    pub fn get_hits(&self) -> u64 {
        self.get(&MetricType::Hit)
    }

    /// Returns the number of Get calls where a value was not found for the corresponding key.
    pub fn get_misses(&self) -> u64 {
        self.get(&MetricType::Miss)
    }

    /// Returns the total number of Set calls where a new key-value item was added.
    pub fn get_keys_added(&self) -> u64 {
        self.get(&MetricType::KeyAdd)
    }

    /// Returns the total number of Set calls where a new key-value item was updated.
    pub fn get_keys_updated(&self) -> u64 {
        self.get(&MetricType::KeyUpdate)
    }

    /// Returns the total number of keys evicted.
    pub fn get_keys_evicted(&self) -> u64 {
        self.get(&MetricType::KeyEvict)
    }

    /// Returns the sum of costs that have been added (successful Set calls).
    pub fn get_cost_added(&self) -> u64 {
        self.get(&MetricType::CostAdd)
    }

    /// Returns the sum of all costs that have been evicted.
    pub fn get_cost_evicted(&self) -> u64 {
        self.get(&MetricType::CostEvict)
    }

    /// Returns the number of Set calls that don't make it into internal
    /// buffers (due to contention or some other reason).
    pub fn get_sets_dropped(&self) -> u64 {
        self.get(&MetricType::DropSets)
    }

    /// Returns the number of Set calls rejected by the policy (TinyLFU).
    pub fn get_sets_rejected(&self) -> u64 {
        self.get(&MetricType::RejectSets)
    }

    /// Returns the number of Get counter increments that are dropped
    /// internally.
    pub fn get_gets_dropped(&self) -> u64 {
        self.get(&MetricType::DropGets)
    }

    /// Returns the number of Get counter increments that are kept.
    pub fn get_gets_kept(&self) -> u64 {
        self.get(&MetricType::KeepGets)
    }

    /// Ratio is the number of Hits over all accesses (Hits + Misses). This is the
    /// percentage of successful Get calls.
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
    pub fn life_expectancy_seconds(
        &self,
    ) -> Histogram<HISTOGRAM_BOUND_SIZE, HISTOGRAM_COUNT_PER_BUCKET_SIZE> {
        self.life.clone()
    }

    /// clear resets all the metrics
    pub fn clear(&self) {
        METRIC_TYPES_ARRAY.iter().for_each(|typ| {
            self.all
                .get(typ)
                .map(|arr| arr.iter().for_each(|val| val.store(0, Ordering::SeqCst)));
        });

        self.life.clear()
    }

    pub(crate) fn track_eviction(&self, num_seconds: i64) {
        self.life.update(num_seconds)
    }

    pub(crate) fn add(&self, typ: MetricType, hash: u64, delta: u64) {
        self.all.get(&typ).map(|val| {
            let idx = ((hash % 25) * 10) as usize;
            val[idx].fetch_add(delta, Ordering::SeqCst);
        });
    }

    pub(crate) fn sub(&self, typ: MetricType, hash: u64, delta: u64) {
        self.all.get(&typ).map(|val| {
            let idx = ((hash % 25) * 10) as usize;
            val[idx].fetch_sub(delta, Ordering::SeqCst);
        });
    }

    fn get(&self, typ: &MetricType) -> u64 {
        let mut total = 0;
        self.all.get(typ).map(|v| {
            v.iter()
                .for_each(|atom| total += atom.load(Ordering::SeqCst));
        });
        total
    }
}

cfg_not_serde! {
    impl Display for MetricsInner {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let mut buf = Vec::new();
            buf.extend("MetricsInner {\n".as_bytes());
            METRIC_TYPES_ARRAY.iter().for_each(|typ| {
                buf.extend(format!("  \"{}\": {},\n", typ, self.get(typ)).as_bytes());
            });

            buf.extend(format!("  \"gets-total\": {},\n", self.get(&MetricType::Hit) + self.get(&MetricType::Miss)).as_bytes());
            buf.extend(format!("  \"hit-ratio\": {:.2}\n}}", self.ratio()).as_bytes());
            write!(f, "{}", String::from_utf8(buf).unwrap())
        }
    }
}

cfg_serde! {
    use serde::{Serialize, Serializer};
    use serde::ser::{SerializeStruct, Error};

    impl Display for MetricsInner {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let str = serde_json::to_string_pretty(self).map_err( std::fmt::Error::custom)?;
            write!(f, "MetricsInner {}", str)
        }
    }

    impl Serialize for MetricsInner {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
            let mut s = serializer.serialize_struct("MetricsInner", 13)?;

            let types: [&'static str; 11] = ["hit", "miss", "keys-added", "keys-updated", "keys-evicted", "cost-added", "cost-evicted", "sets-dropped", "sets-rejected", "gets-dropped", "gets-kept"];

            for (idx, typ) in METRIC_TYPES_ARRAY.iter().enumerate() {
                s.serialize_field(types[idx], &self.get(typ))?;
            }
            s.serialize_field("gets-total", &(self.get(&MetricType::Hit) + self.get(&MetricType::Miss)))?;
            s.serialize_field("hit-ratio", &self.ratio())?;
            s.end()
        }
    }
}

fn new_histogram_bound() -> Vec<f64> {
    (1..=HISTOGRAM_BOUND_SIZE as u64)
        .map(|idx| (1 << idx) as f64)
        .collect()
}

#[cfg(test)]
mod test {
    use crate::metrics::{MetricType, MetricsInner};
    use crate::{cfg_not_serde, cfg_serde};

    #[test]
    fn test_metrics() {
        let m = MetricsInner::new();
        println!(
            "{:?}",
            m.all.keys().map(|typ| *typ).collect::<Vec<MetricType>>()
        );
        println!("{}", m)
    }

    cfg_serde!(
        #[test]
        fn test_display() {
            let m = MetricsInner::new();
            let ms = serde_json::to_string_pretty(&m).unwrap();
            assert_eq!(format!("{}", m), format!("MetricsInner {}", ms));
        }
    );

    cfg_not_serde!(
        #[test]
        fn test_display() {
            let m = MetricsInner::new();
            let exp = "MetricsInner {
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
    );
}
