use std::fmt::{Debug, Formatter};
use crate::histogram::{Histogram};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::utils::vec_to_array;
use std::sync::{Arc};
use parking_lot::RwLock;

const HISTOGRAM_BOUND_SIZE: usize = 16;
const HISTOGRAM_COUNT_PER_BUCKET_SIZE: usize = HISTOGRAM_BOUND_SIZE + 1;

const NUMS_OF_METRIC_TYPE: usize = 11;
const SIZE_FOR_EACH_TYPE: usize = 256;
const METRIC_TYPES_ARRAY: [MetricType; NUMS_OF_METRIC_TYPE] = [
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

#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
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
    DoNotUse,
}

impl Debug for MetricType {
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
            MetricType::DoNotUse => write!(f, "unidentified")
        }
    }
}

pub(crate) struct Metrics {
    /// use Arc and AtomicU64 implement lock-free fearless-concurrency
    all: Arc<BTreeMap<MetricType, [AtomicU64; SIZE_FOR_EACH_TYPE]>>,

    /// tracks the life expectancy of a key
    life: Histogram<HISTOGRAM_BOUND_SIZE, HISTOGRAM_COUNT_PER_BUCKET_SIZE>,
}

impl Metrics {
    pub fn new() -> Self {
        let h = Histogram::<HISTOGRAM_BOUND_SIZE, HISTOGRAM_COUNT_PER_BUCKET_SIZE>::new(new_histogram_bound());

        let map: BTreeMap<MetricType, [AtomicU64; SIZE_FOR_EACH_TYPE]> = METRIC_TYPES_ARRAY.iter().map(|typ| {
            let vec = vec![0; SIZE_FOR_EACH_TYPE].iter().map(|_| AtomicU64::new(0)).collect();
            (*typ, vec_to_array::<AtomicU64, SIZE_FOR_EACH_TYPE>(vec))
        }).collect();

        let this = Self {
            all: Arc::new(map),
            life: h,
        };

        // Arc::new(RwLock::new(this))
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

    /// Ratio is the number of Hits over all accesses (Hits + Misses). This is the
    /// percentage of successful Get calls.
    pub fn ratio(&self) -> f64 {
        let hits = self.get_hits();
        let misses = self.get_misses();
        if hits == 0 && misses == 0 {
            0.0
        } else {
            (hits as f64) / (misses as f64)
        }
    }
    
    fn add(&self, typ: MetricType, hash: u64, delta: u64) {
        self.all.get(&typ).map(|val| {
            let idx = ((hash % 25) * 10) as usize;
            val[idx].fetch_add(delta, Ordering::SeqCst);
        });
    }

    fn get(&self, typ: &MetricType) -> u64 {
        let mut total = 0;
        self.all.get(typ).map(|v| {
            v.iter().for_each(|atom| total += atom.load(Ordering::SeqCst));
        });
        total
    }
}

fn new_histogram_bound() -> Vec<f64> {
    (1..=HISTOGRAM_BOUND_SIZE as u64).map(|idx| (1 << idx) as f64).collect()
}

#[cfg(test)]
mod test {
    use crate::metrics::{Metrics, MetricType};

    #[test]
    fn test_metrics() {
        let m = Metrics::new();
        println!("{:?}", m.all.keys().map(|typ| *typ).collect::<Vec<MetricType>>())
    }
}