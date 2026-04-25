use crate::{histogram::Histogram, utils::vec_to_array};
use std::{
  collections::BTreeMap,
  fmt::{Debug, Display, Formatter},
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

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
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub const fn new() -> Self {
    Self::Noop
  }

  /// Create a new op metrics
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn new_op() -> Self {
    Self::Op(MetricsInner::new())
  }

  /// Return if the metrics is a Op metrics
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn is_op(&self) -> bool {
    match self {
      Metrics::Noop => false,
      Metrics::Op(_) => true,
    }
  }

  /// Return if the metrics is Noop
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn is_noop(&self) -> bool {
    match self {
      Metrics::Noop => true,
      Metrics::Op(_) => false,
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn track_eviction(&self, num_seconds: i64) {
    match self {
      Metrics::Noop => {}
      Metrics::Op(m) => m.track_eviction(num_seconds),
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn add(&self, typ: MetricType, hash: u64, delta: u64) -> bool {
    match self {
      Metrics::Noop => false,
      Metrics::Op(m) => {
        m.add(typ, hash, delta);
        true
      }
    }
  }

  /// Returns the number of `get` calls that found a value for the key.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn hits(&self) -> Option<u64> {
    self.map(|m| m.hits())
  }

  /// Returns the number of `get` calls that did not find a value for the key.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn misses(&self) -> Option<u64> {
    self.map(|m| m.misses())
  }

  /// Returns the total number of `insert` calls that added a new entry.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn keys_added(&self) -> Option<u64> {
    self.map(|m| m.keys_added())
  }

  /// Returns the total number of `insert` calls that updated an existing entry.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn keys_updated(&self) -> Option<u64> {
    self.map(|m| m.keys_updated())
  }

  /// Returns the total number of keys evicted.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn keys_evicted(&self) -> Option<u64> {
    self.map(|m| m.keys_evicted())
  }

  /// Returns the sum of costs that have been added (successful inserts).
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn cost_added(&self) -> Option<u64> {
    self.map(|m| m.cost_added())
  }

  /// Returns the sum of all costs that have been evicted.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn cost_evicted(&self) -> Option<u64> {
    self.map(|m| m.cost_evicted())
  }

  /// Returns the number of `insert` calls that did not make it into the
  /// internal buffers (e.g. due to contention).
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn sets_dropped(&self) -> Option<u64> {
    self.map(|m| m.sets_dropped())
  }

  /// Returns the number of `insert` calls rejected by the policy (TinyLFU).
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn sets_rejected(&self) -> Option<u64> {
    self.map(|m| m.sets_rejected())
  }

  /// Returns the number of `get` counter increments dropped internally.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn gets_dropped(&self) -> Option<u64> {
    self.map(|m| m.gets_dropped())
  }

  /// Returns the number of `get` counter increments kept.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn gets_kept(&self) -> Option<u64> {
    self.map(|m| m.gets_kept())
  }

  /// Ratio is the number of Hits over all accesses (Hits + Misses). This is the
  /// percentage of successful Get calls.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn ratio(&self) -> Option<f64> {
    self.map(|m| m.ratio())
  }

  /// Returns the histogram data of this metrics
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn life_expectancy_seconds(&self) -> Option<Histogram> {
    self.map(|m| m.life_expectancy_seconds())
  }

  /// clear resets all the metrics
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn clear(&self) {
    match self {
      Metrics::Noop => {}
      Metrics::Op(m) => m.clear(),
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
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
  #[cfg_attr(not(tarpaulin), inline(always))]
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

  /// Returns the number of `get` calls that found a value for the key.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn hits(&self) -> u64 {
    self.get(&MetricType::Hit)
  }

  /// Returns the number of `get` calls that did not find a value for the key.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn misses(&self) -> u64 {
    self.get(&MetricType::Miss)
  }

  /// Returns the total number of `insert` calls that added a new entry.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn keys_added(&self) -> u64 {
    self.get(&MetricType::KeyAdd)
  }

  /// Returns the total number of `insert` calls that updated an existing entry.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn keys_updated(&self) -> u64 {
    self.get(&MetricType::KeyUpdate)
  }

  /// Returns the total number of keys evicted.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn keys_evicted(&self) -> u64 {
    self.get(&MetricType::KeyEvict)
  }

  /// Returns the sum of costs that have been added (successful inserts).
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn cost_added(&self) -> u64 {
    self.get(&MetricType::CostAdd)
  }

  /// Returns the sum of all costs that have been evicted.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn cost_evicted(&self) -> u64 {
    self.get(&MetricType::CostEvict)
  }

  /// Returns the number of `insert` calls that did not make it into the
  /// internal buffers (e.g. due to contention).
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn sets_dropped(&self) -> u64 {
    self.get(&MetricType::DropSets)
  }

  /// Returns the number of `insert` calls rejected by the policy (TinyLFU).
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn sets_rejected(&self) -> u64 {
    self.get(&MetricType::RejectSets)
  }

  /// Returns the number of `get` counter increments dropped internally.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn gets_dropped(&self) -> u64 {
    self.get(&MetricType::DropGets)
  }

  /// Returns the number of `get` counter increments kept.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn gets_kept(&self) -> u64 {
    self.get(&MetricType::KeepGets)
  }

  /// Ratio is the number of Hits over all accesses (Hits + Misses). This is the
  /// percentage of successful Get calls.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn ratio(&self) -> f64 {
    let hits = self.hits();
    let misses = self.misses();
    if hits == 0 && misses == 0 {
      0.0
    } else {
      (hits as f64) / ((hits + misses) as f64)
    }
  }

  /// Returns the histogram data of this metrics
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn life_expectancy_seconds(&self) -> Histogram {
    self.life.clone()
  }

  /// clear resets all the metrics
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn clear(&self) {
    METRIC_TYPES_ARRAY.iter().for_each(|typ| {
      if let Some(arr) = self.all.get(typ) {
        arr.iter().for_each(|val| val.store(0, Ordering::SeqCst))
      }
    });

    self.life.clear()
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn track_eviction(&self, num_seconds: i64) {
    self.life.update(num_seconds)
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn add(&self, typ: MetricType, hash: u64, delta: u64) {
    if let Some(val) = self.all.get(&typ) {
      let idx = (hash % SIZE_FOR_EACH_TYPE as u64) as usize;
      val[idx].fetch_add(delta, Ordering::SeqCst);
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
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
use serde::ser::SerializeStruct;
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

fn new_histogram_bound() -> Vec<f64> {
  (1..=HISTOGRAM_BOUND_SIZE as u64)
    .map(|idx| (1 << idx) as f64)
    .collect()
}

#[cfg(test)]
mod test {
  use crate::{Metrics, metrics::MetricsInner};

  #[test]
  fn test_metrics() {
    let m = Metrics::new();
    println!("{}", m);
    assert!(m.hits().is_none());
    assert!(m.misses().is_none());
    assert!(m.keys_added().is_none());
    assert!(m.keys_updated().is_none());
    assert!(m.keys_evicted().is_none());
    assert!(m.sets_dropped().is_none());
    assert!(m.gets_dropped().is_none());
    assert!(m.gets_kept().is_none());
    assert!(m.sets_dropped().is_none());
    assert!(m.sets_rejected().is_none());
    assert!(m.cost_evicted().is_none());
    assert!(m.cost_added().is_none());
    assert!(m.life_expectancy_seconds().is_none());
    m.track_eviction(10);
    assert!(m.is_noop());

    let m = Metrics::new_op();
    m.hits().unwrap();
    m.misses().unwrap();
    m.keys_added().unwrap();
    m.keys_updated().unwrap();
    m.keys_evicted().unwrap();
    m.sets_dropped().unwrap();
    m.gets_dropped().unwrap();
    m.gets_kept().unwrap();
    m.sets_dropped().unwrap();
    m.sets_rejected().unwrap();
    m.cost_evicted().unwrap();
    m.cost_added().unwrap();
    m.life_expectancy_seconds().unwrap();
    m.track_eviction(10);
    assert!(m.is_op());
    println!("{}", m);
  }

  #[test]
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

  #[test]
  fn test_metrics_default_and_is_op_is_noop() {
    let noop = Metrics::default();
    assert!(noop.is_noop());
    assert!(!noop.is_op());

    let op = Metrics::new_op();
    assert!(op.is_op());
    assert!(!op.is_noop());
  }

  #[test]
  fn test_metrics_inner_default() {
    let _ = MetricsInner::default();
  }

  #[test]
  fn test_metric_type_display_do_not_use() {
    use crate::MetricType;
    assert_eq!(format!("{}", MetricType::DoNotUse), "unidentified");
  }

  #[cfg(feature = "serde")]
  #[test]
  fn test_metrics_inner_serialize() {
    let m = MetricsInner::new();
    let json = serde_json::to_string(&m).unwrap();
    assert!(json.contains("\"hit\":0"));
    assert!(json.contains("\"miss\":0"));
    assert!(json.contains("\"gets-total\":0"));
    assert!(json.contains("\"hit-ratio\":0.0"));
  }
}
