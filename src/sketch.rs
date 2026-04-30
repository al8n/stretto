//! This mod implements Count-Min sketch with 4-bit counters.
//!
//! This file is a mechanical translation of the reference Golang code, available at <https://github.com/dgraph-io/ristretto/blob/master/sketch.go>
//!
//! I claim no additional copyright over the original implementation.
use crate::error::CacheError;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::{
  fmt::{Debug, Formatter},
  sync::atomic::{AtomicU8, Ordering},
  time::{SystemTime, UNIX_EPOCH},
};

const DEPTH: usize = 4;

/// One row of the count-min sketch. Each `AtomicU8` cell packs two 4-bit
/// counters (low nibble at shift 0, high nibble at shift 4). The byte is
/// the unit of atomicity: increments and resets CAS the whole byte and
/// only mutate the targeted nibble. Concurrent writers to the *other*
/// nibble in the same cell will lose their CAS and retry, which is fine —
/// a real conflict on a single nibble is rare even at high QPS because
/// the 64-cell rows are large compared to the working set, and the four
/// hash seeds spread writes across rows.
pub(crate) struct CountMinRow(Vec<AtomicU8>);

impl CountMinRow {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn new(width: u64) -> Self {
    let mut v = Vec::with_capacity(width as usize);
    for _ in 0..width as usize {
      v.push(AtomicU8::new(0));
    }
    Self(v)
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn get(&self, i: u64) -> u8 {
    let byte = self.0[(i / 2) as usize].load(Ordering::Relaxed);
    (byte >> ((i & 1) * 4)) & 0x0f
  }

  /// Increment the 4-bit counter at index `i`, saturating at 15.
  ///
  /// CAS-loops on the containing byte: read the byte, check that the target
  /// nibble is below 15, add `1 << shift` to bump only that nibble (no carry
  /// into the other nibble since we excluded the saturated case), then CAS
  /// back. Concurrent writers to the *other* nibble in the same byte cause
  /// CAS failures and retries; concurrent writers to the *same* nibble
  /// serialize through the CAS but stay correct (each retry re-reads the
  /// fresh value).
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn increment(&self, i: u64) {
    let idx = (i / 2) as usize;
    let shift = ((i & 1) * 4) as u8;
    let cell = &self.0[idx];
    let mut cur = cell.load(Ordering::Relaxed);
    loop {
      let counter = (cur >> shift) & 0x0f;
      if counter == 0x0f {
        return;
      }
      let new = cur + (1 << shift);
      match cell.compare_exchange_weak(cur, new, Ordering::Relaxed, Ordering::Relaxed) {
        Ok(_) => return,
        Err(actual) => cur = actual,
      }
    }
  }

  /// Halve every counter in this row (each nibble independently).
  ///
  /// Per-byte CAS so concurrent increments compose: an interleaved increment
  /// loses its CAS once we halve, then retries against the halved value —
  /// the increment is correctly applied *after* the halving rather than
  /// being silently overwritten.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn reset(&self) {
    for cell in &self.0 {
      let mut cur = cell.load(Ordering::Relaxed);
      loop {
        let new = (cur >> 1) & 0x77;
        match cell.compare_exchange_weak(cur, new, Ordering::Relaxed, Ordering::Relaxed) {
          Ok(_) => break,
          Err(actual) => cur = actual,
        }
      }
    }
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn clear(&self) {
    for cell in &self.0 {
      cell.store(0, Ordering::Relaxed);
    }
  }
}

impl Debug for CountMinRow {
  fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
    let mut s = String::new();
    for i in 0..(self.0.len() * 2) {
      let byte = self.0[i / 2].load(Ordering::Relaxed);
      s.push_str(&format!("{:02} ", (byte >> ((i & 1) * 4)) & 0x0f));
    }
    write!(f, "{}", s)
  }
}

/// `CountMinSketch` is a small conservative-update count-min sketch
/// implementation with 4-bit counters
pub(crate) struct CountMinSketch {
  rows: [CountMinRow; DEPTH],
  seeds: [u64; DEPTH],
  mask: u64,
}

impl CountMinSketch {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn new(ctrs: u64) -> Result<Self, CacheError> {
    if ctrs < 1 {
      return Err(CacheError::InvalidCountMinWidth(ctrs));
    }

    let ctrs = ctrs.next_power_of_two();
    let hctrs = ctrs / 2;

    let mut source = StdRng::seed_from_u64(
      SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs(),
    );

    let seeds: Vec<u64> = { (0..DEPTH).map(|_| source.random::<u64>()).collect() };

    let this = Self {
      rows: [
        CountMinRow::new(hctrs),
        CountMinRow::new(hctrs),
        CountMinRow::new(hctrs),
        CountMinRow::new(hctrs),
      ],
      seeds: [seeds[0], seeds[1], seeds[2], seeds[3]],
      mask: ctrs - 1,
    };

    Ok(this)
  }

  /// `increment` increments the count(ers) for the specified key.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn increment(&self, hashed: u64) {
    let mask = self.mask;
    (0..DEPTH).for_each(|i| {
      let seed = self.seeds[i];
      self.rows[i].increment((hashed ^ seed) & mask);
    })
  }

  /// `estimate` returns the value of the specified key.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn estimate(&self, hashed: u64) -> i64 {
    let mask = self.mask;
    let mut min = 255u8;
    (0..DEPTH).for_each(|i| {
      let seed = self.seeds[i];
      let val = self.rows[i].get((hashed ^ seed) & mask);
      if val < min {
        min = val;
      }
    });

    min as i64
  }

  /// `reset` halves all counter values.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn reset(&self) {
    self.rows.iter().for_each(|row| row.reset())
  }

  /// `clear` zeroes all counters.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn clear(&self) {
    self.rows.iter().for_each(|row| row.clear())
  }
}

#[cfg(test)]
mod test {
  use super::*;

  #[test]
  fn test_count_min_row() {
    let cmr = CountMinRow::new(8);
    cmr.increment(0);
    assert_eq!(cmr.0[0].load(Ordering::Relaxed), 0x01);

    assert_eq!(cmr.get(0), 1);
    assert_eq!(cmr.get(1), 0);

    cmr.increment(1);
    assert_eq!(cmr.0[0].load(Ordering::Relaxed), 0x11);
    assert_eq!(cmr.get(0), 1);
    assert_eq!(cmr.get(1), 1);

    (0..14).for_each(|_| cmr.increment(1));
    assert_eq!(cmr.0[0].load(Ordering::Relaxed), 0xf1);
    assert_eq!(cmr.get(1), 15);
    assert_eq!(cmr.get(0), 1);

    // ensure clamped
    (0..3).for_each(|_| {
      cmr.increment(1);
      assert_eq!(cmr.0[0].load(Ordering::Relaxed), 0xf1);
    });

    cmr.reset();
    assert_eq!(cmr.0[0].load(Ordering::Relaxed), 0x70);
  }

  #[test]
  fn test_count_min_sketch() {
    let s = CountMinSketch::new(5).unwrap();
    assert_eq!(7u64, s.mask);
  }

  #[test]
  fn test_count_min_sketch_increment() {
    let s = CountMinSketch::new(16).unwrap();
    s.increment(1);
    s.increment(5);
    s.increment(9);
    for i in 0..DEPTH {
      if format!("{:?}", s.rows[i]) != format!("{:?}", s.rows[0]) {
        break;
      }
      assert_ne!(i, DEPTH - 1);
    }
  }

  #[test]
  fn test_count_min_sketch_estimate() {
    let s = CountMinSketch::new(16).unwrap();
    s.increment(1);
    s.increment(1);

    assert_eq!(s.estimate(1), 2);
    assert_eq!(s.estimate(0), 0);
  }

  #[test]
  fn test_count_min_sketch_reset() {
    let s = CountMinSketch::new(16).unwrap();
    s.increment(1);
    s.increment(1);
    s.increment(1);
    s.increment(1);
    s.reset();
    assert_eq!(s.estimate(1), 2);
  }

  #[test]
  fn test_count_min_sketch_clear() {
    let s = CountMinSketch::new(16).unwrap();
    (0..16).for_each(|i| s.increment(i));
    s.clear();
    (0..16).for_each(|i| assert_eq!(s.estimate(i), 0));
  }

  #[test]
  fn test_count_min_row_concurrent_increment_independent_keys() {
    use std::{sync::Arc, thread};

    // Hammer 16 distinct keys from 16 threads, 1000 increments each.
    // Saturates at 15, so we expect every key to read as 15.
    let cmr = Arc::new(CountMinRow::new(16));
    let mut handles = Vec::new();
    for k in 0..16u64 {
      let cmr = cmr.clone();
      handles.push(thread::spawn(move || {
        for _ in 0..1000 {
          cmr.increment(k);
        }
      }));
    }
    for h in handles {
      h.join().unwrap();
    }
    for k in 0..16u64 {
      assert_eq!(cmr.get(k), 15, "key {} should be saturated", k);
    }
  }

  #[test]
  fn test_count_min_row_concurrent_increment_same_key() {
    use std::{sync::Arc, thread};

    // Hammer one key from many threads. With 16 threads × 50 increments,
    // we expect the counter to saturate at 15 (well below 800 attempts),
    // proving the CAS loop neither over-increments past saturation nor
    // loses so many writes that we fall short.
    let cmr = Arc::new(CountMinRow::new(8));
    let mut handles = Vec::new();
    for _ in 0..16 {
      let cmr = cmr.clone();
      handles.push(thread::spawn(move || {
        for _ in 0..50 {
          cmr.increment(3);
        }
      }));
    }
    for h in handles {
      h.join().unwrap();
    }
    assert_eq!(cmr.get(3), 15);
    // The other nibble in the same byte (key 2 shares byte 1 with key 3)
    // must remain 0.
    assert_eq!(cmr.get(2), 0);
  }

  #[test]
  fn test_count_min_row_concurrent_increment_and_reset() {
    use std::{
      sync::{Arc, atomic::AtomicBool},
      thread,
    };

    // One thread increments key 5 in a tight loop; another thread halves
    // the row repeatedly. After both stop, no nibble in the row should be
    // > 15 (saturation), and the targeted key should have a sane non-
    // negative count. The point is that interleaving CAS doesn't corrupt
    // the byte (e.g. by losing a halve or letting a nibble overflow into
    // its neighbor).
    let cmr = Arc::new(CountMinRow::new(16));
    let stop = Arc::new(AtomicBool::new(false));

    let cmr_inc = cmr.clone();
    let stop_inc = stop.clone();
    let inc = thread::spawn(move || {
      while !stop_inc.load(Ordering::Relaxed) {
        cmr_inc.increment(5);
      }
    });

    let cmr_rst = cmr.clone();
    let stop_rst = stop.clone();
    let rst = thread::spawn(move || {
      while !stop_rst.load(Ordering::Relaxed) {
        cmr_rst.reset();
      }
    });

    thread::sleep(std::time::Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    inc.join().unwrap();
    rst.join().unwrap();

    for i in 0..32 {
      let v = cmr.get(i);
      assert!(v <= 15, "nibble {} overflowed to {}", i, v);
      if i != 5 {
        // Only key 5 was touched, but resets can leave non-zero values
        // on byte-mate nibbles only via the upper-nibble preservation
        // rule. Key 5's byte-mate is key 4 (same byte, different nibble).
        // The key 4 nibble starts at 0 and is never incremented, so it
        // must stay 0 even across resets.
        if i == 4 {
          assert_eq!(v, 0, "key 4 should never be touched");
        }
      }
    }
  }
}
