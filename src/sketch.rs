//! This mod implements Count-Min sketch with 4-bit counters.
//!
//! This file is a mechanical translation of the reference Golang code, available at https://github.com/dgraph-io/ristretto/blob/master/sketch.go
//!
//! I claim no additional copyright over the original implementation.
use crate::error::CacheError;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::fmt::{Debug, Formatter};
use std::ops::{Index, IndexMut};
use std::time::{SystemTime, UNIX_EPOCH};

const DEPTH: usize = 4;

pub(crate) struct CountMinRow(Vec<u8>);

impl CountMinRow {
    pub(crate) fn new(width: u64) -> Self {
        Self(vec![0; width as usize])
    }

    pub(crate) fn get(&self, i: u64) -> u8 {
        ((self[(i / 2) as usize] >> ((i & 1) * 4)) as u8) & 0x0f
    }

    pub(crate) fn increment(&mut self, i: u64) {
        // Index of the counter
        let idx = (i / 2) as usize;
        // shift distance (even 0, odd 4).
        let shift = (i & 1) * 4;
        // counter value
        let v = (self[idx] >> shift) & 0x0f;
        // only increment if not max value (overflow wrap is bad for LFU).
        if v < 15 {
            self[idx] += 1 << shift;
        }
    }

    pub(crate) fn reset(&mut self) {
        // halve each counter
        self.0.iter_mut().for_each(|v| *v = (*v >> 1) & 0x77)
    }

    pub(crate) fn clear(&mut self) {
        // zero each counter
        self.0.iter_mut().for_each(|v| *v = 0)
    }
}

impl Index<usize> for CountMinRow {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        self.0.index(index)
    }
}

impl IndexMut<usize> for CountMinRow {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.0.index_mut(index)
    }
}

impl Debug for CountMinRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        let mut s = String::new();
        for i in 0..(self.0.len() * 2) {
            s.push_str(&format!("{:02} ", (self[i / 2] >> ((i & 1) * 4)) & 0x0f));
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
    pub(crate) fn new(ctrs: u64) -> Result<Self, CacheError> {
        if ctrs < 1 {
            return Err(CacheError::InvalidCountMinWidth(ctrs));
        }

        let ctrs = ctrs.next_power_of_two();
        let hctrs = ctrs / 2;

        let mut source = StdRng::seed_from_u64(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        let seeds: Vec<u64> = { (0..DEPTH).map(|_| source.gen::<u64>()).collect() };

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
    pub(crate) fn increment(&mut self, hashed: u64) {
        let mask = self.mask;
        (0..DEPTH).for_each(|i| {
            let seed = self.seeds[i];
            self.rows[i].increment((hashed ^ seed) & mask);
        })
    }

    /// `estimate` returns the value of the specified key.
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
    pub(crate) fn reset(&mut self) {
        self.rows.iter_mut().for_each(|row| row.reset())
    }

    /// `clear` zeroes all counters.
    pub(crate) fn clear(&mut self) {
        self.rows.iter_mut().for_each(|row| row.clear())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_count_min_row() {
        let mut cmr = CountMinRow::new(8);
        cmr.increment(0);
        assert_eq!(cmr[0], 0x01);

        assert_eq!(cmr.get(0), 1);
        assert_eq!(cmr.get(1), 0);

        cmr.increment(1);
        assert_eq!(cmr[0], 0x11);
        assert_eq!(cmr.get(0), 1);
        assert_eq!(cmr.get(1), 1);

        (0..14).for_each(|_| cmr.increment(1));
        assert_eq!(cmr[0], 0xf1);
        assert_eq!(cmr.get(1), 15);
        assert_eq!(cmr.get(0), 1);

        // ensure clamped
        (0..3).for_each(|_| {
            cmr.increment(1);
            assert_eq!(cmr[0], 0xf1);
        });

        cmr.reset();
        assert_eq!(cmr[0], 0x70);
    }

    #[test]
    fn test_count_min_sketch() {
        let s = CountMinSketch::new(5).unwrap();
        assert_eq!(7u64, s.mask);
    }

    #[test]
    fn test_count_min_sketch_increment() {
        let mut s = CountMinSketch::new(16).unwrap();
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
        let mut s = CountMinSketch::new(16).unwrap();
        s.increment(1);
        s.increment(1);

        assert_eq!(s.estimate(1), 2);
        assert_eq!(s.estimate(0), 0);
    }

    #[test]
    fn test_count_min_sketch_reset() {
        let mut s = CountMinSketch::new(16).unwrap();
        s.increment(1);
        s.increment(1);
        s.increment(1);
        s.increment(1);
        s.reset();
        assert_eq!(s.estimate(1), 2);
    }

    #[test]
    fn test_count_min_sketch_clear() {
        let mut s = CountMinSketch::new(16).unwrap();
        (0..16).for_each(|i| s.increment(i));
        s.clear();
        (0..16).for_each(|i| assert_eq!(s.estimate(i), 0));
    }
}
