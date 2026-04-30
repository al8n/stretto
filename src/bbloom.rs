//! This mod implements a Simple Bloom Filter.
//!
//! This file is a mechanical translation of the reference Golang code, available at <https://github.com/dgraph-io/ristretto/blob/master/z/bbloom.go>
//!
//! I claim no additional copyright over the original implementation.
use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const LN_2: f64 = std::f64::consts::LN_2;

struct Size {
  size: u64,
  exp: u64,
}

fn get_size(n: u64) -> Size {
  let mut n = n;
  if n < 512 {
    n = 512;
  }

  let mut size = 1u64;
  let mut exp = 0u64;
  while size < n {
    size <<= 1;
    exp += 1;
  }

  Size { size, exp }
}

struct EntriesLocs {
  entries: u64,
  locs: u64,
}

fn calc_size_by_wrong_positives(num_entries: usize, wrongs: f64) -> EntriesLocs {
  let num_entries = num_entries as f64;
  let size = -num_entries * wrongs.ln() / LN_2.powf(2f64);
  let locs = (LN_2 * size / num_entries).ceil();

  EntriesLocs {
    entries: size as u64,
    locs: locs as u64,
  }
}

/// Bloom filter.
///
/// Backed by `Arc<[AtomicU64]>` so `Clone` bumps a refcount (bits are
/// shared) instead of deep-copying. The slice is allocated once at
/// construction and never resized; mutation goes through the atomic
/// cells via interior mutability.
#[derive(Clone)]
pub(crate) struct Bloom {
  bitset: Arc<[AtomicU64]>,
  size_exp: u64,
  size: u64,
  set_locs: u64,
  shift: u64,
}

/// Upper bound on the requested bitset size (in bits) before rounding up to
/// the next power of two. 2^40 bits = 128 GiB — already absurd for a
/// doorkeeper. The cap exists to keep `get_size`'s doubling loop and the
/// subsequent `vec![0; size >> 6]` allocation bounded; without it, a tiny
/// `false_positive_ratio` plus a large `cap` produces `f64::INFINITY` →
/// `u64::MAX` after the cast, which overflows the loop and aborts the
/// allocator.
const MAX_ENTRIES: u64 = 1 << 40;

/// Upper bound on `set_locs` (probes per insert / lookup). Practical Bloom
/// filters use single-digit values (~7 at the 0.01 false-positive ratio).
/// 64 is a generous ceiling that still rejects the pathological case where
/// a large finite `false_positive_ratio` (treated as a literal locs count)
/// would make `add`/`contains` loop billions of times per call.
const MAX_LOCS: u64 = 64;

impl Bloom {
  pub fn new(cap: usize, false_positive_ratio: f64) -> Self {
    // Reject inputs that flow through the FP math into a saturated `entries`
    // or `locs == 0`. A zero `set_locs` makes `contains()` vacuously return
    // true (the for-loop body never runs), so the doorkeeper would let
    // every hash through and silently defeat TinyLFU's admission filter.
    assert!(
      false_positive_ratio.is_finite() && false_positive_ratio > 0.0,
      "false_positive_ratio must be finite and > 0; got {false_positive_ratio}"
    );
    let cap = cap.max(1);

    let entries_locs = {
      if false_positive_ratio < 1f64 {
        calc_size_by_wrong_positives(cap, false_positive_ratio)
      } else {
        EntriesLocs {
          entries: cap as u64,
          locs: false_positive_ratio as u64,
        }
      }
    };

    // Reject saturated derived sizes. After `f64 -> u64` an infinite or
    // huge intermediate becomes `u64::MAX`, which would otherwise overflow
    // `get_size`'s doubling loop and trigger an absurd allocation.
    assert!(
      entries_locs.entries > 0 && entries_locs.entries <= MAX_ENTRIES,
      "bloom entries out of range: {} (cap={cap}, ratio={false_positive_ratio}); \
       max is {MAX_ENTRIES}",
      entries_locs.entries
    );
    assert!(
      entries_locs.locs > 0 && entries_locs.locs <= MAX_LOCS,
      "bloom set_locs out of range: {} (ratio={false_positive_ratio}); \
       max is {MAX_LOCS}",
      entries_locs.locs
    );

    let size = get_size(entries_locs.entries);

    let words = (size.size >> 6) as usize;
    let mut bitset = Vec::with_capacity(words);
    bitset.resize_with(words, || AtomicU64::new(0));

    Self {
      bitset: Arc::from(bitset),
      size: size.size - 1,
      size_exp: size.exp,
      set_locs: entries_locs.locs,
      shift: 64 - size.exp,
    }
  }

  /// `reset` resets the `Bloom` filter
  pub fn reset(&self) {
    for word in self.bitset.iter() {
      word.store(0, Ordering::Relaxed);
    }
  }

  /// Returns the exp of the size
  #[cfg_attr(not(tarpaulin), inline(always))]
  #[allow(dead_code)]
  pub fn size_exp(&self) -> u64 {
    self.size_exp
  }

  /// `clear` clear the `Bloom` filter
  pub fn clear(&self) {
    for word in self.bitset.iter() {
      word.store(0, Ordering::Relaxed);
    }
  }

  /// `set` sets the bit[idx] of bitset
  pub fn set(&self, idx: usize) {
    let word = idx >> 6;
    let mask = 1u64 << (idx & 63);
    self.bitset[word].fetch_or(mask, Ordering::Relaxed);
  }

  /// `is_set` checks if bit[idx] of bitset is set, returns true/false.
  pub fn is_set(&self, idx: usize) -> bool {
    let word = idx >> 6;
    let mask = 1u64 << (idx & 63);
    self.bitset[word].load(Ordering::Relaxed) & mask != 0
  }

  /// `add` adds hash of a key to the bloom filter
  pub fn add(&self, hash: u64) {
    let h = hash >> self.shift;
    let l = (hash << self.shift) >> self.shift;
    (0..self.set_locs).for_each(|i| {
      self.set(((h + i * l) & self.size) as usize);
    });
  }

  /// `contains` checks if bit(s) for entry hash is/are set,
  /// returns true if the hash was added to the Bloom Filter.
  pub fn contains(&self, hash: u64) -> bool {
    let h = hash >> self.shift;
    let l = (hash << self.shift) >> self.shift;
    for i in 0..self.set_locs {
      if !self.is_set(((h + i * l) & self.size) as usize) {
        return false;
      }
    }
    true
  }

  /// `contains_or_add` only Adds hash, if it's not present in the bloomfilter.
  /// Returns true if hash was added.
  /// Returns false if hash was already registered in the bloomfilter.
  ///
  /// The check-then-add is intentionally non-atomic — concurrent calls for the
  /// same hash may both observe `contains == false` and both `add`. This is
  /// harmless because `add` is idempotent (`fetch_or` of the same mask) and
  /// the doorkeeper is a probabilistic structure where the worst case is a
  /// duplicate `true` return; TinyLFU treats both observations identically.
  pub fn contains_or_add(&self, hash: u64) -> bool {
    if self.contains(hash) {
      false
    } else {
      self.add(hash);
      true
    }
  }

  /// `total_size` returns the total size of the bloom filter.
  #[allow(dead_code)]
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn total_size(&self) -> usize {
    // Four u64 metadata fields (size_exp, size, set_locs, shift) plus the
    // bitset (Vec<u64>). The Vec's heap header is intentionally excluded —
    // this counts the live data, matching the Go reference.
    self.bitset.len() * 8 + 4 * 8
  }
}

#[cfg(test)]
mod test {
  use crate::bbloom::Bloom;
  use rand::{RngExt, distr::Alphanumeric};
  use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
  };

  const N: usize = 1 << 16;

  fn get_word_list() -> Vec<Vec<u8>> {
    let mut word_list = Vec::<Vec<u8>>::with_capacity(N);
    (0..N).for_each(|_| {
      let rand_string: String = rand::rng()
        .sample_iter(Alphanumeric)
        .take(30)
        .map(char::from)
        .collect();
      word_list.push(rand_string.as_bytes().to_vec());
    });
    word_list
  }

  fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
  }

  #[test]
  fn test_number_of_wrongs() {
    let bf = Bloom::new(N * 10, 7f64);

    let mut cnt = 0;
    get_word_list().into_iter().for_each(|words| {
      let hash = calculate_hash(&words);
      if !bf.contains_or_add(hash) {
        cnt += 1;
      }
    });

    eprintln!(
      "Bloomfilter(size = {}) Check for 'false positives': {} wrong positive 'Has' results on 2^16 entries => {}%",
      bf.bitset.len() << 6,
      cnt,
      cnt as f64 / N as f64
    );
  }

  #[test]
  fn test_total_size() {
    let bf = Bloom::new(10, 7f64);
    // 8 u64 bitset words (512 bits, the floor for tiny filters) + 4 u64
    // metadata fields = 64 + 32 = 96.
    assert_eq!(bf.total_size(), 96);
  }

  #[test]
  fn test_size_exp() {
    let bf = Bloom::new(10, 7f64);
    assert_eq!(bf.size_exp(), 9);
  }

  #[test]
  #[should_panic(expected = "false_positive_ratio must be finite")]
  fn test_new_rejects_zero_ratio() {
    let _ = Bloom::new(10, 0.0);
  }

  #[test]
  #[should_panic(expected = "false_positive_ratio must be finite")]
  fn test_new_rejects_nan_ratio() {
    let _ = Bloom::new(10, f64::NAN);
  }

  #[test]
  #[should_panic(expected = "false_positive_ratio must be finite")]
  fn test_new_rejects_negative_ratio() {
    let _ = Bloom::new(10, -0.5);
  }

  #[test]
  fn test_new_clamps_zero_cap() {
    // cap == 0 used to flow into NaN -> 0 for set_locs, which made
    // contains() vacuously return true. With cap.max(1), the constructor
    // produces a usable filter.
    let bf = Bloom::new(0, 0.01);
    assert!(bf.set_locs > 0);
  }

  #[test]
  #[should_panic(expected = "bloom entries out of range")]
  fn test_new_rejects_saturating_entries() {
    // A huge cap with a tiny ratio drives the f64 size to +inf, which casts
    // to u64::MAX and would overflow get_size's doubling loop / trigger an
    // absurd allocation. Must be rejected before constructing the filter.
    let _ = Bloom::new(usize::MAX, 1e-300);
  }

  #[test]
  #[should_panic(expected = "bloom set_locs out of range")]
  fn test_new_rejects_huge_locs() {
    // ratio >= 1.0 is interpreted as a literal locs count. A huge finite
    // value would make add/contains loop billions of times per call.
    let _ = Bloom::new(10, 1e15);
  }

  #[test]
  fn test_bloom_concurrent_add_distinct_keys() {
    // 16 threads, each adds 256 distinct hashes. After joining, every hash
    // must be observable via contains() — no lost updates from racing
    // fetch_or on the same word.
    use std::{sync::Arc, thread};

    let bf = Arc::new(Bloom::new(8192, 0.01));
    let mut handles = Vec::with_capacity(16);
    for t in 0..16u64 {
      let bf = Arc::clone(&bf);
      handles.push(thread::spawn(move || {
        let base = t * 1_000_000;
        for i in 0..256u64 {
          bf.add(base + i);
        }
      }));
    }
    for h in handles {
      h.join().unwrap();
    }

    for t in 0..16u64 {
      let base = t * 1_000_000;
      for i in 0..256u64 {
        assert!(
          bf.contains(base + i),
          "hash {} (thread {}) not present after concurrent add",
          base + i,
          t
        );
      }
    }
  }

  #[test]
  fn test_bloom_concurrent_add_same_key() {
    // Many threads adding the same hash must all observe it. fetch_or is
    // idempotent so this is the easy case — guards against any future
    // refactor that introduces a non-idempotent set primitive.
    use std::{sync::Arc, thread};

    let bf = Arc::new(Bloom::new(1024, 0.01));
    let mut handles = Vec::with_capacity(8);
    for _ in 0..8 {
      let bf = Arc::clone(&bf);
      handles.push(thread::spawn(move || {
        for _ in 0..1000 {
          bf.add(0xdead_beef);
        }
      }));
    }
    for h in handles {
      h.join().unwrap();
    }
    assert!(bf.contains(0xdead_beef));
  }

  #[test]
  fn test_bloom_concurrent_add_and_reset() {
    // One thread adds, another resets. After all threads join we don't
    // assert specific contents (reset/add ordering is genuinely racy), only
    // that no panic / UB occurred and contains is still callable.
    use std::{
      sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
      },
      thread,
    };

    let bf = Arc::new(Bloom::new(2048, 0.01));
    let stop = Arc::new(AtomicBool::new(false));

    let adder = {
      let bf = Arc::clone(&bf);
      let stop = Arc::clone(&stop);
      thread::spawn(move || {
        let mut h = 1u64;
        while !stop.load(Ordering::Relaxed) {
          bf.add(h);
          h = h.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        }
      })
    };
    let resetter = {
      let bf = Arc::clone(&bf);
      let stop = Arc::clone(&stop);
      thread::spawn(move || {
        for _ in 0..1000 {
          if stop.load(Ordering::Relaxed) {
            break;
          }
          bf.reset();
        }
      })
    };

    resetter.join().unwrap();
    stop.store(true, Ordering::Relaxed);
    adder.join().unwrap();

    let _ = bf.contains(0);
  }
}
