//! This mod implements a Simple Bloom Filter.
//!
//! This file is a mechanical translation of the reference Golang code, available at <https://github.com/dgraph-io/ristretto/blob/master/z/bbloom.go>
//!
//! I claim no additional copyright over the original implementation.
use std::ptr::addr_of;

const MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
const LN_2: f64 = 0.69314718056;

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

fn calc_size_by_wrong_positives(num_entries: f64, wrongs: f64) -> EntriesLocs {
    let size = -1f64 * num_entries * wrongs.ln() / LN_2.powf(2f64);
    let locs = (LN_2 * size / num_entries).ceil();

    EntriesLocs {
        entries: size as u64,
        locs: locs as u64,
    }
}

/// Bloom filter
pub(crate) struct Bloom {
    bitset: Vec<u64>,
    elem_num: u64,
    size_exp: u64,
    size: u64,
    set_locs: u64,
    shift: u64,
}

impl Bloom {
    pub fn new(cap: usize, false_positive_ratio: f64) -> Self {
        let entries_locs = {
            if false_positive_ratio < 1f64 {
                calc_size_by_wrong_positives(cap as f64, false_positive_ratio)
            } else {
                EntriesLocs {
                    entries: cap as u64,
                    locs: false_positive_ratio as u64,
                }
            }
        };

        let size = get_size(entries_locs.entries);
        let this = Self {
            bitset: vec![0; (size.size >> 6) as usize],
            elem_num: 0,
            size: size.size - 1,
            size_exp: size.exp,
            set_locs: entries_locs.locs,
            shift: 64 - size.exp,
        };
        this
    }

    /// `size` makes Bloom filter with as bitset of size sz.
    #[inline]
    #[allow(dead_code)]
    pub fn size(&mut self, sz: usize) {
        self.bitset = vec![0; sz >> 6]
    }

    /// `reset` resets the `Bloom` filter
    pub fn reset(&mut self) {
        self.bitset.iter_mut().for_each(|v| *v = 0);
    }

    /// Returns the exp of the size
    #[inline]
    #[allow(dead_code)]
    pub fn size_exp(&self) -> u64 {
        self.size_exp
    }

    /// `clear` clear the `Bloom` filter
    pub fn clear(&mut self) {
        self.bitset.iter_mut().for_each(|v| *v = 0);
    }

    /// `set` sets the bit[idx] of bitset
    pub fn set(&mut self, idx: usize) {
        let raw = (addr_of!(self.bitset[idx >> 6]) as *const u64) as u64;
        let offset = ((idx % 64) >> 3) as u64;
        let ptr = (raw + offset) as *mut u64;
        unsafe {
            *ptr |= MASK[idx % 8] as u64;
        }
    }

    /// `is_set` checks if bit[idx] of bitset is set, returns true/false.
    pub fn is_set(&self, idx: usize) -> bool {
        let raw = (addr_of!(self.bitset[idx >> 6]) as *const u64) as u64;
        let offset = ((idx % 64) >> 3) as u64;
        let ptr = (raw + offset) as *mut u64;
        unsafe {
            let r = (*ptr >> (idx % 8)) & 1;
            r == 1
        }
    }

    /// `add` adds hash of a key to the bloom filter
    pub fn add(&mut self, hash: u64) {
        let h = hash >> self.shift;
        let l = (hash << self.shift) >> self.shift;
        (0..self.set_locs).for_each(|i| {
            self.set(((h + i * l) & self.size) as usize);
            self.elem_num += 1;
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
    pub fn contains_or_add(&mut self, hash: u64) -> bool {
        if self.contains(hash) {
            false
        } else {
            self.add(hash);
            true
        }
    }

    /// `total_size` returns the total size of the bloom filter.
    #[allow(dead_code)]
    #[inline]
    pub fn total_size(&self) -> usize {
        // The bl struct has 5 members and each one is 8 byte. The bitset is a
        // uint64 byte slice.
        self.bitset.len() * 8 + 5 * 8
    }
}

#[cfg(test)]
mod test {
    use crate::bbloom::Bloom;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    const N: usize = 1 << 16;

    fn get_word_list() -> Vec<Vec<u8>> {
        let mut word_list = Vec::<Vec<u8>>::with_capacity(N);
        (0..N).for_each(|_| {
            let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
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
        let mut bf = Bloom::new(N * 10, 7f64);

        let mut cnt = 0;
        get_word_list().into_iter().for_each(|words| {
            let hash = calculate_hash(&words);
            if !bf.contains_or_add(hash) {
                cnt += 1;
            }
        });

        eprintln!("Bloomfilter(size = {}) Check for 'false positives': {} wrong positive 'Has' results on 2^16 entries => {}%", bf.bitset.len() << 6, cnt, cnt as f64 / N as f64);
    }

    #[test]
    fn test_total_size() {
        let bf = Bloom::new(10, 7f64);
        assert_eq!(bf.total_size(), 104);
    }

    #[test]
    fn test_size_exp() {
        let bf = Bloom::new(10, 7f64);
        assert_eq!(bf.size_exp(), 9);
    }

    #[test]
    fn test_size() {
        let mut bf = Bloom::new(10, 7f64);
        bf.size(1024);
        assert_eq!(bf.bitset.len(), 16);
    }
}
