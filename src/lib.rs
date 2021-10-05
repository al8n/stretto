#![cfg_attr(feature = "nightly", feature(auto_traits, negative_impls))]
/// This package includes multiple probabalistic data structures needed for
/// admission/eviction metadata. Most are Counting Bloom Filter variations, but
/// a caching-specific feature that is also required is a "freshness" mechanism,
/// which basically serves as a "lifetime" process. This freshness mechanism
/// was described in the original TinyLFU paper [1], but other mechanisms may
/// be better suited for certain data distributions.
///
/// [1]: https://arxiv.org/abs/1512.00727
pub mod error;
mod histogram;
mod metrics;
mod policy;
mod pool;
mod ring;
mod store;
mod ttl;
pub(crate) mod utils;

#[macro_use]
mod macros;
mod bbloom;
mod cache;
mod sketch;

extern crate atomic;
#[macro_use]
extern crate crossbeam;
#[macro_use]
extern crate log;
extern crate serde;

use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::marker::PhantomData;
use twox_hash::XxHash64;

pub trait Coster<V> {
    /// cost evaluates a value and outputs a corresponding cost. This function
    /// is ran after insert is called for a new item or an item update with a cost
    /// param of 0.
    fn cost(&self, val: &V) -> i64;
}

pub trait KeyHasher<K: Hash + Eq + ?Sized> {
    fn hash_key(&self, k: &K) -> (u64, u64);
}

/// DefaultKeyHasher supports some popular type for cache's key:
///
/// For the below types, the DefaultKeyHasher will do nothing but passthrough, the result is (T as u64, 0)
/// - `u8`, `u16`, `u32`, `u64`, `usize`
/// - `i8`, `i16`, `i32`, `i64`, `isize`
/// - `bool`
///
/// For the below types, DefaultKeyHasher will use SipHasher and XxHash64 to hash key
/// - `String`, `&str`, `Vec<u8>`, `[u8]`
///
/// If you want to use other type as key, you can use `impl_default_key_hasher` macro or write a new impl.
///
/// For example:
/// ```rust
/// use ristretto::{impl_default_key_hasher, KeyHasher, DefaultKeyHasher};
/// use std::hash::{Hash, Hasher};
///
/// #[derive(Hash, Eq)]
/// struct Bytes([u8]);
/// impl_default_key_hasher! {
///     Bytes
/// }
///
/// struct NewImpl;
///
/// impl KeyHasher<NewImpl> for DefaultKeyHasher {
///     fn hash_key(&self, k: &NewImpl) -> (u64, u64) {
///         let mut s = self.build_sip_hasher();
///         k.hash(&mut s);
///         let mut x = self.build_xx_hasher();
///         k.hash(&mut x);
///         (s.finish(), !x.finish())
///     }
/// }
///
/// ```
#[derive(Debug)]
pub struct DefaultKeyHasher {
    s: RandomState,
    xx: BuildHasherDefault<XxHash64>,
}

impl DefaultKeyHasher {
    pub fn build_sip_hasher(&self) -> DefaultHasher {
        self.s.build_hasher()
    }

    pub fn build_xxhasher(&self) -> XxHash64 {
        self.xx.build_hasher()
    }
}

impl Default for DefaultKeyHasher {
    fn default() -> Self {
        Self {
            s: Default::default(),
            xx: Default::default(),
        }
    }
}

#[macro_export]
macro_rules! impl_default_key_hasher {
    ($($t:ty),*) => {
        $(
            impl KeyHasher<$t> for DefaultKeyHasher {
                fn hash_key(&self, k: &$t) -> (u64, u64) {
                    let mut s = self.s.build_hasher();
                    k.hash(&mut s);
                    let mut x = self.xx.build_hasher();
                    k.hash(&mut x);
                    (s.finish(), x.finish())
                }
            }
        )*
    }
}

impl_default_key_hasher! {
    String,
    &str,
    Vec<u8>,
    [u8]
}

macro_rules! impl_passthrough {
    ($($t:ty),*) => {
        $(
            impl KeyHasher<$t> for DefaultKeyHasher {
                fn hash_key(&self, k: &$t) -> (u64, u64) {
                    ((*k) as u64, 0)
                }
            }
        )*
    }
}

impl_passthrough! {
    bool,
    u8,
    u16,
    u32,
    u64,
    usize,
    i8,
    i16,
    i32,
    i64,
    isize
}

pub struct Item<V> {
    pub val: Option<V>,
}

impl<V> Item<V> {}

pub trait UpdateValidator<V> {
    /// should_update is called when a value already exists in cache and is being updated.
    fn should_update(&self, prev: &V, curr: &V) -> bool;
}

pub trait CacheCallback<V> {
    /// on_evict is called for every eviction and passes the hashed key, value,
    /// and cost to the function.
    fn on_exit(&self, val: Option<V>);

    /// on_exit is called whenever a value is removed from cache. This can be
    /// used to do manual memory deallocation. Would also be called on eviction
    /// and rejection of the value.
    fn on_evict(&self, item: Item<V>);

    /// on_reject is called for every rejection done via the policy.
    fn on_reject(&self, item: Item<V>);
}

// #[derive(Copy, Clone, Debug)]
// pub struct DefaultCacheCallback<T>{_marker: PhantomData<T>}
//
// impl<T> CacheCallback for DefaultCacheCallback<T> {
//     type Value = T;
//
//     fn on_exit(&self, _val: Option<Self::Value>) {}
//
//     fn on_evict(&self, item: Item<Self::Value>) {
//         self.on_exit(item.val)
//     }
//
//     fn on_reject(&self, item: Item<Self::Value>) {
//         self.on_exit(item.val)
//     }
// }

#[cfg(test)]
mod test {
    use crate::{DefaultKeyHasher, KeyHasher};

    #[test]
    fn test_default_key_hasher() {
        let kh = DefaultKeyHasher::default();
        let v1 = kh.hash_key(&vec![8u8; 8]);
        let v2 = kh.hash_key(&vec![8u8; 8]);
        assert_eq!(v1, v2);
        println!("{:?} {:?}", v1, v2);
    }
}
