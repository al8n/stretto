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
#[allow(dead_code)]
mod metrics;
#[allow(dead_code)]
mod policy;
mod ring;
mod store;
#[allow(dead_code)]
mod ttl;
#[allow(dead_code)]
pub(crate) mod utils;

#[macro_use]
mod macros;
mod bbloom;
#[allow(dead_code)]
mod cache;
mod sketch;

extern crate atomic;
#[macro_use]
extern crate crossbeam;
#[macro_use]
extern crate log;
extern crate serde;

use crate::ttl::Time;
use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::marker::PhantomData;
use twox_hash::XxHash64;

pub struct Item<V> {
    pub val: Option<V>,
    pub key: u64,
    pub conflict: u64,
    pub cost: i64,
    pub exp: Time,
}

impl<V> Item<V> {}

pub trait UpdateValidator<V>: Send + Sync + 'static {
    /// should_update is called when a value already exists in cache and is being updated.
    fn should_update(&self, prev: &V, curr: &V) -> bool;
}

/// DefaultUpdateValidator is a noop update validator.
#[doc(hidden)]
pub struct DefaultUpdateValidator<V: Send + Sync> {
    _marker: PhantomData<fn(V)>,
}

impl<V: Send + Sync> Default for DefaultUpdateValidator<V> {
    fn default() -> Self {
        Self {
            _marker: PhantomData::<fn(V)>,
        }
    }
}

impl<V: Send + Sync + 'static> UpdateValidator<V> for DefaultUpdateValidator<V> {
    #[inline]
    fn should_update(&self, _prev: &V, _curr: &V) -> bool {
        true
    }
}

pub trait CacheCallback<V: Send + Sync>: Send + Sync + 'static {
    /// on_exit is called whenever a value is removed from cache. This can be
    /// used to do manual memory deallocation. Would also be called on eviction
    /// and rejection of the value.
    fn on_exit(&self, val: Option<V>);

    /// on_evict is called for every eviction and passes the hashed key, value,
    /// and cost to the function.
    fn on_evict(&self, item: Item<V>) {
        self.on_exit(item.val)
    }

    /// on_reject is called for every rejection done via the policy.
    fn on_reject(&self, item: Item<V>) {
        self.on_exit(item.val)
    }
}

/// DefaultCacheCallback is a noop CacheCallback implementation.
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct DefaultCacheCallback<V> {
    _marker: PhantomData<V>,
}

impl<V> Default for DefaultCacheCallback<V> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<V: Send + Sync + 'static> CacheCallback<V> for DefaultCacheCallback<V> {
    fn on_exit(&self, _val: Option<V>) {}
}

pub trait Coster<V>: Send + Sync + 'static {
    /// cost evaluates a value and outputs a corresponding cost. This function
    /// is ran after insert is called for a new item or an item update with a cost
    /// param of 0.
    fn cost(&self, val: &V) -> i64;
}

/// DefaultCoster is a noop Coster implementation.
#[doc(hidden)]
pub struct DefaultCoster<V> {
    _marker: PhantomData<fn(V)>,
}

impl<V> Default for DefaultCoster<V> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<V: 'static> Coster<V> for DefaultCoster<V> {
    #[inline]
    fn cost(&self, _val: &V) -> i64 {
        0
    }
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
///
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
pub struct CacheKeyHasher {
    s: RandomState,
    xx: BuildHasherDefault<XxHash64>,
}

impl CacheKeyHasher {
    pub fn build_sip_hasher(&self) -> DefaultHasher {
        self.s.build_hasher()
    }

    pub fn build_xxhasher(&self) -> XxHash64 {
        self.xx.build_hasher()
    }
}

impl Default for CacheKeyHasher {
    fn default() -> Self {
        Self {
            s: Default::default(),
            xx: Default::default(),
        }
    }
}

impl<K: Hash + Eq + ?Sized> KeyHasher<K> for CacheKeyHasher {
    fn hash_key(&self, k: &K) -> (u64, u64) {
        let mut s = self.s.build_hasher();
        k.hash(&mut s);
        let mut x = self.xx.build_hasher();
        k.hash(&mut x);
        (s.finish(), x.finish())
    }
}

pub trait TransparentCacheKey: Hash + Eq {
    fn to_u64(&self) -> u64;
}

#[derive(Default)]
pub struct TransparentCacheKeyHasher;

impl<K: TransparentCacheKey> KeyHasher<K> for TransparentCacheKeyHasher {
    fn hash_key(&self, k: &K) -> (u64, u64) {
        (k.to_u64(), 0)
    }
}

macro_rules! impl_transparent_key {
    ($($t:ty),*) => {
        $(
            impl TransparentCacheKey for $t {
                fn to_u64(&self) -> u64 {
                    *self as u64
                }
            }
        )*
    }
}

impl_transparent_key! {
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

#[cfg(test)]
mod test {
    use crate::{CacheKeyHasher, KeyHasher};

    #[test]
    fn test_default_key_hasher() {
        let kh = CacheKeyHasher::default();
        let v1 = kh.hash_key(&vec![8u8; 8]);
        let v2 = kh.hash_key(&vec![8u8; 8]);
        assert_eq!(v1, v2);
        println!("{:?} {:?}", v1, v2);
    }
}
