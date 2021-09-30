#![cfg_attr(feature = "nightly", feature(generic_const_exprs))]
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

use core::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;

pub trait KeyHasher<K: Hash + Eq> {
    fn hash_key<Q>(&self, k: &Q) -> u64
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
}

#[derive(Debug)]
pub struct DefaultKeyHasher<K: Hash + Eq> {
    s: RandomState,
    marker: PhantomData<K>,
}

impl<K: Hash + Eq> Default for DefaultKeyHasher<K> {
    fn default() -> Self {
        Self {
            marker: Default::default(),
            s: Default::default(),
        }
    }
}

impl<K: Hash + Eq> KeyHasher<K> for DefaultKeyHasher<K> {
    fn hash_key<Q>(&self, k: &Q) -> u64
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut s = self.s.build_hasher();
        k.hash(&mut s);
        s.finish()
    }
}
