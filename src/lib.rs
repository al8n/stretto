#![deny(missing_docs)]
pub mod error;
#[macro_use]
mod macros;
mod bbloom;
mod cache;
mod histogram;
mod metrics;
/// This package includes multiple probabalistic data structures needed for
/// admission/eviction metadata. Most are Counting Bloom Filter variations, but
/// a caching-specific feature that is also required is a "freshness" mechanism,
/// which basically serves as a "lifetime" process. This freshness mechanism
/// was described in the original TinyLFU paper [1], but other mechanisms may
/// be better suited for certain data distributions.
///
/// [1]: https://arxiv.org/abs/1512.00727
mod policy;
mod ring;
mod sketch;
mod store;
mod ttl;
pub(crate) mod utils;

extern crate atomic;

#[cfg(feature = "log")]
#[macro_use]
extern crate log;

#[cfg(feature = "serde")]
extern crate serde;

cfg_async! {
    pub(crate) use tokio::sync::mpsc::{channel as bounded, Sender, UnboundedSender, Receiver, UnboundedReceiver};
    pub(crate) use tokio::time::{Instant, sleep};
    pub(crate) use tokio::task::{spawn, JoinHandle};
    pub(crate) use tokio::select;
    use tokio::sync::mpsc::{unbounded_channel};

    pub(crate) fn stop_channel() -> (Sender<()>, Receiver<()>) {
        bounded(1)
    }

    pub(crate) fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
        unbounded_channel::<T>()
    }
}

cfg_not_async! {
    pub(crate) use std::time::Instant;
    pub(crate) use std::thread::{JoinHandle, spawn, sleep};
    pub(crate) use crossbeam_channel::{unbounded, bounded, Sender, Receiver, select};

    pub(crate) type UnboundedSender<T> = Sender<T>;
    pub(crate) type UnboundedReceiver<T> = Receiver<T>;

    pub(crate) fn stop_channel() -> (Sender<()>, Receiver<()>) {
        bounded(0)
    }
}

pub use cache::{Cache, CacheBuilder};
pub use error::CacheError;
pub use metrics::{MetricType, Metrics};
pub use utils::{ValueRef, ValueRefMut};

use crate::ttl::Time;
use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::marker::PhantomData;
use twox_hash::XxHash64;
use std::fmt::{Debug, Formatter};

/// Item is the parameter when Cache reject, evict value,
pub struct Item<V> {
    pub val: Option<V>,
    pub index: u64,
    pub conflict: u64,
    pub cost: i64,
    pub exp: Time,
}

impl<V: Clone> Clone for Item<V> {
    fn clone(&self) -> Self {
        Self {
            val: self.val.clone(),
            index: self.index,
            conflict: self.conflict,
            cost: self.cost,
            exp: self.exp
        }
    }
}

impl<V: Copy> Copy for Item<V> {}

impl<V: Debug> Debug for Item<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Item")
            .field("value", &self.val)
            .field("cost", &self.cost)
            .field("ttl", &self.exp)
            .finish()
    }
}

/// By default, the Cache will always update the value if the value already exists in the cache,
/// this trait is for you to check if the value should be updated.
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

/// CacheCallback is for customize some extra operations on values when related event happens.
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

/// Cost is a trait you can pass to the CacheBuilder in order to evaluate
/// item cost at runtime, and only for the `insert` calls that aren't dropped (this is
/// useful if calculating item cost is particularly expensive, and you don't want to
/// waste time on items that will be dropped anyways).
///
/// To signal to Stretto that you'd like to use this Coster trait:
///
/// 1. Set the Coster field to your own Coster implementation.
/// 2. When calling `insert` for new items or item updates, use a `cost` of 0.
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

/// KeyBuilder is the hashing algorithm used for every key. In Stretto, the Cache will never store the real key.
/// The key will be processed by `KeyBuilder`.
/// Stretto has two default built-in key builder, one is [`TransparentKeyBuilder`],
/// the other is [`DefaultKeyBuilder`]. If your key is `u*` or `i*`(such as `u64`, `i64`),
/// you can use [`TransparentKeyBuilder`] which is faster than [`DefaultKeyBuilder`].
/// You can also implement your own `KeyBuilder`.
///
///
/// Note that if you want 128bit hashes you should use the full `[2]uint64`,
/// otherwise just fill the `uint64` at the `0` position, and it will behave like
/// any 64bit hash.
///
/// [`TransparentKeyBuilder`]: struct.TransparentKeyBuilder.html
/// [`DefaultKeyBuilder`]: struct.DefaultKeyBuilder.html
pub trait KeyBuilder<K: Hash + Eq + ?Sized> {
    /// `hash_index` is used to hash the key to u64
    fn hash_index(&self, key: &K) -> u64;

    /// if you want a 128bit hashes, you should implement this method,
    /// or leave this method return 0
    fn hash_conflict(&self, key: &K) -> u64 {
        let _ = key;
        0
    }

    /// build the key to 128bit hashes.
    fn build_key(&self, k: &K) -> (u64, u64) {
        (self.hash_index(k), self.hash_conflict(k))
    }
}

/// DefaultKeyBuilder is a built-in `KeyBuilder` for the Cache.
///
/// If the key implements [`TransparentKey`] trait, use [`TransparentKeyBuilder`].
/// u8, u16, u32, u64, i8, i16, i32, i64, bool implement [`TransparentKey`] by default.
///
/// See [`KeyBuilder`] if you want to write a customized [`KeyBuilder`].
///
/// [`KeyBuilder`]: trait.KeyBuilder.html
/// [`TransparentKey`]: trait.TransparentKey.html
/// [`TransparentKeyBuilder`]: struct.TransparentKeyBuilder.html
#[derive(Debug)]
pub struct DefaultKeyBuilder {
    s: RandomState,
    xx: BuildHasherDefault<XxHash64>,
}

impl DefaultKeyBuilder {
    pub fn build_sip_hasher(&self) -> DefaultHasher {
        self.s.build_hasher()
    }

    pub fn build_xxhasher(&self) -> XxHash64 {
        self.xx.build_hasher()
    }
}

impl Default for DefaultKeyBuilder {
    fn default() -> Self {
        Self {
            s: Default::default(),
            xx: Default::default(),
        }
    }
}

impl<K: Hash + Eq + ?Sized> KeyBuilder<K> for DefaultKeyBuilder {
    #[inline]
    fn hash_index(&self, key: &K) -> u64 {
        let mut s = self.s.build_hasher();
        key.hash(&mut s);
        s.finish()
    }

    #[inline]
    fn hash_conflict(&self, key: &K) -> u64 {
        let mut x = self.xx.build_hasher();
        key.hash(&mut x);
        x.finish()
    }
}

/// Implement this trait for the key, if you want to use [`TransparentKeyBuilder`] as the [`KeyBuilder`]
/// for the [`Cache`].
///
/// u8, u16, u32, u64, i8, i16, i32, i64, bool implement TransparentKey by default.
///
/// [`TransparentKeyBuilder`]: struct.TransparentKeyBuilder.html
/// [`KeyBuilder`]: trait.KeyBuilder.html
/// [`Cache`]: struct.Cache.html
pub trait TransparentKey: Hash + Eq {
    /// convert self to `u64`
    fn to_u64(&self) -> u64;
}

/// TransparentKeyBuilder converts key to `u64`.
/// If the key does not implement the trait [`TransparentKey`], please use [`DefaultKeyBuilder`]
/// or write a custom key builder.
///
/// [`DefaultKeyBuilder`]: struct.DefaultKeyBuilder.html
/// [`TransparentKey`]: trait.TransparentKey.html
#[derive(Default, Copy, Clone, Eq, PartialEq, Debug)]
pub struct TransparentKeyBuilder<K: TransparentKey>{ _marker: PhantomData<K>}

impl<K: TransparentKey> KeyBuilder<K> for TransparentKeyBuilder<K> {
    #[inline]
    fn hash_index(&self, key: &K) -> u64 {
        key.to_u64()
    }

    #[inline]
    fn hash_conflict(&self, _key: &K) -> u64 {
        0
    }
}

macro_rules! impl_transparent_key {
    ($($t:ty),*) => {
        $(
            impl TransparentKey for $t {
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
