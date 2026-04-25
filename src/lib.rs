#![doc = include_str!("../README.md")]
#![deny(missing_docs)]
#![allow(clippy::too_many_arguments, clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
mod bbloom;
mod cache;
mod error;
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
#[allow(dead_code)]
mod policy;
mod ring;
#[cfg(any(feature = "sync", feature = "async"))]
mod semaphore;
mod sketch;
mod store;
mod ttl;
pub(crate) mod utils;

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub(crate) mod axync {
  pub(crate) use async_channel::{Receiver, RecvError, Sender, TrySendError, bounded, unbounded};
  pub(crate) use futures::{channel::oneshot, select};

  /// Signaling half of a one-shot barrier used by `Item::Wait` / `Item::Clear`.
  ///
  /// Wraps `oneshot::Sender<()>` so the shared processor handler can call
  /// `.done()` with the same shape as the sync path's `wg::WaitGroup::done`.
  /// Dropping the waiter without calling `done` still wakes the receiver
  /// (they observe `Err(Canceled)`), which the caller treats as "signaled".
  pub(crate) struct Waiter(oneshot::Sender<()>);

  impl Waiter {
    pub(crate) fn new() -> (Self, oneshot::Receiver<()>) {
      let (tx, rx) = oneshot::channel();
      (Waiter(tx), rx)
    }

    pub(crate) fn done(self) {
      let _ = self.0.send(());
    }
  }

  pub(crate) fn stop_channel() -> (Sender<()>, Receiver<()>) {
    bounded(1)
  }
}
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use cache::{AsyncCache, AsyncCacheBuilder};

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use agnostic_lite::tokio::TokioRuntime;

#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub use agnostic_lite::smol::SmolRuntime;

#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
pub(crate) mod sync {
  pub(crate) use crossbeam_channel::{Receiver, Sender, bounded, select};
  pub(crate) use std::{
    thread::{JoinHandle, spawn},
    time::Instant,
  };

  pub(crate) type WaitGroup = wg::WaitGroup;

  /// Signaling half of the `Item::Wait` / `Item::Clear` barrier.
  ///
  /// Mirrors the async `Waiter` shape: `done(self)` consumes the signal, and
  /// `Drop` fires `wg.done()` if the explicit call was skipped — so a panic
  /// during `on_exit` callbacks on the processor still unblocks the caller
  /// parked on `wg.wait()`. `wg::WaitGroup::done` takes `&self` and has no
  /// Drop-based signaling of its own, so without this wrapper a callback
  /// panic would leave `clear()` / `wait()` hung forever.
  pub(crate) struct Signal(Option<WaitGroup>);

  impl Signal {
    #[inline]
    pub(crate) fn new(wg: WaitGroup) -> Self {
      Self(Some(wg))
    }

    #[inline]
    pub(crate) fn done(mut self) {
      if let Some(wg) = self.0.take() {
        wg.done();
      }
    }
  }

  impl Drop for Signal {
    fn drop(&mut self) {
      if let Some(wg) = self.0.take() {
        wg.done();
      }
    }
  }

  pub(crate) fn stop_channel() -> (Sender<()>, Receiver<()>) {
    bounded(0)
  }
}

#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
pub use cache::{Cache, CacheBuilder};

pub use error::CacheError;
pub use histogram::Histogram;
pub use metrics::{MetricType, Metrics};
pub use utils::{ValueRef, ValueRefMut};

use crate::ttl::Time;
use seahash::SeaHasher;
use std::{
  fmt::{Debug, Formatter},
  hash::{BuildHasher, BuildHasherDefault, Hash, Hasher},
  marker::PhantomData,
};

/// Item is the parameter when Cache reject, evict value,
pub struct Item<V> {
  /// the value of the entry
  pub val: Option<V>,

  /// the index of the entry(created by [`KeyBuilder`])
  ///
  /// [`KeyBuilder`]: struct.KeyBuilder.html
  pub index: u64,

  /// the conflict of the entry(created by [`KeyBuilder`])
  ///
  /// [`KeyBuilder`]: struct.KeyBuilder.html
  pub conflict: u64,

  /// the cost when store the entry in Cache.
  pub cost: i64,

  /// exp contains the ttl information.
  pub exp: Time,
}

impl<V: Clone> Clone for Item<V> {
  fn clone(&self) -> Self {
    Self {
      val: self.val.clone(),
      index: self.index,
      conflict: self.conflict,
      cost: self.cost,
      exp: self.exp,
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
pub trait UpdateValidator: Send + Sync + 'static {
  /// Value
  type Value: Send + Sync + 'static;

  /// should_update is called when a value already exists in cache and is being updated.
  fn should_update(&self, prev: &Self::Value, curr: &Self::Value) -> bool;
}

/// DefaultUpdateValidator is a noop update validator.
#[doc(hidden)]
pub struct DefaultUpdateValidator<V> {
  _marker: PhantomData<fn(V)>,
}

impl<V: Send + Sync> Default for DefaultUpdateValidator<V> {
  fn default() -> Self {
    Self {
      _marker: PhantomData::<fn(V)>,
    }
  }
}

impl<V: Send + Sync + 'static> UpdateValidator for DefaultUpdateValidator<V> {
  type Value = V;

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn should_update(&self, _prev: &Self::Value, _curr: &Self::Value) -> bool {
    true
  }
}

/// CacheCallback is for customize some extra operations on values when related event happens.
pub trait CacheCallback: Send + Sync + 'static {
  /// Value
  type Value: Send + Sync + 'static;

  /// on_exit is called whenever a value is removed from cache. This can be
  /// used to do manual memory deallocation. Would also be called on eviction
  /// and rejection of the value.
  fn on_exit(&self, val: Option<Self::Value>);

  /// on_evict is called for every eviction and passes the hashed key, value,
  /// and cost to the function.
  fn on_evict(&self, item: Item<Self::Value>) {
    self.on_exit(item.val)
  }

  /// on_reject is called for every rejection done via the policy.
  fn on_reject(&self, item: Item<Self::Value>) {
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

impl<V: Send + Sync + 'static> CacheCallback for DefaultCacheCallback<V> {
  type Value = V;

  fn on_exit(&self, _val: Option<Self::Value>) {}
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
pub trait Coster: Send + Sync + 'static {
  /// Value
  type Value: Send + Sync + 'static;

  /// cost evaluates a value and outputs a corresponding cost. This function
  /// is ran after insert is called for a new item or an item update with a cost
  /// param of 0.
  fn cost(&self, val: &Self::Value) -> i64;
}

/// DefaultCoster is a noop Coster implementation.
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

impl<V: Send + Sync + 'static> Coster for DefaultCoster<V> {
  type Value = V;

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn cost(&self, _val: &V) -> i64 {
    0
  }
}

/// [`KeyBuilder`] is the hashing algorithm used for every key. In Stretto, the Cache will never store the real key.
/// The key will be processed by [`KeyBuilder`]. Stretto has two default built-in key builder,
/// one is [`TransparentKeyBuilder`], the other is [`DefaultKeyBuilder`]. If your key implements [`TransparentKey`] trait,
/// you can use [`TransparentKeyBuilder`] which is faster than [`DefaultKeyBuilder`]. Otherwise, you should use [`DefaultKeyBuilder`]
/// You can also write your own key builder for the Cache, by implementing [`KeyBuilder`] trait.
///
/// Note that if you want 128bit hashes you should use the full `(u64, u64)`,
/// otherwise just fill the `u64` at the `0` position, and it will behave like
/// any 64bit hash.
///
/// [`KeyBuilder`]: trait.KeyBuilder.html
/// [`TransparentKey`]: trait.TransparentKey.html
/// [`TransparentKeyBuilder`]: struct.TransparentKeyBuilder.html
/// [`DefaultKeyBuilder`]: struct.DefaultKeyBuilder.html
pub trait KeyBuilder {
  /// Key
  type Key: Hash + Eq + ?Sized;

  /// `hash_index` is used to hash the key to u64
  fn hash_index<Q>(&self, key: &Q) -> u64
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized;

  /// if you want a 128bit hashes, you should implement this method,
  /// or leave this method return 0
  fn hash_conflict<Q>(&self, key: &Q) -> u64
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    let _ = key;
    0
  }

  /// build the key to 128bit hashes.
  fn build_key<Q>(&self, k: &Q) -> (u64, u64)
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
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
pub struct DefaultKeyBuilder<K> {
  xx: xxhash_rust::xxh64::Xxh64Builder,
  sea: BuildHasherDefault<SeaHasher>,
  _marker: PhantomData<K>,
}

impl<K> Debug for DefaultKeyBuilder<K> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("DefaultKeyBuilder").finish()
  }
}

impl<K> Default for DefaultKeyBuilder<K> {
  fn default() -> Self {
    let seed = rand::random::<u64>();
    Self {
      xx: xxhash_rust::xxh64::Xxh64Builder::new(seed),
      sea: Default::default(),
      _marker: Default::default(),
    }
  }
}

impl<K: Hash + Eq> KeyBuilder for DefaultKeyBuilder<K> {
  type Key = K;

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn hash_index<Q>(&self, key: &Q) -> u64
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    self.sea.hash_one(key)
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn hash_conflict<Q>(&self, key: &Q) -> u64
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    self.xx.hash_one(key)
  }
}

/// Dummy hasher will do nothing. Used by [`TransparentKeyBuilder`].
#[derive(Default, Copy, Clone, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct TransparentHasher {
  data: u64,
}

impl Hasher for TransparentHasher {
  #[cfg_attr(not(tarpaulin), inline(always))]
  fn finish(&self) -> u64 {
    self.data
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn write(&mut self, bytes: &[u8]) {
    let mut data = [0u8; core::mem::size_of::<u64>()];
    if bytes.len() > core::mem::size_of::<u64>() {
      data.copy_from_slice(&bytes[..core::mem::size_of::<u64>()]);
    } else {
      data[..bytes.len()].copy_from_slice(bytes);
    }
    self.data = u64::from_ne_bytes(data);
  }

  fn write_u8(&mut self, i: u8) {
    self.data = i as u64;
  }

  fn write_u16(&mut self, i: u16) {
    self.data = i as u64;
  }

  fn write_u32(&mut self, i: u32) {
    self.data = i as u64;
  }

  fn write_u64(&mut self, i: u64) {
    self.data = i;
  }

  fn write_u128(&mut self, i: u128) {
    self.data = i as u64;
  }

  fn write_usize(&mut self, i: usize) {
    self.data = i as u64;
  }

  fn write_i8(&mut self, i: i8) {
    self.data = i as u64;
  }

  fn write_i16(&mut self, i: i16) {
    self.data = i as u64;
  }

  fn write_i32(&mut self, i: i32) {
    self.data = i as u64;
  }

  fn write_i64(&mut self, i: i64) {
    self.data = i as u64;
  }

  fn write_i128(&mut self, i: i128) {
    self.data = i as u64;
  }

  fn write_isize(&mut self, i: isize) {
    self.data = i as u64;
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
#[derive(Default, Clone, Eq, PartialEq, Debug)]
pub struct TransparentKeyBuilder<K: TransparentKey> {
  // hasher: TransparentHasher<K>,
  _marker: PhantomData<K>,
}

impl<K: TransparentKey> KeyBuilder for TransparentKeyBuilder<K> {
  type Key = K;

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn hash_index<Q>(&self, key: &Q) -> u64
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    let mut hasher = TransparentHasher { data: 0 };
    key.hash(&mut hasher);
    hasher.finish()
  }

  #[cfg_attr(not(tarpaulin), inline(always))]
  fn hash_conflict<Q>(&self, _key: &Q) -> u64
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
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

#[cfg(test)]
mod lib_tests {
  use super::*;

  #[test]
  fn test_item_clone_and_debug() {
    let item: Item<u64> = Item {
      val: Some(42),
      index: 1,
      conflict: 2,
      cost: 3,
      exp: Time::now(),
    };
    #[allow(clippy::clone_on_copy)]
    let cloned = item.clone();
    assert_eq!(cloned.val, Some(42));
    assert_eq!(cloned.index, 1);
    assert_eq!(cloned.conflict, 2);
    assert_eq!(cloned.cost, 3);
    let s = format!("{:?}", item);
    assert!(s.contains("Item"));
    assert!(s.contains("42"));
  }

  #[test]
  fn test_default_key_builder_debug() {
    let kb = DefaultKeyBuilder::<u64>::default();
    let s = format!("{:?}", kb);
    assert!(s.contains("DefaultKeyBuilder"));
  }

  #[test]
  fn test_transparent_hasher_write_variants() {
    let mut h = TransparentHasher::default();
    h.write_u8(7);
    assert_eq!(h.finish(), 7);
    h.write_u16(8);
    assert_eq!(h.finish(), 8);
    h.write_u32(9);
    assert_eq!(h.finish(), 9);
    h.write_u64(10);
    assert_eq!(h.finish(), 10);
    h.write_u128(11);
    assert_eq!(h.finish(), 11);
    h.write_usize(12);
    assert_eq!(h.finish(), 12);
    h.write_i8(-1);
    assert_eq!(h.finish(), u64::MAX);
    h.write_i16(-2);
    h.write_i32(-3);
    h.write_i64(-4);
    h.write_i128(-5);
    h.write_isize(-6);

    let mut h2 = TransparentHasher::default();
    h2.write(&[1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let mut h3 = TransparentHasher::default();
    h3.write(&[1, 2, 3]);
  }

  #[test]
  fn test_transparent_key_to_u64_all_types() {
    assert_eq!(true.to_u64(), 1);
    assert_eq!(false.to_u64(), 0);
    assert_eq!(1u8.to_u64(), 1);
    assert_eq!(1u16.to_u64(), 1);
    assert_eq!(1u32.to_u64(), 1);
    assert_eq!(1u64.to_u64(), 1);
    assert_eq!(1usize.to_u64(), 1);
    assert_eq!(1i8.to_u64(), 1);
    assert_eq!(1i16.to_u64(), 1);
    assert_eq!(1i32.to_u64(), 1);
    assert_eq!(1i64.to_u64(), 1);
    assert_eq!(1isize.to_u64(), 1);
  }

  #[test]
  fn test_key_builder_default_methods() {
    struct MinimalKB;
    impl KeyBuilder for MinimalKB {
      type Key = u64;

      fn hash_index<Q>(&self, _key: &Q) -> u64
      where
        Self::Key: core::borrow::Borrow<Q>,
        Q: core::hash::Hash + Eq + ?Sized,
      {
        7
      }
    }
    let kb = MinimalKB;
    assert_eq!(kb.hash_conflict(&1u64), 0);
    assert_eq!(kb.build_key(&1u64), (7, 0));
  }

  #[test]
  fn test_default_update_validator_coster_callback() {
    let uv = DefaultUpdateValidator::<u64>::default();
    assert!(uv.should_update(&1, &2));

    let coster = DefaultCoster::<u64>::default();
    assert_eq!(coster.cost(&100), 0);

    let cb = DefaultCacheCallback::<u64>::default();
    cb.on_exit(Some(1));
    let item: Item<u64> = Item {
      val: Some(1),
      index: 0,
      conflict: 0,
      cost: 0,
      exp: Time::now(),
    };
    #[allow(clippy::clone_on_copy)]
    cb.on_evict(item.clone());
    cb.on_reject(item);
    let s = format!("{:?}", cb);
    assert!(s.contains("DefaultCacheCallback"));
  }
}
