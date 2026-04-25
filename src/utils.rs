/*
 * Copyright 2021 Al Liu (https://github.com/al8n/stretto). Licensed under Apache-2.0.
 *
 * Copy some code from Dashmap(https://github.com/xacrimon/dashmap). Licensed under MIT.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use crate::ttl::Time;
use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard};
use std::{
  fmt::{Debug, Display, Formatter},
  time::Duration,
};

/// ValueRef is returned when invoking `get` method of the Cache.
/// It wraps a `parking_lot::MappedRwLockReadGuard` projected directly to the
/// inner value, so the shard read lock is held for the lifetime of the ref.
///
/// Auto-trait inheritance follows `parking_lot`'s `GuardMarker`:
/// - **Without** the `send_guard` feature (default), the marker is
///   `GuardNoSend` (a `*mut ()`), which makes `ValueRef` neither `Send`
///   nor `Sync`. This is the right default for `parking_lot`, whose
///   `RwLock` permits same-thread read-guard → write-guard upgrades —
///   sending a guard across threads would break that invariant.
/// - **With** the `send_guard` feature, `parking_lot` switches the marker
///   to `GuardSend`, and `ValueRef` becomes `Send` (and `Sync` when `V` is)
///   automatically. Enabling that feature opts the whole `RwLock` family
///   out of guard upgrades — see the `parking_lot` docs for the trade-offs.
pub struct ValueRef<'a, V> {
  guard: MappedRwLockReadGuard<'a, V>,
  expiration: Time,
}

impl<'a, V> ValueRef<'a, V> {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn new(guard: MappedRwLockReadGuard<'a, V>, expiration: Time) -> Self {
    Self { guard, expiration }
  }

  /// Get the reference of the inner value.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn value(&self) -> &V {
    &self.guard
  }

  /// Drop self, release the inner `RwLockReadGuard`, which is the same as `drop()`
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn release(self) {
    drop(self)
  }

  /// Get the expiration time.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn ttl(&self) -> Duration {
    self.expiration.get_ttl()
  }
}

impl<V: Copy> ValueRef<'_, V> {
  /// Get the value and drop the inner RwLockReadGuard.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn read(self) -> V {
    let v = *self.value();
    drop(self);
    v
  }
}

impl<V> AsRef<V> for ValueRef<'_, V> {
  fn as_ref(&self) -> &V {
    self.value()
  }
}

impl<V: Debug> Debug for ValueRef<'_, V> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ValueRef")
      .field("value", self.value())
      .field("expiration", &self.expiration)
      .finish()
  }
}

impl<V: Display> Display for ValueRef<'_, V> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.value())
  }
}

/// ValueRefMut is returned when invoking `get_mut` method of the Cache.
/// It wraps a `parking_lot::MappedRwLockWriteGuard` projected directly to
/// the inner value, so the shard write lock is held for the lifetime of
/// the ref.
///
/// Auto-trait inheritance follows `parking_lot`'s `GuardMarker`, exactly
/// as documented on [`ValueRef`]: by default the wrapped guard is
/// neither `Send` nor `Sync`; enabling the `send_guard` feature flips
/// the marker to `GuardSend` and `ValueRefMut` inherits the stronger
/// auto-traits.
pub struct ValueRefMut<'a, V> {
  guard: MappedRwLockWriteGuard<'a, V>,
}

impl<'a, V> ValueRefMut<'a, V> {
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub(crate) fn new(guard: MappedRwLockWriteGuard<'a, V>) -> Self {
    Self { guard }
  }

  /// Get the reference of the inner value.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn value(&self) -> &V {
    &self.guard
  }

  /// Get the mutable reference of the inner value.
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn value_mut(&mut self) -> &mut V {
    &mut self.guard
  }

  /// Set the value
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn write(&mut self, val: V) {
    *self.guard = val
  }

  /// Set the value, and release the inner `RwLockWriteGuard` automatically
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn write_once(mut self, val: V) {
    *self.guard = val;
    self.release();
  }

  /// Drop self, release the inner `RwLockWriteGuard`, which is the same as `drop()`
  #[cfg_attr(not(tarpaulin), inline(always))]
  pub fn release(self) {
    drop(self)
  }
}

impl<V: Clone> ValueRefMut<'_, V> {
  /// Clone the inner value
  pub fn clone_inner(&self) -> V {
    (*self.guard).clone()
  }
}

impl<V: Copy> ValueRefMut<'_, V> {
  /// Read the inner value and drop the inner `RwLockWriteGuard`.
  pub fn read(self) -> V {
    let v = *self.guard;
    drop(self);
    v
  }
}

impl<V: Debug> Debug for ValueRefMut<'_, V> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{:?}", &*self.guard)
  }
}

impl<V: Display> Display for ValueRefMut<'_, V> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", &*self.guard)
  }
}

impl<V> AsRef<V> for ValueRefMut<'_, V> {
  fn as_ref(&self) -> &V {
    self.value()
  }
}

impl<V> AsMut<V> for ValueRefMut<'_, V> {
  fn as_mut(&mut self) -> &mut V {
    self.value_mut()
  }
}

pub(crate) fn vec_to_array<T, const N: usize>(v: Vec<T>) -> [T; N] {
  v.try_into()
    .unwrap_or_else(|v: Vec<T>| panic!("Expected a Vec of length {} but it was {}", N, v.len()))
}

#[cfg(test)]
mod test {
  use crate::{ValueRef, ValueRefMut, store::StoreItem, ttl::Time};
  use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
  use std::collections::HashMap;

  #[test]
  fn test_value_ref() {
    let mut m = HashMap::new();
    m.insert(
      1,
      StoreItem {
        key: 1,
        conflict: 0,
        version: 0,
        generation: 0,
        value: 3,
        expiration: Time::now(),
      },
    );
    m.insert(
      2,
      StoreItem {
        key: 2,
        conflict: 0,
        version: 0,
        generation: 0,
        value: 3,
        expiration: Time::now(),
      },
    );
    let lm = RwLock::new(m);

    let l = lm.read();
    let expiration = l.get(&1).unwrap().expiration;
    let mapped = RwLockReadGuard::map(l, |m| &m.get(&1).unwrap().value);
    let vr = ValueRef::new(mapped, expiration);
    assert_eq!(vr.as_ref(), &3);
    eprintln!("{}", vr);
    eprintln!("{:?}", vr);
  }

  #[test]
  fn test_value_ref_mut() {
    let mut m = HashMap::new();
    m.insert(
      1,
      StoreItem {
        key: 1,
        conflict: 0,
        version: 0,
        generation: 0,
        value: 3,
        expiration: Time::now(),
      },
    );
    m.insert(
      2,
      StoreItem {
        key: 2,
        conflict: 0,
        version: 0,
        generation: 0,
        value: 3,
        expiration: Time::now(),
      },
    );
    let lm = RwLock::new(m);

    let l = lm.write();
    let mapped = RwLockWriteGuard::map(l, |m| &mut m.get_mut(&1).unwrap().value);
    let mut vr = ValueRefMut::new(mapped);
    assert_eq!(vr.as_ref(), &3);
    assert_eq!(vr.as_mut(), &mut 3);
    assert_eq!(vr.value(), &3);
    assert_eq!(vr.clone_inner(), 3);
    eprintln!("{}", vr);
    eprintln!("{:?}", vr);
    vr.write_once(4);
  }
}
