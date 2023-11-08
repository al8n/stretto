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
use crate::store::StoreItem;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::cell::UnsafeCell;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{Debug, Display, Formatter};
use std::hash::BuildHasher;
use std::time::Duration;

/// ValueRef is returned when invoking `get` method of the Cache.
/// It contains a `RwLockReadGuard` and a value reference.
pub struct ValueRef<'a, V, S = RandomState> {
    _guard: RwLockReadGuard<'a, HashMap<u64, StoreItem<V>, S>>,
    val: &'a StoreItem<V>,
}

unsafe impl<'a, V: Send, S: BuildHasher> Send for ValueRef<'a, V, S> {}

unsafe impl<'a, V: Send + Sync, S: BuildHasher> Sync for ValueRef<'a, V, S> {}

impl<'a, V, S: BuildHasher> ValueRef<'a, V, S> {
    #[inline]
    pub(crate) fn new(
        guard: RwLockReadGuard<'a, HashMap<u64, StoreItem<V>, S>>,
        val: &'a StoreItem<V>,
    ) -> Self {
        Self { _guard: guard, val }
    }

    /// Get the reference of the inner value.
    #[inline]
    pub fn value(&self) -> &V {
        self.val.value.get()
    }

    /// Drop self, release the inner `RwLockReadGuard`, which is the same as `drop()`
    #[inline]
    pub fn release(self) {
        drop(self)
    }

    /// Get the expiration time.
    #[inline]
    pub fn ttl(&self) -> Duration {
        self.val.expiration.get_ttl()
    }
}

impl<'a, V: Copy, S: BuildHasher> ValueRef<'a, V, S> {
    /// Get the value and drop the inner RwLockReadGuard.
    #[inline]
    pub fn read(self) -> V {
        let v = *self.value();
        drop(self);
        v
    }
}

impl<'a, V, S: BuildHasher> AsRef<V> for ValueRef<'a, V, S> {
    fn as_ref(&self) -> &V {
        self.value()
    }
}

impl<'a, V: Debug, S: BuildHasher> Debug for ValueRef<'a, V, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.val)
    }
}

impl<'a, V: Display, S: BuildHasher> Display for ValueRef<'a, V, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}

/// ValueRefMut is returned when invoking `get_mut` method of the Cache.
/// It contains a `RwLockWriteGuard` and a mutable value reference.
pub struct ValueRefMut<'a, V, S = RandomState> {
    _guard: RwLockWriteGuard<'a, HashMap<u64, StoreItem<V>, S>>,
    val: &'a mut V,
}

unsafe impl<'a, V: Send, S: BuildHasher> Send for ValueRefMut<'a, V, S> {}

unsafe impl<'a, V: Send + Sync, S: BuildHasher> Sync for ValueRefMut<'a, V, S> {}

impl<'a, V, S: BuildHasher> ValueRefMut<'a, V, S> {
    #[inline]
    pub(crate) fn new(
        guard: RwLockWriteGuard<'a, HashMap<u64, StoreItem<V>, S>>,
        val: &'a mut V,
    ) -> Self {
        Self { _guard: guard, val }
    }

    /// Get the reference of the inner value.
    #[inline]
    pub fn value(&self) -> &V {
        self.val
    }

    /// Get the mutable reference of the inner value.
    #[inline]
    pub fn value_mut(&mut self) -> &mut V {
        self.val
    }

    /// Set the value
    #[inline]
    pub fn write(&mut self, val: V) {
        *self.val = val
    }

    /// Set the value, and release the inner `RwLockWriteGuard` automatically
    #[inline]
    pub fn write_once(self, val: V) {
        *self.val = val;
        self.release();
    }

    /// Drop self, release the inner `RwLockReadGuard`, which is the same as `drop()`
    #[inline]
    pub fn release(self) {
        drop(self)
    }
}

impl<'a, V: Clone, S: BuildHasher> ValueRefMut<'a, V, S> {
    /// Clone the inner value
    pub fn clone_inner(&self) -> V {
        self.val.clone()
    }
}

impl<'a, V: Copy, S: BuildHasher> ValueRefMut<'a, V, S> {
    /// Read the inner value and drop the inner `RwLockReadGuard`.
    pub fn read(self) -> V {
        let v = *self.val;
        drop(self);
        v
    }
}

impl<'a, V: Debug, S: BuildHasher> Debug for ValueRefMut<'a, V, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.val)
    }
}

impl<'a, V: Display, S: BuildHasher> Display for ValueRefMut<'a, V, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.val)
    }
}

impl<'a, V, S: BuildHasher> AsRef<V> for ValueRefMut<'a, V, S> {
    fn as_ref(&self) -> &V {
        self.value()
    }
}

impl<'a, V, S: BuildHasher> AsMut<V> for ValueRefMut<'a, V, S> {
    fn as_mut(&mut self) -> &mut V {
        self.value_mut()
    }
}

#[repr(transparent)]
pub struct SharedValue<T> {
    value: UnsafeCell<T>,
}

impl<T: Clone> Clone for SharedValue<T> {
    fn clone(&self) -> Self {
        let inner = self.get().clone();

        Self {
            value: UnsafeCell::new(inner),
        }
    }
}

unsafe impl<T: Send> Send for SharedValue<T> {}

unsafe impl<T: Sync> Sync for SharedValue<T> {}

impl<T> SharedValue<T> {
    /// Create a new `SharedValue<T>`
    pub const fn new(value: T) -> Self {
        Self {
            value: UnsafeCell::new(value),
        }
    }

    /// Get a shared reference to `T`
    pub fn get(&self) -> &T {
        unsafe { &*self.value.get() }
    }

    /// Get an unique reference to `T`
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.value.get() }
    }

    /// Unwraps the value
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }

    /// Get a mutable raw pointer to the underlying value
    pub(crate) fn as_ptr(&self) -> *mut T {
        self.value.get()
    }
}

pub(crate) fn vec_to_array<T, const N: usize>(v: Vec<T>) -> [T; N] {
    v.try_into()
        .unwrap_or_else(|v: Vec<T>| panic!("Expected a Vec of length {} but it was {}", N, v.len()))
}

/// # Safety
///
/// Requires that you ensure the reference does not become invalid.
/// The object has to outlive the reference.
pub(crate) unsafe fn change_lifetime_const<'b, T>(x: &T) -> &'b T {
    &*(x as *const T)
}

#[cfg(test)]
mod test {
    use crate::store::StoreItem;
    use crate::ttl::Time;
    use crate::utils::{
        change_lifetime_const,
        // SharedNonNull,
        SharedValue,
    };
    use crate::{ValueRef, ValueRefMut};
    use parking_lot::RwLock;
    use std::collections::HashMap;

    #[test]
    fn test_value_ref() {
        let mut m = HashMap::new();
        m.insert(
            1,
            StoreItem {
                key: 1,
                conflict: 0,
                value: SharedValue::new(3),
                expiration: Time::now(),
            },
        );
        m.insert(
            2,
            StoreItem {
                key: 2,
                conflict: 0,
                value: SharedValue::new(3),
                expiration: Time::now(),
            },
        );
        let lm = RwLock::new(m);

        let l = lm.read();
        let item = l.get(&1).unwrap();
        let v = unsafe { change_lifetime_const(item) };
        let vr = ValueRef::new(l, v);
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
                value: SharedValue::new(3),
                expiration: Time::now(),
            },
        );
        m.insert(
            2,
            StoreItem {
                key: 2,
                conflict: 0,
                value: SharedValue::new(3),
                expiration: Time::now(),
            },
        );
        let lm = RwLock::new(m);

        let l = lm.write();
        let v = unsafe { &mut *l.get(&1).unwrap().value.as_ptr() };
        let mut vr = ValueRefMut::new(l, v);
        assert_eq!(vr.as_ref(), &3);
        assert_eq!(vr.as_mut(), &mut 3);
        assert_eq!(vr.value(), &3);
        assert_eq!(vr.clone_inner(), 3);
        eprintln!("{}", vr);
        eprintln!("{:?}", vr);
        vr.write_once(4);
    }

    #[test]
    fn test_shared_value() {
        let sv = SharedValue::new(3);
        assert_eq!(sv.get(), &3);
    }

    // #[test]
    // fn test_shared_non_null() {
    //     let snn = SharedNonNull::new(&mut 3);
    //     let r = unsafe { snn.as_ref() };
    //     assert_eq!(r, &3);
    //     let snn1 = snn;
    //     unsafe {
    //         assert_eq!(snn1.as_ref(), &3);
    //     }
    // }
}
