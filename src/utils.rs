use crate::store::StoreItem;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::cell::UnsafeCell;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::convert::TryInto;
use std::hash::BuildHasher;
use std::ptr::NonNull;

pub struct ValueRef<'a, V, S = RandomState> {
    guard: RwLockReadGuard<'a, HashMap<u64, StoreItem<V>, S>>,
    val: &'a V,
}

unsafe impl<'a, V: Send, S: BuildHasher> Send for ValueRef<'a, V, S> {}

unsafe impl<'a, V: Send + Sync, S: BuildHasher> Sync for ValueRef<'a, V, S> {}

impl<'a, V, S: BuildHasher> ValueRef<'a, V, S> {
    pub(crate) fn new(
        guard: RwLockReadGuard<'a, HashMap<u64, StoreItem<V>, S>>,
        val: &'a V,
    ) -> Self {
        Self { guard, val }
    }

    pub fn value(&self) -> &V {
        self.val
    }

    pub fn release(self) {
        drop(self)
    }
}

impl<'a, V: Copy, S: BuildHasher> ValueRef<'a, V, S> {
    pub fn read(self) -> V {
        let v = *self.val;
        drop(self);
        v
    }
}

pub struct ValueRefMut<'a, V, S = RandomState> {
    guard: RwLockWriteGuard<'a, HashMap<u64, StoreItem<V>, S>>,
    val: &'a mut V,
}

unsafe impl<'a, V: Send, S: BuildHasher> Send for ValueRefMut<'a, V, S> {}

unsafe impl<'a, V: Send + Sync, S: BuildHasher> Sync for ValueRefMut<'a, V, S> {}

impl<'a, V, S: BuildHasher> ValueRefMut<'a, V, S> {
    pub(crate) fn new(
        guard: RwLockWriteGuard<'a, HashMap<u64, StoreItem<V>, S>>,
        val: &'a mut V,
    ) -> Self {
        Self { guard, val }
    }

    pub fn value(&self) -> &V {
        self.val
    }

    pub fn value_mut(&mut self) -> &mut V {
        self.val
    }

    pub fn write(&mut self, val: V) {
        *self.val = val
    }

    pub fn release(self) {
        drop(self)
    }
}

impl<'a, V: Clone, S: BuildHasher> ValueRefMut<'a, V, S> {
    pub fn clone_inner(&self) -> V {
        self.val.clone()
    }
}

impl<'a, V: Copy, S: BuildHasher> ValueRefMut<'a, V, S> {
    pub fn read(self) -> V {
        let v = *self.val;
        drop(self);
        v
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
pub(crate) unsafe fn change_lifetime_const<'a, 'b, T>(x: &'a T) -> &'b T {
    &*(x as *const T)
}

/// # Safety
///
/// Requires that you ensure the reference does not become invalid.
/// The object has to outlive the reference.
pub(crate) unsafe fn change_lifetime_mut<'a, 'b, T>(x: &'a mut T) -> &'b mut T {
    &mut *(x as *mut T)
}

// TODO: should use SharedNonNull to replace Arc?
#[repr(transparent)]
pub(crate) struct SharedNonNull<T: ?Sized> {
    ptr: NonNull<T>,
}

impl<T> SharedNonNull<T> {
    pub fn new(ptr: *mut T) -> Self {
        unsafe {
            Self {
                ptr: NonNull::new_unchecked(ptr),
            }
        }
    }

    pub unsafe fn as_ref(&self) -> &T {
        self.ptr.as_ref()
    }
}

impl<T: ?Sized> Copy for SharedNonNull<T> {}

impl<T: ?Sized> Clone for SharedNonNull<T> {
    fn clone(&self) -> Self {
        *self
    }
}

unsafe impl<T> Send for SharedNonNull<T> {}
unsafe impl<T> Sync for SharedNonNull<T> {}
