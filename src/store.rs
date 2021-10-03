use crate::cache::Item;
use crate::ttl::{ExpirationMap, Time};
use crate::SharedRef;
use parking_lot::{RwLock, RawRwLock, RwLockWriteGuard};
use std::borrow::{Borrow, BorrowMut};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::mem;
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use parking_lot::lock_api::RwLockReadGuard;
use std::cell::{RefCell};
use crate::utils::{change_lifetime_const, change_lifetime_mut};

const NUM_OF_SHARDS: usize = 256;

pub struct ValueRef<'a, V, S = RandomState> {
    guard: RwLockReadGuard<'a, RawRwLock, HashMap<u64, StoreItem<V>, S>>,
    val: &'a V,
}

unsafe impl<'a, V: Send, S: BuildHasher> Send for ValueRef<'a, V, S> {}

unsafe impl<'a, V: Send + Sync, S: BuildHasher> Sync
for ValueRef<'a, V, S> {}

impl<'a, V, S: BuildHasher> ValueRef<'a, V, S> {
    fn new(guard: RwLockReadGuard<'a, RawRwLock, HashMap<u64, StoreItem<V>, S>>, val: &'a V) -> Self {
        Self {
            guard,
            val,
        }
    }

    pub fn value(&self) -> &V {
        self.val
    }
}

pub struct ValueRefMut<'a, V, S = RandomState> {
    guard: RwLockWriteGuard<'a, HashMap<u64, StoreItem<V>, S>>,
    val: &'a mut V,
}

unsafe impl<'a, V: Send, S: BuildHasher> Send for ValueRefMut<'a, V, S> {}

unsafe impl<'a, V: Send + Sync, S: BuildHasher> Sync
for ValueRefMut<'a, V, S> {}

impl<'a, V, S: BuildHasher> ValueRefMut<'a, V, S> {
    fn new(guard: RwLockWriteGuard<'a, HashMap<u64, StoreItem<V>, S>>, val: &'a mut V) -> Self {
        Self {
            guard,
            val,
        }
    }

    pub fn value(&self) -> &V {
        self.val
    }

    pub fn value_mut(&mut self) -> &mut V {
        self.val
    }
}

struct StoreItem<V> {
    key: u64,
    conflict: u64,
    value: V,
    expiration: Option<Time>,
}

impl<V> Debug for StoreItem<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreItem")
            .field("key", &self.key)
            .field("conflict", &self.conflict)
            .field("expiration", &self.expiration)
            .finish()
    }
}

pub(crate) struct ShardedMap<V, DS = RandomState, S = RandomState> {
    shards: Box<[RwLock<HashMap<u64, StoreItem<V>, DS>>; NUM_OF_SHARDS]>,
    em: ExpirationMap<S>,
}

impl<V> ShardedMap<V> {
    pub fn new() -> Self {
        let em = ExpirationMap::new();

        let shards: [RwLock<_>; NUM_OF_SHARDS] = (0..NUM_OF_SHARDS).map(|_| RwLock::new(HashMap::new())).collect::<Vec<_>>().try_into().unwrap();

        Self {
            shards: Box::new(shards),
            em,
        }
    }

    pub fn get(&self, key: u64, conflict: u64) -> Option<ValueRef<'_, V>> {
        let data = self.shards[(key as usize) % NUM_OF_SHARDS].read();

        if let Some(item) =  data.get(&key) {
            if conflict != 0 && (conflict != item.conflict) {
                return None;
            }

            if let Some(expiration) = item.expiration {
                if !expiration.is_zero() && expiration.is_expired() {
                    return None;
                }
            }

            unsafe {
                let vptr = change_lifetime_const(&item.value);
                Some(ValueRef::new(data, vptr))
            }
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, key: u64, conflict: u64) -> Option<ValueRefMut<'_, V>> {
        let mut data = self.shards[(key as usize) % NUM_OF_SHARDS].write();

        if let Some(item) =  data.get_mut(&key) {
            if conflict != 0 && (conflict != item.conflict) {
                return None;
            }

            if let Some(expiration) = item.expiration {
                if !expiration.is_zero() && expiration.is_expired() {
                    return None;
                }
            }

            unsafe {
                let vptr = change_lifetime_mut(&mut item.value);
                Some(ValueRefMut::new(data, vptr))
            }
        } else {
            None
        }
    }

    pub fn insert(&mut self, item: Item<V>) {
        let key = item.get_key();
        let conflict = item.get_conflict();
        let expiration = item.get_expiration();

        let mut data = self.shards[(key as usize) % NUM_OF_SHARDS].write();

        match data.get(&key) {
            None => {
                // The value is not in the map already. There's no need to return anything.
                // Simply add the expiration map.
                if let Some(expiration) = expiration {
                    self.em.insert(key, conflict, expiration);
                }
            }
            Some(sitem) => {
                // The item existed already. We need to check the conflict key and reject the
                // update if they do not match. Only after that the expiration map is updated.
                if conflict != 0 && (conflict != sitem.conflict) {
                    return;
                }

                //TODO: should update check
                if let Some(expiration) = expiration {
                    if let Some(old_expiration) = sitem.expiration {
                        self.em.update(key, conflict, old_expiration, expiration);
                    }
                }
            }
        }

        data.insert(
            key,
            StoreItem {
                key,
                conflict,
                value: item.value,
                expiration,
            },
        );
    }
}

struct ShardedInnerMap<V, DS = RandomState, S = RandomState> {
    data: HashMap<u64, StoreItem<V>, DS>,
    em: SharedRef<ExpirationMap<S>>,
}

impl<V> ShardedInnerMap<V> {
    fn new(em: SharedRef<ExpirationMap>) -> Self {
        Self {
            data: HashMap::new(),
            em,
        }
    }
}

// impl<V, DS: BuildHasher, S: BuildHasher + Default> ShardedInnerMap<V, DS, S> {
//     fn with_hasher(em: SharedRef<ExpirationMap<S>>, hasher: DS) -> Self {
//         Self {
//             data: HashMap::with_hasher(hasher),
//             em,
//         }
//     }
//
//     pub fn get(&self, k: u64, conflict: u64) -> Option<&StoreItem<V>> {
//         match self.data.get(&k) {
//             None => None,
//             Some(item) => {
//                 if conflict != 0 && (conflict != item.conflict) {
//                     return None;
//                 }
//
//                 if !item.expiration.is_zero() && item.expiration.is_expired() {
//                     return None;
//                 }
//
//                 Some(item)
//             }
//         }
//     }
//
//     pub fn expiration(&self, key: u64) -> Option<Time> {
//         self.data.get(&key).map(|item| item.expiration)
//     }
//
//     fn em_ref(&self) -> &ExpirationMap<S> {
//         unsafe { self.em.0.as_ref().unwrap() }
//     }
//
//     pub fn set(&mut self, item: Item<V>) {
//         let key = item.get_key();
//         let conflict = item.get_conflict();
//         let expiration = item.get_expiration();
//         match self.data.get(&key) {
//             None => {
//                 // The value is not in the map already. There's no need to return anything.
//                 // Simply add the expiration map.
//                 self.em_ref().insert(key, conflict, expiration);
//             }
//             Some(sitem) => {
//                 // The item existed already. We need to check the conflict key and reject the
//                 // update if they do not match. Only after that the expiration map is updated.
//                 if conflict != 0 && (conflict != sitem.conflict) {
//                     return;
//                 }
//
//                 //TODO: should update check
//                 self.em_ref()
//                     .update(key, conflict, sitem.expiration, expiration);
//             }
//         }
//
//         self.data.insert(
//             key,
//             StoreItem {
//                 key,
//                 conflict,
//                 value: item.value,
//                 expiration,
//             },
//         );
//     }
//
//     pub fn update(&mut self, mut new_item: Item<V>) -> Option<V> {
//         let nk = new_item.get_key();
//
//         match self.data.get_mut(&nk) {
//             None => None,
//             Some(item) => {
//                 let nc = new_item.get_conflict();
//                 let ne = new_item.get_expiration();
//                 if nc != 0 && (nc != item.conflict) {
//                     return None;
//                 }
//
//                 // TODO: check update
//
//                 mem::swap(&mut new_item.value, &mut item.value);
//
//                 let exp = item.expiration;
//                 self.em_ref().update(nk, nc, exp, ne);
//
//                 Some(new_item.value)
//             }
//         }
//     }
//
//     pub fn remove(&mut self, key: u64, conflict: u64) -> (u64, Option<V>) {
//         match self.data.get(&key) {
//             None => (0, None),
//             Some(item) => {
//                 if conflict != 0 && (conflict != item.conflict) {
//                     return (0, None);
//                 }
//
//                 if !item.expiration.is_zero() {
//                     self.em_ref().remove(key, item.expiration);
//                 }
//
//                 unsafe {
//                     self.data
//                         .remove(&key)
//                         .map_or((0, None), |item| (item.conflict, Some(item.value)))
//                 }
//             }
//         }
//     }
//
//     pub fn clear(&mut self) {
//         //TODO: onevict
//         self.data.clear()
//     }
// }

impl<V, DS, S> Debug for ShardedInnerMap<V, DS, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedInnerMap").field("data", &self.data.len()).finish()
    }
}

#[cfg(test)]
mod test {
    use crate::store::ShardedMap;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use crate::cache::{Item, ItemFlag, ItemMeta};

    #[test]
    fn test_store() {
        let s: ShardedMap<u64> = ShardedMap::new();
        let mut v = 3;
        let r = &v as *const i32;
        v = 6;
        unsafe {
            println!("{}", &*r);
        }
    }

    #[test]
    fn test_store_set_get() {
        let mut s: ShardedMap<u64> = ShardedMap::new();

        let item = Item {
            flag: ItemFlag::New,
            meta: ItemMeta::new(1, 2, 2),
            value: 2,
            expiration: None,
            wg: None,
        };

        s.insert(item);
        let val = s.get(1, 2).unwrap();
        assert_eq!(&2, val.value());
        drop(val);

        let mut val = s.get_mut(1, 2).unwrap();

        *val.value_mut() = 3;

        drop(val);
        let v = s.get(1, 2).unwrap();
        assert_eq!(&3, v.value());

    }
}