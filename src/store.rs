use crate::cache::Item;
use crate::ttl::{ExpirationMap, Time};
use crate::SharedRef;
use parking_lot::RwLock;
use std::borrow::{Borrow, BorrowMut};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::mem;

const NUM_OF_SHARDS: usize = 256;

struct StoreItem<V> {
    key: u64,
    conflict: u64,
    value: Option<V>,
    expiration: Time,
}

pub(crate) struct ShardedMap<V, DS = RandomState, S = RandomState> {
    shards: [RwLock<ShardedInnerMap<V, DS, S>>; NUM_OF_SHARDS],
    em: ExpirationMap<S>,
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

impl<V, DS: BuildHasher, S: BuildHasher + Default> ShardedInnerMap<V, DS, S> {
    fn with_hasher(em: SharedRef<ExpirationMap<S>>, hasher: DS) -> Self {
        Self {
            data: HashMap::with_hasher(hasher),
            em,
        }
    }

    pub fn get(&self, k: u64, conflict: u64) -> Option<&StoreItem<V>> {
        match self.data.get(&k) {
            None => None,
            Some(item) => {
                if conflict != 0 && (conflict != item.conflict) {
                    return None;
                }

                if !item.expiration.is_zero() && item.expiration.is_expired() {
                    return None;
                }

                Some(item)
            }
        }
    }

    pub fn expiration(&self, key: u64) -> Option<Time> {
        self.data.get(&key).map(|item| item.expiration)
    }

    fn em_ref(&self) -> &ExpirationMap<S> {
        unsafe { self.em.0.as_ref().unwrap() }
    }

    pub fn set(&mut self, item: Item<V>) {
        let key = item.get_key();
        let conflict = item.get_conflict();
        let expiration = item.get_expiration();
        match self.data.get(&key) {
            None => {
                // The value is not in the map already. There's no need to return anything.
                // Simply add the expiration map.
                self.em_ref().insert(key, conflict, expiration);
            }
            Some(sitem) => {
                // The item existed already. We need to check the conflict key and reject the
                // update if they do not match. Only after that the expiration map is updated.
                if conflict != 0 && (conflict != sitem.conflict) {
                    return;
                }

                //TODO: should update check
                self.em_ref()
                    .update(key, conflict, sitem.expiration, expiration);
            }
        }

        self.data.insert(
            key,
            StoreItem {
                key,
                conflict,
                value: item.value,
                expiration,
            },
        );
    }

    pub fn update(&mut self, mut new_item: Item<V>) -> Option<V> {
        let nk = new_item.get_key();

        match self.data.get_mut(&nk) {
            None => None,
            Some(item) => {
                let nc = new_item.get_conflict();
                let ne = new_item.get_expiration();
                if nc != 0 && (nc != item.conflict) {
                    return None;
                }

                // TODO: check update

                mem::swap(&mut new_item.value, &mut item.value);

                let exp = item.expiration;
                self.em_ref().update(nk, nc, exp, ne);

                new_item.value
            }
        }
    }

    pub fn remove(&mut self, key: u64, conflict: u64) -> (u64, Option<V>) {
        match self.data.get(&key) {
            None => (0, None),
            Some(item) => {
                if conflict != 0 && (conflict != item.conflict) {
                    return (0, None);
                }

                if !item.expiration.is_zero() {
                    self.em_ref().remove(key, item.expiration);
                }

                self.data
                    .remove(&key)
                    .map_or((0, None), |item| (item.conflict, item.value))
            }
        }
    }

    pub fn clear(&mut self) {
        //TODO: onevict
        self.data.clear()
    }
}
