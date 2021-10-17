use crate::policy::LFUPolicy;
use crate::ttl::{ExpirationMap, Time};
use crate::utils::{change_lifetime_const, SharedValue, ValueRef, ValueRefMut};
use crate::{DefaultUpdateValidator, Item as CrateItem, UpdateValidator};
use parking_lot::RwLock;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::hash::BuildHasher;
use std::mem;
use std::sync::Arc;

const NUM_OF_SHARDS: usize = 256;

pub(crate) struct StoreItem<V> {
    pub(crate) key: u64,
    pub(crate) conflict: u64,
    pub(crate) value: SharedValue<V>,
    pub(crate) expiration: Time,
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

pub(crate) struct ShardedMap<
    V: Send + Sync + 'static,
    U = DefaultUpdateValidator<V>,
    SS = RandomState,
    ES = RandomState,
> {
    shards: Box<[RwLock<HashMap<u64, StoreItem<V>, SS>>; NUM_OF_SHARDS]>,
    em: ExpirationMap<ES>,
    store_item_size: usize,
    validator: U,
}

impl<V: Send + Sync + 'static> ShardedMap<V> {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::with_validator(ExpirationMap::new(), DefaultUpdateValidator::default())
    }
}

impl<V: Send + Sync + 'static, U: UpdateValidator<V>> ShardedMap<V, U> {
    #[allow(dead_code)]
    pub fn with_validator(em: ExpirationMap<RandomState>, validator: U) -> Self {
        let shards = Box::new(
            (0..NUM_OF_SHARDS)
                .map(|_| RwLock::new(HashMap::new()))
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        );

        let size = mem::size_of::<StoreItem<V>>();
        Self {
            shards,
            em,
            store_item_size: size,
            validator,
        }
    }
}

impl<
        V: Send + Sync + 'static,
        U: UpdateValidator<V>,
        SS: BuildHasher + Clone + 'static,
        ES: BuildHasher + Clone + 'static,
    > ShardedMap<V, U, SS, ES>
{
    pub fn with_validator_and_hasher(em: ExpirationMap<ES>, validator: U, hasher: SS) -> Self {
        let shards = Box::new(
            (0..NUM_OF_SHARDS)
                .map(|_| RwLock::new(HashMap::with_hasher(hasher.clone())))
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        );

        let size = mem::size_of::<StoreItem<V>>();
        Self {
            shards,
            em,
            store_item_size: size,
            validator,
        }
    }

    pub fn get(&self, key: &u64, conflict: u64) -> Option<ValueRef<'_, V, SS>> {
        let data = self.shards[(*key as usize) % NUM_OF_SHARDS].read();

        if let Some(item) = data.get(key) {
            if conflict != 0 && (conflict != item.conflict) {
                return None;
            }

            // Handle expired items
            if !item.expiration.is_zero() && item.expiration.is_expired() {
                return None;
            }

            unsafe {
                let vptr = change_lifetime_const(item.value.get());
                Some(ValueRef::new(data, vptr))
            }
        } else {
            None
        }
    }

    pub fn get_mut(&self, key: &u64, conflict: u64) -> Option<ValueRefMut<'_, V, SS>> {
        let data = self.shards[(*key as usize) % NUM_OF_SHARDS].write();

        if let Some(item) = data.get(key) {
            if conflict != 0 && (conflict != item.conflict) {
                return None;
            }

            // Handle expired items
            if !item.expiration.is_zero() && item.expiration.is_expired() {
                return None;
            }

            unsafe {
                let vptr = &mut *item.value.as_ptr();
                Some(ValueRefMut::new(data, vptr))
            }
        } else {
            None
        }
    }

    pub fn insert(&self, key: u64, val: V, conflict: u64, expiration: Time) {
        let mut data = self.shards[(key as usize) % NUM_OF_SHARDS].write();

        match data.get(&key) {
            None => {
                // The value is not in the map already. There's no need to return anything.
                // Simply add the expiration map.
                self.em.insert(key, conflict, expiration);
            }
            Some(sitem) => {
                // The item existed already. We need to check the conflict key and reject the
                // update if they do not match. Only after that the expiration map is updated.
                if conflict != 0 && (conflict != sitem.conflict) {
                    return;
                }

                if !self.validator.should_update(sitem.value.get(), &val) {
                    return;
                }

                self.em.update(key, conflict, sitem.expiration, expiration);
            }
        }

        data.insert(
            key,
            StoreItem {
                key,
                conflict,
                value: SharedValue::new(val),
                expiration,
            },
        );
    }

    pub fn update(&self, key: u64, mut val: V, conflict: u64, expiration: Time) -> UpdateResult<V> {
        let mut data = self.shards[(key as usize) % NUM_OF_SHARDS].write();
        match data.get_mut(&key) {
            None => UpdateResult::NotExist(val),
            Some(item) => {
                if conflict != 0 && (conflict != item.conflict) {
                    return UpdateResult::Conflict(val);
                }

                if !self.validator.should_update(item.value.get(), &val) {
                    return UpdateResult::Reject(val);
                }

                self.em.update(key, conflict, item.expiration, expiration);
                mem::swap(&mut val, &mut item.value.get_mut());
                item.expiration = expiration;
                UpdateResult::Update(val)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|l| l.read().len()).sum()
    }

    pub fn remove(&self, key: &u64, conflict: u64) -> Option<StoreItem<V>> {
        let mut data = self.shards[(*key as usize) % NUM_OF_SHARDS].write();

        match data.get(&key) {
            None => None,
            Some(item) => {
                if conflict != 0 && (conflict != item.conflict) {
                    return None;
                }

                if !item.expiration.is_zero() {
                    self.em.remove(key, item.expiration);
                }

                data.remove(key)
            }
        }
    }

    pub fn expiration(&self, key: &u64) -> Option<Time> {
        self.shards[((*key) as usize) % NUM_OF_SHARDS]
            .read()
            .get(key)
            .map(|val| val.expiration)
    }

    pub fn cleanup<PS: BuildHasher + Clone + 'static>(
        &self,
        policy: Arc<LFUPolicy<PS>>,
    ) -> Vec<CrateItem<V>> {
        let now = Time::now();
        self.em.cleanup(now).map_or(Vec::with_capacity(0), |m| {
            m.iter()
                // Sanity check. Verify that the store agrees that this key is expired.
                .filter_map(|(k, v)| {
                    self.expiration(k).and_then(|t| {
                        if t.is_expired() {
                            let cost = policy.cost(k);
                            policy.remove(k);
                            self.remove(k, *v).map(|sitem| CrateItem {
                                val: Some(sitem.value.into_inner()),
                                index: sitem.key,
                                conflict: sitem.conflict,
                                cost,
                                exp: t,
                            })
                        } else {
                            None
                        }
                    })
                })
                .collect()
        })
    }

    pub fn clear(&self) {
        // TODO: item call back
        self.shards.iter().for_each(|shard| shard.write().clear());
    }

    pub fn item_size(&self) -> usize {
        self.store_item_size
    }
}

unsafe impl<V: Send + Sync + 'static, U: UpdateValidator<V>, SS: BuildHasher, ES: BuildHasher> Send
    for ShardedMap<V, U, SS, ES>
{
}
unsafe impl<V: Send + Sync + 'static, U: UpdateValidator<V>, SS: BuildHasher, ES: BuildHasher> Sync
    for ShardedMap<V, U, SS, ES>
{
}

pub(crate) enum UpdateResult<V: Send + Sync + 'static> {
    NotExist(V),
    Reject(V),
    Conflict(V),
    Update(V),
}

#[cfg(test)]
impl<V: Send + Sync + 'static> UpdateResult<V> {
    fn into_inner(self) -> V {
        match self {
            UpdateResult::NotExist(v) => v,
            UpdateResult::Reject(v) => v,
            UpdateResult::Conflict(v) => v,
            UpdateResult::Update(v) => v,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::store::{ShardedMap, StoreItem};
    use crate::ttl::Time;
    use crate::utils::SharedValue;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_store_item_debug() {
        let item = StoreItem {
            key: 0,
            conflict: 0,
            value: SharedValue::new(3),
            expiration: Time::now(),
        };

        eprintln!("{:?}", item);
    }

    #[test]
    fn test_store() {
        let _s: ShardedMap<u64> = ShardedMap::new();
    }

    #[test]
    fn test_store_set_get() {
        let s: ShardedMap<u64> = ShardedMap::new();

        s.insert(1, 2, 0, Time::now());
        let val = s.get(&1, 0).unwrap();
        assert_eq!(&2, val.value());
        val.release();

        let mut val = s.get_mut(&1, 0).unwrap();
        *val.value_mut() = 3;
        val.release();

        let v = s.get(&1, 0).unwrap();
        assert_eq!(&3, v.value());
    }

    #[test]
    fn test_concurrent_get_insert() {
        let s = Arc::new(ShardedMap::new());
        let s1 = s.clone();

        std::thread::spawn(move || {
            s.insert(1, 2, 0, Time::now());
        });

        loop {
            match s1.get(&1, 0) {
                None => continue,
                Some(val) => {
                    assert_eq!(val.read(), 2);
                    break;
                }
            }
        }
    }

    #[test]
    fn test_concurrent_get_mut_insert() {
        let s = Arc::new(ShardedMap::new());
        let s1 = s.clone();

        std::thread::spawn(move || {
            s.insert(1, 2, 0, Time::now());
            loop {
                match s.get(&1, 0) {
                    None => continue,
                    Some(val) => {
                        let val = val.read();
                        if val == 2 {
                            continue;
                        } else if val == 7 {
                            break;
                        } else {
                            panic!("get wrong value")
                        }
                    }
                }
            }
        });

        loop {
            match s1.get(&1, 0) {
                None => continue,
                Some(val) => {
                    assert_eq!(val.read(), 2);
                    break;
                }
            }
        }

        s1.get_mut(&1, 0).unwrap().write(7);
    }

    #[test]
    fn test_store_remove() {
        let s: ShardedMap<u64> = ShardedMap::new();

        s.insert(1, 2, 0, Time::now());
        assert_eq!(s.remove(&1, 0).unwrap().value.into_inner(), 2);
        let v = s.get(&1, 0);
        assert!(v.is_none());
        assert!(s.remove(&2, 0).is_none());
    }

    #[test]
    fn test_store_update() {
        let s = ShardedMap::new();
        s.insert(1, 1, 0, Time::now());
        let v = s.update(1, 2, 0, Time::now());
        assert_eq!(v.into_inner(), 1);

        assert_eq!(s.get(&1, 0).unwrap().read(), 2);

        let v = s.update(1, 3, 0, Time::now());
        assert_eq!(v.into_inner(), 2);

        assert_eq!(s.get(&1, 0).unwrap().read(), 3);

        let v = s.update(2, 2, 0, Time::now());
        assert_eq!(v.into_inner(), 2);
        let v = s.get(&2, 0);
        assert!(v.is_none());
    }

    #[test]
    fn test_store_expiration() {
        let exp = Time::now_with_expiration(Duration::from_secs(1));
        let s = ShardedMap::new();
        s.insert(1, 1, 0, exp);

        assert_eq!(s.get(&1, 0).unwrap().read(), 1);

        let ttl = s.expiration(&1);
        assert_eq!(exp, ttl.unwrap());

        s.remove(&1, 0);
        assert!(s.get(&1, 0).is_none());
        let ttl = s.expiration(&1);
        assert!(ttl.is_none());

        assert!(s.expiration(&4340958203495).is_none());
    }

    #[test]
    fn test_store_collision() {
        let s = ShardedMap::new();
        let mut data1 = s.shards[1].write();
        data1.insert(
            1,
            StoreItem {
                key: 1,
                conflict: 0,
                value: SharedValue::new(1),
                expiration: Time::now(),
            },
        );
        drop(data1);
        assert!(s.get(&1, 1).is_none());

        s.insert(1, 2, 1, Time::now());
        assert_ne!(s.get(&1, 0).unwrap().read(), 2);

        let v = s.update(1, 2, 1, Time::now());
        assert_eq!(v.into_inner(), 2);
        assert_ne!(s.get(&1, 0).unwrap().read(), 2);

        assert!(s.remove(&1, 1).is_none());
        assert_eq!(s.get(&1, 0).unwrap().read(), 1);
    }
}
