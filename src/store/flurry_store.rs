// use std::cell::Cell;
// use flurry::HashMap as FlurryHashMap;
// use std::collections::hash_map::RandomState;
// use std::fmt::{Debug, Formatter};
// use std::hash::BuildHasher;
// use std::mem;
// use std::sync::Arc;
// use flurry::epoch::Guard;
// #[cfg(feature = "async")]
// use crate::policy::AsyncLFUPolicy;
// use crate::policy::LFUPolicy;
// use crate::store::NUM_OF_SHARDS;
// use crate::ttl::{FlurryExpirationMap, Time};
// use crate::{DefaultUpdateValidator, Item as CrateItem, UpdateValidator};
//
// pub struct ValueRef<'g, V> {
//     _g: &'g Guard,
//     item: &'g FlurryStoreItem<V>,
// }
//
// pub(crate) struct FlurryStoreItem<V> {
//     pub(crate) key: u64,
//     pub(crate) conflict: u64,
//     pub(crate) value: V,
//     pub(crate) expiration: Cell<Time>,
// }
//
// impl<V> Debug for FlurryStoreItem<V> {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("FlurryStoreItem")
//             .field("key", &self.key)
//             .field("conflict", &self.conflict)
//             .field("expiration", &self.expiration)
//             .finish()
//     }
// }
//
// unsafe impl<V: Send + Sync + 'static> Send for FlurryStoreItem<V> {}
// unsafe impl<V: Send + Sync + 'static> Sync for FlurryStoreItem<V> {}
//
// pub(crate) struct ShardedMap<
//     V: Send + Sync + 'static,
//     U = DefaultUpdateValidator<V>,
//     SS = RandomState,
//     ES = RandomState,
// > {
//     shards: Box<[FlurryHashMap<u64, FlurryStoreItem<V>, SS>; NUM_OF_SHARDS]>,
//     em: FlurryExpirationMap<ES>,
//     store_item_size: usize,
//     validator: U,
// }
//
// impl<V: Send + Sync + 'static> ShardedMap<V> {
//     #[allow(dead_code)]
//     pub fn new() -> Self {
//         Self::with_validator(FlurryExpirationMap::new(), DefaultUpdateValidator::default())
//     }
// }
//
// impl<V: Send + Sync + 'static, U: UpdateValidator<V>> ShardedMap<V, U> {
//     #[allow(dead_code)]
//     pub fn with_validator(em: FlurryExpirationMap<RandomState>, validator: U) -> Self {
//         let shards = Box::new(
//             (0..NUM_OF_SHARDS)
//                 .map(|_| FlurryHashMap::with_hasher(RandomState::new()))
//                 .collect::<Vec<_>>()
//                 .try_into()
//                 .unwrap(),
//         );
//
//         let size = mem::size_of::<FlurryStoreItem<V>>();
//         Self {
//             shards,
//             em,
//             store_item_size: size,
//             validator,
//         }
//     }
// }
//
// impl<
//     V: Send + Sync + 'static,
//     U: UpdateValidator<V>,
//     SS: BuildHasher + Clone + Send + Sync + 'static,
//     ES: BuildHasher + Clone + Send + Sync + 'static,
// > ShardedMap<V, U, SS, ES>
// {
//     pub fn with_validator_and_hasher(em: FlurryExpirationMap<ES>, validator: U, hasher: SS) -> Self {
//         let shards = Box::new(
//             (0..NUM_OF_SHARDS)
//                 .map(|_| FlurryHashMap::with_hasher(hasher.clone()))
//                 .collect::<Vec<_>>()
//                 .try_into()
//                 .unwrap(),
//         );
//
//         let size = mem::size_of::<FlurryStoreItem<V>>();
//         Self {
//             shards,
//             em,
//             store_item_size: size,
//             validator,
//         }
//     }
//
//     pub fn get<'a: 'g, 'g>(&'a self, key: &u64, conflict: u64, g: &'g Guard) -> Option<ValueRef<'g, V>> {
//         let data = &self.shards[(*key as usize) % NUM_OF_SHARDS];
//         if let Some(item) = data.get(key, g) {
//             if conflict != 0 && (conflict != item.conflict) {
//                 return None;
//             }
//
//             // Handle expired items
//             let exp = item.expiration.get();
//             if !exp.is_zero() && exp.is_expired() {
//                 return None;
//             }
//
//             Some(ValueRef {
//                 _g: g,
//                 item,
//             })
//         } else {
//             None
//         }
//     }
//
//     pub fn insert(&self, key: u64, val: V, conflict: u64, expiration: Time, g: &Guard) {
//         let data = &self.shards[(key as usize) % NUM_OF_SHARDS];
//
//         match data.get(&key, g) {
//             None => {
//                 // The value is not in the map already. There's no need to return anything.
//                 // Simply add the expiration map.
//                 self.em.insert(key, conflict, expiration, g);
//             }
//             Some(sitem) => {
//                 // The item existed already. We need to check the conflict key and reject the
//                 // update if they do not match. Only after that the expiration map is updated.
//                 if conflict != 0 && (conflict != sitem.conflict) {
//                     return;
//                 }
//
//                 if !self.validator.should_update(&sitem.value, &val) {
//                     return;
//                 }
//
//                 self.em.update(key, conflict, sitem.expiration.get(), expiration, g);
//             }
//         }
//
//         data.insert(
//             key,
//             FlurryStoreItem {
//                 key,
//                 conflict,
//                 value: val,
//                 expiration: Cell::new(expiration),
//             },
//             g
//         );
//     }
//
//     pub fn update<'g>(&'g self, key: u64, val: V, conflict: u64, expiration: Time, g: &'g Guard) -> UpdateResult<'g, V> {
//         let data = &self.shards[(key as usize) % NUM_OF_SHARDS];
//         match data.get(&key, g) {
//             None => UpdateResult::NotExist(val),
//             Some(item) => {
//                 if conflict != 0 && (conflict != item.conflict) {
//                     return UpdateResult::Conflict(val);
//                 }
//
//                 if !self.validator.should_update(&item.value, &val) {
//                     return UpdateResult::Reject(val);
//                 }
//
//                 self.em.update(key, conflict, item.expiration.get(), expiration, g);
//                 data.insert(
//                     key,
//                     FlurryStoreItem {
//                         key,
//                         conflict,
//                         value: val,
//                         expiration: Cell::new(expiration),
//                     },
//                     g
//                 ).map(|item| UpdateResult::Update(ValueRef {
//                     _g: g,
//                     item,
//                 })).unwrap()
//             }
//         }
//     }
//
//     #[inline]
//     pub fn len(&self) -> usize {
//         self.shards.iter().map(|l| l.len()).sum()
//     }
//
//     pub fn remove<'g>(&'g self, key: &u64, conflict: u64, g: &'g Guard) -> Option<ValueRef<'g, V>> {
//         let data = &self.shards[(*key as usize) % NUM_OF_SHARDS];
//         match data.get(&key, &g) {
//             None => None,
//             Some(item) => {
//                 if conflict != 0 && (conflict != item.conflict) {
//                     return None;
//                 }
//
//                 if !item.expiration.get().is_zero() {
//                     self.em.remove(key, item.expiration.get(), g);
//                 }
//
//                 data.remove(key, &g).map(|item| ValueRef {
//                     _g: g,
//                     item
//                 })
//             }
//         }
//     }
//
//     pub fn expiration<'g>(&self, key: &u64, g: &'g Guard) -> Option<Time> {
//         self.shards[((*key) as usize) % NUM_OF_SHARDS]
//             .get(key, g)
//             .map(|val| val.expiration.get())
//     }
//
//     pub fn cleanup<PS: BuildHasher + Clone + 'static>(
//         &self,
//         policy: Arc<LFUPolicy<PS>>,
//         g: &Guard
//     ) -> Vec<CrateItem<V>> {
//         let now = Time::now();
//         self.em.cleanup(now, g).map_or(Vec::with_capacity(0), |m| {
//             m.bucket.map().iter()
//                 // Sanity check. Verify that the store agrees that this key is expired.
//                 .filter_map(|(k, v)| {
//                     self.expiration(k, g).and_then(|t| {
//                         if t.is_expired() {
//                             let cost = policy.cost(k);
//                             policy.remove(k);
//                             self.remove(k, *v, g).map(|sitem| CrateItem {
//                                 // val: Some(sitem.item.value),
//                                 val: None,
//                                 index: sitem.item.key,
//                                 conflict: sitem.item.conflict,
//                                 cost,
//                                 exp: t,
//                             })
//                         } else {
//                             None
//                         }
//                     })
//                 })
//                 .collect()
//         })
//     }
//
//     #[cfg(feature = "async")]
//     pub fn cleanup_async<PS: BuildHasher + Clone + 'static>(
//         &self,
//         policy: Arc<AsyncLFUPolicy<PS>>,
//         g: &Guard
//     ) -> Vec<CrateItem<V>> {
//         let now = Time::now();
//         self.em.cleanup(now, g).map_or(Vec::with_capacity(0), |m| {
//             m.iter()
//                 // Sanity check. Verify that the store agrees that this key is expired.
//                 .filter_map(|(k, v)| {
//                     self.expiration(k, g).and_then(|t| {
//                         if t.is_expired() {
//                             let cost = policy.cost(k);
//                             policy.remove(k);
//                             self.remove(k, *v, g).map(|sitem| CrateItem {
//                                 val: Some(sitem.item.value.into_inner()),
//                                 index: sitem.item.key,
//                                 conflict: sitem.item.conflict,
//                                 cost,
//                                 exp: t,
//                             })
//                         } else {
//                             None
//                         }
//                     })
//                 })
//                 .collect()
//         })
//     }
//
//     pub fn clear(&self, g: &Guard) {
//         // TODO: item call back
//         self.shards.iter().for_each(|shard| shard.clear(g));
//     }
//
//     pub fn hasher(&self) -> ES {
//         self.em.hasher()
//     }
//
//     pub fn item_size(&self) -> usize {
//         self.store_item_size
//     }
// }
//
// unsafe impl<V: Send + Sync + 'static, U: UpdateValidator<V>, SS: BuildHasher, ES: BuildHasher> Send
// for ShardedMap<V, U, SS, ES>
// {
// }
// unsafe impl<V: Send + Sync + 'static, U: UpdateValidator<V>, SS: BuildHasher, ES: BuildHasher> Sync
// for ShardedMap<V, U, SS, ES>
// {
// }
//
// pub(crate) enum UpdateResult<'g, V: Send + Sync + 'static> {
//     NotExist(V),
//     Reject(V),
//     Conflict(V),
//     Update(ValueRef<'g, V>),
// }
//
// // #[cfg(test)]
// // impl<'g, V: Send + Sync + 'static> UpdateResult<'g, V> {
// //     fn value(&self) -> &V {
// //         match self {
// //             UpdateResult::NotExist(v) => v,
// //             UpdateResult::Reject(v) => v,
// //             UpdateResult::Conflict(v) => v,
// //             UpdateResult::Update(v) => &v.item.value,
// //         }
// //     }
// // }
//
// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::ttl::Time;
//     use std::sync::Arc;
//     use std::time::Duration;
//
//     #[test]
//     fn test_store_item_debug() {
//         let item = FlurryStoreItem {
//             key: 0,
//             conflict: 0,
//             value: 3,
//             expiration: Cell::new(Time::now()),
//         };
//         // let g = crossbeam_epoch::pin();
//         // let map = flurry::HashMap::new();
//         // map.insert(1, 1, &g);
//         // eprintln!("{:?}", item);
//     }
//
//     #[test]
//     fn test_store() {
//         let _s: ShardedMap<u64> = ShardedMap::new();
//     }
//
//     #[test]
//     fn test_store_set_get() {
//         let s: ShardedMap<u64> = ShardedMap::new();
//         let g = flurry::epoch::pin();
//         s.insert(1, 2, 0, Time::now(), &g);
//         let val = s.get(&1, 0, &g).unwrap();
//         assert_eq!(2, val.item.value);
//
//         let _val = s.get(&1, 0, &g).unwrap();
//         s.update(1, 3, 0,Time::now(), &g);
//
//         let v = s.get(&1, 0, &g).unwrap();
//         assert_eq!(3, v.item.value);
//     }
//
//     #[test]
//     fn test_concurrent_get_insert() {
//         let s = Arc::new(ShardedMap::new());
//         let s1 = s.clone();
//
//         std::thread::spawn(move || {
//             let tg = flurry::epoch::pin();
//             s.insert(1, 2, 0, Time::now(), &tg);
//         });
//
//         let g = flurry::epoch::pin();
//         loop {
//             match s1.get(&1, 0, &g) {
//                 None => continue,
//                 Some(val) => {
//                     assert_eq!(val.item.value, 2);
//                     break;
//                 }
//             }
//         }
//     }
//
//     #[test]
//     fn test_concurrent_get_mut_insert() {
//         let s = Arc::new(ShardedMap::new());
//         let s1 = s.clone();
//
//         std::thread::spawn(move || {
//             let tg = flurry::epoch::pin();
//             s.insert(1, 2, 0, Time::now(), &tg);
//             loop {
//                 match s.get(&1, 0, &tg) {
//                     None => continue,
//                     Some(val) => {
//                         if val.item.value == 2 {
//                             continue;
//                         } else if val.item.value == 7 {
//                             break;
//                         } else {
//                             panic!("get wrong value")
//                         }
//                     }
//                 }
//             }
//         });
//
//         let g = flurry::epoch::pin();
//         loop {
//             match s1.get(&1, 0, &g) {
//                 None => continue,
//                 Some(val) => {
//                     assert_eq!(val.item.value, 2);
//                     break;
//                 }
//             }
//         }
//     }
//
//     #[test]
//     fn test_store_remove() {
//         let s: ShardedMap<u64> = ShardedMap::new();
//         let g = flurry::epoch::pin();
//         s.insert(1, 2, 0, Time::now(), &g);
//         assert_eq!(s.remove(&1, 0, &g).unwrap().item.value, 2);
//         let v = s.get(&1, 0, &g);
//         assert!(v.is_none());
//         assert!(s.remove(&2, 0, &g).is_none());
//     }
//
//     #[test]
//     fn test_store_update() {
//         let s = ShardedMap::new();
//         let g = flurry::epoch::pin();
//         s.insert(1, 1, 0, Time::now(), &g);
//         let v = s.update(1, 2, 0, Time::now(), &g);
//         assert_eq!(v.value(), &1);
//
//         assert_eq!(s.get(&1, 0, &g).unwrap().item.value, 2);
//
//         let v = s.update(1, 3, 0, Time::now(), &g);
//         assert_eq!(v.value(), &2);
//
//         assert_eq!(s.get(&1, 0, &g).unwrap().item.value, 3);
//
//         let v = s.update(2, 2, 0, Time::now(), &g);
//         assert_eq!(v.value(), &2);
//         let v = s.get(&2, 0, &g);
//         assert!(v.is_none());
//     }
//
//     #[test]
//     fn test_store_expiration() {
//         let exp = Time::now_with_expiration(Duration::from_secs(1));
//         let s = ShardedMap::new();
//         let g = flurry::epoch::pin();
//         s.insert(1, 1, 0, exp, &g);
//
//         assert_eq!(s.get(&1, 0, &g).unwrap().item.value, 1);
//
//         let ttl = s.expiration(&1, &g);
//         assert_eq!(exp, ttl.unwrap());
//
//         s.remove(&1, 0, &g);
//         assert!(s.get(&1, 0, &g).is_none());
//         let ttl = s.expiration(&1, &g);
//         assert!(ttl.is_none());
//
//         assert!(s.expiration(&4340958203495, &g).is_none());
//     }
//
//     #[test]
//     fn test_store_collision() {
//         let s = ShardedMap::new();
//         let data1 = &s.shards[1];
//         let g = flurry::epoch::pin();
//         data1.insert(
//             1,
//             FlurryStoreItem {
//                 key: 1,
//                 conflict: 0,
//                 value: 1,
//                 expiration: Cell::new(Time::now()),
//             },
//             &g
//         );
//
//         assert!(s.get(&1, 1, &g).is_none());
//
//         s.insert(1, 2, 1, Time::now(), &g);
//         assert_ne!(s.get(&1, 0, &g).unwrap().item.value, 2);
//
//         let v = s.update(1, 2, 1, Time::now(), &g);
//         assert_eq!(v.value(), &2);
//         assert_ne!(s.get(&1, 0, &g).unwrap().item.value, 2);
//
//         assert!(s.remove(&1, 1, &g).is_none());
//         assert_eq!(s.get(&1, 0, &g).unwrap().item.value, 1);
//     }
// }