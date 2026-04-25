use crate::{
  CacheError, DefaultUpdateValidator, Item as CrateItem, UpdateValidator,
  ttl::{ExpirationMap, Time},
  utils::{ValueRef, ValueRefMut},
};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
  collections::{HashMap, hash_map::RandomState},
  fmt::{Debug, Formatter},
  hash::BuildHasher,
  mem,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

const NUM_OF_SHARDS: usize = 256;

pub(crate) struct StoreItem<V> {
  pub(crate) key: u64,
  pub(crate) conflict: u64,
  pub(crate) version: u64,
  /// Clear-generation stamp captured by the writer that last wrote this row.
  /// Pre-clear callers whose captured generation is lower than this row's
  /// generation are refused at the store level, so a stale writer cannot
  /// clobber a post-clear insert/update. See `try_insert`/`try_update`.
  pub(crate) generation: u64,
  pub(crate) value: V,
  pub(crate) expiration: Time,
}

impl<V> Debug for StoreItem<V> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("StoreItem")
      .field("key", &self.key)
      .field("conflict", &self.conflict)
      .field("version", &self.version)
      .field("generation", &self.generation)
      .field("expiration", &self.expiration)
      .finish()
  }
}

type Shards<V, SS> = Box<[RwLock<HashMap<u64, StoreItem<V>, SS>>; NUM_OF_SHARDS]>;

pub(crate) struct ShardedMap<V, U = DefaultUpdateValidator<V>, SS = RandomState, ES = RandomState> {
  shards: Shards<V, SS>,
  em: ExpirationMap<ES>,
  store_item_size: usize,
  validator: U,
  version: AtomicU64,
}

impl<V: Send + Sync + 'static> ShardedMap<V> {
  #[allow(dead_code)]
  pub fn new() -> Self {
    Self::with_validator(ExpirationMap::new(), DefaultUpdateValidator::default())
  }
}

impl<V: Send + Sync + 'static, U: UpdateValidator<Value = V>> ShardedMap<V, U> {
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
      // Versions start at 1 so 0 is reserved as a "no row" sentinel used by
      // Item::Delete (when the eager remove found nothing) and by the
      // EagerInsertGuard armed check. If this were 0, the first inserted
      // row would legitimately have version 0 and collide with the sentinel.
      version: AtomicU64::new(1),
    }
  }
}

impl<
  V: Send + Sync + 'static,
  U: UpdateValidator<Value = V>,
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
      // See `new` for why this starts at 1.
      version: AtomicU64::new(1),
    }
  }

  pub fn get(&self, key: &u64, conflict: u64) -> Option<ValueRef<'_, V>> {
    let data = self.shards[(*key as usize) % NUM_OF_SHARDS].read();

    let expiration = match data.get(key) {
      Some(item) => {
        if conflict != 0 && (conflict != item.conflict) {
          return None;
        }
        // Handle expired items
        if !item.expiration.is_zero() && item.expiration.is_expired() {
          return None;
        }
        item.expiration
      }
      None => return None,
    };

    // Project the read guard to the inner value. The lock is still held, so
    // the second lookup in the closure cannot fail — but `try_map` returns a
    // Result and we propagate `None` defensively.
    let mapped = RwLockReadGuard::try_map(data, |m| m.get(key).map(|item| &item.value)).ok()?;
    Some(ValueRef::new(mapped, expiration))
  }

  pub fn get_mut(&self, key: &u64, conflict: u64) -> Option<ValueRefMut<'_, V>> {
    let data = self.shards[(*key as usize) % NUM_OF_SHARDS].write();

    match data.get(key) {
      Some(item) => {
        if conflict != 0 && (conflict != item.conflict) {
          return None;
        }
        // Handle expired items
        if !item.expiration.is_zero() && item.expiration.is_expired() {
          return None;
        }
      }
      None => return None,
    }

    let mapped =
      RwLockWriteGuard::try_map(data, |m| m.get_mut(key).map(|item| &mut item.value)).ok()?;
    Some(ValueRefMut::new(mapped))
  }

  pub fn try_insert(
    &self,
    key: u64,
    val: V,
    conflict: u64,
    expiration: Time,
    generation: u64,
  ) -> Result<Option<u64>, CacheError> {
    let mut data = self.shards[(key as usize) % NUM_OF_SHARDS].write();

    match data.get(&key) {
      None => {
        // The value is not in the map already. There's no need to return anything.
        // Simply add the expiration map.
        self.em.try_insert(key, conflict, expiration)?;
      }
      Some(sitem) => {
        // The item existed already. We need to check the conflict key and reject the
        // update if they do not match. Only after that the expiration map is updated.
        if conflict != 0 && (conflict != sitem.conflict) {
          return Ok(None);
        }

        // Stale-writer refusal: the existing row was written under a later
        // clear generation than this caller captured. Allowing the overwrite
        // would destroy a legitimate post-clear row on behalf of a pre-clear
        // writer that was scheduled-out across `clear()`. Refuse cleanly so
        // the caller enqueues nothing and the post-clear row is preserved.
        if sitem.generation > generation {
          return Ok(None);
        }

        if !self.validator.should_update(&sitem.value, &val) {
          return Ok(None);
        }

        self
          .em
          .try_update(key, conflict, sitem.expiration, expiration)?;
      }
    }

    let version = self.version.fetch_add(1, Ordering::Relaxed);
    data.insert(
      key,
      StoreItem {
        key,
        conflict,
        version,
        generation,
        value: val,
        expiration,
      },
    );

    Ok(Some(version))
  }

  pub fn try_update(
    &self,
    key: u64,
    mut val: V,
    conflict: u64,
    expiration: Time,
    generation: u64,
  ) -> Result<UpdateResult<V>, CacheError> {
    let mut data = self.shards[(key as usize) % NUM_OF_SHARDS].write();
    match data.get_mut(&key) {
      None => Ok(UpdateResult::NotExist(val)),
      Some(item) => {
        if conflict != 0 && (conflict != item.conflict) {
          return Ok(UpdateResult::Conflict(val));
        }

        // Stale-writer refusal. See `try_insert` for the full rationale. A
        // pre-clear caller that resumes after clear+reinsert must not be
        // allowed to overwrite the post-clear row in place — doing so would
        // destroy the new row and leave the processor reaping stale state.
        if item.generation > generation {
          return Ok(UpdateResult::Stale(val));
        }

        if !self.validator.should_update(&item.value, &val) {
          return Ok(UpdateResult::Reject(val));
        }

        self
          .em
          .try_update(key, conflict, item.expiration, expiration)?;
        mem::swap(&mut val, &mut item.value);
        item.expiration = expiration;
        let new_version = self.version.fetch_add(1, Ordering::Relaxed);
        item.version = new_version;
        item.generation = generation;
        Ok(UpdateResult::Update(val, new_version))
      }
    }
  }

  pub fn len(&self) -> usize {
    self.shards.iter().map(|l| l.read().len()).sum()
  }

  /// Returns true iff the store currently holds a live row at `(key, conflict)`
  /// whose version exactly matches `version`. Used by the New handler to gate
  /// policy admission: a concurrent Delete from a different producer can land
  /// in the MPSC insert buffer before the matching New even though the caller-
  /// side eager insert happened first, and without this check the New would
  /// add to policy a key whose store row has already been deleted — leaving a
  /// ghost admission outside both store and eviction accounting.
  pub fn contains_version(&self, key: &u64, conflict: u64, version: u64) -> bool {
    let data = self.shards[(*key as usize) % NUM_OF_SHARDS].read();
    match data.get(key) {
      Some(item) => {
        if conflict != 0 && conflict != item.conflict {
          return false;
        }
        item.version == version
      }
      None => false,
    }
  }

  /// Returns true iff the store currently holds *any* live row at
  /// `(key, conflict)`, regardless of version. Used by the Delete handler to
  /// distinguish "caller removed the last row" from "a newer insert has since
  /// re-admitted the key" — in the latter case the policy entry already
  /// reflects the new row (the New handler updated it instead of re-adding)
  /// and wiping it now would orphan the fresh admission outside accounting.
  pub fn contains_key(&self, key: &u64, conflict: u64) -> bool {
    let data = self.shards[(*key as usize) % NUM_OF_SHARDS].read();
    match data.get(key) {
      Some(item) => conflict == 0 || conflict == item.conflict,
      None => false,
    }
  }

  pub fn try_remove(&self, key: &u64, conflict: u64) -> Result<Option<StoreItem<V>>, CacheError> {
    let mut data = self.shards[(*key as usize) % NUM_OF_SHARDS].write();

    match data.get(key) {
      None => Ok(None),
      Some(item) => {
        if conflict != 0 && (conflict != item.conflict) {
          return Ok(None);
        }

        if !item.expiration.is_zero() {
          self.em.try_remove(key, item.expiration)?;
        }

        Ok(data.remove(key))
      }
    }
  }

  /// Caller-gated remove: refuses to destroy a row whose generation is later
  /// than the caller's captured generation. Used by the public `remove` paths
  /// so a pre-clear caller that resumes after `clear()` and a post-clear
  /// reinsert cannot delete the fresh row. `Ok(None)` is returned for both
  /// "no row present" and "stale writer refused" — the caller treats both as
  /// "nothing to enqueue."
  pub fn try_remove_if_not_stale(
    &self,
    key: &u64,
    conflict: u64,
    generation: u64,
  ) -> Result<Option<StoreItem<V>>, CacheError> {
    let mut data = self.shards[(*key as usize) % NUM_OF_SHARDS].write();

    match data.get(key) {
      None => Ok(None),
      Some(item) => {
        if conflict != 0 && (conflict != item.conflict) {
          return Ok(None);
        }
        if item.generation > generation {
          return Ok(None);
        }

        if !item.expiration.is_zero() {
          self.em.try_remove(key, item.expiration)?;
        }

        Ok(data.remove(key))
      }
    }
  }

  pub fn try_remove_if_version(
    &self,
    key: &u64,
    conflict: u64,
    version: u64,
  ) -> Result<Option<StoreItem<V>>, CacheError> {
    let mut data = self.shards[(*key as usize) % NUM_OF_SHARDS].write();

    match data.get(key) {
      None => Ok(None),
      Some(item) => {
        if conflict != 0 && (conflict != item.conflict) {
          return Ok(None);
        }
        // Only remove if the generation matches; a later insert/update bumps gen.
        if item.version != version {
          return Ok(None);
        }

        if !item.expiration.is_zero() {
          self.em.try_remove(key, item.expiration)?;
        }

        Ok(data.remove(key))
      }
    }
  }

  pub fn expiration(&self, key: &u64) -> Option<Time> {
    self.shards[((*key) as usize) % NUM_OF_SHARDS]
      .read()
      .get(key)
      .map(|val| val.expiration)
  }

  #[cfg(feature = "sync")]
  pub fn try_cleanup<PS: BuildHasher + Clone + 'static>(
    &self,
    policy: Arc<crate::policy::LFUPolicy<PS>>,
  ) -> Result<Vec<CrateItem<V>>, CacheError> {
    let now = Time::now();
    Ok(
      self
        .em
        .try_cleanup(now)?
        .map_or(Vec::with_capacity(0), |m| {
          m.iter()
                    // Sanity check. Verify that the store agrees that this key is expired.
                    // `!t.is_zero()` gate: a zero-TTL row is "never expires"; without
                    // the gate, `is_expired()` returns true unconditionally (elapsed
                    // is always >= 0), so the store row would be destroyed any time
                    // a stale bucket happened to name this key.
                    .filter_map(|(k, v)| {
                        self.expiration(k)
                            .and_then(|t| {
                                if !t.is_zero() && t.is_expired() {
                                    let cost = policy.cost(k);
                                    policy.remove(k);
                                    self.try_remove(k, *v)
                                        .map(|maybe_sitem| {
                                            maybe_sitem.map(|sitem| CrateItem {
                                                val: Some(sitem.value),
                                                index: sitem.key,
                                                conflict: sitem.conflict,
                                                cost,
                                                exp: t,
                                            })
                                        })
                                        .ok()
                                } else {
                                    None
                                }
                            })
                            .flatten()
                    })
                    .collect()
        }),
    )
  }

  #[cfg(feature = "async")]
  pub fn try_cleanup_async<PS: BuildHasher + Clone + 'static>(
    &self,
    policy: Arc<crate::policy::AsyncLFUPolicy<PS>>,
  ) -> Result<Vec<CrateItem<V>>, CacheError> {
    let now = Time::now();
    let items = self.em.try_cleanup(now)?;

    let mut removed_items = Vec::new();
    if let Some(items) = items {
      for (k, v) in items.iter() {
        let expiration = self.expiration(k);
        if let Some(t) = expiration {
          // See the matching `!t.is_zero()` guard in the sync cleanup
          // above: zero-TTL rows must not be treated as expired.
          if !t.is_zero() && t.is_expired() {
            let cost = policy.cost(k);
            policy.remove(k);
            let removed_item = self.try_remove(k, *v)?;
            if let Some(sitem) = removed_item {
              removed_items.push(CrateItem {
                val: Some(sitem.value),
                index: sitem.key,
                conflict: sitem.conflict,
                cost,
                exp: t,
              })
            }
          }
        }
      }
    }

    Ok(removed_items)
  }

  /// Wipe every shard AND the expiration map, returning the drained values so
  /// the caller can dispatch `CacheCallback::on_exit` (or equivalent) AFTER
  /// the rest of the cache state (policy, metrics, `clear_generation`) has
  /// been reset.
  ///
  /// The em wipe is load-bearing: without it, a bucket created by a pre-clear
  /// TTL insert survives `clear`, and when the cleanup ticker processes that
  /// bucket later it calls `try_remove(key, stale_conflict)`. For a
  /// deterministic `KeyBuilder`, a post-clear reinsert of the same key
  /// produces the same conflict, so the stale bucket entry would match and
  /// delete the fresh row. See the matching `!t.is_zero()` guards in
  /// `try_cleanup`/`try_cleanup_async` which close the same hole from the
  /// other side (zero-TTL rows must not be treated as expired).
  ///
  /// Returning values here — instead of firing callbacks in-place — is
  /// load-bearing on two fronts:
  ///
  /// 1. All shard write locks are released before any callback runs, so a
  ///    callback that re-enters the cache via a shard-lock-taking path
  ///    (`get`, `len`, `contains_key`, ...) does not self-deadlock on the
  ///    processor thread (parking_lot RwLocks are not reentrant).
  /// 2. It lets the caller (the `Item::Clear` handler) run callbacks AFTER
  ///    bumping `clear_generation`. Otherwise an `on_exit` that re-enters
  ///    via `insert` captures the pre-bump generation, enqueues an
  ///    `Item::New` stamped with that stale generation, and the processor
  ///    then rejects it as stale — silently discarding the user's insert.
  pub fn clear(&self) -> Vec<V> {
    let mut drained: Vec<V> = Vec::new();
    for shard in self.shards.iter() {
      let mut map = shard.write();
      drained.extend(map.drain().map(|(_, item)| item.value));
    }
    self.em.clear();
    drained
  }

  pub fn hasher(&self) -> ES {
    self.em.hasher()
  }

  pub fn item_size(&self) -> usize {
    self.store_item_size
  }
}

pub(crate) enum UpdateResult<V: Send + Sync + 'static> {
  NotExist(V),
  Reject(V),
  Conflict(V),
  /// The existing row was stamped with a later clear generation than the
  /// caller captured. The update is refused so a pre-clear writer cannot
  /// clobber a post-clear row. Carries the value back so the caller can
  /// drop or forward it as needed.
  Stale(V),
  /// The update committed. Carries the previous value and the new version
  /// assigned to the store entry — callers need the version to scope a
  /// cancellation rollback to the row they just wrote.
  Update(V, u64),
}

#[cfg(test)]
impl<V: Send + Sync + 'static> UpdateResult<V> {
  fn into_inner(self) -> V {
    match self {
      UpdateResult::NotExist(v) => v,
      UpdateResult::Reject(v) => v,
      UpdateResult::Conflict(v) => v,
      UpdateResult::Stale(v) => v,
      UpdateResult::Update(v, _) => v,
    }
  }
}

#[cfg(test)]
mod test {
  use crate::{
    store::{ShardedMap, StoreItem, UpdateResult},
    ttl::Time,
  };
  use std::{sync::Arc, time::Duration};

  #[test]
  fn test_store_item_debug() {
    let item = StoreItem {
      key: 0,
      conflict: 0,
      version: 0,
      generation: 0,
      value: 3,
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

    s.try_insert(1, 2, 0, Time::now(), 0).unwrap();
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
      s.try_insert(1, 2, 0, Time::now(), 0).unwrap();
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
      s.try_insert(1, 2, 0, Time::now(), 0).unwrap();
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

    s.try_insert(1, 2, 0, Time::now(), 0).unwrap();
    assert_eq!(s.try_remove(&1, 0).unwrap().unwrap().value, 2);
    let v = s.get(&1, 0);
    assert!(v.is_none());
    assert!(s.try_remove(&2, 0).unwrap().is_none());
  }

  #[test]
  fn test_store_update() {
    let s = ShardedMap::new();
    s.try_insert(1, 1, 0, Time::now(), 0).unwrap();
    let v = s.try_update(1, 2, 0, Time::now(), 0).unwrap();
    assert_eq!(v.into_inner(), 1);

    assert_eq!(s.get(&1, 0).unwrap().read(), 2);

    let v = s.try_update(1, 3, 0, Time::now(), 0).unwrap();
    assert_eq!(v.into_inner(), 2);

    assert_eq!(s.get(&1, 0).unwrap().read(), 3);

    let v = s.try_update(2, 2, 0, Time::now(), 0).unwrap();
    assert_eq!(v.into_inner(), 2);
    let v = s.get(&2, 0);
    assert!(v.is_none());
  }

  #[test]
  fn test_store_expiration() {
    let exp = Time::now_with_expiration(Duration::from_secs(1));
    let s = ShardedMap::new();
    s.try_insert(1, 1, 0, exp, 0).unwrap();

    assert_eq!(s.get(&1, 0).unwrap().read(), 1);

    let ttl = s.expiration(&1);
    assert_eq!(exp, ttl.unwrap());

    s.try_remove(&1, 0).unwrap();
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
        version: 0,
        generation: 0,
        value: 1,
        expiration: Time::now(),
      },
    );
    drop(data1);
    assert!(s.get(&1, 1).is_none());

    s.try_insert(1, 2, 1, Time::now(), 0).unwrap();
    assert_ne!(s.get(&1, 0).unwrap().read(), 2);

    let v = s.try_update(1, 2, 1, Time::now(), 0).unwrap();
    assert_eq!(v.into_inner(), 2);
    assert_ne!(s.get(&1, 0).unwrap().read(), 2);

    assert!(s.try_remove(&1, 1).unwrap().is_none());
    assert_eq!(s.get(&1, 0).unwrap().read(), 1);
  }

  #[test]
  fn test_store_get_mut_conflict_and_expired() {
    // conflict mismatch
    let s: ShardedMap<u64> = ShardedMap::new();
    s.try_insert(1, 2, 7, Time::now(), 0).unwrap();
    assert!(s.get_mut(&1, 9).is_none());
    // matching conflict works
    assert_eq!(s.get_mut(&1, 7).unwrap().read(), 2);

    // expired entry returns None
    let past = Time::now_with_expiration(Duration::from_millis(1));
    std::thread::sleep(Duration::from_millis(10));
    let s2: ShardedMap<u64> = ShardedMap::new();
    s2.try_insert(2, 2, 0, past, 0).unwrap();
    assert!(s2.get_mut(&2, 0).is_none());
  }

  // Regression guard for the version=0 sentinel invariant. Item::Delete uses
  // version=0 to mean "eager remove found no row" and EagerInsertGuard's
  // `armed = !is_update && version != 0` check relies on the first real
  // inserted version being non-zero. If the counter started at 0, the first
  // `try_insert` would legitimately return 0, collide with the sentinel,
  // disable the cancellation rollback for that first insert, and let the
  // Delete handler misinterpret a real row's version as "no row". Keep
  // this invariant or fix both consumers.
  #[test]
  fn test_store_version_starts_at_one() {
    let s: ShardedMap<u64> = ShardedMap::new();
    let version = s.try_insert(1, 1, 0, Time::now(), 0).unwrap().unwrap();
    assert!(
      version >= 1,
      "first store version must be >= 1; version=0 is the reserved sentinel"
    );
  }

  #[test]
  fn test_store_try_remove_if_version() {
    let s: ShardedMap<u64> = ShardedMap::new();
    let exp = Time::now_with_expiration(Duration::from_secs(60));
    let version = s.try_insert(10, 20, 7, exp, 0).unwrap().unwrap();

    // conflict mismatch returns None
    assert!(
      s.try_remove_if_version(&10, 999, version)
        .unwrap()
        .is_none()
    );

    // wrong version returns None
    assert!(
      s.try_remove_if_version(&10, 7, version + 100)
        .unwrap()
        .is_none()
    );

    // matching version + conflict removes
    let removed = s.try_remove_if_version(&10, 7, version).unwrap().unwrap();
    assert_eq!(removed.value, 20);
    assert!(s.get(&10, 7).is_none());
  }

  // Regression guard for the clear-generation race: a pre-clear writer (with
  // a lower captured generation) must not be able to overwrite or delete a
  // row written by a post-clear writer. The store refuses the mutation so
  // no stale Item ever leaves the caller.
  #[test]
  fn test_store_generation_stamp_refuses_stale_writer() {
    let s: ShardedMap<u64> = ShardedMap::new();

    // Post-clear writer stamps the row with generation=5.
    let version = s.try_insert(1, 100, 0, Time::now(), 5).unwrap().unwrap();

    // Pre-clear writer (captured generation=3) tries to update — refused.
    let result = s.try_update(1, 999, 0, Time::now(), 3).unwrap();
    assert!(matches!(result, UpdateResult::Stale(_)));
    assert_eq!(result.into_inner(), 999);
    // Row is untouched: same version, same value, same generation.
    let row_value = s.get(&1, 0).unwrap().read();
    assert_eq!(row_value, 100);

    // Same caller's try_insert is also refused (returns None) so the new row
    // cannot clobber the post-clear row.
    let inserted = s.try_insert(1, 888, 0, Time::now(), 3).unwrap();
    assert!(inserted.is_none());
    assert_eq!(s.get(&1, 0).unwrap().read(), 100);

    // Stale remove is refused too.
    let removed = s.try_remove_if_not_stale(&1, 0, 3).unwrap();
    assert!(removed.is_none());
    assert_eq!(s.get(&1, 0).unwrap().read(), 100);

    // Post-clear writer at the same generation succeeds.
    let result = s.try_update(1, 200, 0, Time::now(), 5).unwrap();
    assert!(matches!(result, UpdateResult::Update(_, v) if v != version));
    assert_eq!(s.get(&1, 0).unwrap().read(), 200);

    // A later-generation writer can always proceed.
    let result = s.try_update(1, 300, 0, Time::now(), 9).unwrap();
    assert!(matches!(result, UpdateResult::Update(_, _)));
    assert_eq!(s.get(&1, 0).unwrap().read(), 300);
  }

  #[test]
  fn test_store_try_remove_if_not_stale() {
    let s: ShardedMap<u64> = ShardedMap::new();
    s.try_insert(10, 20, 0, Time::now(), 5).unwrap();

    // Stale caller — refused.
    assert!(s.try_remove_if_not_stale(&10, 0, 3).unwrap().is_none());
    assert!(s.get(&10, 0).is_some());

    // Same-or-newer generation — removes.
    let removed = s.try_remove_if_not_stale(&10, 0, 5).unwrap().unwrap();
    assert_eq!(removed.value, 20);
    assert!(s.get(&10, 0).is_none());
  }
}
