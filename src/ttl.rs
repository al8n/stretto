use parking_lot::RwLock;
use std::{
  collections::{HashMap, hash_map::RandomState},
  hash::BuildHasher,
  ops::{Deref, DerefMut},
  time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::CacheError;

fn storage_bucket(t: Time) -> i64 {
  (t.unix() + 1) as i64
}

fn cleanup_bucket(t: Time) -> i64 {
  // The bucket to cleanup is always behind the storage bucket by one so that
  // no elements in that bucket (which might not have expired yet) are deleted.
  storage_bucket(t) - 1
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Time {
  d: Duration,
  created_at: SystemTime,
}

impl Time {
  pub fn now() -> Self {
    Self {
      d: Duration::ZERO,
      created_at: SystemTime::now(),
    }
  }

  pub fn now_with_expiration(duration: Duration) -> Self {
    Self {
      d: duration,
      created_at: SystemTime::now(),
    }
  }

  pub fn is_zero(&self) -> bool {
    self.d.is_zero()
  }

  pub fn unix(&self) -> u64 {
    self
      .created_at
      .duration_since(UNIX_EPOCH)
      .map(|d| d + self.d)
      .unwrap()
      .as_secs()
  }

  pub fn elapsed(&self) -> Duration {
    self.created_at.elapsed().unwrap_or(Duration::ZERO)
  }

  pub fn is_expired(&self) -> bool {
    self.created_at.elapsed().is_ok_and(|d| d >= self.d)
  }

  pub fn get_ttl(&self) -> Duration {
    if self.d.is_zero() {
      return Duration::MAX;
    }
    let elapsed = self.created_at.elapsed().unwrap_or(Duration::ZERO);
    if elapsed >= self.d {
      Duration::ZERO
    } else {
      self.d - elapsed
    }
  }
}

/// Bucket is a map of key to conflict.
#[derive(Debug, Default)]
struct Bucket<S = RandomState> {
  map: HashMap<u64, u64, S>,
}

impl<S: BuildHasher> Bucket<S> {
  pub fn with_hasher(hasher: S) -> Self {
    Self {
      map: HashMap::with_hasher(hasher),
    }
  }
}

impl<S: BuildHasher> Deref for Bucket<S> {
  type Target = HashMap<u64, u64, S>;

  fn deref(&self) -> &Self::Target {
    &self.map
  }
}

impl<S: BuildHasher> DerefMut for Bucket<S> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.map
  }
}

#[derive(Debug)]
pub(crate) struct ExpirationMap<S = RandomState> {
  buckets: RwLock<HashMap<i64, Bucket<S>, S>>,
  hasher: S,
}

impl Default for ExpirationMap {
  fn default() -> Self {
    let hasher = RandomState::default();
    Self {
      buckets: RwLock::new(HashMap::with_hasher(hasher.clone())),
      hasher,
    }
  }
}

impl ExpirationMap {
  pub fn new() -> Self {
    Self::default()
  }
}

impl<S: BuildHasher + Clone + 'static> ExpirationMap<S> {
  pub(crate) fn with_hasher(hasher: S) -> ExpirationMap<S> {
    ExpirationMap {
      buckets: RwLock::new(HashMap::with_hasher(hasher.clone())),
      hasher,
    }
  }

  pub fn try_insert(&self, key: u64, conflict: u64, expiration: Time) -> Result<(), CacheError> {
    // Items that don't expire don't need to be in the expiration map.
    if expiration.is_zero() {
      return Ok(());
    }

    let bucket_num = storage_bucket(expiration);

    let mut m = self.buckets.write();

    match m.get_mut(&bucket_num) {
      None => {
        let mut bucket = Bucket::with_hasher(self.hasher.clone());
        bucket.map.insert(key, conflict);
        m.insert(bucket_num, bucket);
      }
      Some(bucket) => {
        bucket.map.insert(key, conflict);
      }
    }

    Ok(())
  }

  pub fn try_update(
    &self,
    key: u64,
    conflict: u64,
    old_exp_time: Time,
    new_exp_time: Time,
  ) -> Result<(), CacheError> {
    if old_exp_time.is_zero() && new_exp_time.is_zero() {
      return Ok(());
    }

    let old_bucket_num = (!old_exp_time.is_zero()).then(|| storage_bucket(old_exp_time));
    let new_bucket_num = (!new_exp_time.is_zero()).then(|| storage_bucket(new_exp_time));

    if old_bucket_num == new_bucket_num {
      return Ok(());
    }

    let mut m = self.buckets.write();

    if let Some(old_bn) = old_bucket_num {
      if let Some(bucket) = m.get_mut(&old_bn) {
        bucket.map.remove(&key);
      }
    }

    if let Some(new_bn) = new_bucket_num {
      match m.get_mut(&new_bn) {
        None => {
          let mut bucket = Bucket::with_hasher(self.hasher.clone());
          bucket.map.insert(key, conflict);
          m.insert(new_bn, bucket);
        }
        Some(bucket) => {
          bucket.map.insert(key, conflict);
        }
      }
    }

    Ok(())
  }

  pub fn try_remove(&self, key: &u64, expiration: Time) -> Result<(), CacheError> {
    let bucket_num = storage_bucket(expiration);
    let mut m = self.buckets.write();
    if let Some(bucket) = m.get_mut(&bucket_num) {
      bucket.remove(key);
    };

    Ok(())
  }

  pub fn try_cleanup(&self, now: Time) -> Result<Option<HashMap<u64, u64, S>>, CacheError> {
    // Remove every bucket that is at or below the current cleanup bucket. A
    // single bucket per call is not enough: when the cleanup interval is larger
    // than one second (the bucket granularity), some buckets never line up with
    // a tick and would otherwise leak forever. Everything with index <= the
    // cleanup bucket has definitely expired, so merging them is safe.
    let target = cleanup_bucket(now);
    let mut m = self.buckets.write();
    let expired: Vec<i64> = m.keys().copied().filter(|b| *b <= target).collect();
    if expired.is_empty() {
      return Ok(None);
    }

    let mut merged = HashMap::with_hasher(self.hasher.clone());
    for b in expired {
      if let Some(bucket) = m.remove(&b) {
        merged.extend(bucket.map);
      }
    }
    Ok(Some(merged))
  }

  pub fn hasher(&self) -> S {
    self.hasher.clone()
  }

  /// Wipe all buckets. Used by `ShardedMap::clear` so that buckets created
  /// before a cache-level `clear()` cannot outlive their rows and then
  /// incorrectly delete fresh post-clear entries at the same key when the
  /// cleanup ticker later catches up.
  pub fn clear(&self) {
    self.buckets.write().clear();
  }
}

#[cfg(test)]
mod expiration_map_tests {
  use super::*;

  // Regression test for https://github.com/al8n/stretto/issues/55: with a
  // cleanup interval longer than one second, buckets that never align with a
  // tick used to leak. try_cleanup now sweeps every bucket whose index is at
  // or below the current cleanup bucket.
  #[test]
  fn try_cleanup_sweeps_all_past_buckets() {
    let em = ExpirationMap::new();
    let now = Time::now();
    let base = storage_bucket(now);

    let mut m = em.buckets.write();
    m.insert(base - 5, {
      let mut b = Bucket::with_hasher(em.hasher.clone());
      b.map.insert(10, 100);
      b
    });
    m.insert(base - 3, {
      let mut b = Bucket::with_hasher(em.hasher.clone());
      b.map.insert(20, 200);
      b
    });
    m.insert(base - 1, {
      let mut b = Bucket::with_hasher(em.hasher.clone());
      b.map.insert(30, 300);
      b
    });
    // A future bucket that must NOT be cleaned.
    m.insert(base + 10, {
      let mut b = Bucket::with_hasher(em.hasher.clone());
      b.map.insert(40, 400);
      b
    });
    drop(m);

    let cleaned = em
      .try_cleanup(now)
      .unwrap()
      .expect("expected expired entries");
    assert_eq!(cleaned.len(), 3, "should drain all past buckets");
    assert_eq!(cleaned.get(&10), Some(&100));
    assert_eq!(cleaned.get(&20), Some(&200));
    assert_eq!(cleaned.get(&30), Some(&300));

    let remaining = em.buckets.read();
    assert!(remaining.contains_key(&(base + 10)));
    assert_eq!(remaining.len(), 1);
  }

  #[test]
  fn time_get_ttl_zero_when_expired() {
    let t = Time::now_with_expiration(Duration::from_millis(1));
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(t.get_ttl(), Duration::ZERO);
  }

  #[test]
  fn time_get_ttl_max_when_zero_duration() {
    assert_eq!(Time::now().get_ttl(), Duration::MAX);
  }

  #[test]
  fn bucket_deref_and_deref_mut() {
    let mut b: Bucket = Bucket::with_hasher(RandomState::new());
    // DerefMut: insert through the deref
    let m: &mut HashMap<u64, u64, _> = &mut b;
    m.insert(7, 70);
    // Deref: read through the deref
    let m: &HashMap<u64, u64, _> = &b;
    assert_eq!(m.get(&7), Some(&70));
  }

  #[test]
  fn try_update_same_bucket_is_noop() {
    let em = ExpirationMap::new();
    let exp = Time::now_with_expiration(Duration::from_secs(60));
    em.try_insert(1, 100, exp).unwrap();
    // Same exp ⇒ same bucket ⇒ early return without touching the map.
    em.try_update(1, 100, exp, exp).unwrap();
    let m = em.buckets.read();
    assert_eq!(m.len(), 1);
  }

  #[test]
  fn try_update_into_existing_bucket() {
    let em = ExpirationMap::new();
    // Both keys land in the same future bucket so the second try_update
    // exercises the `Some(bucket)` branch when migrating into it.
    let exp_old = Time::now_with_expiration(Duration::from_secs(1));
    let exp_new = Time::now_with_expiration(Duration::from_secs(60));
    em.try_insert(1, 100, exp_old).unwrap();
    em.try_insert(2, 200, exp_new).unwrap();
    em.try_update(1, 100, exp_old, exp_new).unwrap();

    let m = em.buckets.read();
    let bucket = m.get(&storage_bucket(exp_new)).unwrap();
    assert_eq!(bucket.map.get(&1), Some(&100));
    assert_eq!(bucket.map.get(&2), Some(&200));
  }

  #[test]
  fn try_update_zero_zero_is_noop() {
    let em = ExpirationMap::new();
    let zero = Time::now();
    em.try_update(1, 100, zero, zero).unwrap();
    assert!(em.buckets.read().is_empty());
  }
}
