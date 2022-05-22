use parking_lot::RwLock;
use std::cell::RefCell;
use std::collections::{hash_map::RandomState, HashMap};
use std::hash::BuildHasher;
use std::ops::{Deref, DerefMut};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
        self.created_at
            .duration_since(UNIX_EPOCH)
            .map(|d| d + self.d)
            .unwrap()
            .as_secs()
    }

    pub fn elapsed(&self) -> Duration {
        self.created_at.elapsed().unwrap()
    }

    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed().map_or(false, |d| d >= self.d)
    }

    pub fn get_ttl(&self) -> Duration {
        if self.d.is_zero() {
            return Duration::MAX;
        }
        let elapsed = self.created_at.elapsed().unwrap();
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
    buckets: RwLock<RefCell<HashMap<i64, Bucket<S>, S>>>,
    hasher: S,
}

impl Default for ExpirationMap {
    fn default() -> Self {
        let hasher = RandomState::default();
        Self {
            buckets: RwLock::new(RefCell::new(HashMap::with_hasher(hasher.clone()))),
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
            buckets: RwLock::new(RefCell::new(HashMap::with_hasher(hasher.clone()))),
            hasher,
        }
    }

    pub fn try_insert(&self, key: u64, conflict: u64, expiration: Time) -> Result<(), CacheError> {
        // Items that don't expire don't need to be in the expiration map.
        if expiration.is_zero() {
            return Ok(());
        }

        let bucket_num = storage_bucket(expiration);

        let m = self.buckets.read();

        let mut m = m
            .try_borrow_mut()
            .map_err(|e| CacheError::InsertError(e.to_string()))?;
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

        let m = self.buckets.read();
        let (old_bucket_num, new_bucket_num) =
            (storage_bucket(old_exp_time), storage_bucket(new_exp_time));

        if old_bucket_num == new_bucket_num {
            return Ok(());
        }

        let mut m = m
            .try_borrow_mut()
            .map_err(|e| CacheError::UpdateError(e.to_string()))?;
        m.remove(&old_bucket_num);

        match m.get_mut(&new_bucket_num) {
            None => {
                let mut bucket = Bucket::with_hasher(self.hasher.clone());
                bucket.map.insert(key, conflict);
                m.insert(new_bucket_num, bucket);
            }
            Some(bucket) => {
                bucket.map.insert(key, conflict);
            }
        }

        Ok(())
    }

    pub fn try_remove(&self, key: &u64, expiration: Time) -> Result<(), CacheError> {
        let bucket_num = storage_bucket(expiration);
        let m = self.buckets.read();
        if let Some(bucket) = m
            .try_borrow_mut()
            .map_err(|e| CacheError::RemoveError(e.to_string()))?
            .get_mut(&bucket_num)
        {
            bucket.remove(key);
        };

        Ok(())
    }

    pub fn try_cleanup(&self, now: Time) -> Result<Option<HashMap<u64, u64, S>>, CacheError> {
        let bucket_num = cleanup_bucket(now);
        Ok(self
            .buckets
            .read()
            .try_borrow_mut()
            .map_err(|e| CacheError::CleanupError(e.to_string()))?
            .remove(&bucket_num)
            .map(|bucket| bucket.map))
    }

    pub fn hasher(&self) -> S {
        self.hasher.clone()
    }
}

unsafe impl<S: BuildHasher + Clone + 'static> Send for ExpirationMap<S> {}

unsafe impl<S: BuildHasher + Clone + 'static> Sync for ExpirationMap<S> {}
