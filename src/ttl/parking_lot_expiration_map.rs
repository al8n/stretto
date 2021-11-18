use std::cell::RefCell;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::ops::{Deref, DerefMut};
use parking_lot::RwLock;
use crate::ttl::{cleanup_bucket, storage_bucket, Time};

/// Bucket is a map of key to conflict.
#[derive(Debug)]
struct Bucket<S = RandomState> {
    map: HashMap<u64, u64, S>,
}

impl Default for Bucket {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
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

    pub fn insert(&self, key: u64, conflict: u64, expiration: Time) {
        // Items that don't expire don't need to be in the expiration map.
        if expiration.is_zero() {
            return;
        }

        let bucket_num = storage_bucket(expiration);

        let m = self.buckets.read();

        let mut m = m.borrow_mut();
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
    }

    pub fn update(&self, key: u64, conflict: u64, old_exp_time: Time, new_exp_time: Time) {
        if old_exp_time.is_zero() && new_exp_time.is_zero() {
            return;
        }

        let m = self.buckets.read();
        let (old_bucket_num, new_bucket_num) =
            (storage_bucket(old_exp_time), storage_bucket(new_exp_time));

        if old_bucket_num == new_bucket_num {
            return;
        }

        let mut m = m.borrow_mut();
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
    }

    pub fn remove(&self, key: &u64, expiration: Time) {
        let bucket_num = storage_bucket(expiration);
        let m = self.buckets.read();
        if let Some(bucket) = m.borrow_mut().get_mut(&bucket_num) {
            bucket.remove(key);
        };
    }

    pub fn cleanup(&self, now: Time) -> Option<HashMap<u64, u64, S>> {
        let bucket_num = cleanup_bucket(now);
        self.buckets
            .read()
            .borrow_mut()
            .remove(&bucket_num)
            .map(|bucket| bucket.map)
    }

    pub fn hasher(&self) -> S {
        self.hasher.clone()
    }
}

unsafe impl<S: BuildHasher + Clone + 'static> Send for ExpirationMap<S> {}

unsafe impl<S: BuildHasher + Clone + 'static> Sync for ExpirationMap<S> {}
