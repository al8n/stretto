use std::cell::UnsafeCell;
use flurry::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::collections::HashMap as StdHashMap;
use flurry::epoch::Guard;
use crate::ttl::{cleanup_bucket, storage_bucket, Time};

pub(crate) struct FlurryBucketRef<'g, S> {
    _g: &'g Guard,
    pub(crate) bucket: &'g FlurryBucket<S>,
}

/// FlurryBucket is a map of key to conflict.
#[derive(Debug)]
pub(crate) struct FlurryBucket<S = RandomState> {
    pub(crate) map: UnsafeCell<StdHashMap<u64, u64, S>>,
}

impl Default for FlurryBucket {
    fn default() -> Self {
        Self {
            map: UnsafeCell::new(StdHashMap::new()),
        }
    }
}

impl<S: BuildHasher + Send + Sync + 'static> FlurryBucket<S> {
    #[inline]
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            map: UnsafeCell::new(StdHashMap::with_hasher(hasher)),
        }
    }

    #[inline]
    pub fn insert(&self, key: u64, val: u64) {
        let map = unsafe {
            &mut *self.map.get()
        };
        map.insert(key, val);
    }
    
    #[inline]
    pub fn remove(&self, key: &u64) {
        let map = unsafe {
            &mut *self.map.get()
        };
        map.remove(key);
    }

    #[inline]
    pub fn map(&self) -> &mut std::collections::HashMap<u64, u64, S> {
        unsafe {
            &mut *self.map.get()
        }
    }
}

unsafe impl<S: BuildHasher + Send + Sync + 'static> Send for FlurryBucket<S> {}
unsafe impl<S: BuildHasher + Send + Sync + 'static> Sync for FlurryBucket<S> {}


#[derive(Debug)]
pub(crate) struct FlurryExpirationMap<S = RandomState> {
    buckets: HashMap<i64, FlurryBucket<S>, S>,
    hasher: S,
}

impl Default for FlurryExpirationMap {
    fn default() -> Self {
        let hasher = RandomState::default();
        Self {
            buckets: HashMap::with_hasher(hasher.clone()),
            hasher,
        }
    }
}

impl FlurryExpirationMap {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<S: BuildHasher + Send + Sync + Clone + 'static> FlurryExpirationMap<S> {
    pub(crate) fn with_hasher(hasher: S) -> FlurryExpirationMap<S> {
        FlurryExpirationMap {
            buckets: HashMap::with_hasher(hasher.clone()),
            hasher,
        }
    }

    pub fn insert(&self, key: u64, conflict: u64, expiration: Time, g: &Guard) {
        // Items that don't expire don't need to be in the expiration map.
        if expiration.is_zero() {
            return;
        }

        let bucket_num = storage_bucket(expiration);
        match self.buckets.get(&bucket_num, g) {
            None => {
                let bucket = FlurryBucket::with_hasher(self.hasher.clone());
                bucket.insert(key, conflict);
                self.buckets.insert(bucket_num, bucket, g);
            }
            Some(bucket) => {
                bucket.insert(key, conflict)
            }
        }
    }

    pub fn update(&self, key: u64, conflict: u64, old_exp_time: Time, new_exp_time: Time, g: &Guard) {
        if old_exp_time.is_zero() && new_exp_time.is_zero() {
            return;
        }

        let (old_bucket_num, new_bucket_num) =
            (storage_bucket(old_exp_time), storage_bucket(new_exp_time));

        if old_bucket_num == new_bucket_num {
            return;
        }

        self.buckets.remove(&old_bucket_num, g);
        match self.buckets.get(&new_bucket_num, g) {
            None => {
                let bucket = FlurryBucket::with_hasher(self.hasher.clone());
                bucket.insert(key, conflict);
                self.buckets.insert(new_bucket_num, bucket, g);
            }
            Some(bucket) => {
                bucket.insert(key, conflict);
            }
        }
    }

    pub fn remove<'g>(&self, key: &u64, expiration: Time, g: &'g Guard) {
        let bucket_num = storage_bucket(expiration);
        if let Some(bucket) = self.buckets.get(&bucket_num, g) {
            bucket.remove(key);
        };
    }

    pub fn cleanup<'g>(&'g self, now: Time, g: &'g Guard) -> Option<FlurryBucketRef<'g, S>> {
        let bucket_num = cleanup_bucket(now);
        self.buckets.remove(&bucket_num, g).map(|item| FlurryBucketRef {
            _g: g,
            bucket: item,
        })
    }

    pub fn hasher(&self) -> S {
        self.hasher.clone()
    }
}

unsafe impl<S: BuildHasher + Send + Sync + Clone + 'static> Send for FlurryExpirationMap<S> {}
unsafe impl<S: BuildHasher + Send + Sync + Clone + 'static> Sync for FlurryExpirationMap<S> {}
