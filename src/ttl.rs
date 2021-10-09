use parking_lot::RwLock;
use std::cell::RefCell;
use std::collections::{hash_map::RandomState, HashMap};
use std::hash::BuildHasher;
use std::ops::{Deref, DerefMut};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
        self.created_at
            .elapsed()
            .map_or(false, |d| if d >= self.d { true } else { false })
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

impl Bucket {
    fn new() -> Self {
        Self::default()
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
pub(crate) struct Buckets<S = RandomState>(HashMap<i64, Bucket<S>, S>);

impl<S: BuildHasher> Buckets<S> {
    fn new(hasher: S) -> Self {
        Self(HashMap::with_hasher(hasher))
    }
}

unsafe impl<S: BuildHasher> Send for Buckets<S> {}
unsafe impl<S: BuildHasher> Sync for Buckets<S> {}

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
    pub fn hasher(&self) -> &S {
        &self.hasher
    }

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
}

unsafe impl<S: BuildHasher + Clone + 'static> Send for ExpirationMap<S> {}

unsafe impl<S: BuildHasher + Clone + 'static> Sync for ExpirationMap<S> {}
