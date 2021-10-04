use parking_lot::RwLock;
use std::cell::RefCell;
use std::collections::{hash_map::RandomState, HashMap};
use std::hash::BuildHasher;
use std::ops::{Deref, DerefMut};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// (1969*365 + 1969/4 - 1969/100 + 1969/400) * (60 * 60 * 24)
const UNIX_TO_INTERNAL: u64 = 62135596800;

const BUCKET_DURATION_SECS: i64 = 5;

fn storage_bucket<const N: i64>(t: Time) -> i64 {
    (t.unix() / N) + 1
}

fn cleanup_bucket(t: Time) -> i64 {
    // The bucket to cleanup is always behind the storage bucket by one so that
    // no elements in that bucket (which might not have expired yet) are deleted.
    storage_bucket::<BUCKET_DURATION_SECS>(t) - 1
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
            created_at: SystemTime::now()
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

    pub fn unix(&self) -> i64 {
        (self
            .created_at
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()) as i64
    }

    pub fn is_expired(&self) -> bool {
        self.created_at
            .elapsed()
            .map_or(false, |d| if d >= self.d { true } else { false })
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
pub(crate) struct ExpirationMap<BS = RandomState, S = RandomState> {
    buckets: RwLock<RefCell<HashMap<i64, Bucket<BS>, S>>>,
}

impl Default for ExpirationMap {
    fn default() -> Self {
        Self {
            buckets: RwLock::new(RefCell::new(HashMap::new())),
        }
    }
}

impl ExpirationMap {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<BS: BuildHasher + Default, S: BuildHasher> ExpirationMap<BS, S> {
    // fn with_hasher(hasher: S) -> ExpirationMap<BS, S> {
    //     ExpirationMap {
    //         buckets: RwLock::new(RefCell::new(HashMap::<i64, Bucket<BS>>::with_hasher(hasher))),
    //     }
    // }

    pub fn insert(&self, key: u64, conflict: u64, expiration: Time) {
        // Items that don't expire don't need to be in the expiration map.
        if expiration.is_zero() {
            return;
        }

        let bucket_num = storage_bucket::<BUCKET_DURATION_SECS>(expiration);

        let m = self.buckets.read();

        let mut m = m.borrow_mut();
        match m.get_mut(&bucket_num) {
            None => {
                let mut bucket = Bucket::with_hasher(BS::default());
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
        let (old_bucket_num, new_bucket_num) = (
            storage_bucket::<BUCKET_DURATION_SECS>(old_exp_time),
            storage_bucket::<BUCKET_DURATION_SECS>(new_exp_time),
        );

        if old_bucket_num == new_bucket_num {
            return;
        }

        let mut m = m.borrow_mut();
        m.remove(&old_bucket_num);

        match m.get_mut(&new_bucket_num) {
            None => {
                let mut bucket = Bucket::with_hasher(BS::default());
                bucket.map.insert(key, conflict);
                m.insert(new_bucket_num, bucket);
            }
            Some(bucket) => {
                bucket.map.insert(key, conflict);
            }
        }
    }

    pub fn remove(&self, key: u64, expiration: Time) {
        let bucket_num = storage_bucket::<BUCKET_DURATION_SECS>(expiration);
        let m = self.buckets.read();
        if let Some(bucket) = m.borrow_mut().get_mut(&bucket_num) {
            bucket.remove(&key);
        };
    }
}

unsafe impl<BS: BuildHasher, S: BuildHasher> Send for ExpirationMap<BS, S> {}

unsafe impl<BS: BuildHasher, S: BuildHasher> Sync
for ExpirationMap<BS, S> {}
