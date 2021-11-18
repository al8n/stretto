mod flurry_expiration_map;
pub(crate) use flurry_expiration_map::FlurryExpirationMap;

mod parking_lot_expiration_map;
pub(crate) use parking_lot_expiration_map::ExpirationMap;
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

