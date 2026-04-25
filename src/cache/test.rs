use crate::{CacheCallback, Coster, Item as CrateItem, KeyBuilder, TransparentHasher};
use parking_lot::Mutex;
use rand::RngExt;
use std::{
  collections::HashSet,
  hash::Hasher,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

static CHARSET: &[u8] = "abcdefghijklmnopqrstuvwxyz0123456789".as_bytes();

fn get_key() -> [u8; 2] {
  let mut rng = rand::rng();
  let k1 = CHARSET[rng.random::<u64>() as usize % CHARSET.len()];
  let k2 = CHARSET[rng.random::<u64>() as usize % CHARSET.len()];
  [k1, k2]
}

struct TestCallback {
  evicted: Arc<Mutex<HashSet<u64>>>,
}

impl TestCallback {
  fn new(map: Arc<Mutex<HashSet<u64>>>) -> Self {
    Self { evicted: map }
  }
}

struct KHTest {
  ctr: Arc<AtomicU64>,
}

impl KeyBuilder for KHTest {
  type Key = u64;

  fn hash_index<Q>(&self, key: &Q) -> u64
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    let mut hasher = TransparentHasher { data: 0 };
    key.hash(&mut hasher);
    hasher.finish()
  }

  fn hash_conflict<Q>(&self, _key: &Q) -> u64
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    0
  }

  fn build_key<Q>(&self, k: &Q) -> (u64, u64)
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    self.ctr.fetch_add(1, Ordering::SeqCst);
    (self.hash_index(k), self.hash_conflict(k))
  }
}

#[derive(Default)]
struct TestCoster {}

impl Coster for TestCoster {
  fn cost(&self, val: &u64) -> i64 {
    *val as i64
  }

  type Value = u64;
}

impl CacheCallback for TestCallback {
  fn on_exit(&self, _val: Option<u64>) {}

  fn on_evict(&self, item: CrateItem<u64>) {
    let mut evicted = self.evicted.lock();
    evicted.insert(item.index);
    self.on_exit(item.val)
  }

  type Value = u64;
}

struct TestCallbackDropUpdates {
  set: Arc<Mutex<HashSet<u64>>>,
}

impl CacheCallback for TestCallbackDropUpdates {
  fn on_exit(&self, _val: Option<String>) {}

  fn on_evict(&self, item: CrateItem<String>) {
    let last_evicted_insert = item.val.unwrap();

    assert!(
      !self
        .set
        .lock()
        .contains(&last_evicted_insert.parse().unwrap()),
      "val = {} was dropped but it got evicted. Dropped items: {:?}",
      last_evicted_insert,
      self.set.lock().iter().copied().collect::<Vec<u64>>()
    );
  }

  type Value = String;
}

#[cfg(feature = "sync")]
mod sync_test {
  use super::*;
  use crate::{
    Cache, CacheBuilder, DefaultCacheCallback, DefaultCoster, DefaultKeyBuilder,
    DefaultUpdateValidator, TransparentKeyBuilder, UpdateValidator, cache::sync::Item,
  };
  use crossbeam_channel::{bounded, select};
  use std::{
    collections::hash_map::RandomState,
    hash::Hash,
    thread::{sleep, spawn},
    time::Duration,
  };

  fn new_test_cache<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<Key = K>>(
    kh: KH,
  ) -> Cache<K, V, KH> {
    Cache::new_with_key_builder(100, 10, kh).unwrap()
  }

  fn retry_set<
    C: Coster<Value = u64>,
    U: UpdateValidator<Value = u64>,
    CB: CacheCallback<Value = u64>,
  >(
    c: &Cache<u64, u64, TransparentKeyBuilder<u64>, C, U, CB>,
    key: u64,
    val: u64,
    cost: i64,
    ttl: Duration,
  ) {
    loop {
      let insert = c.insert_with_ttl(key, val, cost, ttl);
      if !insert {
        sleep(Duration::from_millis(100));
        continue;
      }
      sleep(Duration::from_millis(100));
      assert_eq!(c.get(&key).unwrap().read(), val);
      return;
    }
  }

  #[test]
  fn test_cache_builder() {
    let _: Cache<u64, u64, DefaultKeyBuilder<u64>> =
      CacheBuilder::new_with_key_builder(100, 10, TransparentKeyBuilder::default())
        .set_coster(DefaultCoster::default())
        .set_update_validator(DefaultUpdateValidator::default())
        .set_callback(DefaultCacheCallback::default())
        .set_num_counters(200)
        .set_max_cost(100)
        .set_cleanup_duration(Duration::from_secs(1))
        .set_buffer_size(1000)
        .set_key_builder(DefaultKeyBuilder::default())
        .set_hasher(RandomState::default())
        .finalize()
        .unwrap();
  }

  #[test]
  fn test_cache_key_to_hash() {
    let ctr = Arc::new(AtomicU64::new(0));

    let c: Cache<u64, u64, KHTest> =
      Cache::new_with_key_builder(10, 1000, KHTest { ctr: ctr.clone() }).unwrap();

    assert!(c.insert(1, 1, 1));
    sleep(Duration::from_millis(10));

    loop {
      match c.get(&1) {
        None => continue,
        Some(val) => {
          assert_eq!(val.read(), 1);
          c.remove(&1);
          assert_eq!(3, ctr.load(Ordering::SeqCst));
          break;
        }
      }
    }
  }

  #[test]
  fn test_cache_max_cost() {
    let c = Arc::new(
      Cache::builder(12960, 1e6 as i64)
        .set_metrics(true)
        .finalize()
        .unwrap(),
    );

    let (stop_tx, stop_rx) = bounded::<()>(8);

    for _ in 0..8 {
      let rx = stop_rx.clone();
      let tc = c.clone();

      spawn(move || {
        loop {
          select! {
              recv(rx) -> _ => return,
              default => {
                  let k = get_key();
                  if tc.get(&k).is_none() {
                      let rv = rand::random::<u64>() as usize % 100;

                      let val = if rv < 10 {
                          "test".to_string()
                      } else {
                          vec!["a"; 1000].join("")
                      };
                      let cost = val.len() + 2;
                      tc.insert(get_key(), val, cost as i64);
                  }
              }
          }
        }
      });
    }

    for _ in 0..20 {
      sleep(Duration::from_millis(180));
      let (cost_added, cost_evicted) = (
        c.0.metrics.cost_added().unwrap(),
        c.0.metrics.cost_evicted().unwrap(),
      );
      let cost = cost_added - cost_evicted;
      eprintln!("{}", c.0.metrics);
      assert!(cost as f64 <= (1e6 * 1.05));
    }
    c.wait().unwrap();
    for _ in 0..8 {
      let _ = stop_tx.send(());
    }
  }

  #[test]
  fn test_cache_update_max_cost() {
    let c = Cache::builder(10, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(false)
      .finalize()
      .unwrap();

    assert_eq!(c.max_cost(), 10);
    assert!(c.insert(1, 1, 1));

    sleep(Duration::from_secs(1));
    // Set is rejected because the cost of the entry is too high
    // when accounting for the internal cost of storing the entry.
    assert!(c.get(&1).is_none());

    // Update the max cost of the cache and retry.
    c.update_max_cost(1000);
    assert_eq!(c.max_cost(), 1000);
    assert!(c.insert(1, 1, 1));

    sleep(Duration::from_millis(200));
    assert_eq!(c.get(&1).unwrap().read(), 1);
    c.remove(&1);
  }

  #[test]
  fn test_cache_drop_is_safe() {
    let c: Cache<i64, i64, TransparentKeyBuilder<i64>> =
      Cache::new_with_key_builder(100, 10, TransparentKeyBuilder::default()).unwrap();
    drop(c);
  }

  #[test]
  fn test_cache_process_items() {
    let cb = Arc::new(Mutex::new(HashSet::new()));
    let c = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_coster(TestCoster::default())
      .set_callback(TestCallback::new(cb.clone()))
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    assert!(c.insert(1, 1, 0));

    sleep(Duration::from_secs(1));
    assert!(c.0.policy.contains(&1));
    assert_eq!(c.0.policy.cost(&1), 1);

    let _ = c.insert_if_present(1, 2, 0);
    sleep(Duration::from_secs(1));
    assert_eq!(c.0.policy.cost(&1), 2);

    c.remove(&1);
    sleep(Duration::from_secs(1));
    assert!(c.0.store.get(&1, 0).is_none());
    assert!(!c.0.policy.contains(&1));

    c.insert(2, 2, 3);
    c.insert(3, 3, 3);
    c.insert(4, 3, 3);
    c.insert(5, 3, 5);
    sleep(Duration::from_secs(1));
    assert_ne!(cb.lock().len(), 0);
  }

  #[test]
  fn test_cache_get() {
    let c = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .set_metrics(true)
      .finalize()
      .unwrap();

    c.insert(1, 1, 0);
    sleep(Duration::from_secs(1));
    match c.get_mut(&1) {
      None => {}
      Some(mut val) => {
        val.write(10);
      }
    }

    assert!(c.get_mut(&2).is_none());

    // 0.5 and not 1.0 because we tried Getting each item twice
    assert_eq!(c.0.metrics.ratio().unwrap(), 0.5);

    assert_eq!(c.get_mut(&1).unwrap().read(), 10);
  }

  #[test]
  fn test_cache_set() {
    let c = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .set_metrics(true)
      .finalize()
      .unwrap();

    retry_set(&c, 1, 1, 1, Duration::ZERO);

    c.insert(1, 2, 2);
    assert_eq!(c.get(&1).unwrap().read(), 2);

    // Simulate the end-state of processor shutdown: the insert
    // semaphore has been closed by the processor's RAII drop-guard.
    // Subsequent inserts must fail at the acquire step and record a
    // dropped set. In production this is driven by `Cache::drop`
    // disconnecting `stop_tx`; driving it directly here avoids a
    // dependency on scheduling between the cache and policy processors
    // (they share `stop_rx`, so a single `send(())` only wakes one).
    c.0.insert_sem.close();

    assert!(!c.insert(2, 2, 1));
    assert_eq!(c.0.metrics.sets_dropped().unwrap(), 1);
  }

  #[test]
  fn test_cache_internal_cost() {
    let c = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(false)
      .set_metrics(true)
      .finalize()
      .unwrap();

    // Get should return None because the cache's cost is too small to store the item
    // when accounting for the internal cost.
    c.insert_with_ttl(1, 1, 1, Duration::ZERO);
    sleep(Duration::from_millis(100));
    assert!(c.get(&1).is_none())
  }

  #[test]
  fn test_recache_with_ttl() {
    let c = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .set_metrics(true)
      .finalize()
      .unwrap();

    // Set initial value for key = 1
    assert!(c.insert_with_ttl(1, 1, 1, Duration::from_secs(5)));

    sleep(Duration::from_secs(2));

    // Get value from cache for key = 1
    assert_eq!(c.get(&1).unwrap().read(), 1);

    // wait for expiration
    sleep(Duration::from_secs(5));

    // The cached value for key = 1 should be gone
    assert!(c.get(&1).is_none());

    // set new value for key = 1
    assert!(c.insert_with_ttl(1, 2, 1, Duration::from_secs(5)));

    sleep(Duration::from_secs(2));
    // get value from cache for key = 1;
    assert_eq!(c.get(&1).unwrap().read(), 2);
  }

  #[test]
  fn test_cache_set_with_ttl() {
    let cb = Arc::new(Mutex::new(HashSet::new()));
    let c = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_callback(TestCallback::new(cb.clone()))
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    retry_set(&c, 1, 1, 1, Duration::from_secs(1));

    // Sleep to make sure the item has expired after execution resumes.
    sleep(Duration::from_secs(2));
    assert!(c.get(&1).is_none());

    // Sleep to ensure that the bucket where the item was stored has been cleared
    // from the expiration map.
    sleep(Duration::from_secs(5));
    assert_eq!(cb.lock().len(), 1);

    // Verify that expiration times are overwritten.
    retry_set(&c, 2, 1, 1, Duration::from_secs(1));
    retry_set(&c, 2, 2, 1, Duration::from_secs(100));
    sleep(Duration::from_secs(3));
    assert_eq!(c.get(&2).unwrap().read(), 2);

    // Verify that entries with no expiration are overwritten.
    retry_set(&c, 3, 1, 1, Duration::ZERO);
    retry_set(&c, 3, 1, 1, Duration::from_secs(1));
    sleep(Duration::from_secs(3));
    assert!(c.get(&3).is_none());
  }

  #[test]
  fn test_cache_remove() {
    let c = new_test_cache(TransparentKeyBuilder::default());

    c.insert(1, 1, 1);
    c.remove(&1);

    // The deletes and sets are pushed through the setbuf. It might be possible
    // that the delete is not processed before the following get is called. So
    // wait for a millisecond for things to be processed.
    sleep(Duration::from_millis(1));
    assert!(c.get(&1).is_none());
  }

  #[test]
  fn test_cache_remove_with_ttl() {
    let c = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    retry_set(&c, 3, 1, 1, Duration::from_secs(10));
    sleep(Duration::from_secs(1));

    // remove the item
    c.remove(&3);

    // ensure the key is deleted
    assert!(c.get(&3).is_none());
  }

  #[test]
  fn test_cache_get_ttl() {
    let c = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_metrics(true)
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    // try expiration with valid ttl item
    {
      let expiration = Duration::from_secs(5);
      retry_set(&c, 1, 1, 1, expiration);

      assert_eq!(c.get(&1).unwrap().read(), 1);
      assert!(c.get_ttl(&1).unwrap() < expiration);

      c.remove(&1);

      assert!(c.get_ttl(&1).is_none());
    }

    // try expiration with no ttl
    {
      retry_set(&c, 2, 2, 1, Duration::ZERO);
      assert_eq!(c.get(&2).unwrap().read(), 2);
      assert_eq!(c.get_ttl(&2).unwrap(), Duration::MAX);
    }

    // try expiration with missing item
    {
      assert!(c.get_ttl(&3).is_none());
    }

    // try expiration with expired item
    {
      let expiration = Duration::from_secs(1);
      retry_set(&c, 3, 3, 1, expiration);

      assert_eq!(c.get(&3).unwrap().read(), 3);
      sleep(Duration::from_secs(1));
      assert!(c.get_ttl(&3).is_none());
    }
  }

  #[test]
  fn test_cache_blockon_clear() {
    let c: Arc<Cache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
      Cache::builder(100, 10)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap(),
    );

    let (stop_tx, stop_rx) = bounded(1);

    let tc = c.clone();
    spawn(move || {
      for _ in 0..10 {
        let tc = tc.clone();
        tc.wait().unwrap();
      }
      stop_tx.send(()).unwrap();
    });

    for _ in 0..10 {
      c.clear().unwrap();
    }

    select! {
        recv(stop_rx) -> _  => {},
        default(Duration::from_secs(1)) => {
            panic!("timed out while waiting on cache")
        }
    }
  }

  #[test]
  fn test_cache_clear() {
    let c = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_metrics(true)
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    (0..10).for_each(|i| {
      c.insert(i, i, 1);
    });
    sleep(Duration::from_millis(100));
    assert_eq!(c.0.metrics.keys_added(), Some(10));
    c.clear().unwrap();
    assert_eq!(c.0.metrics.keys_added(), Some(0));

    (0..10).for_each(|i| {
      assert!(c.get(&i).is_none());
    })
  }

  #[test]
  fn test_cache_metrics_clear() {
    let c = Arc::new(
      Cache::builder(100, 10)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_metrics(true)
        .finalize()
        .unwrap(),
    );

    c.insert(1, 1, 1);

    let (stop_tx, stop_rx) = bounded(0);
    let tc = c.clone();
    spawn(move || {
      loop {
        select! {
            recv(stop_rx) -> _ => return,
            default => {
                tc.get(&1);
            }
        }
      }
    });

    sleep(Duration::from_millis(100));
    let _ = c.clear();
    stop_tx.send(()).unwrap();
    c.0.metrics.clear();
  }

  // Regression test for bug https://github.com/dgraph-io/ristretto/issues/167
  #[test]
  fn test_cache_drop_updates() {
    fn test() {
      let set = Arc::new(Mutex::new(HashSet::new()));
      let c = Cache::builder(100, 10)
                .set_callback(TestCallbackDropUpdates { set: set.clone() })
                .set_metrics(true)
                // This is important. The race condition shows up only when the insert buf
                // is full and that's why we reduce the buf size here. The test will
                // try to fill up the insert buf to it's capacity and then perform an
                // update on a key.
                .set_buffer_size(10)
                .finalize()
                .unwrap();

      for i in 0..50 {
        let v = format!("{:0100}", i);
        // We're updating the same key.
        if !c.insert(0, v, 1) {
          // The race condition doesn't show up without this sleep.
          sleep(Duration::from_millis(1));
          set.lock().insert(i);
        }
      }

      // Wait for all the items to be processed.
      sleep(Duration::from_millis(5));
      // This will cause eviction from the cache.
      assert!(c.insert(1, "0".to_string(), 10));
    }

    // Run the test 100 times since it's not reliable.
    (0..100).for_each(|_| test())
  }

  #[test]
  fn test_cache_with_ttl() {
    let mut process_win = 0;
    let mut clean_win = 0;

    for _ in 0..10 {
      let c = Cache::builder(100, 1000)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_metrics(true)
        .finalize()
        .unwrap();

      // Set initial value for key = 1
      assert!(c.insert_with_ttl(1, 1, 0, Duration::from_millis(800)));

      sleep(Duration::from_millis(100));

      // Get value from cache for key = 1
      match c.get(&1) {
        None => {
          clean_win += 1;
        }
        Some(_) => {
          process_win += 1;
        }
      }
      // assert_eq!(c.get(&1).unwrap().read(), 1);

      sleep(Duration::from_millis(1200));
      assert!(c.get(&1).is_none());
    }
    eprintln!("process: {} cleanup: {}", process_win, clean_win);
  }

  #[test]
  fn test_valueref_ttl() {
    let ttl = Duration::from_secs(1);
    let c = Cache::builder(100, 1000)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_metrics(true)
      .finalize()
      .unwrap();
    c.try_insert_with_ttl(1, 1, 1, ttl).unwrap();
    c.wait().unwrap();
    let val = c.get(&1).unwrap();
    assert!(val.ttl() > Duration::from_millis(900));
  }

  #[test]
  fn test_sync_finalize_errors() {
    let err = match CacheBuilder::<u64, u64>::new(0, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .finalize()
    {
      Ok(_) => panic!("expected error"),
      Err(e) => e,
    };
    assert!(matches!(err, crate::CacheError::InvalidNumCounters));

    let err = match CacheBuilder::<u64, u64>::new(10, 0)
      .set_key_builder(TransparentKeyBuilder::default())
      .finalize()
    {
      Ok(_) => panic!("expected error"),
      Err(e) => e,
    };
    assert!(matches!(err, crate::CacheError::InvalidMaxCost));

    let err = match CacheBuilder::<u64, u64>::new(10, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_buffer_size(0)
      .finalize()
    {
      Ok(_) => panic!("expected error"),
      Err(e) => e,
    };
    assert!(matches!(err, crate::CacheError::InvalidBufferSize));
  }

  #[test]
  fn test_sync_set_buffer_items_and_len() {
    let c: Cache<u64, u64, TransparentKeyBuilder<u64>> = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_buffer_items(32)
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    assert!(c.is_empty());
    assert_eq!(c.len(), 0);

    assert!(c.insert(1, 1, 1));
    c.wait().unwrap();
    assert_eq!(c.len(), 1);
    assert!(!c.is_empty());
  }

  #[test]
  fn test_sync_as_ref() {
    let c: Cache<u64, u64, TransparentKeyBuilder<u64>> = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();
    let r: &Cache<u64, u64, TransparentKeyBuilder<u64>> = c.as_ref();
    assert_eq!(r.max_cost(), 10);
  }

  #[test]
  fn test_sync_insert_if_present_missing() {
    let c: Cache<u64, u64, TransparentKeyBuilder<u64>> = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();
    // Key does not exist — insert_if_present should return false and not add the key.
    assert!(!c.insert_if_present(42, 0, 1));
    c.wait().unwrap();
    assert!(c.get(&42).is_none());
  }

  #[test]
  fn test_sync_ring_overflow_and_fill() {
    let c: Cache<u64, u64, TransparentKeyBuilder<u64>> = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_buffer_items(4)
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    // Fire enough gets that the ring buffer fills and flushes multiple times,
    // and enough bursts that some batches may be dropped by the policy (bounded tx).
    for _ in 0..200 {
      for k in 0..16u64 {
        let _ = c.get(&k);
      }
    }
  }

  #[test]
  fn test_sync_drop_shuts_down_cleanly() {
    // Dropping the cache must block until the processor has finished, so that
    // no callbacks fire after drop returns. This is the user-facing contract
    // that replaces the old explicit close() API.
    let c = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_metrics(true)
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    for i in 0..10u64 {
      assert!(c.insert(i, i, 1));
    }
    c.wait().unwrap();
    assert_eq!(c.len(), 10);

    drop(c);
  }

  #[test]
  fn test_sync_clear_then_insert_preserves_writes() {
    // Regression: clear() used to signal the processor via a separate channel,
    // so inserts made on the same thread immediately after clear() returned
    // could be drained away by the clear handler before the processor
    // admitted them. The ordered-clear marker routes clear through the insert
    // buffer, which guarantees inserts enqueued after clear() returns are
    // processed against the freshly cleared state and survive.
    for _ in 0..32 {
      let c: Cache<u64, u64, TransparentKeyBuilder<u64>> = Cache::builder(100, 100)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap();
      for i in 0..10u64 {
        c.insert(i, i, 1);
      }
      c.wait().unwrap();

      c.clear().unwrap();
      // No intervening wait() — this is the exact race the fix closes.
      for i in 100..110u64 {
        c.insert(i, i, 1);
      }
      c.wait().unwrap();

      for i in 0..10u64 {
        assert!(c.get(&i).is_none(), "pre-clear key {} survived clear", i);
      }
      for i in 100..110u64 {
        let v = c
          .get(&i)
          .unwrap_or_else(|| panic!("post-clear insert of {} was lost", i));
        assert_eq!(v.read(), i);
      }
    }
  }

  // Regression: `clear()` used to wipe the store silently, violating the
  // public `CacheCallback::on_exit` contract ("called whenever a value is
  // removed from the cache"). Users relying on `on_exit` to release
  // resources (file handles, refcounted external state) would leak on
  // every clear. The fix drains values through `clear_with` and fires
  // `on_exit` for each.
  #[test]
  fn test_sync_clear_fires_on_exit() {
    use std::sync::atomic::{AtomicU64, Ordering as AOrd};

    struct CountingCB {
      on_exit: Arc<AtomicU64>,
    }
    impl CacheCallback for CountingCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {
        self.on_exit.fetch_add(1, AOrd::Relaxed);
      }
    }

    let on_exit_count = Arc::new(AtomicU64::new(0));
    let c: Cache<
      u64,
      u64,
      TransparentKeyBuilder<u64>,
      DefaultCoster<u64>,
      DefaultUpdateValidator<u64>,
      CountingCB,
    > = CacheBuilder::new_with_key_builder(1000, 1000, TransparentKeyBuilder::default())
      .set_callback(CountingCB {
        on_exit: on_exit_count.clone(),
      })
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    for i in 0..50u64 {
      assert!(c.insert(i, i * 10, 1));
    }
    c.wait().unwrap();
    assert_eq!(c.len(), 50);
    // Baseline: no on_exit fires from plain inserts.
    assert_eq!(on_exit_count.load(AOrd::Relaxed), 0);

    c.clear().unwrap();

    assert_eq!(
      on_exit_count.load(AOrd::Relaxed),
      50,
      "on_exit must fire once per live entry drained by clear()",
    );
    assert_eq!(c.len(), 0);
  }

  // Regression: `ShardedMap::clear_with` used to fire `on_exit` from inside
  // `shard.write()` / `map.drain()`, so a callback that re-entered the cache
  // on a path that takes a shard lock (e.g. `get`, `len`, `contains`) would
  // self-deadlock on the same processor thread — parking_lot RwLocks are
  // not reentrant. The fix drains every shard into a buffer, drops all
  // write locks, and only then runs the callbacks. This test exercises that
  // exact re-entry (callback calls `cache.len()` and `cache.get()`) — under
  // the old code the test would hang until the harness timeout.
  #[test]
  fn test_sync_clear_on_exit_can_reenter_cache() {
    use std::sync::{
      OnceLock,
      atomic::{AtomicU64, Ordering as AOrd},
    };

    type C = Cache<
      u64,
      u64,
      TransparentKeyBuilder<u64>,
      DefaultCoster<u64>,
      DefaultUpdateValidator<u64>,
      ReentrantCB,
    >;

    struct ReentrantCB {
      cache: Arc<OnceLock<C>>,
      reentries: Arc<AtomicU64>,
    }
    impl CacheCallback for ReentrantCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {
        if let Some(c) = self.cache.get() {
          // Both take shard locks; would deadlock under the pre-fix
          // `clear_with` while its write lock was still held.
          let _ = c.len();
          let _ = c.get(&999_999u64);
          self.reentries.fetch_add(1, AOrd::Relaxed);
        }
      }
    }

    let cache_once: Arc<OnceLock<C>> = Arc::new(OnceLock::new());
    let reentries = Arc::new(AtomicU64::new(0));

    let c: C = CacheBuilder::new_with_key_builder(200, 200, TransparentKeyBuilder::default())
      .set_callback(ReentrantCB {
        cache: cache_once.clone(),
        reentries: reentries.clone(),
      })
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    // Plumb the cache handle into the callback AFTER construction so the
    // callback can re-enter. This creates a reference cycle (cache keeps the
    // OnceLock alive, OnceLock keeps a Cache clone alive) — acceptable for a
    // regression test; the process exits cleanly.
    cache_once.set(c.clone()).ok().expect("set once");

    for i in 0..20u64 {
      assert!(c.insert(i, i, 1));
    }
    c.wait().unwrap();
    assert_eq!(c.len(), 20);

    // If the fix is reverted, this call hangs forever: on_exit's `c.len()`
    // tries to take a shard read lock the processor thread already holds
    // for write.
    c.clear().unwrap();

    assert_eq!(
      reentries.load(AOrd::Relaxed),
      20,
      "on_exit must have observed every drained entry via the re-entrant path",
    );
  }

  // Regression: the Item::Clear handler used to fire `on_exit` BEFORE bumping
  // `clear_generation`. An `on_exit` that re-entered the cache via `insert`
  // captured the pre-bump generation, did its eager store write, and enqueued
  // an `Item::New` stamped with that (soon-to-be-stale) generation. Once the
  // handler finished firing callbacks and bumped the generation, the queued
  // `Item::New` was treated as stale by the New handler and its store row
  // was reaped — the user's insert silently vanished. The fix runs callbacks
  // AFTER `policy.clear()` / `metrics.clear()` / `start_ts.clear()` and AFTER
  // the generation bump, so re-entrant inserts capture the fresh generation
  // and are admitted normally.
  #[test]
  fn test_sync_clear_on_exit_reentrant_insert_survives() {
    use std::sync::{
      OnceLock,
      atomic::{AtomicU64, Ordering as AOrd},
    };

    type C = Cache<
      u64,
      u64,
      TransparentKeyBuilder<u64>,
      DefaultCoster<u64>,
      DefaultUpdateValidator<u64>,
      InsertingCB,
    >;

    struct InsertingCB {
      cache: Arc<OnceLock<C>>,
      inserts_attempted: Arc<AtomicU64>,
      offset: u64,
    }
    impl CacheCallback for InsertingCB {
      type Value = u64;
      fn on_exit(&self, v: Option<u64>) {
        if let (Some(v), Some(c)) = (v, self.cache.get()) {
          // Re-insert under a post-clear key so we can distinguish these
          // from the pre-clear values when probing afterwards.
          let k = self.offset + v;
          let _ = c.insert(k, v, 1);
          self.inserts_attempted.fetch_add(1, AOrd::Relaxed);
        }
      }
    }

    const PRE_CLEAR_COUNT: u64 = 20;
    const POST_CLEAR_OFFSET: u64 = 1_000_000;

    let cache_once: Arc<OnceLock<C>> = Arc::new(OnceLock::new());
    let inserts_attempted = Arc::new(AtomicU64::new(0));

    let c: C = CacheBuilder::new_with_key_builder(1000, 1000, TransparentKeyBuilder::default())
      .set_callback(InsertingCB {
        cache: cache_once.clone(),
        inserts_attempted: inserts_attempted.clone(),
        offset: POST_CLEAR_OFFSET,
      })
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    cache_once.set(c.clone()).ok().expect("set once");

    for i in 0..PRE_CLEAR_COUNT {
      // Use v = i + 1 so the re-insert key `offset + v` is unique per entry.
      assert!(c.insert(i, i + 1, 1));
    }
    c.wait().unwrap();
    assert_eq!(c.len(), PRE_CLEAR_COUNT as usize);

    c.clear().unwrap();
    c.wait().unwrap();

    assert_eq!(
      inserts_attempted.load(AOrd::Relaxed),
      PRE_CLEAR_COUNT,
      "on_exit should fire once per drained entry",
    );

    // The re-entrant inserts must have been admitted — before the fix they
    // were enqueued with a stale generation and reaped as ghosts.
    let mut survived = 0u64;
    for i in 0..PRE_CLEAR_COUNT {
      let k = POST_CLEAR_OFFSET + (i + 1);
      if let Some(v) = c.get(&k) {
        assert_eq!(*v.value(), i + 1);
        survived += 1;
      }
    }
    assert_eq!(
      survived, PRE_CLEAR_COUNT,
      "every on_exit re-entrant insert must survive the clear",
    );
  }

  // Regression: if an `on_exit` callback panics during `Item::Clear`, the
  // processor used to unwind past the bare `wg.done()` call, leaving the
  // WaitGroup counter stuck at 1 and hanging the caller parked on
  // `wg.wait()`. `wg::WaitGroup::done` takes `&self` and has no Drop-based
  // signaling, so nothing would ever wake the caller. The fix wraps the
  // WaitGroup in a `Signal` whose `Drop` fires `wg.done()` if the explicit
  // consume call was skipped — so the panic-unwind of the match arm still
  // unblocks `clear()` before the processor thread dies.
  #[test]
  fn test_sync_clear_unblocks_on_on_exit_panic() {
    use std::sync::{
      atomic::{AtomicBool, Ordering as AOrd},
      mpsc,
    };

    struct PanickingCB(Arc<AtomicBool>);
    impl CacheCallback for PanickingCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {
        self.0.store(true, AOrd::Relaxed);
        panic!("intentional on_exit panic for regression test");
      }
    }

    let fired = Arc::new(AtomicBool::new(false));
    let c: Cache<
      u64,
      u64,
      TransparentKeyBuilder<u64>,
      DefaultCoster<u64>,
      DefaultUpdateValidator<u64>,
      PanickingCB,
    > = CacheBuilder::new_with_key_builder(100, 100, TransparentKeyBuilder::default())
      .set_callback(PanickingCB(fired.clone()))
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    for i in 0..5u64 {
      assert!(c.insert(i, i, 1));
    }
    c.wait().unwrap();
    assert_eq!(c.len(), 5);

    // Call clear() from a worker thread with a bounded wait. If the Signal
    // drop-guard is reverted, the processor panics before firing `wg.done()`
    // and `wg.wait()` blocks forever — the recv below times out. With the
    // fix, the panic-unwind drops the Signal, `wg.done()` runs, and clear()
    // returns before the deadline.
    let (tx, rx) = mpsc::channel();
    let c2 = c.clone();
    std::thread::spawn(move || {
      // The processor thread's unwind catches inside `std::thread::spawn`'s
      // boundary, so the caller's `clear()` just sees the Signal drop fire.
      let res = c2.clear();
      let _ = tx.send(res);
    });

    let outcome = rx
      .recv_timeout(Duration::from_secs(5))
      .expect("clear() must return within 5s even after on_exit panic");
    outcome.expect("clear() should return Ok once the Signal drop-guard fires");

    assert!(
      fired.load(AOrd::Relaxed),
      "the panicking on_exit should have been reached at least once",
    );
  }

  // Regression: clear() used to wipe only the shards and leave TTL buckets
  // in the ExpirationMap behind. A post-clear reinsert of the same key —
  // with or without TTL — would then be deleted by the next cleanup tick,
  // because the stale bucket still named that (key, conflict) and
  // `try_remove` matched the fresh row's conflict for a deterministic
  // KeyBuilder. The fix clears the ExpirationMap as part of
  // `ShardedMap::clear`, and also guards the cleanup `is_expired()` check
  // with `!t.is_zero()` so a zero-TTL post-clear row is never treated as
  // expired.
  #[test]
  fn test_sync_clear_wipes_ttl_buckets() {
    let c: Cache<u64, u64, TransparentKeyBuilder<u64>> = Cache::builder(100, 100)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      // Short cleanup tick so the bucket is processed during the test.
      .set_cleanup_duration(Duration::from_millis(50))
      .finalize()
      .unwrap();

    // Seed with a short TTL so a bucket exists.
    assert!(c.insert_with_ttl(1u64, 100u64, 1, Duration::from_millis(200)));
    c.wait().unwrap();
    assert!(c.get(&1).is_some());

    // Clear before the TTL fires. Stale bucket would survive without the fix.
    c.clear().unwrap();

    // Reinsert the SAME key with NO TTL. The deterministic KeyBuilder
    // produces the same conflict, so a surviving stale bucket would match
    // on cleanup.
    assert!(c.insert(1u64, 999u64, 1));
    c.wait().unwrap();

    // Wait past the original TTL + two cleanup ticks. The stale bucket
    // would fire during this window and delete the fresh row.
    sleep(Duration::from_millis(500));

    let v = c
      .get(&1)
      .expect("post-clear zero-TTL row must survive TTL-bucket cleanup");
    assert_eq!(v.read(), 999);
  }

  // Regression: exercises the race codex flagged where one thread issues
  // clear() while other threads are mid-insert. `try_update` eagerly writes
  // the store before its Item::New lands in the insert buffer, so without
  // the generation gate a Clear marker could slip between the store write
  // and the policy admission — leaving policy to reference an entry the
  // clear handler had already erased. The fix stamps every admission
  // request with the generation observed at store-write time, and the
  // processor drops any request whose generation no longer matches.
  //
  // The assertion here is indirect but decisive: after the race phase we
  // do one more clear + batch of inserts on a single thread. If the
  // generation gate is wrong, a stale admission from the race phase can
  // still occupy policy cost and starve these final inserts, making some
  // of them unreadable.
  #[test]
  fn test_sync_concurrent_clear_and_insert_consistency() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, Ordering as AOrd},
    };

    for _ in 0..4 {
      let c: Arc<Cache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        Cache::builder(500, 5_000)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .finalize()
          .unwrap(),
      );

      let stop = Arc::new(AtomicBool::new(false));
      let mut handles = Vec::new();
      for tid in 0..4u64 {
        let c2 = c.clone();
        let stop2 = stop.clone();
        handles.push(spawn(move || {
          let base = tid * 1_000_000;
          let mut k = 0u64;
          while !stop2.load(AOrd::Relaxed) {
            c2.insert(base + k, base + k, 1);
            k = (k + 1) % 10_000;
          }
        }));
      }

      let c3 = c.clone();
      let stop3 = stop.clone();
      let clearer = spawn(move || {
        for _ in 0..20 {
          sleep(Duration::from_millis(2));
          if stop3.load(AOrd::Relaxed) {
            break;
          }
          c3.clear().unwrap();
        }
      });

      clearer.join().unwrap();
      stop.store(true, AOrd::Relaxed);
      for h in handles {
        h.join().unwrap();
      }
      c.wait().unwrap();

      // Clean slate, then a batch of fresh inserts that must all survive.
      c.clear().unwrap();
      c.wait().unwrap();
      for i in 0..50u64 {
        c.insert(i, i, 1);
      }
      c.wait().unwrap();
      for i in 0..50u64 {
        let v = c
          .get(&i)
          .unwrap_or_else(|| panic!("post-race insert {} was lost", i));
        assert_eq!(v.read(), i);
      }
    }
  }

  // Regression for the clear-generation ordering bug in the Item::Clear
  // handler. The handler must wipe store/policy/metrics/start_ts BEFORE
  // bumping `clear_generation`, not after. With the wrong ordering:
  //
  //   1. Processor bumps gen → new_gen.
  //   2. Racing caller reads new_gen, does eager `store.try_insert` (row
  //      lands in store), enqueues `Item::New{gen = new_gen}`.
  //   3. Processor runs `store.clear()` / `policy.clear()` — the eager
  //      row is erased.
  //   4. Processor later handles that `Item::New`: `captured_gen ==
  //      current_gen` passes the generation gate, so it's admitted to
  //      policy. But the store no longer holds the row → policy has a
  //      ghost admission (counted against max_cost, not retrievable).
  //
  // The fix (wipe first, bump second) ensures racing eager writes
  // capture the OLD generation; their `Item::New` is rejected at the
  // gate and removed from the store via `try_remove_if_version`. After
  // wait(), the invariant `policy.contains(k) ⇒ store.get(k).is_some()`
  // must hold for every key that was ever inserted.
  #[test]
  fn test_sync_clear_bump_vs_wipe_ordering() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, Ordering as AOrd},
    };

    const THREADS: u64 = 8;
    // Each insert uses a brand-new key (no cycling) so every worker call
    // hits the Item::New admission path — that is the path the buggy
    // ordering turns into a ghost admission. Cycling keys quickly
    // degrade to Item::Update, which has no ghost hazard.
    const INSERTS_PER_THREAD: u64 = 40_000;
    // Total ghost count accumulated across race rounds. Each round
    // fires ONE clear() with workers racing against it, then we sample
    // policy-vs-store before the next round wipes the evidence.
    let mut total_ghosts = 0usize;

    for round in 0..10u64 {
      let c: Arc<Cache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        Cache::builder(20_000, 1_000_000)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .finalize()
          .unwrap(),
      );

      let start_signal = Arc::new(AtomicBool::new(false));
      let mut handles = Vec::new();
      for tid in 0..THREADS {
        let c2 = c.clone();
        let start = start_signal.clone();
        let round_base = round * THREADS * INSERTS_PER_THREAD;
        handles.push(spawn(move || {
          while !start.load(AOrd::Acquire) {
            std::hint::spin_loop();
          }
          let base = round_base + tid * INSERTS_PER_THREAD;
          for i in 0..INSERTS_PER_THREAD {
            let k = base + i;
            c2.insert(k, k, 1);
          }
        }));
      }

      // Fire exactly ONE clear while workers are mid-spray. Workers that
      // start before the clear() returns have the best chance of hitting
      // the bump-vs-wipe window.
      let c3 = c.clone();
      let start = start_signal.clone();
      let clearer = spawn(move || {
        start.store(true, AOrd::Release);
        // Let workers ramp up and saturate the insert buffer, then clear.
        sleep(Duration::from_micros(50));
        c3.clear().unwrap();
      });

      clearer.join().unwrap();
      for h in handles {
        h.join().unwrap();
      }
      c.wait().unwrap();

      // Sample BEFORE the next round's clear wipes the evidence. For
      // TransparentKeyBuilder<u64>, index == key and conflict == 0.
      for tid in 0..THREADS {
        let base = round * THREADS * INSERTS_PER_THREAD + tid * INSERTS_PER_THREAD;
        for i in 0..INSERTS_PER_THREAD {
          let index = base + i;
          if c.0.policy.contains(&index) && c.0.store.get(&index, 0).is_none() {
            total_ghosts += 1;
          }
        }
      }
    }
    assert_eq!(
      total_ghosts, 0,
      "policy holds {} ghost admissions across 10 clear/insert races — \
       clear handler bumped clear_generation before wiping store/policy",
      total_ghosts,
    );
  }

  #[test]
  fn test_sync_reject_update_and_conflict() {
    struct NoUpdate;
    impl UpdateValidator for NoUpdate {
      type Value = u64;
      fn should_update(&self, _prev: &u64, _curr: &u64) -> bool {
        false
      }
    }

    let c = Cache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_update_validator(NoUpdate)
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    assert!(c.insert(1u64, 1u64, 1));
    c.wait().unwrap();
    // With NoUpdate validator, the second insert should be rejected as an update.
    let _ = c.insert(1u64, 2u64, 1);
    c.wait().unwrap();
    assert_eq!(c.get(&1).unwrap().read(), 1);
  }

  // Regression test for https://github.com/al8n/stretto/issues/55 — inserting
  // many items with a TTL used to leave most of them around after expiry
  // because the cleaner only drained one bucket per tick. With the default 2s
  // cleanup interval and 1-second bucket granularity, buckets that never lined
  // up with a tick were leaked forever.
  #[test]
  fn test_sync_ttl_cleanup_drains_all_buckets() {
    const N: u64 = 200;

    let c = Cache::builder(1000, 10_000)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    // Spread inserts across ~3 seconds so keys land in multiple storage
    // buckets, guaranteeing at least one bucket that a 2-second ticker skips.
    for i in 0..N {
      assert!(c.insert_with_ttl(i, i, 1, Duration::from_secs(1)));
      if i % 60 == 0 {
        sleep(Duration::from_millis(1000));
      }
    }
    c.wait().unwrap();

    // Wait well past the longest TTL plus a few cleanup ticks so every bucket
    // has had a chance to be swept.
    sleep(Duration::from_secs(8));

    let leftover: u64 = (0..N).filter(|k| c.get(k).is_some()).count() as u64;
    assert_eq!(
      leftover, 0,
      "expected all TTL entries to be cleaned up, {} remained",
      leftover
    );
    assert_eq!(c.len(), 0, "store should be empty after cleanup");
  }

  // Regression for the absent-remove ghost admission (src/cache.rs Delete
  // handler). `try_remove` used to enqueue Item::Delete unconditionally,
  // stamping version=0 when the eager remove found nothing. The handler
  // then called policy.remove() unconditionally while skipping the store
  // cleanup on version=0 — so if a concurrent insert admitted the same
  // key into policy between the remove's eager pass and the Delete's
  // handler, the Delete wiped the policy entry while leaving the store
  // row intact. Net result: a row readable from the store but invisible
  // to policy accounting (bypasses max_cost, never evictable).
  //
  // The fix skips the Delete enqueue entirely when no row was removed.
  // This test spams insert/remove on the same fresh key pool so every
  // remove races an in-flight Item::New and some subset land in the bug
  // window. After draining, policy.contains(k) must imply store has k.
  #[test]
  fn test_sync_absent_remove_vs_insert_no_ghost() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, Ordering as AOrd},
    };

    const ROUNDS: u64 = 10;
    const KEYS_PER_ROUND: u64 = 5_000;
    let mut total_ghosts = 0usize;

    for round in 0..ROUNDS {
      let c: Arc<Cache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        Cache::builder(20_000, 1_000_000)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .finalize()
          .unwrap(),
      );

      let base = round * KEYS_PER_ROUND;
      let start = Arc::new(AtomicBool::new(false));

      let c_ins = c.clone();
      let start_ins = start.clone();
      let inserter = spawn(move || {
        while !start_ins.load(AOrd::Acquire) {
          std::hint::spin_loop();
        }
        for k in base..base + KEYS_PER_ROUND {
          c_ins.insert(k, k, 1);
        }
      });

      let c_rem = c.clone();
      let start_rem = start.clone();
      let remover = spawn(move || {
        while !start_rem.load(AOrd::Acquire) {
          std::hint::spin_loop();
        }
        // Walk the same key range in parallel. Many of these removes
        // race the inserter's eager writes; the ones that fire before
        // the eager `store.try_insert` lands used to enqueue a
        // version=0 Delete and corrupt a concurrent admission.
        for k in base..base + KEYS_PER_ROUND {
          c_rem.remove(&k);
        }
      });

      start.store(true, AOrd::Release);
      inserter.join().unwrap();
      remover.join().unwrap();
      c.wait().unwrap();

      for k in base..base + KEYS_PER_ROUND {
        if c.0.policy.contains(&k) && c.0.store.get(&k, 0).is_none() {
          total_ghosts += 1;
        }
      }
    }
    assert_eq!(
      total_ghosts, 0,
      "policy holds {} ghost admissions after absent-remove vs insert races — \
       Item::Delete handler removed policy state of a concurrent admission",
      total_ghosts,
    );
  }

  // Regression for the present-remove-vs-reinsert race. Pre-seeds each key
  // (so policy.contains(k) is true going in), then races remove(k) against
  // a reinsert at a fresh version.
  //
  // Two distinct hazards exist:
  //
  //   1. Queue order [New(v2), Delete(v1)]: `policy.add` sees the stale v1
  //      policy entry and returns an updated-cost outcome. A caller that
  //      conflates that with a rejection would remove the fresh v2 store
  //      row — `insert()` returned true but the value disappears.
  //
  //   2. Queue order [Delete(v1), New(v2)] plus processor interleaving where
  //      the Delete handler still finds v2 in the store: an unconditional
  //      `policy.remove` would wipe the policy entry that the New path had
  //      just refreshed for v2, leaving the store row outside cost/eviction
  //      accounting (ghost).
  //
  // Both are avoided by (a) the `AddOutcome::UpdatedExisting` variant in the
  // New handler and (b) the `contains_key` gate in the Delete handler. After
  // wait(), a reinserted key must be readable and no ghosts may remain.
  #[test]
  fn test_sync_present_remove_vs_reinsert_keeps_value() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, Ordering as AOrd},
    };

    const ROUNDS: u64 = 8;
    const KEYS_PER_ROUND: u64 = 5_000;
    const REINSERT_VAL: u64 = 0xDEAD_BEEF;
    let mut missing = 0usize;
    let mut ghosts = 0usize;

    for round in 0..ROUNDS {
      let c: Arc<Cache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        Cache::builder(20_000, 1_000_000)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .finalize()
          .unwrap(),
      );

      let base = round * KEYS_PER_ROUND;

      // Seed every key and drain the insert buffer so policy has the
      // original admissions before the race begins.
      for k in base..base + KEYS_PER_ROUND {
        c.insert(k, k, 1);
      }
      c.wait().unwrap();

      let start = Arc::new(AtomicBool::new(false));

      let c_rem = c.clone();
      let start_rem = start.clone();
      let remover = spawn(move || {
        while !start_rem.load(AOrd::Acquire) {
          std::hint::spin_loop();
        }
        for k in base..base + KEYS_PER_ROUND {
          c_rem.remove(&k);
        }
      });

      let c_ins = c.clone();
      let start_ins = start.clone();
      let reinserter = spawn(move || {
        while !start_ins.load(AOrd::Acquire) {
          std::hint::spin_loop();
        }
        // Reinsert a distinctive sentinel so we can tell the original
        // admission from the post-remove reinsertion.
        for k in base..base + KEYS_PER_ROUND {
          c_ins.insert(k, REINSERT_VAL, 1);
        }
      });

      start.store(true, AOrd::Release);
      remover.join().unwrap();
      reinserter.join().unwrap();
      c.wait().unwrap();

      for k in base..base + KEYS_PER_ROUND {
        let in_store = c.0.store.get(&k, 0).is_some();
        let in_policy = c.0.policy.contains(&k);
        if in_policy && !in_store {
          ghosts += 1;
        }
        if in_store && !in_policy {
          // Store has the row but policy has forgotten it — cost/eviction
          // accounting is now wrong, even though `get()` still works.
          missing += 1;
        }
      }
    }
    assert_eq!(
      (ghosts, missing),
      (0, 0),
      "present-remove-vs-reinsert race: {} ghosts (policy tracks a key the \
       store has dropped), {} unaccounted live rows (store has a key the \
       policy has forgotten — bypasses max_cost / eviction)",
      ghosts,
      missing,
    );
  }

  // Regression for the oversized-reinsert ghost. Pre-seed a key (so policy
  // contains it), then race `remove(k)` against an oversized reinsert whose
  // cost exceeds `max_cost`. The hazardous interleaving is:
  //
  //   1. Delete(v1) is processed while store still holds v2 (eager insert),
  //      so the `contains_key` gate skips `policy.remove`. Policy retains
  //      the v1 entry.
  //   2. New(v2, huge) is processed. `policy.add` early-returns
  //      `RejectedByCost` at the top, before the `costs.update` probe, so
  //      the pre-existing policy entry is neither updated nor removed. The
  //      handler's rollback then drops store v2.
  //
  // Result before the fix: policy contained a key the store had no row for,
  // `costs.used` stayed inflated, and no future `remove()` could heal it
  // because `try_remove` on an absent key never enqueues another Delete.
  // Fix: after a rejected rollback, if the store has no row for this key,
  // the handler wipes the orphaned policy entry.
  #[test]
  fn test_sync_oversized_reinsert_no_ghost_policy_entry() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, Ordering as AOrd},
    };

    const ROUNDS: u64 = 8;
    const KEYS_PER_ROUND: u64 = 5_000;
    const MAX_COST: i64 = 1_000;
    const OVERSIZED: i64 = MAX_COST + 1;

    let mut ghosts = 0usize;

    for round in 0..ROUNDS {
      let c: Arc<Cache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        Cache::builder(KEYS_PER_ROUND as usize * 2, MAX_COST)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .finalize()
          .unwrap(),
      );

      let base = round * KEYS_PER_ROUND;

      for k in base..base + KEYS_PER_ROUND {
        c.insert(k, k, 1);
      }
      c.wait().unwrap();

      let start = Arc::new(AtomicBool::new(false));

      let c_rem = c.clone();
      let start_rem = start.clone();
      let remover = spawn(move || {
        while !start_rem.load(AOrd::Acquire) {
          std::hint::spin_loop();
        }
        for k in base..base + KEYS_PER_ROUND {
          c_rem.remove(&k);
        }
      });

      let c_ins = c.clone();
      let start_ins = start.clone();
      let reinserter = spawn(move || {
        while !start_ins.load(AOrd::Acquire) {
          std::hint::spin_loop();
        }
        for k in base..base + KEYS_PER_ROUND {
          // Oversized cost: drives `policy.add` into the `RejectedByCost`
          // branch, triggering the rollback path.
          c_ins.insert(k, 0xDEAD_BEEF, OVERSIZED);
        }
      });

      start.store(true, AOrd::Release);
      remover.join().unwrap();
      reinserter.join().unwrap();
      c.wait().unwrap();

      for k in base..base + KEYS_PER_ROUND {
        let in_store = c.0.store.get(&k, 0).is_some();
        let in_policy = c.0.policy.contains(&k);
        if in_policy && !in_store {
          ghosts += 1;
        }
      }
    }
    assert_eq!(
      ghosts, 0,
      "oversized-reinsert race stranded {} ghost policy entries (policy \
       tracks a key whose store row was rolled back for cost > max_cost)",
      ghosts,
    );
  }

  // Regression for the Delete-handler conflict-collision bug. Policy is
  // keyed by hash_index alone, but the Delete gate used `contains_key(&key,
  // conflict)` which ALSO filtered on the conflict field. When two distinct
  // keys share a hash_index but have different hash_conflicts, and the
  // processor drains New-for-B before the stale Delete-for-A, the Delete
  // gate saw (idx, C_A) missing from the store (B's C_B row is live) and
  // wiped policy — leaving B's row as a ghost outside policy accounting.
  //
  // The natural concurrent reproduction is unreliable: with `insert` +
  // `remove` both performing eager store work under the shard lock, whoever
  // grabs the lock first typically also enqueues first, so the [New, Delete]
  // ordering needed to hit the bug is rare. We simulate that ordering
  // directly by injecting crafted Item::New / Item::Delete into the buffer,
  // after priming store and policy into the pre-race state.
  //
  // Fix: pass conflict=0 so the gate asks "any row at this index". A live
  // row at the shared index — regardless of its conflict — blocks the wipe
  // and keeps the shared policy entry consistent with the store.
  #[test]
  fn test_sync_index_collision_delete_preserves_policy() {
    use crate::ttl::Time;
    use std::sync::atomic::Ordering as AOrd;

    // Custom KeyBuilder: hash_index = k >> 1, hash_conflict preserves the
    // low bit (and sets a high bit so no conflict is zero — `store` treats
    // conflict=0 as "any conflict"). Thus key A=0 and B=1 collide at
    // hash_index=0 but have distinct non-zero hash_conflicts.
    #[derive(Default, Clone)]
    struct PairedIdxKH;

    impl KeyBuilder for PairedIdxKH {
      type Key = u64;

      fn hash_index<Q>(&self, key: &Q) -> u64
      where
        Self::Key: core::borrow::Borrow<Q>,
        Q: core::hash::Hash + Eq + ?Sized,
      {
        let mut h = TransparentHasher { data: 0 };
        key.hash(&mut h);
        h.finish() >> 1
      }

      fn hash_conflict<Q>(&self, key: &Q) -> u64
      where
        Self::Key: core::borrow::Borrow<Q>,
        Q: core::hash::Hash + Eq + ?Sized,
      {
        let mut h = TransparentHasher { data: 0 };
        key.hash(&mut h);
        h.finish() | (1u64 << 63)
      }
    }

    let kh = PairedIdxKH;
    let key_a: u64 = 0;
    let key_b: u64 = 1;
    let idx = kh.hash_index(&key_a);
    assert_eq!(idx, kh.hash_index(&key_b), "keys must share hash_index");
    let c_a = kh.hash_conflict(&key_a);
    let c_b = kh.hash_conflict(&key_b);
    assert_ne!(c_a, c_b, "keys must have distinct hash_conflicts");

    let c: Cache<u64, u64, PairedIdxKH> = Cache::builder(64, 100)
      .set_key_builder(PairedIdxKH)
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    // 1. Admit A normally. Processor drains → policy tracks idx; store
    //    holds (idx, C_A, v_a). This is the shared policy entry both keys
    //    would map to.
    c.insert(key_a, 0xAAAA, 1);
    c.wait().unwrap();
    assert!(
      c.0.policy.contains(&idx),
      "policy must track idx after A admit"
    );

    // 2. Swap A's row for a B row at the same idx with conflict C_B. Direct
    //    store mutation bypasses the cache's insert path — no Item::* lands
    //    in the buffer — and yields A's actual live version via the removed
    //    StoreItem. Pre-race state: store holds (idx, C_B, v_b); policy
    //    still tracks idx (from step 1's admission).
    let v_a = c
      .0
      .store
      .try_remove(&idx, c_a)
      .unwrap()
      .expect("A's row must still be live")
      .version;
    let current_gen = c.0.clear_generation.load(AOrd::Acquire);
    let v_b = c
      .0
      .store
      .try_insert(idx, 0xBBBB, c_b, Time::now(), current_gen)
      .unwrap()
      .expect("fresh insert at empty slot");

    // 3. Inject queued items in the exact order needed to hit the bug:
    //    New for B first (policy.add returns UpdatedExisting; store row
    //    already matches), then stale Delete for A (version v_a that no
    //    longer matches any store row).
    c.0
      .insert_buf_tx
      .send(Item::new(idx, c_b, 1, Time::now(), v_b, current_gen))
      .unwrap();
    c.0
      .insert_buf_tx
      .send(Item::delete(idx, c_a, current_gen, v_a))
      .unwrap();
    c.wait().unwrap();

    // Bug path: Delete gate computed `contains_key(idx, c_a)` → false (store
    // has C_B) → policy.remove(idx). Version-gated cleanup then refuses to
    // touch the live v_b row, leaving B's store row outside policy.
    // Fix path: `contains_key(idx, 0)` → true (C_B present) → policy kept.
    let in_store = c.0.store.get(&idx, 0).is_some();
    let in_policy = c.0.policy.contains(&idx);
    assert!(in_store, "B's store row must remain live after the Delete");
    assert!(
      in_policy,
      "policy must still track idx — Finding-1 ghost if it doesn't",
    );
  }

  // Regression for the stale-Update orphan bug. `try_update` captures the
  // clear generation BEFORE the eager `store.try_update` call, so the
  // captured value can still be stale even when the eager write itself
  // landed AFTER clear. Scenario:
  //
  //   1. T1 (update) loads gen = g0.
  //   2. T2 runs clear(): store.clear(); policy.clear(); gen → g1.
  //   3. T3 (insert) loads gen = g1, places a fresh post-clear row at
  //      (K, C_K, v_N); enqueues New{g1, v_N}.
  //   4. T1 resumes: store.try_update(K, ...) finds the v_N row, overwrites
  //      it to v_{N+1}, enqueues Update{g0, v_{N+1}}.
  //   5. Processor handles New{g1, v_N}: current_gen matches, but
  //      `contains_version(K, C_K, v_N)` is false (T1 bumped to v_{N+1}),
  //      so admission is skipped.
  //   6. Processor handles Update{g0, v_{N+1}}: old code returned early on
  //      gen mismatch, leaving the v_{N+1} row live with no policy entry
  //      (max_cost bypass, not evictable). Fix: mirror the New handler's
  //      stale-gen cleanup — version-gated remove of the row we wrote.
  //
  // The race window is narrow (T1 must stall between load-generation and
  // store.try_update while clear + post-clear-insert both run). Many
  // rounds × many keys give us enough exposure; the assertion is a
  // post-wait `store-row-without-policy-entry` scan.
  #[test]
  fn test_sync_stale_update_across_clear_no_orphan() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, Ordering as AOrd},
    };

    const ROUNDS: u64 = 12;
    const KEYS_PER_ROUND: u64 = 2_000;
    // Big enough to fit all pre-seeded rows and any post-clear inserts —
    // no eviction perturbs the invariant we're testing.
    const MAX_COST: i64 = 200_000;

    let mut orphans = 0usize;

    for round in 0..ROUNDS {
      let c: Arc<Cache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        Cache::builder(KEYS_PER_ROUND as usize * 4, MAX_COST)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .finalize()
          .unwrap(),
      );

      let base = round * KEYS_PER_ROUND;

      // Pre-seed so the update path (try_update's UpdateResult::Update
      // branch) is what T1 hits. Fresh inserts hit the NotExist → New
      // branch and are already covered by `test_sync_clear_bump_vs_wipe_
      // ordering`.
      for k in 0..KEYS_PER_ROUND {
        c.insert(base + k, base + k, 1);
      }
      c.wait().unwrap();

      let start = Arc::new(AtomicBool::new(false));

      // Updater: re-inserts each key, which becomes an Update since the
      // key exists. Generation is captured before the store write.
      let c_upd = c.clone();
      let start_upd = start.clone();
      let updater = spawn(move || {
        while !start_upd.load(AOrd::Acquire) {
          std::hint::spin_loop();
        }
        for k in 0..KEYS_PER_ROUND {
          c_upd.insert(base + k, base + k + 1, 1);
        }
      });

      // Clearer: fires one clear() mid-run.
      let c_clr = c.clone();
      let start_clr = start.clone();
      let clearer = spawn(move || {
        while !start_clr.load(AOrd::Acquire) {
          std::hint::spin_loop();
        }
        sleep(Duration::from_micros(20));
        c_clr.clear().unwrap();
      });

      // Post-clear inserter: fires fresh inserts of the same keys after
      // clear, so updater's delayed store.try_update can find (and
      // overwrite) a post-clear row.
      let c_ins = c.clone();
      let start_ins = start.clone();
      let inserter = spawn(move || {
        while !start_ins.load(AOrd::Acquire) {
          std::hint::spin_loop();
        }
        // Small stagger so most insert() calls land just after clear().
        sleep(Duration::from_micros(40));
        for k in 0..KEYS_PER_ROUND {
          c_ins.insert(base + k, base + k + 100, 1);
        }
      });

      start.store(true, AOrd::Release);
      updater.join().unwrap();
      clearer.join().unwrap();
      inserter.join().unwrap();
      c.wait().unwrap();

      // Invariant: no store row may exist outside policy accounting. With
      // TransparentKeyBuilder<u64> the index equals the key, so iterate
      // the key space we touched this round.
      for k in 0..KEYS_PER_ROUND {
        let idx = base + k;
        let in_store = c.0.store.get(&idx, 0).is_some();
        let in_policy = c.0.policy.contains(&idx);
        if in_store && !in_policy {
          orphans += 1;
        }
      }
    }
    assert_eq!(
      orphans, 0,
      "pre-clear Update vs post-clear insert race stranded {} store rows \
       outside policy accounting (stale Update returned without reaping \
       the row it wrote across the clear boundary)",
      orphans,
    );
  }

  // Regression for the concurrent-insert-on-same-key orphan bug. Without
  // this fix, two inserters racing on the same key can both commit to
  // the store (last writer wins at the row) but neither commits the key
  // to policy:
  //
  //   1. T1 `insert(K, V1)` → eager try_insert places (K, 0, v1).
  //      Enqueues New{v1}.
  //   2. T2 `insert(K, V2)` → eager try_update finds v1, bumps to v2.
  //      Enqueues Update{v2}.
  //   3. Processor drains New{v1}: `contains_version(K, 0, v1)` is false
  //      (store holds v2). Skip admission.
  //   4. Processor drains Update{v2}: `policy.update(K, cost)` returns
  //      false (policy has no entry to update). Old code treated this as
  //      success; the row sits outside policy forever.
  //
  // Fix: when `policy.update` finds no entry and our row is still live
  // (`contains_version` true), fall through to `policy.add` so the row
  // is admitted (or rolled back on rejection, matching Item::New's
  // rollback path).
  #[test]
  fn test_sync_concurrent_same_key_insert_no_orphan() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, Ordering as AOrd},
    };

    const ROUNDS: usize = 6;
    const KEYS: u64 = 2_000;
    // Sized so all admitted rows fit; no eviction interferes.
    const MAX_COST: i64 = 200_000;

    let mut orphans = 0usize;
    for _ in 0..ROUNDS {
      let c: Arc<Cache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        Cache::builder(KEYS as usize * 4, MAX_COST)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .finalize()
          .unwrap(),
      );

      let start = Arc::new(AtomicBool::new(false));

      let c1 = c.clone();
      let s1 = start.clone();
      let t1 = spawn(move || {
        while !s1.load(AOrd::Acquire) {
          std::hint::spin_loop();
        }
        for k in 0..KEYS {
          c1.insert(k, k, 1);
        }
      });

      let c2 = c.clone();
      let s2 = start.clone();
      let t2 = spawn(move || {
        while !s2.load(AOrd::Acquire) {
          std::hint::spin_loop();
        }
        for k in 0..KEYS {
          c2.insert(k, k + 100, 1);
        }
      });

      start.store(true, AOrd::Release);
      t1.join().unwrap();
      t2.join().unwrap();
      c.wait().unwrap();

      for k in 0..KEYS {
        let in_store = c.0.store.get(&k, 0).is_some();
        let in_policy = c.0.policy.contains(&k);
        if in_store && !in_policy {
          orphans += 1;
        }
      }
    }
    assert_eq!(
      orphans, 0,
      "concurrent-insert-on-same-key race stranded {} store rows \
       outside policy accounting (Update handler's policy.update \
       no-opped for a key whose New was skipped by contains_version)",
      orphans,
    );
  }

  /// Regression for Codex adversarial review (sync.rs re-entrancy deadlock):
  /// the sync processor thread invokes user `CacheCallback` methods
  /// synchronously. If such a callback calls `insert()` on the same cache
  /// while the bounded insert buffer is full, a blocking `send` would wait
  /// on the processor — which is this thread — and deadlock forever.
  #[test]
  fn test_sync_callback_reentrant_insert_no_deadlock() {
    use std::sync::atomic::{AtomicU64, Ordering as AOrd};

    struct State {
      hook: parking_lot::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
      reentries: AtomicU64,
    }

    struct ReenterCB(Arc<State>);

    impl CacheCallback for ReenterCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {}
      fn on_evict(&self, _item: CrateItem<u64>) {
        let maybe_hook = self.0.hook.lock().clone();
        if let Some(hook) = maybe_hook {
          hook();
          self.0.reentries.fetch_add(1, AOrd::Relaxed);
        }
      }
    }

    let state = Arc::new(State {
      hook: parking_lot::Mutex::new(None),
      reentries: AtomicU64::new(0),
    });

    // Tiny buffer + small max_cost so eviction fires quickly and the buffer
    // saturates while the processor is paused in our callback.
    let c: Arc<
      Cache<
        u64,
        u64,
        TransparentKeyBuilder<u64>,
        DefaultCoster<u64>,
        DefaultUpdateValidator<u64>,
        ReenterCB,
      >,
    > = Arc::new(
      CacheBuilder::new_with_key_builder(100, 10, TransparentKeyBuilder::default())
        .set_callback(ReenterCB(state.clone()))
        .set_buffer_size(2)
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap(),
    );

    let c_hook = c.clone();
    *state.hook.lock() = Some(Arc::new(move || {
      // Loop far beyond buffer capacity: each iteration's `insert` would
      // block forever on a full buffer without the re-entrancy fix, because
      // the sole consumer (this thread) is stuck in the callback.
      for k in 0..16u64 {
        let _ = c_hook.insert(1_000_000 + k, 0, 1);
      }
    }));

    // Driver keeps the buffer busy so the callback's sends find it full.
    let driver = spawn({
      let c = c.clone();
      move || {
        for i in 0..200u64 {
          c.insert(i, i, 5);
        }
      }
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
      if driver.is_finished() {
        driver.join().unwrap();
        break;
      }
      if std::time::Instant::now() > deadline {
        panic!(
          "re-entrant callback insert deadlocked the sync processor \
           (driver thread still running after 10s)"
        );
      }
      sleep(Duration::from_millis(25));
    }

    // Break the Cache <-> callback reference cycle so the cache can drop.
    *state.hook.lock() = None;

    assert!(
      state.reentries.load(AOrd::Relaxed) > 0,
      "callback never fired — test did not exercise the re-entrant path",
    );
  }

  /// Regression for Codex adversarial review Finding C: a `CacheCallback`
  /// running on the processor thread must NOT be allowed to call `clear()`
  /// or `wait()`. Both primitives enqueue a marker that only the processor
  /// thread can drain and then block on a `WaitGroup` — so the callback
  /// would wait on itself forever. The fix surfaces the re-entry as an
  /// error instead of deadlocking.
  #[test]
  fn test_sync_callback_clear_from_callback_returns_error_not_deadlock() {
    use std::sync::atomic::{AtomicU64, Ordering as AOrd};

    struct State {
      saw_error: AtomicU64,
      fired: AtomicU64,
      hook: parking_lot::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    }

    struct ClearInCB(Arc<State>);

    impl CacheCallback for ClearInCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {}
      fn on_evict(&self, _item: CrateItem<u64>) {
        let maybe_hook = self.0.hook.lock().clone();
        if let Some(hook) = maybe_hook {
          hook();
          self.0.fired.fetch_add(1, AOrd::Relaxed);
        }
      }
    }

    let state = Arc::new(State {
      saw_error: AtomicU64::new(0),
      fired: AtomicU64::new(0),
      hook: parking_lot::Mutex::new(None),
    });

    let c: Arc<
      Cache<
        u64,
        u64,
        TransparentKeyBuilder<u64>,
        DefaultCoster<u64>,
        DefaultUpdateValidator<u64>,
        ClearInCB,
      >,
    > = Arc::new(
      CacheBuilder::new_with_key_builder(100, 10, TransparentKeyBuilder::default())
        .set_callback(ClearInCB(state.clone()))
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap(),
    );

    let c_hook = c.clone();
    let state_hook = state.clone();
    *state.hook.lock() = Some(Arc::new(move || {
      // Calling clear() from here used to self-deadlock; the fix surfaces
      // it as ChannelError.
      let res = c_hook.clear();
      assert!(
        res.is_err(),
        "clear() from a CacheCallback must return an error, not deadlock — got {:?}",
        res
      );
      state_hook.saw_error.fetch_add(1, AOrd::Relaxed);
      // wait() must behave identically.
      let res = c_hook.wait();
      assert!(
        res.is_err(),
        "wait() from a CacheCallback must return an error, not deadlock — got {:?}",
        res
      );
    }));

    // Drive evictions so the callback fires on the processor thread.
    for i in 0..200u64 {
      c.insert(i, i, 5);
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while state.fired.load(AOrd::Relaxed) == 0 {
      if std::time::Instant::now() > deadline {
        panic!("eviction callback never fired in 10s — test did not exercise the re-entry path");
      }
      sleep(Duration::from_millis(25));
    }

    // Break the Cache <-> callback reference cycle so the cache can drop.
    *state.hook.lock() = None;

    assert!(
      state.saw_error.load(AOrd::Relaxed) > 0,
      "the processor-thread re-entry guard on clear()/wait() never tripped",
    );
  }

  // RETIRED: test_sync_reentrant_update_reconciles_policy_cost
  //
  // This test was written for the pre-semaphore design, where a re-entrant
  // `insert()` on the processor thread performed the eager `store.try_update`
  // first and then attempted the channel send. If the buffer was full the
  // send was dropped, leaving the store holding the NEW cost value while
  // policy still tracked the OLD cost — the exact divergence the test
  // asserted against.
  //
  // Under the semaphore-gated insert path, a re-entrant insert on the
  // processor thread uses `try_acquire` (because the processor is the sole
  // releaser and cannot be woken from inside a callback). When all permits
  // are held by items already in the bounded buffer, `try_acquire` returns
  // false and `try_insert_in` returns early WITHOUT touching the store.
  // There is no eager write, so there is no store/policy divergence to
  // reconcile — the store still holds the pre-update value, matching
  // policy's pre-update cost.
  //
  // `reconcile_update_on_send_fail` is retained as defensive code for the
  // `TrySendError::Full` arm (structurally unreachable under the permit
  // invariant but kept to preserve the invariant if future refactors break
  // it). The behavioural guarantee the test originally checked — that the
  // re-entrant path cannot silently grow cost past `max_cost` — is now
  // enforced by the semaphore itself: no uncommitted eager write survives
  // a failed admission.
  #[test]
  #[ignore = "retired: the bug condition (eager-write-before-send divergence) is \
              structurally impossible under semaphore-gated inserts"]
  fn test_sync_reentrant_update_reconciles_policy_cost() {
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AOrd};

    const TARGET: u64 = 42;
    const OLD_COST: i64 = 1;
    const NEW_COST: i64 = 50;
    const BUFFER_SIZE: usize = 2;
    const SENTINEL: i64 = i64::MIN;

    struct State {
      hook: parking_lot::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
      fired: AtomicU64,
      observed_cost: AtomicI64,
    }

    struct UpdateInCB(Arc<State>);

    impl CacheCallback for UpdateInCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {}
      fn on_evict(&self, item: CrateItem<u64>) {
        // Only fire the hook when a NON-TARGET key is evicted. If TARGET
        // itself is the victim, the follow-up re-entrant insert would
        // hit the Item::New path instead of Item::Update, defeating the
        // test. Fire exactly once via CAS.
        if item.index == TARGET {
          return;
        }
        if self
          .0
          .fired
          .compare_exchange(0, 1, AOrd::Relaxed, AOrd::Relaxed)
          .is_err()
        {
          return;
        }
        let maybe_hook = self.0.hook.lock().clone();
        if let Some(hook) = maybe_hook {
          hook();
        }
      }
    }

    let state = Arc::new(State {
      hook: parking_lot::Mutex::new(None),
      fired: AtomicU64::new(0),
      observed_cost: AtomicI64::new(SENTINEL),
    });

    // max_cost small enough to force evictions during the driver loop,
    // but large enough that TARGET + a handful of driver keys coexist
    // briefly so the callback can observe TARGET in the store.
    let c: Arc<
      Cache<
        u64,
        u64,
        TransparentKeyBuilder<u64>,
        DefaultCoster<u64>,
        DefaultUpdateValidator<u64>,
        UpdateInCB,
      >,
    > = Arc::new(
      CacheBuilder::new_with_key_builder(1000, 100, TransparentKeyBuilder::default())
        .set_callback(UpdateInCB(state.clone()))
        .set_buffer_size(BUFFER_SIZE)
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap(),
    );

    // Seed TARGET at OLD_COST and make sure policy has actually admitted
    // it before the callback fires (otherwise the reconcile path would
    // take the policy.add fallthrough instead of the policy.update fast
    // path, and our assertion semantics would differ).
    assert!(c.insert(TARGET, 0, OLD_COST));
    c.wait().unwrap();
    assert_eq!(
      c.0.policy.cost(&TARGET),
      OLD_COST,
      "precondition: seeded target key should be in policy at OLD_COST"
    );

    // Bump TARGET's tinylfu frequency so it's unlikely to be chosen as a
    // victim before a non-TARGET eviction fires our hook.
    for _ in 0..500 {
      let _ = c.get(&TARGET);
    }
    c.wait().unwrap();

    let c_hook = c.clone();
    let state_for_hook = state.clone();
    *state.hook.lock() = Some(Arc::new(move || {
      // Fill the bounded buffer with re-entrant new-key inserts. The
      // processor is parked in this callback so nothing drains; after
      // BUFFER_SIZE successful try_sends, subsequent sends fail and the
      // try_insert_in drop-on-full branch cleans up their eager writes.
      for i in 0..(BUFFER_SIZE as u64 + 6) {
        let _ = c_hook.insert(1_000_000 + i, 0, 1);
      }
      // Buffer is now full. This Update's try_send must fail. With the
      // bug, NO Item::Update for TARGET ever reaches the processor and
      // policy's cost for TARGET stays at OLD_COST even after the caller
      // has observed a cost-NEW_COST value in the store. With the fix,
      // reconcile_update_on_send_fail runs inline and calls
      // policy.update(TARGET, NEW_COST).
      let _ = c_hook.insert(TARGET, 0, NEW_COST);
      // Observe policy cost IMMEDIATELY inside the callback — the
      // processor is still parked here, so TARGET cannot have been
      // evicted by subsequent drive inserts yet. Reading post-wait()
      // would race against post-callback eviction pressure.
      state_for_hook
        .observed_cost
        .store(c_hook.0.policy.cost(&TARGET), AOrd::SeqCst);
    }));

    // Drive evictions. Keys use cost 10 against a max_cost of 100, so
    // after ~10 admissions every new admission must bump a victim.
    let driver = spawn({
      let c = c.clone();
      move || {
        for i in 0..500u64 {
          c.insert(2_000_000 + i, i, 10);
        }
      }
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while state.fired.load(AOrd::Relaxed) == 0 {
      if std::time::Instant::now() > deadline {
        panic!("callback never fired — test did not exercise the re-entrant update path");
      }
      sleep(Duration::from_millis(25));
    }
    driver.join().unwrap();

    // Break the Cache <-> callback reference cycle so the cache can drop.
    *state.hook.lock() = None;

    // With the bug, policy.cost(TARGET) observed IMMEDIATELY after the
    // failed update send is still OLD_COST because the Item::Update for
    // TARGET was dropped at try_send and never admitted. With the fix,
    // reconcile_update_on_send_fail ran inline on the processor thread
    // and updated policy to NEW_COST before releasing the callback.
    let observed = state.observed_cost.load(AOrd::SeqCst);
    assert_ne!(
      observed, SENTINEL,
      "hook did not record policy.cost(TARGET) — hook closure never completed",
    );
    assert_eq!(
      observed, NEW_COST,
      "re-entrant update's send failed but policy was not reconciled: \
       policy.cost(TARGET) = {} (expected {}). The store holds a cost-{} \
       value for TARGET, so policy is now below the true live cost and \
       max_cost can be silently bypassed.",
      observed, NEW_COST, NEW_COST,
    );
  }

  /// Regression for Codex adversarial review Finding: when a
  /// `CacheCallback` running on the processor thread re-enters
  /// `try_remove()` AND the bounded insert buffer is full, the blocking
  /// `insert_buf_tx.send` that enqueues `Item::Delete` would self-
  /// deadlock — the sole consumer of the buffer (the processor) is
  /// parked in this callback, so no one will drain to open a slot. The
  /// fix short-circuits on the processor thread by applying the Delete
  /// handler's policy reconciliation inline and skipping the send.
  ///
  /// Deterministic setup: inside the callback we first saturate the
  /// bounded buffer with re-entrant new-key inserts (on-processor
  /// inserts use `try_send` and silently drop past capacity, but the
  /// first few succeed and leave the buffer full), then call
  /// `remove(TARGET)`. The eager `store.try_remove` removes the row and
  /// the follow-up `insert_buf_tx.send(Item::Delete)` must NOT block.
  /// With the bug the callback never returns and the watchdog 10s
  /// deadline below panics. With the fix the inline path runs
  /// `policy.remove(TARGET)` and returns immediately; we verify the
  /// policy entry was actually wiped.
  #[test]
  fn test_sync_reentrant_remove_does_not_deadlock() {
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AOrd};

    const TARGET: u64 = 42;
    const BUFFER_SIZE: usize = 2;
    const SENTINEL: i64 = i64::MIN;

    struct State {
      hook: parking_lot::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
      fired: AtomicU64,
      cost_before_remove: AtomicI64,
      cost_after_remove: AtomicI64,
    }

    struct RemoveInCB(Arc<State>);

    impl CacheCallback for RemoveInCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {}
      fn on_evict(&self, item: CrateItem<u64>) {
        // Only fire on NON-TARGET eviction so TARGET is still live in
        // store/policy when the hook re-enters remove(). Fire exactly once.
        if item.index == TARGET {
          return;
        }
        if self
          .0
          .fired
          .compare_exchange(0, 1, AOrd::Relaxed, AOrd::Relaxed)
          .is_err()
        {
          return;
        }
        let maybe_hook = self.0.hook.lock().clone();
        if let Some(hook) = maybe_hook {
          hook();
        }
      }
    }

    let state = Arc::new(State {
      hook: parking_lot::Mutex::new(None),
      fired: AtomicU64::new(0),
      cost_before_remove: AtomicI64::new(SENTINEL),
      cost_after_remove: AtomicI64::new(SENTINEL),
    });

    let c: Arc<
      Cache<
        u64,
        u64,
        TransparentKeyBuilder<u64>,
        DefaultCoster<u64>,
        DefaultUpdateValidator<u64>,
        RemoveInCB,
      >,
    > = Arc::new(
      CacheBuilder::new_with_key_builder(1000, 100, TransparentKeyBuilder::default())
        .set_callback(RemoveInCB(state.clone()))
        .set_buffer_size(BUFFER_SIZE)
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap(),
    );

    assert!(c.insert(TARGET, 0, 1));
    c.wait().unwrap();
    assert_eq!(
      c.0.policy.cost(&TARGET),
      1,
      "precondition: seeded TARGET should be live in policy"
    );

    // Boost TARGET's tinylfu frequency so it survives driver eviction
    // pressure until the callback fires.
    for _ in 0..500 {
      let _ = c.get(&TARGET);
    }
    c.wait().unwrap();

    let c_hook = c.clone();
    let state_for_hook = state.clone();
    *state.hook.lock() = Some(Arc::new(move || {
      // Snapshot policy cost for TARGET right before the remove so the
      // outer assertion can distinguish "TARGET was evicted pre-callback
      // (precondition failure)" from "remove ran but policy wasn't
      // reconciled".
      state_for_hook
        .cost_before_remove
        .store(c_hook.0.policy.cost(&TARGET), AOrd::SeqCst);

      // Saturate the bounded insert buffer with re-entrant new-key
      // inserts. On-processor inserts use try_send, so the first few
      // commit and the rest hit the drop-on-full branch — net effect:
      // the buffer is full for the upcoming remove's send.
      for i in 0..(BUFFER_SIZE as u64 + 6) {
        let _ = c_hook.insert(1_000_000 + i, 0, 1);
      }

      // With the bug: remove()'s blocking send of Item::Delete self-
      // deadlocks because the sole consumer is this callback. With the
      // fix: on_processor_thread() short-circuits, inline reconcile
      // calls policy.remove(TARGET), and the method returns.
      c_hook.remove(&TARGET);

      state_for_hook
        .cost_after_remove
        .store(c_hook.0.policy.cost(&TARGET), AOrd::SeqCst);
    }));

    // Drive evictions: 500 keys at cost 10 against max_cost 100 forces
    // ~10 active keys and steady victim selection.
    let driver = spawn({
      let c = c.clone();
      move || {
        for i in 0..500u64 {
          c.insert(2_000_000 + i, i, 10);
        }
      }
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while state.cost_after_remove.load(AOrd::SeqCst) == SENTINEL {
      if std::time::Instant::now() > deadline {
        panic!(
          "re-entrant remove did not complete within 10s — the on_processor \
           callback path is deadlocking on insert_buf_tx.send(Item::Delete)"
        );
      }
      sleep(Duration::from_millis(25));
    }
    driver.join().unwrap();

    // Break the Cache <-> callback reference cycle so the cache can drop.
    *state.hook.lock() = None;

    let pre = state.cost_before_remove.load(AOrd::SeqCst);
    assert_eq!(
      pre, 1,
      "precondition: TARGET must still be live in policy when the callback \
       fires (cost_before_remove = {}). Test driver evicted TARGET before \
       the re-entrant remove could run — adjust frequency boost / driver cost.",
      pre,
    );

    // `policy.cost` returns -1 when the key is absent from policy's cost
    // map. After the inline reconcile the TARGET entry is gone, so
    // observing -1 here means the fix actually wiped policy.
    let post = state.cost_after_remove.load(AOrd::SeqCst);
    assert_eq!(
      post, -1,
      "re-entrant remove did not clean up policy — policy.cost(TARGET) = {} \
       (expected -1 for absent). The inline reconcile path on processor-thread \
       try_remove is not running and policy still accounts for a key the \
       caller's eager store.try_remove has already dropped from the store.",
      post,
    );
  }

  /// Regression for Codex adversarial review finding: pre-fix, sync
  /// producers blocked on a full insert buffer only AFTER their eager
  /// `store.try_update` write — so N concurrent senders could leave N
  /// live uncharged rows in the store, silently bypassing `max_cost`.
  ///
  /// With the semaphore-gated insert path, each producer acquires a
  /// permit BEFORE the eager write. The permit pool is sized to the
  /// insert buffer, so the number of pre-admission store rows cannot
  /// exceed `insert_buffer_size` regardless of how many senders are
  /// blocked. This test parks the processor inside a `CacheCallback`,
  /// floods producers, and samples `store.len()` while the processor
  /// is stalled.
  #[test]
  fn test_sync_eager_writes_bounded_by_permit_pool() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AOrd};

    const MAX_COST: i64 = 1;
    const BUFFER: usize = 4;
    const PRODUCERS: u64 = 64;
    // Upper bound on the peak store.len() we tolerate while the processor
    // is parked. Composed of:
    //   - the items already admitted to policy (capped at MAX_COST)
    //   - the permit pool (BUFFER) worth of pre-admission eager writes
    //   - SLACK for transient rows around the victim currently being
    //     processed (the handler may have the victim still in store when
    //     the callback is entered).
    const SLACK: usize = 2;

    struct Gate {
      parked: AtomicBool,
      released: parking_lot::Mutex<bool>,
      cv: parking_lot::Condvar,
    }

    struct ParkOnEvictCB(Arc<Gate>);
    impl CacheCallback for ParkOnEvictCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {}
      fn on_evict(&self, _item: CrateItem<u64>) {
        self.0.parked.store(true, AOrd::Release);
        let mut released = self.0.released.lock();
        while !*released {
          self.0.cv.wait(&mut released);
        }
      }
    }

    let gate = Arc::new(Gate {
      parked: AtomicBool::new(false),
      released: parking_lot::Mutex::new(false),
      cv: parking_lot::Condvar::new(),
    });

    let c: Arc<
      Cache<
        u64,
        u64,
        TransparentKeyBuilder<u64>,
        DefaultCoster<u64>,
        DefaultUpdateValidator<u64>,
        ParkOnEvictCB,
      >,
    > = Arc::new(
      CacheBuilder::new_with_key_builder(100, MAX_COST, TransparentKeyBuilder::default())
        .set_callback(ParkOnEvictCB(gate.clone()))
        .set_buffer_size(BUFFER)
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap(),
    );

    // Seed to make policy full, then admit one more key to force an
    // eviction that parks the processor inside `on_evict`.
    assert!(c.insert(0, 0, MAX_COST));
    c.wait().unwrap();
    c.insert(1, 1, MAX_COST);

    // Wait for the processor to enter the gate.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !gate.parked.load(AOrd::Acquire) {
      if std::time::Instant::now() > deadline {
        *gate.released.lock() = true;
        gate.cv.notify_all();
        panic!("processor never entered on_evict within 5s — test setup broken");
      }
      sleep(Duration::from_millis(5));
    }

    // Processor is parked. Flood producers with unique keys. Under the
    // semaphore invariant, only BUFFER of them can reach the eager store
    // write; the rest must block at `insert_sem.acquire()`.
    let started = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..PRODUCERS)
      .map(|k| {
        let c = c.clone();
        let started = started.clone();
        spawn(move || {
          started.fetch_add(1, AOrd::Relaxed);
          c.insert(1_000_000 + k, 0, 1);
        })
      })
      .collect();

    // Give producers enough time to all reach their acquire/eager-write
    // steps. Then sample the peak.
    let settle_deadline = std::time::Instant::now() + Duration::from_secs(3);
    while started.load(AOrd::Relaxed) < PRODUCERS as usize
      && std::time::Instant::now() < settle_deadline
    {
      sleep(Duration::from_millis(5));
    }
    sleep(Duration::from_millis(200));

    let peak = c.0.store.len();
    let bound = (MAX_COST as usize) + BUFFER + SLACK;

    // Release the gate and let everything drain before asserting so that
    // a failure does not strand the producer threads.
    {
      *gate.released.lock() = true;
      gate.cv.notify_all();
    }
    for h in handles {
      let _ = h.join();
    }

    assert!(
      peak <= bound,
      "store.len() = {} exceeded permit bound while processor was parked \
       (max_cost = {}, buffer = {}, slack = {}, bound = {}). Eager writes \
       are not being back-pressured by the insert permit pool; blocked \
       senders can grow the store past max_cost.",
      peak,
      MAX_COST,
      BUFFER,
      SLACK,
      bound,
    );
  }

  /// Regression for Codex adversarial review finding: pre-fix, sync
  /// `try_insert_in` acquired the insert permit and then called
  /// `try_update`, which synchronously fires `CacheCallback::on_exit` on
  /// the caller thread whenever the Update branch hits. A callback that
  /// re-enters the cache via `insert()`, `clear()`, or `wait()` would then
  /// try to acquire a second permit. Under `set_buffer_size(1)` the outer
  /// update owned the only permit and had not enqueued anything yet, so
  /// the re-entrant caller's `insert_sem.acquire()` blocked forever.
  ///
  /// Fix: `try_update` no longer fires `on_exit` itself; it hands the
  /// prior value back to `try_insert_in`, which fires the callback AFTER
  /// the permit has been transferred to the channel or released on a
  /// failure path. User callbacks never run while the caller holds an
  /// insert permit.
  #[test]
  fn test_sync_on_exit_reenters_cache_does_not_deadlock() {
    use std::sync::atomic::{AtomicU64, Ordering as AOrd};

    struct State {
      hook_fired: AtomicU64,
      hook: parking_lot::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    }

    struct ReenterOnExitCB(Arc<State>);

    impl CacheCallback for ReenterOnExitCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {
        // Consume the hook so the re-entrant call's own on_exit (if it
        // ever fires, on a later update) cannot recurse indefinitely.
        let maybe_hook = self.0.hook.lock().take();
        if let Some(hook) = maybe_hook {
          self.0.hook_fired.fetch_add(1, AOrd::Relaxed);
          hook();
        }
      }
      fn on_evict(&self, _item: CrateItem<u64>) {}
      fn on_reject(&self, _item: CrateItem<u64>) {}
    }

    let state = Arc::new(State {
      hook_fired: AtomicU64::new(0),
      hook: parking_lot::Mutex::new(None),
    });

    // `buffer_size=1` means the permit pool has exactly one permit. The
    // outer update's try_insert_in holds it until the item is enqueued;
    // any re-entrant insert in the meantime must wait on the semaphore.
    let c: Arc<
      Cache<
        u64,
        u64,
        TransparentKeyBuilder<u64>,
        DefaultCoster<u64>,
        DefaultUpdateValidator<u64>,
        ReenterOnExitCB,
      >,
    > = Arc::new(
      CacheBuilder::new_with_key_builder(1000, 1000, TransparentKeyBuilder::default())
        .set_callback(ReenterOnExitCB(state.clone()))
        .set_buffer_size(1)
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap(),
    );

    // Seed K=1 and wait for the processor to admit it + release the
    // permit, so the subsequent update has a clean baseline.
    assert!(c.insert(1u64, 100u64, 1));
    c.wait().unwrap();

    // Install the re-entrant hook AFTER the seed so the seed's callback
    // path is not the first one to fire it.
    let c_hook = c.clone();
    *state.hook.lock() = Some(Arc::new(move || {
      // On the buggy code, this call's blocking `insert_sem.acquire()`
      // never returns because the outer update owns the only permit and
      // has not sent anything to the processor yet.
      let _ = c_hook.insert(2u64, 200u64, 1);
    }));

    // Run the update on a worker thread so the test thread can observe
    // the deadlock via a timeout instead of hanging itself.
    let c_worker = c.clone();
    let worker = spawn(move || c_worker.insert(1u64, 999u64, 1));

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !worker.is_finished() {
      if std::time::Instant::now() > deadline {
        panic!(
          "update deadlocked — on_exit callback ran while caller still held \
           the insert permit; re-entrant insert blocked forever on \
           insert_sem.acquire()"
        );
      }
      sleep(Duration::from_millis(10));
    }

    let res = worker.join().unwrap();
    assert!(res, "update should succeed");
    assert_eq!(
      state.hook_fired.load(AOrd::Relaxed),
      1,
      "on_exit hook must have fired exactly once"
    );
  }

  /// Regression for Codex adversarial review finding: pre-fix, sync
  /// `try_insert_in` acquired a semaphore permit, then called `try_update`
  /// which in turn invokes the user-controlled `Coster::cost`. A panic (or
  /// `Err` return) from that user code unwound past the permit-release
  /// call, leaking the permit. With `set_buffer_size(1)` one such leak
  /// would stall every subsequent insert on `insert_sem.acquire()`.
  ///
  /// Fix: wrap the permit in `SyncPermitGuard`. Its Drop releases on
  /// panic unwind or early-return; `transfer()` disarms it when the
  /// permit has been handed off to the processor via a successful send.
  #[test]
  fn test_sync_permit_released_on_coster_panic() {
    use std::{
      panic::AssertUnwindSafe,
      sync::atomic::{AtomicBool, AtomicU64, Ordering as AOrd},
    };

    struct PanicCoster {
      armed: Arc<AtomicBool>,
      panics: Arc<AtomicU64>,
    }
    impl Coster for PanicCoster {
      type Value = u64;
      fn cost(&self, _v: &u64) -> i64 {
        if self.armed.load(AOrd::Acquire) {
          self.panics.fetch_add(1, AOrd::Relaxed);
          panic!("intentional panic from coster");
        }
        1
      }
    }

    let armed = Arc::new(AtomicBool::new(false));
    let panics = Arc::new(AtomicU64::new(0));
    let c: Arc<
      Cache<
        u64,
        u64,
        TransparentKeyBuilder<u64>,
        PanicCoster,
        DefaultUpdateValidator<u64>,
        DefaultCacheCallback<u64>,
      >,
    > = Arc::new(
      CacheBuilder::new_with_key_builder(1000, 1000, TransparentKeyBuilder::default())
        .set_coster(PanicCoster {
          armed: armed.clone(),
          panics: panics.clone(),
        })
        .set_buffer_size(1)
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap(),
    );

    // Arm: the next insert with cost=0 invokes `Coster::cost`, which panics.
    armed.store(true, AOrd::Release);

    // Run the panicking insert on a worker thread so the test thread
    // survives the unwind. `cost=0` forces the cache to call the user
    // Coster from try_update, which is where the panic originates.
    let c_worker = c.clone();
    let worker =
      spawn(move || std::panic::catch_unwind(AssertUnwindSafe(|| c_worker.insert(1u64, 10u64, 0))));
    let panic_result = worker.join().unwrap();
    assert!(
      panic_result.is_err(),
      "coster panic must propagate out of insert()"
    );
    assert_eq!(
      panics.load(AOrd::Relaxed),
      1,
      "panic coster must have been invoked exactly once"
    );

    // Disarm so the recovery insert does not panic.
    armed.store(false, AOrd::Release);

    // Pre-fix, the sole permit was leaked by the panic unwind and this
    // blocking `insert_sem.acquire()` would hang forever. Run on a worker
    // thread with a short deadline so the test surfaces the hang instead
    // of wedging CI.
    let c_next = c.clone();
    let next = spawn(move || c_next.insert(2u64, 20u64, 1));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !next.is_finished() {
      if std::time::Instant::now() > deadline {
        panic!(
          "insert after panicking coster hung on insert_sem.acquire() — \
           the panic leaked the only permit (buffer_size=1). SyncPermitGuard \
           is missing its RAII release."
        );
      }
      sleep(Duration::from_millis(10));
    }
    assert!(
      next.join().unwrap(),
      "insert after panic recovery should succeed"
    );
  }

  /// Regression for Codex adversarial review Finding 1 (Update arm):
  /// pre-fix, the stale-generation Update handler removed the caller's
  /// eager store row but did not inspect policy. If a post-clear insert
  /// at the same key had already been admitted between clear and this
  /// handler, removing the store row orphaned that admission's policy
  /// entry — no live row, but the entry still consumed cost budget.
  ///
  /// Steer the processor queue directly so the race is deterministic:
  ///   1. Seed key K so policy tracks it and the store holds v_pre.
  ///   2. Mutate the store out-of-band (no Item::* in the queue) to
  ///      simulate a post-clear admission re-adding K at a new version
  ///      while policy's entry survived (captured_gen < current_gen).
  ///   3. Inject an Item::Update for the new version with a STALE
  ///      `generation` value (one less than current).
  ///   4. Process: the stale handler removes the store row. Pre-fix,
  ///      policy still contains K — a ghost.
  ///
  /// Fix: on successful stale-arm remove, if `store.contains_key(&key, 0)`
  /// is false, also `policy.remove(&key)`.
  #[test]
  fn test_sync_stale_update_ghost_policy_cleanup() {
    use crate::ttl::Time;
    use std::sync::atomic::Ordering as AOrd;

    let c: Cache<u64, u64, TransparentKeyBuilder<u64>> = Cache::builder(64, 1000)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    // Seed K so policy tracks it (a post-clear admission would look the
    // same from this handler's perspective).
    let k: u64 = 42;
    assert!(c.insert(k, 1u64, 1));
    c.wait().unwrap();
    assert!(
      c.0.policy.contains(&k),
      "seed must leave policy tracking K — test precondition"
    );

    // Out-of-band: overwrite the store row with a fresh version, as if a
    // post-clear insert had landed (for the stale handler's try_remove_if
    // _version to find a match). Direct mutation bypasses the insert
    // buffer, so no Item::* gets queued from this.
    let conflict_k = TransparentKeyBuilder::<u64>::default().hash_conflict(&k);
    let _prior = c
      .0
      .store
      .try_remove(&k, conflict_k)
      .unwrap()
      .expect("seeded row must be present");
    let current_gen = c.0.clear_generation.load(AOrd::Acquire);
    let new_version = c
      .0
      .store
      .try_insert(k, 2u64, conflict_k, Time::now(), current_gen)
      .unwrap()
      .expect("store should accept fresh insert");

    // Inject a stale Item::Update (generation=current-1) for the new row.
    // The handler's stale arm will `try_remove_if_version(k, conflict,
    // new_version)` — matches and removes. Pre-fix: policy still contains
    // K. Post-fix: policy entry is cleaned up because store is now empty
    // at this index.
    let stale_gen = current_gen.wrapping_sub(1);
    c.0
      .insert_buf_tx
      .send(Item::update(
        k,
        conflict_k,
        1,
        0,
        Time::now(),
        new_version,
        stale_gen,
      ))
      .unwrap();
    c.wait().unwrap();

    assert!(
      c.0.store.get(&k, 0).is_none(),
      "stale Update handler should have removed the store row"
    );
    assert!(
      !c.0.policy.contains(&k),
      "stale Update handler must wipe the ghost policy entry when the \
       store is empty at this index — Finding-1 regression"
    );
  }

  /// Regression for Codex adversarial review Finding 1 (Delete arm):
  /// pre-fix, the stale Delete handler returned early on generation
  /// mismatch without inspecting policy. If a post-clear insert at the
  /// same key had already been admitted between clear and this handler,
  /// the caller's eager `store.try_remove` had already torn down the
  /// post-clear row, orphaning policy's entry.
  ///
  /// Steer the processor queue directly: seed K (policy tracks it,
  /// store holds v_pre), wipe the store row out-of-band (simulating
  /// the caller's eager remove), then inject a stale Item::Delete.
  /// Pre-fix: policy still holds K (ghost). Post-fix: policy.remove.
  #[test]
  fn test_sync_stale_delete_ghost_policy_cleanup() {
    use std::sync::atomic::Ordering as AOrd;

    let c: Cache<u64, u64, TransparentKeyBuilder<u64>> = Cache::builder(64, 1000)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    let k: u64 = 77;
    assert!(c.insert(k, 1u64, 1));
    c.wait().unwrap();
    assert!(c.0.policy.contains(&k));

    // Out-of-band remove of the store row: simulates the caller's eager
    // `store.try_remove` having wiped the post-clear row that a
    // post-clear admission (surviving in policy) had briefly owned.
    let conflict_k = TransparentKeyBuilder::<u64>::default().hash_conflict(&k);
    let removed = c
      .0
      .store
      .try_remove(&k, conflict_k)
      .unwrap()
      .expect("seeded row must be present");
    let removed_version = removed.version;

    // Inject a stale Item::Delete with generation=current-1 and
    // version=removed_version (>0, so the handler's version==0 defense
    // does not short-circuit).
    let current_gen = c.0.clear_generation.load(AOrd::Acquire);
    let stale_gen = current_gen.wrapping_sub(1);
    c.0
      .insert_buf_tx
      .send(Item::delete(k, conflict_k, stale_gen, removed_version))
      .unwrap();
    c.wait().unwrap();

    assert!(
      c.0.store.get(&k, 0).is_none(),
      "store must remain empty at this index after the out-of-band remove"
    );
    assert!(
      !c.0.policy.contains(&k),
      "stale Delete handler must wipe the ghost policy entry when the \
       store is empty at this index — Finding-1 regression"
    );
  }

  /// Regression for Codex adversarial review finding: pre-fix, sync
  /// `try_insert_in` acquired the insert permit and then called
  /// `try_update`, which synchronously invokes user-controlled
  /// `Coster::cost` for cost=0 inserts. A Coster that re-enters the
  /// same cache via `insert()` would then try to acquire a second
  /// permit. Under `set_buffer_size(1)` the outer insert owned the only
  /// permit and had not enqueued anything yet, so the re-entrant
  /// caller's blocking `insert_sem.acquire()` hung forever.
  ///
  /// Fix: track permit ownership in the `INSERT_PERMIT_HELD` thread
  /// local. Re-entrant calls from the same thread fall through to
  /// `try_acquire` and fail-fast instead of blocking, and the same
  /// flag makes permit-owning `clear()`/`wait()`/`close()` calls
  /// return an error rather than deadlocking.
  #[test]
  fn test_sync_coster_reenters_cache_does_not_deadlock() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AOrd};

    struct ReenteringCoster {
      armed: Arc<AtomicBool>,
      fired: Arc<AtomicU64>,
      cache: parking_lot::Mutex<
        Option<
          Arc<
            Cache<
              u64,
              u64,
              TransparentKeyBuilder<u64>,
              ReenteringCoster,
              DefaultUpdateValidator<u64>,
              DefaultCacheCallback<u64>,
            >,
          >,
        >,
      >,
    }

    impl Coster for ReenteringCoster {
      type Value = u64;
      fn cost(&self, _v: &u64) -> i64 {
        if self.armed.swap(false, AOrd::AcqRel) {
          self.fired.fetch_add(1, AOrd::Relaxed);
          let cache_ref = self.cache.lock().clone();
          if let Some(c) = cache_ref {
            // Pre-fix: `c.insert` here called `insert_sem.acquire()`
            // which blocked forever because the outer insert (on this
            // same thread) already held the only permit. Post-fix:
            // `insert_permit_held()` is true so `try_acquire` is used
            // and the call returns false without deadlocking.
            let _ = c.insert(2u64, 200u64, 1);
          }
        }
        1
      }
    }

    let armed = Arc::new(AtomicBool::new(false));
    let fired = Arc::new(AtomicU64::new(0));

    let c: Cache<
      u64,
      u64,
      TransparentKeyBuilder<u64>,
      ReenteringCoster,
      DefaultUpdateValidator<u64>,
      DefaultCacheCallback<u64>,
    > = CacheBuilder::new_with_key_builder(1000, 1000, TransparentKeyBuilder::default())
      .set_coster(ReenteringCoster {
        armed: armed.clone(),
        fired: fired.clone(),
        cache: parking_lot::Mutex::new(None),
      })
      .set_buffer_size(1)
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    // Seed key 1 so the next insert hits the Update branch, where
    // `try_update` calls `Coster::cost` for the cost=0 path.
    assert!(c.insert(1u64, 100u64, 1));
    c.wait().unwrap();

    // Hand the cache to the Coster so it can re-enter. We need the
    // Coster to hold an Arc<Cache> to `insert`; wrap in an Arc and
    // stash it via the mutex we left open.
    let c_arc = Arc::new(c);
    *c_arc.0.coster.cache.lock() = Some(c_arc.clone());

    // Arm: the next cost=0 insert invokes Coster::cost, which then
    // calls c_arc.insert from inside the permit-held region.
    armed.store(true, AOrd::Release);

    // Run on a worker thread so the test survives an actual deadlock.
    let c_worker = c_arc.clone();
    let worker = spawn(move || c_worker.insert(1u64, 999u64, 0));

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !worker.is_finished() {
      if std::time::Instant::now() > deadline {
        panic!(
          "outer insert deadlocked — Coster::cost re-entered cache.insert() \
           while the outer call still held the only permit; re-entrant \
           insert blocked forever on insert_sem.acquire()"
        );
      }
      sleep(Duration::from_millis(10));
    }

    let res = worker.join().unwrap();
    assert!(res, "outer insert should succeed");
    assert_eq!(
      fired.load(AOrd::Relaxed),
      1,
      "Coster re-entry hook must have fired exactly once"
    );

    // Drop the self-reference we parked in the Coster so the Arc
    // refcount can drop to zero and trigger the cache's Drop/close.
    *c_arc.0.coster.cache.lock() = None;
  }

  #[test]
  fn test_sync_remove_from_coster_no_policy_leak() {
    // Regression for the re-entrant try_remove policy leak.
    //
    // Old behavior: a remove() called from inside Coster::cost (which
    // runs inside an OUTER insert's permit-held window) performed the
    // eager `store.try_remove` and THEN hit the `insert_permit_held()`
    // early-return. Policy still tracked the removed key's cost — a
    // phantom cost that silently bypassed `max_cost`.
    //
    // New behavior: `insert_permit_held()` is checked BEFORE the eager
    // remove, so the re-entrant call is a clean no-op. The key remains
    // in both store and policy, matching the drop-on-full semantics
    // used for re-entrant inserts.
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AOrd};

    struct RemoverCoster {
      armed: Arc<AtomicBool>,
      fired: Arc<AtomicU64>,
      cache: parking_lot::Mutex<
        Option<
          Arc<
            Cache<
              u64,
              u64,
              TransparentKeyBuilder<u64>,
              RemoverCoster,
              DefaultUpdateValidator<u64>,
              DefaultCacheCallback<u64>,
            >,
          >,
        >,
      >,
    }
    impl Coster for RemoverCoster {
      type Value = u64;
      fn cost(&self, _v: &u64) -> i64 {
        if self.armed.swap(false, AOrd::AcqRel) {
          self.fired.fetch_add(1, AOrd::Relaxed);
          let cache_ref = self.cache.lock().clone();
          if let Some(c) = cache_ref {
            // Pre-fix: eager store.try_remove(1) ran, then early-return
            // skipped the Item::Delete, so policy cost accounting kept
            // the removed key's 1 unit. Post-fix: early-return BEFORE
            // eager remove, so key 1 stays fully present.
            c.remove(&1u64);
          }
        }
        1
      }
    }

    let armed = Arc::new(AtomicBool::new(false));
    let fired = Arc::new(AtomicU64::new(0));

    let c: Cache<
      u64,
      u64,
      TransparentKeyBuilder<u64>,
      RemoverCoster,
      DefaultUpdateValidator<u64>,
      DefaultCacheCallback<u64>,
    > = CacheBuilder::new_with_key_builder(1000, 1000, TransparentKeyBuilder::default())
      .set_coster(RemoverCoster {
        armed: armed.clone(),
        fired: fired.clone(),
        cache: parking_lot::Mutex::new(None),
      })
      .set_buffer_size(1)
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    assert!(c.insert(1u64, 100u64, 1));
    c.wait().unwrap();
    assert_eq!(c.get(&1u64).unwrap().read(), 100u64);

    let c_arc = Arc::new(c);
    *c_arc.0.coster.cache.lock() = Some(c_arc.clone());

    // Arm: next Coster::cost invocation calls c.remove(&1) from inside
    // the outer insert's permit-held window.
    armed.store(true, AOrd::Release);

    // Trigger the re-entry via an Update path (cost=0 → Coster::cost).
    // The outer insert itself may be admitted or dropped; we only care
    // that the re-entrant remove stayed a safe no-op.
    let _ = c_arc.insert(1u64, 999u64, 0);
    c_arc.wait().unwrap();

    assert_eq!(
      fired.load(AOrd::Relaxed),
      1,
      "Coster re-entry must have fired exactly once"
    );

    // Key 1 must still be present: the re-entrant remove should be a
    // no-op (pre-fix it would have emptied the store row, risking a
    // policy phantom cost).
    assert!(
      c_arc.get(&1u64).is_some(),
      "re-entrant remove should be a no-op; key 1 must remain in the cache"
    );

    *c_arc.0.coster.cache.lock() = None;
  }

  /// Regression for a permit-leak when a user callback panics on the
  /// processor thread while clones of the cache remain alive. Before the
  /// SemCloser drop-guard, the panicked thread exited without releasing
  /// the item's semaphore permit, and buffered items dropped with the
  /// channel stranded theirs too. Subsequent `insert`/`clear`/`wait`
  /// calls on any live clone would block in `insert_sem.acquire()`
  /// forever. The guard now closes the semaphore on any processor exit
  /// (normal or panic), so those calls observe `SemaphoreClosed` and
  /// return their graceful-fallback value instead of hanging.
  #[test]
  fn test_sync_callback_panic_does_not_hang_live_clone() {
    struct PanicCB;
    impl CacheCallback for PanicCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {}
      fn on_evict(&self, _item: CrateItem<u64>) {
        panic!("intentional panic from on_evict (test harness)");
      }
    }

    let prior_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let c: Cache<u64, u64, TransparentKeyBuilder<u64>, _, _, PanicCB> =
      CacheBuilder::new_with_key_builder(64, 2, TransparentKeyBuilder::default())
        .set_callback(PanicCB)
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap();

    // Keep a live clone so the processor's exit does NOT also drop the
    // cache — the failure mode is strictly "live handle with dead
    // processor hangs on insert".
    let clone = c.clone();

    // Force eviction: on_evict panics on the processor thread.
    for i in 0..64u64 {
      let _ = c.insert(i, i, 1);
    }
    sleep(Duration::from_millis(200));

    // The regression: any of these would block indefinitely on
    // `insert_sem.acquire()` without the drop-guard.
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
      let _ = clone.insert(9_999u64, 0, 1);
      let _ = clone.wait();
      let _ = clone.clear();
      drop(clone);
      let _ = done_tx.send(());
    });

    match done_rx.recv_timeout(Duration::from_secs(5)) {
      Ok(()) => {}
      Err(_) => {
        std::panic::set_hook(prior_hook);
        panic!("live clone hung on insert_sem.acquire() after processor panic");
      }
    }
    drop(c);
    std::panic::set_hook(prior_hook);
  }

  /// Regression for the panic-safe shutdown fix (WaitGroup → JoinHandle):
  /// if a user `CacheCallback` panics on the processor thread, the thread
  /// unwinds past its normal drain/signal path. The old WaitGroup-based
  /// handshake would have skipped `done()` and left `Drop` parked on
  /// `wait()` forever. With `JoinHandle::join`, `drop` returns even on
  /// a panicked processor.
  #[test]
  fn test_sync_drop_completes_after_processor_callback_panics() {
    struct PanicCB;
    impl CacheCallback for PanicCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {}
      fn on_evict(&self, _item: CrateItem<u64>) {
        panic!("intentional panic from on_evict (test harness)");
      }
    }

    // Silence the panic backtrace while the processor unwinds. The panic
    // is intentional; we just don't want it polluting test output.
    let prior_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let c: Cache<u64, u64, TransparentKeyBuilder<u64>, _, _, PanicCB> =
      CacheBuilder::new_with_key_builder(64, 2, TransparentKeyBuilder::default())
        .set_callback(PanicCB)
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap();

    // Drive enough inserts to force eviction. The victim's `on_evict` fires
    // on the processor thread and panics, unwinding that thread.
    for i in 0..64u64 {
      let _ = c.insert(i, i, 1);
    }
    // Give the processor a moment to drain some items and hit the panic.
    sleep(Duration::from_millis(100));

    // Drop in a helper thread with a deadline. Hanging here is the
    // regression we're guarding against.
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
      drop(c);
      let _ = done_tx.send(());
    });

    match done_rx.recv_timeout(Duration::from_secs(5)) {
      Ok(()) => {}
      Err(_) => {
        std::panic::set_hook(prior_hook);
        panic!("Cache drop hung after processor thread panicked");
      }
    }
    std::panic::set_hook(prior_hook);
  }

  // Cloning Cache is cheap (Arc bump) and clones share backing state. After
  // the original is dropped, the clone keeps the processor alive and the
  // entries are still visible.
  #[test]
  fn test_sync_clone_shares_state() {
    let c: Cache<u64, u64, TransparentKeyBuilder<u64>> =
      CacheBuilder::new_with_key_builder(100, 10, TransparentKeyBuilder::default())
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap();

    let c2 = c.clone();
    assert!(c.insert(7, 42, 1));
    c.wait().unwrap();

    let v = c2
      .get(&7)
      .expect("clone must observe value inserted via original");
    assert_eq!(*v.value(), 42);
    drop(v);

    drop(c);
    // Clone keeps the cache alive: reading still works.
    let v = c2.get(&7).expect("cache must survive original drop");
    assert_eq!(*v.value(), 42);
  }
}

#[cfg(feature = "async")]
mod async_test {
  use super::*;
  use crate::{
    AsyncCache, AsyncCacheBuilder, DefaultCacheCallback, DefaultCoster, DefaultKeyBuilder,
    DefaultUpdateValidator, TransparentKeyBuilder, UpdateValidator,
  };
  use agnostic_lite::tokio::TokioRuntime;
  use std::{collections::hash_map::RandomState, hash::Hash, time::Duration};
  use tokio::{sync::mpsc::channel, task::spawn, time::sleep};

  async fn new_test_cache<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<Key = K>>(
    kh: KH,
  ) -> AsyncCache<K, V, KH> {
    AsyncCache::new_with_key_builder::<TokioRuntime>(100, 10, kh).unwrap()
  }

  async fn retry_set<
    C: Coster<Value = u64>,
    U: UpdateValidator<Value = u64>,
    CB: CacheCallback<Value = u64>,
  >(
    c: &AsyncCache<u64, u64, TransparentKeyBuilder<u64>, C, U, CB>,
    key: u64,
    val: u64,
    cost: i64,
    ttl: Duration,
  ) {
    loop {
      let insert = c.insert_with_ttl(key, val, cost, ttl).await;
      if !insert {
        sleep(Duration::from_millis(100)).await;
        continue;
      }
      sleep(Duration::from_millis(100)).await;
      assert_eq!(c.get(&key).await.unwrap().read(), val);
      return;
    }
  }

  #[tokio::test]
  async fn test_cache_builder() {
    let _: AsyncCache<u64, u64, DefaultKeyBuilder<u64>> =
      AsyncCacheBuilder::new_with_key_builder(100, 10, TransparentKeyBuilder::default())
        .set_coster(DefaultCoster::default())
        .set_update_validator(DefaultUpdateValidator::default())
        .set_callback(DefaultCacheCallback::default())
        .set_num_counters(200)
        .set_max_cost(100)
        .set_cleanup_duration(Duration::from_secs(1))
        .set_buffer_size(1000)
        .set_key_builder(DefaultKeyBuilder::default())
        .set_hasher(RandomState::default())
        .finalize::<TokioRuntime>()
        .unwrap();
  }

  #[tokio::test]
  async fn test_wait() {
    let max_cost = 10_000;
    let lru = AsyncCacheBuilder::new(max_cost * 10, max_cost as i64)
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .expect("failed to create cache");

    for i in 0..10_000 {
      println!("i = {i}, len before insert = {}", lru.len());

      let value = 123;
      let cost = 1;
      lru.insert(i, value, cost).await;
      lru.wait().await.unwrap(); // <-- freezes here
    }
  }

  #[tokio::test]
  async fn test_cache_key_to_hash() {
    let ctr = Arc::new(AtomicU64::new(0));

    let c: AsyncCache<u64, u64, KHTest> =
      AsyncCache::new_with_key_builder::<TokioRuntime>(10, 1000, KHTest { ctr: ctr.clone() })
        .unwrap();

    assert!(c.insert(1, 1, 1).await);
    sleep(Duration::from_millis(10)).await;

    loop {
      match c.get(&1).await {
        None => continue,
        Some(val) => {
          assert_eq!(val.read(), 1);
          c.remove(&1).await;
          assert_eq!(3, ctr.load(Ordering::SeqCst));
          break;
        }
      }
    }
  }

  #[tokio::test]
  async fn test_cache_update_max_cost() {
    let c = AsyncCache::builder(10, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(false)
      .finalize::<TokioRuntime>()
      .unwrap();

    assert_eq!(c.max_cost(), 10);
    assert!(c.insert(1, 1, 1).await);

    sleep(Duration::from_secs(1)).await;
    // Set is rejected because the cost of the entry is too high
    // when accounting for the internal cost of storing the entry.
    assert!(c.get(&1).await.is_none());

    // Update the max cost of the cache and retry.
    c.update_max_cost(1000);
    assert_eq!(c.max_cost(), 1000);
    assert!(c.insert(1, 1, 1).await);

    sleep(Duration::from_millis(200)).await;
    assert_eq!(c.get(&1).await.unwrap().read(), 1);
    c.remove(&1).await;
  }

  #[tokio::test]
  async fn test_cache_drop_is_safe() {
    let c: AsyncCache<i64, i64, TransparentKeyBuilder<i64>> =
      AsyncCache::new_with_key_builder::<TokioRuntime>(100, 10, TransparentKeyBuilder::default())
        .unwrap();

    drop(c);
  }

  #[tokio::test]
  async fn test_cache_process_items() {
    let cb = Arc::new(Mutex::new(HashSet::new()));
    let c = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_coster(TestCoster::default())
      .set_callback(TestCallback::new(cb.clone()))
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    assert!(c.insert(1, 1, 0).await);

    sleep(Duration::from_secs(1)).await;
    assert!(c.0.policy.contains(&1));
    assert_eq!(c.0.policy.cost(&1), 1);

    let _ = c.insert_if_present(1, 2, 0).await;
    sleep(Duration::from_secs(1)).await;
    assert_eq!(c.0.policy.cost(&1), 2);

    c.remove(&1).await;
    sleep(Duration::from_secs(1)).await;
    assert!(c.0.store.get(&1, 0).is_none());
    assert!(!c.0.policy.contains(&1));

    c.insert(2, 2, 3).await;
    c.insert(3, 3, 3).await;
    c.insert(4, 3, 3).await;
    c.insert(5, 3, 5).await;
    sleep(Duration::from_secs(1)).await;
    assert_ne!(cb.lock().len(), 0);
  }

  #[tokio::test]
  async fn test_cache_get() {
    let c = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .set_metrics(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    c.insert(1, 1, 0).await;
    sleep(Duration::from_secs(1)).await;
    match c.get_mut(&1).await {
      None => {}
      Some(mut val) => {
        val.write(10);
      }
    }

    assert!(c.get_mut(&2).await.is_none());

    // 0.5 and not 1.0 because we tried Getting each item twice
    assert_eq!(c.0.metrics.ratio().unwrap(), 0.5);

    assert_eq!(c.get_mut(&1).await.unwrap().read(), 10);
  }

  #[tokio::test]
  async fn test_cache_set() {
    let c = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .set_metrics(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    retry_set(&c, 1, 1, 1, Duration::ZERO).await;

    c.insert(1, 2, 2).await;
    assert_eq!(c.get(&1).await.unwrap().read(), 2);

    // Simulate the end-state of processor shutdown: the insert semaphore
    // has been closed by the processor's RAII SemCloser. Close it directly
    // because the processor and policy share `stop_rx`, so a single
    // `send(())` would race and only one consumer would wake.
    c.0.insert_sem.close();

    // Post-close insert bails at the permit acquire (semaphore closed) and
    // returns `Ok(false)` before touching metrics, so DropSets stays 0.
    // Pre-close data is preserved.
    assert!(!c.insert(2, 2, 1).await);
    assert_eq!(c.get(&1).await.unwrap().read(), 2);
    assert_eq!(c.0.metrics.sets_dropped().unwrap(), 0);
  }

  #[tokio::test]
  async fn test_cache_internal_cost() {
    let c = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(false)
      .set_metrics(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    // Get should return None because the cache's cost is too small to store the item
    // when accounting for the internal cost.
    c.insert_with_ttl(1, 1, 1, Duration::ZERO).await;
    sleep(Duration::from_millis(100)).await;
    assert!(c.get(&1).await.is_none())
  }

  #[tokio::test]
  async fn test_recache_with_ttl() {
    let c = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .set_metrics(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    // Set initial value for key = 1
    assert!(c.insert_with_ttl(1, 1, 1, Duration::from_secs(5)).await);

    sleep(Duration::from_secs(2)).await;

    // Get value from cache for key = 1
    assert_eq!(c.get(&1).await.unwrap().read(), 1);

    // wait for expiration
    sleep(Duration::from_secs(5)).await;

    // The cached value for key = 1 should be gone
    assert!(c.get(&1).await.is_none());

    // set new value for key = 1
    assert!(c.insert_with_ttl(1, 2, 1, Duration::from_secs(5)).await);

    sleep(Duration::from_secs(2)).await;
    // get value from cache for key = 1;
    assert_eq!(c.get(&1).await.unwrap().read(), 2);
  }

  #[tokio::test]
  async fn test_cache_set_with_ttl() {
    let cb = Arc::new(Mutex::new(HashSet::new()));
    let c = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_callback(TestCallback::new(cb.clone()))
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    retry_set(&c, 1, 1, 1, Duration::from_secs(1)).await;

    // Sleep to make sure the item has expired after execution resumes.
    sleep(Duration::from_secs(2)).await;
    assert!(c.get(&1).await.is_none());

    // Sleep to ensure that the bucket where the item was stored has been cleared
    // from the expiration map.
    sleep(Duration::from_secs(5)).await;
    assert_eq!(cb.lock().len(), 1);

    // Verify that expiration times are overwritten.
    retry_set(&c, 2, 1, 1, Duration::from_secs(1)).await;
    retry_set(&c, 2, 2, 1, Duration::from_secs(100)).await;
    sleep(Duration::from_secs(3)).await;
    assert_eq!(c.get(&2).await.unwrap().read(), 2);

    // Verify that entries with no expiration are overwritten.
    retry_set(&c, 3, 1, 1, Duration::ZERO).await;
    retry_set(&c, 3, 1, 1, Duration::from_secs(1)).await;
    sleep(Duration::from_secs(3)).await;
    assert!(c.get(&3).await.is_none());
  }

  #[tokio::test]
  async fn test_cache_remove() {
    let c = new_test_cache(TransparentKeyBuilder::default()).await;

    c.insert(1, 1, 1).await;
    c.remove(&1).await;

    // The deletes and sets are pushed through the setbuf. It might be possible
    // that the delete is not processed before the following get is called. So
    // wait for a millisecond for things to be processed.
    sleep(Duration::from_millis(1)).await;
    assert!(c.get(&1).await.is_none());
  }

  #[tokio::test]
  async fn test_cache_remove_with_ttl() {
    let c = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    retry_set(&c, 3, 1, 1, Duration::from_secs(10)).await;
    sleep(Duration::from_secs(1)).await;

    // remove the item
    c.remove(&3).await;

    // ensure the key is deleted
    assert!(c.get(&3).await.is_none());
  }

  #[tokio::test]
  async fn test_cache_get_ttl() {
    let c = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_metrics(true)
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    // try expiration with valid ttl item
    {
      let expiration = Duration::from_secs(5);
      retry_set(&c, 1, 1, 1, expiration).await;

      assert_eq!(c.get(&1).await.unwrap().read(), 1);
      assert!(c.get_ttl(&1).unwrap() < expiration);

      c.remove(&1).await;

      assert!(c.get_ttl(&1).is_none());
    }

    // try expiration with no ttl
    {
      retry_set(&c, 2, 2, 1, Duration::ZERO).await;
      assert_eq!(c.get(&2).await.unwrap().read(), 2);
      assert_eq!(c.get_ttl(&2).unwrap(), Duration::MAX);
    }

    // try expiration with missing item
    {
      assert!(c.get_ttl(&3).is_none());
    }

    // try expiration with expired item
    {
      let expiration = Duration::from_secs(1);
      retry_set(&c, 3, 3, 1, expiration).await;

      assert_eq!(c.get(&3).await.unwrap().read(), 3);
      sleep(Duration::from_secs(1)).await;
      assert!(c.get_ttl(&3).is_none());
    }
  }

  #[tokio::test]
  async fn test_cache_clear() {
    let c = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_metrics(true)
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    for i in 0..10 {
      c.insert(i, i, 1).await;
    }
    sleep(Duration::from_millis(100)).await;
    assert_eq!(c.0.metrics.keys_added(), Some(10));
    c.clear().await.unwrap();
    assert_eq!(c.0.metrics.keys_added(), Some(0));

    for i in 0..10 {
      assert!(c.get(&i).await.is_none());
    }
  }

  #[tokio::test]
  async fn test_cache_metrics_clear() {
    let c = Arc::new(
      AsyncCache::builder(100, 10)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_metrics(true)
        .finalize::<TokioRuntime>()
        .unwrap(),
    );

    c.insert(1, 1, 1).await;

    let (stop_tx, mut stop_rx) = channel(1);
    let tc = c.clone();
    spawn(async move {
      loop {
        tokio::select! {
            _ = stop_rx.recv() => return,
            else => {
                let _ = tc.get(&1).await;
            }
        }
      }
    });

    sleep(Duration::from_millis(100)).await;
    let _ = c.clear().await;
    stop_tx.send(()).await.unwrap();
    c.0.metrics.clear();
  }

  // Regression test for bug https://github.com/dgraph-io/ristretto/issues/167
  #[tokio::test]
  async fn test_cache_drop_updates() {
    async fn test() {
      let set = Arc::new(Mutex::new(HashSet::new()));
      let c = AsyncCache::builder(100, 10)
                .set_callback(TestCallbackDropUpdates { set: set.clone() })
                .set_metrics(true)
                // This is important. The race condition shows up only when the insert buf
                // is full and that's why we reduce the buf size here. The test will
                // try to fill up the insert buf to it's capacity and then perform an
                // update on a key.
                .set_buffer_size(10)
                .finalize::<TokioRuntime>()
                .unwrap();

      for i in 0..50 {
        let v = format!("{:0100}", i);
        // We're updating the same key.
        if !c.insert(0, v, 1).await {
          // The race condition doesn't show up without this sleep.
          sleep(Duration::from_millis(1)).await;
          set.lock().insert(i);
        }
      }

      // Wait for all the items to be processed.
      sleep(Duration::from_millis(1)).await;
      // This will cause eviction from the cache.
      assert!(c.insert(1, "0".to_string(), 10).await);
    }

    // Run the test 100 times since it's not reliable.
    for _ in 0..100 {
      test().await;
    }
  }

  #[tokio::test]
  async fn test_cache_with_ttl() {
    let mut process_win = 0;
    let mut clean_win = 0;

    for _ in 0..10 {
      let c = AsyncCache::builder(100, 1000)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_metrics(true)
        .finalize::<TokioRuntime>()
        .unwrap();

      // Set initial value for key = 1
      assert!(c.insert_with_ttl(1, 1, 0, Duration::from_millis(800)).await);

      sleep(Duration::from_millis(100)).await;

      // Get value from cache for key = 1
      match c.get(&1).await {
        None => {
          clean_win += 1;
        }
        Some(_) => {
          process_win += 1;
        }
      }
      // assert_eq!(c.get(&1).unwrap().read(), 1);

      sleep(Duration::from_millis(1200)).await;
      assert!(c.get(&1).await.is_none());
    }
    eprintln!("process: {} cleanup: {}", process_win, clean_win);
  }

  #[tokio::test]
  async fn test_cache_max_cost() {
    let c = Arc::new(
      AsyncCache::builder(12960, 1e6 as i64)
        .set_metrics(true)
        .finalize::<TokioRuntime>()
        .unwrap(),
    );

    let mut txs = Vec::new();
    let mut rxs = Vec::new();

    (0..8).for_each(|_| {
      let (stop_tx, stop_rx) = channel::<()>(1);
      txs.push(stop_tx);
      rxs.push(stop_rx);
    });

    let tc = c.clone();
    spawn(async move {
      for _ in 0..20 {
        sleep(Duration::from_millis(500)).await;
        let (cost_added, cost_evicted) = (
          tc.0.metrics.cost_added().unwrap(),
          tc.0.metrics.cost_evicted().unwrap(),
        );
        let cost = cost_added - cost_evicted;
        assert!(cost as f64 <= (1e6 * 1.05));
      }
      tc.wait().await.unwrap();
      for tx in txs {
        let _ = tx.send(()).await;
      }
    })
    .await
    .unwrap();

    for mut rx in rxs {
      loop {
        match rx.try_recv() {
          Ok(_) => break,
          Err(_) => {
            let k = get_key();
            if c.get(&k).await.is_none() {
              let rv = rand::random::<u64>() as usize % 100;
              let val = if rv < 10 {
                "test".to_string()
              } else {
                vec!["a"; 1000].join("")
              };
              let cost = val.len() + 2;
              assert!(c.insert(get_key(), val, cost as i64).await);
            }
          }
        }
      }
    }
  }

  #[tokio::test]
  async fn test_cache_blockon_clear() {
    let c: Arc<AsyncCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
      AsyncCache::builder(100, 10)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_ignore_internal_cost(true)
        .finalize::<TokioRuntime>()
        .unwrap(),
    );

    let (stop_tx, mut stop_rx) = channel(1);

    let tc = c.clone();
    spawn(async move {
      for _ in 0..10 {
        let tc = tc.clone();
        tc.wait().await.unwrap();
      }
      stop_tx.send(()).await.unwrap();
    });

    for _ in 0..10 {
      c.clear().await.unwrap();
    }

    let sleep = sleep(Duration::from_secs(1));
    tokio::pin!(sleep);

    tokio::select! {
        _ = stop_rx.recv() => {},
        _ = &mut sleep => {
            panic!("timed out while waiting on cache")
        }
    }
  }

  #[tokio::test]
  async fn test_insert_after_clear() {
    let ttl = Duration::from_secs(60);
    let c: AsyncCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    assert!(c.insert_with_ttl(0, 1, 1, ttl).await);
    assert!(c.wait().await.is_ok());
    assert_eq!(c.get(&0).await.unwrap().value(), &1);
    assert!(c.clear().await.is_ok());
    assert!(c.wait().await.is_ok());
    assert!(c.get(&0).await.is_none());
    assert!(c.insert_with_ttl(2, 3, 1, ttl).await);
    assert!(c.wait().await.is_ok());
    assert_eq!(c.get(&2).await.unwrap().value(), &3);
  }

  #[tokio::test]
  async fn test_async_finalize_errors() {
    let err = match AsyncCacheBuilder::<u64, u64>::new(0, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .finalize::<TokioRuntime>()
    {
      Ok(_) => panic!("expected error"),
      Err(e) => e,
    };
    assert!(matches!(err, crate::CacheError::InvalidNumCounters));

    let err = match AsyncCacheBuilder::<u64, u64>::new(10, 0)
      .set_key_builder(TransparentKeyBuilder::default())
      .finalize::<TokioRuntime>()
    {
      Ok(_) => panic!("expected error"),
      Err(e) => e,
    };
    assert!(matches!(err, crate::CacheError::InvalidMaxCost));

    let err = match AsyncCacheBuilder::<u64, u64>::new(10, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_buffer_size(0)
      .finalize::<TokioRuntime>()
    {
      Ok(_) => panic!("expected error"),
      Err(e) => e,
    };
    assert!(matches!(err, crate::CacheError::InvalidBufferSize));
  }

  #[tokio::test]
  async fn test_async_set_buffer_items_and_len() {
    let c: AsyncCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_buffer_items(32)
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    assert!(c.is_empty());
    assert_eq!(c.len(), 0);

    assert!(c.insert(1, 1, 1).await);
    c.wait().await.unwrap();
    assert_eq!(c.len(), 1);
    assert!(!c.is_empty());
  }

  #[tokio::test]
  async fn test_async_as_ref() {
    let c: AsyncCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();
    let r: &AsyncCache<u64, u64, TransparentKeyBuilder<u64>> = c.as_ref();
    assert_eq!(r.max_cost(), 10);
  }

  #[tokio::test]
  async fn test_async_insert_if_present_missing() {
    let c: AsyncCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();
    assert!(!c.insert_if_present(42, 0, 1).await);
    c.wait().await.unwrap();
    assert!(c.get(&42).await.is_none());
  }

  #[tokio::test]
  async fn test_async_ring_overflow_and_fill() {
    let c: AsyncCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_buffer_items(4)
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    for _ in 0..200 {
      for k in 0..16u64 {
        let _ = c.get(&k).await;
      }
    }
  }

  #[tokio::test]
  async fn test_async_clear_then_insert_preserves_writes() {
    // Regression: clear() used to signal the processor via a separate channel,
    // so inserts issued immediately after clear() returned could be drained
    // away by the clear handler before admission. The ordered-clear marker
    // routes clear through the insert buffer, so inserts enqueued after
    // clear() returns are processed against the freshly cleared state.
    for _ in 0..32 {
      let c: AsyncCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCache::builder(100, 100)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_ignore_internal_cost(true)
        .finalize::<TokioRuntime>()
        .unwrap();
      for i in 0..10u64 {
        c.insert(i, i, 1).await;
      }
      c.wait().await.unwrap();

      c.clear().await.unwrap();
      // No intervening wait() — this is the exact race the fix closes.
      for i in 100..110u64 {
        c.insert(i, i, 1).await;
      }
      c.wait().await.unwrap();

      for i in 0..10u64 {
        assert!(
          c.get(&i).await.is_none(),
          "pre-clear key {} survived clear",
          i,
        );
      }
      for i in 100..110u64 {
        let v = c
          .get(&i)
          .await
          .unwrap_or_else(|| panic!("post-clear insert of {} was lost", i));
        assert_eq!(v.read(), i);
      }
    }
  }

  // Async analogue of test_sync_clear_wipes_ttl_buckets. Same hazard:
  // clear() left stale ExpirationMap buckets pointing at the pre-clear
  // conflict, so a post-clear reinsert of the same key (zero or nonzero
  // TTL) could be deleted by the next cleanup tick.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn test_async_clear_wipes_ttl_buckets() {
    let c: AsyncCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCache::builder(100, 100)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .set_cleanup_duration(Duration::from_millis(50))
      .finalize::<TokioRuntime>()
      .unwrap();

    assert!(
      c.insert_with_ttl(1u64, 100u64, 1, Duration::from_millis(200))
        .await
    );
    c.wait().await.unwrap();
    assert!(c.get(&1).await.is_some());

    c.clear().await.unwrap();

    assert!(c.insert(1u64, 999u64, 1).await);
    c.wait().await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let v = c
      .get(&1)
      .await
      .expect("post-clear zero-TTL row must survive TTL-bucket cleanup");
    assert_eq!(v.read(), 999);
  }

  // Async analogue of test_sync_clear_fires_on_exit. See that test for the
  // motivating contract: on_exit must fire once per drained entry during clear().
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn test_async_clear_fires_on_exit() {
    use std::sync::atomic::{AtomicU64, Ordering as AOrd};

    struct CountingCB {
      on_exit: Arc<AtomicU64>,
    }
    impl CacheCallback for CountingCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {
        self.on_exit.fetch_add(1, AOrd::Relaxed);
      }
    }

    let on_exit_count = Arc::new(AtomicU64::new(0));
    let c: AsyncCache<
      u64,
      u64,
      TransparentKeyBuilder<u64>,
      DefaultCoster<u64>,
      DefaultUpdateValidator<u64>,
      CountingCB,
    > = AsyncCacheBuilder::new_with_key_builder(1000, 1000, TransparentKeyBuilder::default())
      .set_callback(CountingCB {
        on_exit: on_exit_count.clone(),
      })
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    for i in 0..50u64 {
      assert!(c.insert(i, i * 10, 1).await);
    }
    c.wait().await.unwrap();
    assert_eq!(c.len(), 50);
    assert_eq!(on_exit_count.load(AOrd::Relaxed), 0);

    c.clear().await.unwrap();

    assert_eq!(
      on_exit_count.load(AOrd::Relaxed),
      50,
      "on_exit must fire once per live entry drained by clear()",
    );
    assert_eq!(c.len(), 0);
  }

  // Async analogue of the sync concurrent-clear stress. Same rationale:
  // ensures post-race inserts remain admissible even after many racing
  // inserts and clears, which can only hold if the generation gate rejects
  // admission requests whose eager store write was wiped by a later Clear.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_async_concurrent_clear_and_insert_consistency() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, Ordering as AOrd},
    };

    for _ in 0..4 {
      let c: Arc<AsyncCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        AsyncCacheBuilder::new(500, 5_000)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .finalize::<TokioRuntime>()
          .unwrap(),
      );

      let stop = Arc::new(AtomicBool::new(false));
      let mut handles = Vec::new();
      for tid in 0..4u64 {
        let c2 = c.clone();
        let stop2 = stop.clone();
        handles.push(tokio::spawn(async move {
          let base = tid * 1_000_000;
          let mut k = 0u64;
          while !stop2.load(AOrd::Relaxed) {
            c2.insert(base + k, base + k, 1).await;
            k = (k + 1) % 10_000;
          }
        }));
      }

      let c3 = c.clone();
      let stop3 = stop.clone();
      let clearer = tokio::spawn(async move {
        for _ in 0..20 {
          sleep(Duration::from_millis(2)).await;
          if stop3.load(AOrd::Relaxed) {
            break;
          }
          c3.clear().await.unwrap();
        }
      });

      clearer.await.unwrap();
      stop.store(true, AOrd::Relaxed);
      for h in handles {
        h.await.unwrap();
      }
      c.wait().await.unwrap();

      c.clear().await.unwrap();
      c.wait().await.unwrap();
      for i in 0..50u64 {
        c.insert(i, i, 1).await;
      }
      c.wait().await.unwrap();
      for i in 0..50u64 {
        let v = c
          .get(&i)
          .await
          .unwrap_or_else(|| panic!("post-race insert {} was lost", i));
        assert_eq!(v.read(), i);
      }
    }
  }

  // Async analogue of the sync clear-generation ordering regression.
  // Guards the same invariant — after a clear/insert race and a drain,
  // the policy must not hold any key whose store row has been wiped.
  // See `test_sync_clear_bump_vs_wipe_ordering` for the full scenario;
  // the bug lives in the shared Item::Clear handler, so both runtimes
  // must exercise it.
  //
  // Each round fires exactly one clear() while per-round-unique keys
  // are sprayed in concurrently. Checking policy-vs-store before the
  // next round wipes the evidence is essential: subsequent clears
  // erase both the live rows AND any ghost policy entries.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_async_clear_bump_vs_wipe_ordering() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, Ordering as AOrd},
    };

    const THREADS: u64 = 8;
    const INSERTS_PER_THREAD: u64 = 40_000;
    let mut total_ghosts = 0usize;

    for round in 0..10u64 {
      let c: Arc<AsyncCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        AsyncCacheBuilder::new(20_000, 1_000_000)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .finalize::<TokioRuntime>()
          .unwrap(),
      );

      let start_signal = Arc::new(AtomicBool::new(false));
      let mut handles = Vec::new();
      for tid in 0..THREADS {
        let c2 = c.clone();
        let start = start_signal.clone();
        let round_base = round * THREADS * INSERTS_PER_THREAD;
        handles.push(tokio::spawn(async move {
          while !start.load(AOrd::Acquire) {
            tokio::task::yield_now().await;
          }
          let base = round_base + tid * INSERTS_PER_THREAD;
          for i in 0..INSERTS_PER_THREAD {
            let k = base + i;
            c2.insert(k, k, 1).await;
          }
        }));
      }

      let c3 = c.clone();
      let start = start_signal.clone();
      let clearer = tokio::spawn(async move {
        start.store(true, AOrd::Release);
        sleep(Duration::from_micros(50)).await;
        c3.clear().await.unwrap();
      });

      clearer.await.unwrap();
      for h in handles {
        h.await.unwrap();
      }
      c.wait().await.unwrap();

      for tid in 0..THREADS {
        let base = round * THREADS * INSERTS_PER_THREAD + tid * INSERTS_PER_THREAD;
        for i in 0..INSERTS_PER_THREAD {
          let index = base + i;
          if c.0.policy.contains(&index) && c.0.store.get(&index, 0).is_none() {
            total_ghosts += 1;
          }
        }
      }
    }
    assert_eq!(
      total_ghosts, 0,
      "policy holds {} ghost admissions across 10 clear/insert races — \
       clear handler bumped clear_generation before wiping store/policy",
      total_ghosts,
    );
  }

  // Regression for the async cancellation leak: when `insert()` is wrapped
  // in `tokio::time::timeout` (or otherwise dropped mid-await on a
  // saturated insert buffer), the eager store write performed by
  // `try_update` used to leak — the send future was dropped before
  // enqueuing the Item::New, so the store held a row the policy never
  // admitted and max-cost accounting never saw. `EagerInsertGuard` rolls
  // the eager mutation back on drop.
  //
  // We force saturation with `set_insert_buffer_size(1)` plus many
  // concurrent senders, and a tight 1ms per-insert timeout ensures most
  // futures are cancelled while blocked on send. If the guard is missing,
  // the store accumulates orphan rows uncapped by max_cost; with the
  // guard, store size stays within max_cost.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_async_insert_cancellation_does_not_leak() {
    const MAX_COST: i64 = 100;
    const N: u64 = 5_000;

    let c: Arc<AsyncCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
      AsyncCacheBuilder::new(1000, MAX_COST)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_ignore_internal_cost(true)
        .set_buffer_size(1)
        .finalize::<TokioRuntime>()
        .unwrap(),
    );

    let mut handles = Vec::new();
    for k in 0..N {
      let c2 = c.clone();
      handles.push(tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_millis(1), c2.insert(k, k, 1)).await;
      }));
    }
    for h in handles {
      let _ = h.await;
    }

    c.wait().await.unwrap();

    // Without the guard, cancelled inserts leave eager store writes that
    // never pass through policy admission or cost accounting, so the store
    // grows unbounded past max_cost. With the guard, every cancellation
    // rolls back its eager write and the store stays within the policy's
    // cost bound (plus a small slack for legitimate admissions that
    // haven't been evicted yet; the key signal is that len() doesn't
    // blow past max_cost by orders of magnitude).
    assert!(
      (c.len() as i64) <= MAX_COST * 2,
      "store size {} exceeds 2x max_cost {} — cancellation likely leaked eager inserts",
      c.len(),
      MAX_COST,
    );
  }

  // Retired: `test_async_update_cancellation_preserves_value`.
  //
  // This test verified that cancelling an `AsyncCache::insert` that had
  // already completed its eager `store.try_update` (Update path) did not
  // remove the newly written row via `EagerInsertGuard::drop`. It wedged
  // the processor with `stop_tx.send(())`, filled the insert buffer, then
  // aborted a task parked on `insert_buf_tx.send(...).await` to force the
  // guard's drop to fire.
  //
  // Two independent changes made this scenario unreachable:
  //   1. `try_insert_in` now acquires an `insert_sem` permit before the
  //      eager `store.try_update` and uses `try_send` (not `send().await`)
  //      under the permit invariant. There is no `.await` point between
  //      the eager write and the enqueue, so a cancellation cannot occur
  //      mid-update. The only remaining cancellation window is
  //      `insert_sem.acquire().await`, which is cancellation-safe: no
  //      state has been mutated yet.
  //   2. `stop_tx.send(())` now drives the full close contract — drain,
  //      clear store/metrics/policy/start_ts, and shut down the policy
  //      processor — so using it to "pause" the processor additionally
  //      wipes the state the test was asserting about.
  //
  // The invariant the test guarded (cancelled updates do not destroy live
  // data) still holds by construction: no cancellation point exists where
  // an Update can leave a ghost write. No active regression exists.

  // Retired: `test_async_cancelled_update_reconciles_policy_cost` and
  // `test_async_cancelled_update_admits_via_fallthrough_when_new_skipped`.
  //
  // Both tests wedged the processor on `policy.inner` while queueing
  // inserts, then aborted a parked `insert_buf_tx.send(...).await` to
  // exercise the `EagerInsertGuard`'s Drop-based reconcile/fallthrough
  // path. The insert-buffer semaphore (see src/semaphore.rs) now gates
  // every caller BEFORE the eager `store.try_update`; the only remaining
  // cancellation point in `try_insert_in` is `insert_sem.acquire().await`,
  // which is cancellation-safe (drop-before-grant leaves no state to
  // undo). The "cancelled between eager write and send" window those
  // tests covered is therefore structurally unreachable, and the
  // Drop-based reconcile/fallthrough paths they exercised cannot fire
  // from cancellation anymore. Under the new design the wedged
  // processor also prevents any caller from even acquiring a permit,
  // so the test's `c.insert(..).await` setup blocks indefinitely
  // instead of filling the buffer. The equivalent guarantee — that
  // store growth is bounded by the permit pool and cannot bypass
  // `max_cost` via cancellation — is covered by
  // `test_async_insert_cancellation_does_not_leak`.

  #[tokio::test]
  async fn test_async_reject_update() {
    struct NoUpdate;
    impl UpdateValidator for NoUpdate {
      type Value = u64;
      fn should_update(&self, _prev: &u64, _curr: &u64) -> bool {
        false
      }
    }

    let c = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_update_validator(NoUpdate)
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    assert!(c.insert(1u64, 1u64, 1).await);
    c.wait().await.unwrap();
    let _ = c.insert(1u64, 2u64, 1).await;
    c.wait().await.unwrap();
    assert_eq!(c.get(&1).await.unwrap().read(), 1);
  }

  // Regression test for https://github.com/al8n/stretto/issues/55 — the async
  // cleaner had the same single-bucket-per-tick bug as sync. See
  // `test_sync_ttl_cleanup_drains_all_buckets` for the underlying reason.
  #[tokio::test]
  async fn test_async_ttl_cleanup_drains_all_buckets() {
    const N: u64 = 200;

    let c = AsyncCacheBuilder::new(1000, 10_000)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    for i in 0..N {
      assert!(c.insert_with_ttl(i, i, 1, Duration::from_secs(1)).await);
      if i % 60 == 0 {
        sleep(Duration::from_millis(1000)).await;
      }
    }
    c.wait().await.unwrap();

    sleep(Duration::from_secs(8)).await;

    let mut leftover = 0u64;
    for k in 0..N {
      if c.get(&k).await.is_some() {
        leftover += 1;
      }
    }
    assert_eq!(
      leftover, 0,
      "expected all TTL entries to be cleaned up, {} remained",
      leftover
    );
    assert_eq!(c.len(), 0, "store should be empty after cleanup");
  }

  // Regression test for https://github.com/al8n/stretto/issues/32 — inserting a
  // `Box<dyn Any + Send + Sync>` into an AsyncCache inside an async context used
  // to fail to compile with a higher-ranked lifetime error. The agnostic-lite
  // migration removed the internal Box<dyn Future> that caused the HRTB, so this
  // now type-checks and round-trips the value.
  #[tokio::test]
  async fn test_async_insert_box_dyn_any() {
    use std::any::Any;

    let c: AsyncCache<String, Box<dyn Any + Send + Sync>> = AsyncCacheBuilder::new(100, 10)
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    let key = "session".to_string();
    let value: Box<dyn Any + Send + Sync> = Box::new(42u64);
    assert!(c.insert(key.clone(), value, 1).await);
    c.wait().await.unwrap();

    let got = c.get(&key).await.expect("value should be present");
    let boxed: &Box<dyn Any + Send + Sync> = got.value();
    assert_eq!(boxed.downcast_ref::<u64>().copied(), Some(42));
  }

  // Regression test: under 16 concurrent tokio clients each inserting thousands
  // of items, the policy-processor task used to be starved by the client tasks
  // — the `default =>` arm in try_insert_in fired for every full-buffer send
  // and silently dropped the item. In mokabench that manifested as <3 % hit
  // ratio on S3/DS1 at n=16. The backpressure fix (awaiting send) turns this
  // into natural rate-limiting; dropped-set count must stay near zero and the
  // cache must actually hold a meaningful fraction of the working set.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_async_insert_no_starvation_under_high_concurrency() {
    use std::sync::Arc;

    let cache: AsyncCache<u64, u64> = AsyncCacheBuilder::new(100_000, 10_000)
      .set_metrics(true)
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    const CLIENTS: u64 = 16;
    const PER_CLIENT: u64 = 5_000;

    let cache = Arc::new(cache);
    let mut handles = Vec::with_capacity(CLIENTS as usize);
    for client in 0..CLIENTS {
      let c = Arc::clone(&cache);
      handles.push(tokio::spawn(async move {
        let base = client * PER_CLIENT;
        for i in 0..PER_CLIENT {
          c.insert(base + i, base + i, 1).await;
        }
      }));
    }
    for h in handles {
      h.await.unwrap();
    }
    cache.wait().await.unwrap();

    let total = CLIENTS * PER_CLIENT;
    let dropped = cache.0.metrics.sets_dropped().unwrap_or(0);
    // Backpressure makes dropped-sets essentially zero; pre-fix this was ~total.
    assert!(
      dropped < total / 20,
      "too many dropped sets: {dropped} of {total}",
    );
  }

  // Async analogue of `test_sync_absent_remove_vs_insert_no_ghost`. See that
  // test for the full rationale. Both cache variants share the Delete
  // handler in src/cache.rs, so the bug surfaces in both.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_async_absent_remove_vs_insert_no_ghost() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, Ordering as AOrd},
    };

    const ROUNDS: u64 = 10;
    const KEYS_PER_ROUND: u64 = 5_000;
    let mut total_ghosts = 0usize;

    for round in 0..ROUNDS {
      let c: Arc<AsyncCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        AsyncCacheBuilder::new(20_000, 1_000_000)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .finalize::<TokioRuntime>()
          .unwrap(),
      );

      let base = round * KEYS_PER_ROUND;
      let start = Arc::new(AtomicBool::new(false));

      let c_ins = c.clone();
      let start_ins = start.clone();
      let inserter = tokio::spawn(async move {
        while !start_ins.load(AOrd::Acquire) {
          tokio::task::yield_now().await;
        }
        for k in base..base + KEYS_PER_ROUND {
          c_ins.insert(k, k, 1).await;
        }
      });

      let c_rem = c.clone();
      let start_rem = start.clone();
      let remover = tokio::spawn(async move {
        while !start_rem.load(AOrd::Acquire) {
          tokio::task::yield_now().await;
        }
        for k in base..base + KEYS_PER_ROUND {
          c_rem.remove(&k).await;
        }
      });

      start.store(true, AOrd::Release);
      inserter.await.unwrap();
      remover.await.unwrap();
      c.wait().await.unwrap();

      for k in base..base + KEYS_PER_ROUND {
        if c.0.policy.contains(&k) && c.0.store.get(&k, 0).is_none() {
          total_ghosts += 1;
        }
      }
    }
    assert_eq!(
      total_ghosts, 0,
      "policy holds {} ghost admissions after absent-remove vs insert races — \
       Item::Delete handler removed policy state of a concurrent admission",
      total_ghosts,
    );
  }

  /// Regression for Codex adversarial review finding: pre-fix, async
  /// `try_remove` ran the eager `store.try_remove` BEFORE awaiting
  /// `insert_sem.acquire()`. That await is a cancellation point. If the
  /// remove future is aborted / times out while parked on acquire, no
  /// `Item::Delete` is ever enqueued, yet the store row is already gone:
  /// policy keeps charging/tracking a key whose value has vanished.
  /// Under tight `max_cost` this phantom accounting can reject or evict
  /// legitimate later inserts until a clear or same-key admission
  /// repairs it.
  ///
  /// Fix: acquire the insert permit FIRST, so the only cancellation
  /// point is before any store/policy mutation. After the acquire, the
  /// rest of `try_remove` has no further `.await`, so cancellation
  /// cannot split the store mutation from its Delete.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn test_async_try_remove_cancel_mid_acquire_no_ghost() {
    // `buffer_size=1` means exactly one permit. Stealing it deterministically
    // parks every subsequent `insert_sem.acquire().await`.
    let c: Arc<AsyncCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
      AsyncCacheBuilder::new(1000, 1000)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_buffer_size(1)
        .set_ignore_internal_cost(true)
        .finalize::<TokioRuntime>()
        .unwrap(),
    );

    // Seed K so `try_remove` has something to remove on the post-acquire
    // fast path (this is the branch where pre-fix stranded policy state).
    assert!(c.insert(1u64, 100u64, 1).await);
    c.wait().await.unwrap();
    assert!(c.0.policy.contains(&1u64));
    assert!(c.0.store.get(&1u64, 0).is_some());

    // Steal the sole permit. Subsequent acquirers park indefinitely.
    c.0.insert_sem.acquire().await.unwrap();

    // Spawn `try_remove` on its own task so we can abort it while it is
    // parked at `insert_sem.acquire().await`.
    let c_rem = c.clone();
    let handle = tokio::spawn(async move { c_rem.try_remove(&1u64).await });

    // Give the spawned task time to reach the acquire await point.
    sleep(Duration::from_millis(100)).await;

    // Abort mid-acquire. This is the exact cancellation scenario the
    // finding describes: a cancelled/timed-out caller while parked on
    // acquire. Pre-fix: the eager `store.try_remove` had already run, so
    // the store row was already gone, while the policy entry survived
    // because the Delete was never enqueued.
    handle.abort();
    let _ = handle.await;

    // Release the permit we stole so the processor-side accounting
    // resumes; otherwise subsequent inserts would hang.
    c.0.insert_sem.release();

    // Drain any in-flight work the processor still has.
    c.wait().await.unwrap();

    // Post-fix invariant: cancellation at the acquire point touches no
    // state, so the key must still be present in BOTH store and policy
    // (the remove never happened at all from the cache's perspective).
    //
    // Pre-fix: `store.get` returned None while `policy.contains` returned
    // true — a ghost policy entry with no row behind it.
    let in_store = c.0.store.get(&1u64, 0).is_some();
    let in_policy = c.0.policy.contains(&1u64);
    assert!(
      in_store,
      "cancelled try_remove must not have completed the eager store \
       remove before the acquire await — store is empty at K=1"
    );
    assert!(
      in_policy,
      "cancelled try_remove must leave policy state intact"
    );
    assert_eq!(
      in_store, in_policy,
      "async try_remove aborted mid-acquire stranded cache state: \
       store={} / policy={} — Finding 2 regression",
      in_store, in_policy,
    );
  }

  // Async analogue of `test_sync_present_remove_vs_reinsert_keeps_value`.
  // See that test for the full rationale — the race is in the shared New /
  // Delete handlers in src/cache.rs, so both variants exercise it.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_async_present_remove_vs_reinsert_keeps_value() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, Ordering as AOrd},
    };

    const ROUNDS: u64 = 8;
    const KEYS_PER_ROUND: u64 = 5_000;
    const REINSERT_VAL: u64 = 0xDEAD_BEEF;
    let mut missing = 0usize;
    let mut ghosts = 0usize;

    for round in 0..ROUNDS {
      let c: Arc<AsyncCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        AsyncCacheBuilder::new(20_000, 1_000_000)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .finalize::<TokioRuntime>()
          .unwrap(),
      );

      let base = round * KEYS_PER_ROUND;

      for k in base..base + KEYS_PER_ROUND {
        c.insert(k, k, 1).await;
      }
      c.wait().await.unwrap();

      let start = Arc::new(AtomicBool::new(false));

      let c_rem = c.clone();
      let start_rem = start.clone();
      let remover = tokio::spawn(async move {
        while !start_rem.load(AOrd::Acquire) {
          tokio::task::yield_now().await;
        }
        for k in base..base + KEYS_PER_ROUND {
          c_rem.remove(&k).await;
        }
      });

      let c_ins = c.clone();
      let start_ins = start.clone();
      let reinserter = tokio::spawn(async move {
        while !start_ins.load(AOrd::Acquire) {
          tokio::task::yield_now().await;
        }
        for k in base..base + KEYS_PER_ROUND {
          c_ins.insert(k, REINSERT_VAL, 1).await;
        }
      });

      start.store(true, AOrd::Release);
      remover.await.unwrap();
      reinserter.await.unwrap();
      c.wait().await.unwrap();

      for k in base..base + KEYS_PER_ROUND {
        let in_store = c.0.store.get(&k, 0).is_some();
        let in_policy = c.0.policy.contains(&k);
        if in_policy && !in_store {
          ghosts += 1;
        }
        if in_store && !in_policy {
          missing += 1;
        }
      }
    }
    assert_eq!(
      (ghosts, missing),
      (0, 0),
      "present-remove-vs-reinsert race: {} ghosts (policy tracks a key the \
       store has dropped), {} unaccounted live rows (store has a key the \
       policy has forgotten — bypasses max_cost / eviction)",
      ghosts,
      missing,
    );
  }

  // Async analogue of `test_sync_oversized_reinsert_no_ghost_policy_entry`.
  // See that test for the full rationale.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_async_oversized_reinsert_no_ghost_policy_entry() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, Ordering as AOrd},
    };

    const ROUNDS: u64 = 8;
    const KEYS_PER_ROUND: u64 = 5_000;
    const MAX_COST: i64 = 1_000;
    const OVERSIZED: i64 = MAX_COST + 1;

    let mut ghosts = 0usize;

    for round in 0..ROUNDS {
      let c: Arc<AsyncCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        AsyncCacheBuilder::new(KEYS_PER_ROUND as usize * 2, MAX_COST)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .finalize::<TokioRuntime>()
          .unwrap(),
      );

      let base = round * KEYS_PER_ROUND;

      for k in base..base + KEYS_PER_ROUND {
        c.insert(k, k, 1).await;
      }
      c.wait().await.unwrap();

      let start = Arc::new(AtomicBool::new(false));

      let c_rem = c.clone();
      let start_rem = start.clone();
      let remover = tokio::spawn(async move {
        while !start_rem.load(AOrd::Acquire) {
          tokio::task::yield_now().await;
        }
        for k in base..base + KEYS_PER_ROUND {
          c_rem.remove(&k).await;
        }
      });

      let c_ins = c.clone();
      let start_ins = start.clone();
      let reinserter = tokio::spawn(async move {
        while !start_ins.load(AOrd::Acquire) {
          tokio::task::yield_now().await;
        }
        for k in base..base + KEYS_PER_ROUND {
          c_ins.insert(k, 0xDEAD_BEEF, OVERSIZED).await;
        }
      });

      start.store(true, AOrd::Release);
      remover.await.unwrap();
      reinserter.await.unwrap();
      c.wait().await.unwrap();

      for k in base..base + KEYS_PER_ROUND {
        let in_store = c.0.store.get(&k, 0).is_some();
        let in_policy = c.0.policy.contains(&k);
        if in_policy && !in_store {
          ghosts += 1;
        }
      }
    }
    assert_eq!(
      ghosts, 0,
      "oversized-reinsert race stranded {} ghost policy entries (policy \
       tracks a key whose store row was rolled back for cost > max_cost)",
      ghosts,
    );
  }

  /// Async analogue of `test_sync_eager_writes_bounded_by_permit_pool`.
  ///
  /// Regression for Codex adversarial review finding: pre-fix, async
  /// producers awaited on `send(item)` to a full buffer AFTER their eager
  /// `store.try_update` write — so N pending tasks could leave N live
  /// uncharged rows in the store, silently bypassing `max_cost`.
  ///
  /// With the semaphore-gated insert path, each producer awaits
  /// `insert_sem.acquire()` BEFORE the eager write. The permit pool is
  /// sized to the insert buffer, so the number of pre-admission store
  /// rows cannot exceed `insert_buffer_size` regardless of how many
  /// producers are queued.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_async_eager_writes_bounded_by_permit_pool() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AOrd};

    const MAX_COST: i64 = 1;
    const BUFFER: usize = 4;
    const PRODUCERS: u64 = 64;
    const SLACK: usize = 2;

    struct Gate {
      parked: AtomicBool,
      released: parking_lot::Mutex<bool>,
      cv: parking_lot::Condvar,
    }

    struct ParkOnEvictCB(Arc<Gate>);
    impl CacheCallback for ParkOnEvictCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {}
      fn on_evict(&self, _item: CrateItem<u64>) {
        // Blocking the synchronous `on_evict` stops the processor task
        // but leaves the rest of the tokio runtime free — other tasks
        // (our producers, the timer) keep running.
        self.0.parked.store(true, AOrd::Release);
        let mut released = self.0.released.lock();
        while !*released {
          self.0.cv.wait(&mut released);
        }
      }
    }

    let gate = Arc::new(Gate {
      parked: AtomicBool::new(false),
      released: parking_lot::Mutex::new(false),
      cv: parking_lot::Condvar::new(),
    });

    let c: Arc<
      AsyncCache<
        u64,
        u64,
        TransparentKeyBuilder<u64>,
        DefaultCoster<u64>,
        DefaultUpdateValidator<u64>,
        ParkOnEvictCB,
      >,
    > = Arc::new(
      AsyncCacheBuilder::new(100, MAX_COST)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_callback(ParkOnEvictCB(gate.clone()))
        .set_buffer_size(BUFFER)
        .set_ignore_internal_cost(true)
        .finalize::<TokioRuntime>()
        .unwrap(),
    );

    assert!(c.insert(0, 0, MAX_COST).await);
    c.wait().await.unwrap();
    c.insert(1, 1, MAX_COST).await;

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !gate.parked.load(AOrd::Acquire) {
      if std::time::Instant::now() > deadline {
        *gate.released.lock() = true;
        gate.cv.notify_all();
        panic!("processor never entered on_evict within 5s — test setup broken");
      }
      sleep(Duration::from_millis(5)).await;
    }

    let started = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(PRODUCERS as usize);
    for k in 0..PRODUCERS {
      let c = c.clone();
      let started = started.clone();
      handles.push(spawn(async move {
        started.fetch_add(1, AOrd::Relaxed);
        c.insert(1_000_000 + k, 0, 1).await;
      }));
    }

    let settle_deadline = std::time::Instant::now() + Duration::from_secs(3);
    while started.load(AOrd::Relaxed) < PRODUCERS as usize
      && std::time::Instant::now() < settle_deadline
    {
      sleep(Duration::from_millis(5)).await;
    }
    sleep(Duration::from_millis(200)).await;

    let peak = c.0.store.len();
    let bound = (MAX_COST as usize) + BUFFER + SLACK;

    {
      *gate.released.lock() = true;
      gate.cv.notify_all();
    }
    for h in handles {
      let _ = h.await;
    }

    assert!(
      peak <= bound,
      "store.len() = {} exceeded permit bound while processor was parked \
       (max_cost = {}, buffer = {}, slack = {}, bound = {}). Eager writes \
       are not being back-pressured by the insert permit pool; pending \
       producers can grow the store past max_cost.",
      peak,
      MAX_COST,
      BUFFER,
      SLACK,
      bound,
    );
  }

  // close() on an idle cache resolves promptly and subsequent inserts are
  // rejected. Verifies the happy-path sentinel + wg barrier.
  // Cloning AsyncCache is cheap (Arc bump) and the clones share backing state.
  #[tokio::test]
  async fn test_async_clone_shares_state() {
    let c: AsyncCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    let c2 = c.clone();
    assert!(c.insert(7, 42, 1).await);
    c.wait().await.unwrap();

    let val = c2
      .get(&7)
      .await
      .expect("clone must see value written via original");
    assert_eq!(*val.value(), 42);
    drop(val);

    // Drop the first handle; the second handle must keep the cache alive.
    drop(c);
    let val = c2.get(&7).await.expect("cache must survive original drop");
    assert_eq!(*val.value(), 42);
  }

  // Dropping the cache must stop the policy's background LFU worker.
  // Policy shutdown is Drop-driven: the cache owns `stop_tx`, which the
  // policy processor watches via `stop_rx`; dropping the cache disconnects
  // both the cache processor and the policy processor. The observable
  // signal that the policy worker has exited is that `policy.push` can no
  // longer enqueue — `items_rx` has been dropped by the exited processor.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn test_drop_stops_policy_worker() {
    let c: AsyncCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCache::builder(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize::<TokioRuntime>()
      .unwrap();

    assert!(c.insert(1, 1, 1).await);
    c.wait().await.unwrap();

    // Keep an Arc to the policy alive past the cache drop so we can
    // observe it. Before drop, the worker is alive and push succeeds.
    let policy = c.0.policy.clone();
    assert!(
      policy.push(vec![1]).is_none(),
      "policy must accept pushes before drop"
    );

    drop(c);

    // Wait for the processor to observe the stop_tx disconnect and exit,
    // dropping items_rx. After that, `push` can no longer enqueue.
    for _ in 0..50 {
      if policy.push(vec![1]).is_some() {
        return;
      }
      tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("policy worker did not exit after cache drop");
  }

  // Regression for async processor permit-leak on user-callback panic.
  // Before the SemCloser drop-guard, a panicking `on_evict` (or any other
  // user callback) unwound the spawned future without releasing the
  // current item's permit; buffered items dropped with the channel
  // stranded their permits too. Subsequent `insert*`/`clear`/`wait`/`close`
  // calls on any live clone awaited `insert_sem.acquire()` forever. The
  // guard closes the semaphore on future-drop (including panic-unwind),
  // so those callers observe `SemaphoreClosed` and fall through to the
  // graceful `is_err` branch.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn test_async_callback_panic_does_not_hang_live_clone() {
    struct PanicCB;
    impl CacheCallback for PanicCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {}
      fn on_evict(&self, _item: CrateItem<u64>) {
        panic!("intentional panic from on_evict (test harness)");
      }
    }

    let prior_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let c: AsyncCache<u64, u64, TransparentKeyBuilder<u64>, _, _, PanicCB> =
      AsyncCacheBuilder::new_with_key_builder(64, 2, TransparentKeyBuilder::default())
        .set_callback(PanicCB)
        .set_ignore_internal_cost(true)
        .finalize::<TokioRuntime>()
        .unwrap();

    let clone = c.clone();
    for i in 0..64u64 {
      let _ = c.insert(i, i, 1).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Without the drop-guard, these awaits would park on
    // `insert_sem.acquire()` indefinitely.
    let r = tokio::time::timeout(Duration::from_secs(5), async move {
      let _ = clone.insert(9_999u64, 0, 1).await;
      let _ = clone.wait().await;
      let _ = clone.clear().await;
    })
    .await;

    std::panic::set_hook(prior_hook);
    assert!(
      r.is_ok(),
      "live clone hung on insert_sem.acquire() after processor panic",
    );
  }
}
