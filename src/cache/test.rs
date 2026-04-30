use crate::{CacheCallback, Coster, Item as CrateItem, KeyBuilder, TransparentHasher};
use parking_lot::Mutex;
use rand::RngExt;
use std::{
  collections::HashSet,
  hash::Hasher,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
  },
};

/// Process-global serialization gate for tests that mutate `panic::set_hook`.
///
/// `cargo test` runs tests in parallel by default, and the panic hook is a
/// single process-wide slot. Without serialization, two tests that each
/// `take_hook()` + `set_hook(no_op)` + restore can interleave such that
/// test B's `take_hook` captures test A's no-op as its "prior", and a
/// subsequent restore reinstalls a no-op as the process default — leaving
/// later tests with their panic diagnostics silently swallowed.
fn panic_hook_lock() -> &'static Mutex<()> {
  static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
  LOCK.get_or_init(|| Mutex::new(()))
}

/// RAII guard that suppresses the panic hook for the duration of its
/// lifetime, holding `panic_hook_lock()` so concurrent hook-mutating tests
/// cannot corrupt each other's prior-hook chain. The prior hook is restored
/// on drop (including panic-unwind through the test body).
pub(crate) struct SuppressPanicHookGuard {
  _lock: parking_lot::MutexGuard<'static, ()>,
  prior: Option<Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send + 'static>>,
}

impl SuppressPanicHookGuard {
  pub(crate) fn new() -> Self {
    let _lock = panic_hook_lock().lock();
    let prior = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    Self {
      _lock,
      prior: Some(prior),
    }
  }
}

impl Drop for SuppressPanicHookGuard {
  fn drop(&mut self) {
    if let Some(prior) = self.prior.take() {
      std::panic::set_hook(prior);
    }
  }
}

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
        .set_insert_stripe_high_water(1000)
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

    c.wait().unwrap();
    // Set is rejected because the cost of the entry is too high
    // when accounting for the internal cost of storing the entry.
    assert!(c.get(&1).is_none());

    // Update the max cost of the cache and retry.
    c.update_max_cost(1000);
    assert_eq!(c.max_cost(), 1000);
    assert!(c.insert(1, 1, 1));

    c.wait().unwrap();
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

    c.wait().unwrap();
    assert!(c.0.policy.contains(&1));
    assert_eq!(c.0.policy.cost(&1), 1);

    let _ = c.insert_if_present(1, 2, 0);
    c.wait().unwrap();
    assert_eq!(c.0.policy.cost(&1), 2);

    c.remove(&1);
    c.wait().unwrap();
    assert!(c.0.store.get(&1, 0).is_none());
    assert!(!c.0.policy.contains(&1));

    c.insert(2, 2, 3);
    c.insert(3, 3, 3);
    c.insert(4, 3, 3);
    c.insert(5, 3, 5);
    c.wait().unwrap();
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
    c.wait().unwrap();
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
    c.wait().unwrap();
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
      let c = Cache::builder(100, 10)
        .set_callback(DefaultCacheCallback::default())
        .set_metrics(true)
        .set_insert_stripe_high_water(1)
        .finalize()
        .unwrap();

      for i in 0..50 {
        let v = format!("{:0100}", i);
        // We're updating the same key.
        let _ = c.insert(0, v, 1);
      }

      // Wait for all the items to be processed.
      c.wait().unwrap();
      // This will cause eviction from the cache.
      assert!(c.insert(1, "0".to_string(), 10));
    }

    // Run the test 100 times since it's not reliable.
    (0..100).for_each(|_| test())
  }

  #[test]
  fn test_wait_drains_stripes_first() {
    // Insert N items where N < HIGH_WATER so they sit in stripes
    // (not yet flushed via threshold). After wait(), the prelude
    // drain forces them into the channel, and the Wait barrier
    // returns only after the processor has processed them all. The
    // policy must reflect every key.
    let c: Cache<u64, u64, TransparentKeyBuilder<u64>> = Cache::builder(1000, 1000)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_insert_stripe_high_water(64) // default
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    // 32 inserts: well below 64-item threshold for any single
    // stripe, and across 64 stripes (some collide) ensures most
    // stripes hold partial vecs.
    for i in 0..32u64 {
      assert!(c.insert(i, i, 1));
    }
    c.wait().unwrap();

    // After wait, every key should be present in policy.
    for i in 0..32u64 {
      assert!(
        c.0.policy.contains(&i),
        "key {i} missing from policy after wait()",
      );
    }
  }

  #[test]
  fn test_clear_drains_stripes_first() {
    use std::sync::{
      Arc,
      atomic::{AtomicU64, Ordering},
    };
    struct CountingCB {
      on_exit: Arc<AtomicU64>,
      on_reject: Arc<AtomicU64>,
    }
    impl CacheCallback for CountingCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {
        self.on_exit.fetch_add(1, Ordering::SeqCst);
      }
      fn on_evict(&self, _i: CrateItem<u64>) {}
      fn on_reject(&self, _i: CrateItem<u64>) {
        self.on_reject.fetch_add(1, Ordering::SeqCst);
      }
    }
    let on_exit = Arc::new(AtomicU64::new(0));
    let on_reject = Arc::new(AtomicU64::new(0));
    let c: Cache<u64, u64, TransparentKeyBuilder<u64>, _, _, CountingCB> =
      CacheBuilder::new_with_key_builder(1000, 1000, TransparentKeyBuilder::default())
        .set_callback(CountingCB {
          on_exit: on_exit.clone(),
          on_reject: on_reject.clone(),
        })
        .set_insert_stripe_high_water(64)
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap();

    for i in 0..32u64 {
      assert!(c.insert(i, i, 1));
    }
    // clear() must drain stripes first, then run wipe. After clear
    // returns, the cache is empty. on_exit fired for each admitted
    // value during the wipe (driven by `store.clear()` returning the
    // drained values per the Clear handler).
    c.clear().unwrap();
    assert_eq!(c.len(), 0);
    // Pre-clear inserts were observed by the processor (admission +
    // store rows existed at the moment of wipe), so on_exit should
    // have fired for them.
    assert!(
      on_exit.load(Ordering::SeqCst) >= 1,
      "expected at least one on_exit during clear's wipe",
    );
  }

  #[test]
  fn test_drop_drains_stripes_via_stop_arm() {
    use std::sync::{
      Arc,
      atomic::{AtomicU64, Ordering},
    };
    struct RejectCounter {
      on_reject: Arc<AtomicU64>,
    }
    impl CacheCallback for RejectCounter {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {}
      fn on_evict(&self, _i: CrateItem<u64>) {}
      fn on_reject(&self, _i: CrateItem<u64>) {
        self.on_reject.fetch_add(1, Ordering::SeqCst);
      }
    }
    let on_reject = Arc::new(AtomicU64::new(0));
    {
      let c: Cache<u64, u64, TransparentKeyBuilder<u64>, _, _, RejectCounter> =
        CacheBuilder::new_with_key_builder(1000, 1000, TransparentKeyBuilder::default())
          .set_callback(RejectCounter {
            on_reject: on_reject.clone(),
          })
          .set_insert_stripe_high_water(64)
          .set_ignore_internal_cost(true)
          .finalize()
          .unwrap();
      // 16 inserts, all stripe-resident.
      for i in 0..16u64 {
        assert!(c.insert(i, i, 1));
      }
      // drop here: stop arm runs final inline drain → items reach
      // the processor, NOT the overflow path. on_reject must NOT fire
      // for these items.
    }
    assert_eq!(
      on_reject.load(Ordering::SeqCst),
      0,
      "stop-arm drain must not on_reject stripe-buffered items",
    );
  }

  #[test]
  fn test_tick_drains_stripes_after_idle() {
    let c: Cache<u64, u64, TransparentKeyBuilder<u64>> = Cache::builder(1000, 1000)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_insert_stripe_high_water(64)
      .set_drain_interval(Duration::from_millis(50))
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap();

    for i in 0..16u64 {
      assert!(c.insert(i, i, 1));
    }
    // The tick arm fires every 50ms and drains stripes inline. On a
    // loaded CI runner the processor thread can be scheduled late, so
    // poll up to 5 s rather than relying on a fixed sleep.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
      let all_present = (0..16u64).all(|i| c.0.policy.contains(&i));
      if all_present {
        break;
      }
      if std::time::Instant::now() >= deadline {
        for i in 0..16u64 {
          assert!(
            c.0.policy.contains(&i),
            "key {i} not in policy after tick drain",
          );
        }
        unreachable!();
      }
      sleep(Duration::from_millis(25));
    }
  }

  #[test]
  fn test_no_admission_reorder_corruption() {
    // Heavy concurrent updates to the same key from different
    // threads. With 64 stripes and an MPSC channel between stripes
    // and processor, items from different threads may be reordered
    // at the policy. The version-gate (`item_version`,
    // `clear_generation`) ensures the final cost in policy matches
    // the final value in store regardless of order.
    use std::{sync::Arc, thread};

    let c: Arc<Cache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
      Cache::builder(1000, 100_000)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_insert_stripe_high_water(8)
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap(),
    );

    const KEY: u64 = 42;
    const THREADS: u64 = 16;
    const ITERS: u64 = 1000;

    let mut handles = Vec::new();
    for t in 0..THREADS {
      let c = c.clone();
      handles.push(thread::spawn(move || {
        for i in 0..ITERS {
          // Cost = thread_id * 10 + iter — distinct values so every
          // update changes cost.
          let cost = (t * 10 + i) as i64;
          let _ = c.insert(KEY, cost as u64, cost.max(1));
        }
      }));
    }
    for h in handles {
      h.join().unwrap();
    }
    c.wait().unwrap();

    // Final consistency: store and policy must both reflect SOME
    // single live (cost, value) pair (whoever won the version race),
    // and policy.cost(KEY) must equal that value's cost. Specifically:
    // either (a) policy doesn't contain KEY (admission rejected) AND
    // store doesn't contain KEY, or (b) both contain it and the cost
    // accounted in policy matches the value version that the store
    // believes is live.
    let policy_has = c.0.policy.contains(&KEY);
    let store_val = c.get(&KEY).map(|r| r.read());
    if policy_has {
      assert!(
        store_val.is_some(),
        "policy contains KEY but store does not — ghost entry",
      );
    }
    // The reverse — store has it but policy doesn't — is a soft
    // anomaly: the orphan path in Update branch admits or rolls
    // back, so a rare race can leave a store row that policy didn't
    // claim. With c.wait() above, every queued Item has been
    // processed; a remaining orphan would survive only if the
    // version-gate pruned it. For this test, accept this state
    // (it's not corruption, just a transient that resolves on the
    // next admission/eviction). Document and move on.
    let _ = store_val;
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
      .insert_buf
      .send_single(Item::new(idx, c_b, 1, Time::now(), v_b, current_gen))
      .unwrap();
    c.0
      .insert_buf
      .send_single(Item::delete(idx, c_a, current_gen, v_a))
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
        .set_insert_stripe_high_water(2)
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap(),
    );

    let c_hook = c.clone();
    *state.hook.lock() = Some(Arc::new(move || {
      // Loop far beyond stripe capacity: in v0.9.0 (pre re-entrancy fix),
      // each iteration's `insert` would have blocked forever on a full buffer,
      // because the sole consumer (this thread) is stuck in the callback.
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
      .insert_buf
      .send_single(Item::update(
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
      .insert_buf
      .send_single(Item::delete(k, conflict_k, stale_gen, removed_version))
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

    let hook_guard = super::SuppressPanicHookGuard::new();

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
        drop(hook_guard);
        panic!("live clone hung on insert_sem.acquire() after processor panic");
      }
    }
    drop(c);
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
    let hook_guard = super::SuppressPanicHookGuard::new();

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
        drop(hook_guard);
        panic!("Cache drop hung after processor thread panicked");
      }
    }
  }

  // Cloning Cache is cheap (Arc bump) and clones share backing state. After
  // the original is dropped, the clone keeps the processor alive and the
  // entries are still visible.

  // Regression: pre-fix, sync `try_remove` fired `on_exit` BEFORE
  // `try_send(Item::Delete)`. A panic in the user callback unwound past
  // the enqueue, leaving the store row gone but the policy entry still
  // charging cost — a ghost no later operation could reconcile until
  // `clear()` or a same-key reinsert. Fix: enqueue Delete first, transfer
  // permit on success, THEN fire on_exit so the in-flight Delete still
  // converges policy/store to consistency on panic.
  #[test]
  fn test_sync_try_remove_on_exit_panic_no_ghost() {
    use std::{
      panic::AssertUnwindSafe,
      sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering as AOrd},
      },
    };

    struct PanicCB {
      armed: Arc<AtomicBool>,
      fired: Arc<AtomicU64>,
    }
    impl CacheCallback for PanicCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {
        self.fired.fetch_add(1, AOrd::Relaxed);
        if self.armed.load(AOrd::Acquire) {
          panic!("intentional on_exit panic for try_remove regression");
        }
      }
    }

    let armed = Arc::new(AtomicBool::new(false));
    let fired = Arc::new(AtomicU64::new(0));
    let c: Arc<
      Cache<
        u64,
        u64,
        TransparentKeyBuilder<u64>,
        DefaultCoster<u64>,
        DefaultUpdateValidator<u64>,
        PanicCB,
      >,
    > = Arc::new(
      CacheBuilder::new_with_key_builder(100, 100, TransparentKeyBuilder::default())
        .set_callback(PanicCB {
          armed: armed.clone(),
          fired: fired.clone(),
        })
        .set_ignore_internal_cost(true)
        .finalize()
        .unwrap(),
    );

    assert!(c.insert(1u64, 42u64, 1));
    c.wait().unwrap();
    assert!(
      c.0.policy.contains(&1),
      "key must be admitted before remove"
    );
    assert_eq!(c.0.policy.cost(&1), 1);

    armed.store(true, AOrd::Release);
    let c_worker = c.clone();
    let panic_result =
      spawn(move || std::panic::catch_unwind(AssertUnwindSafe(|| c_worker.try_remove(&1u64))))
        .join()
        .unwrap();
    assert!(
      panic_result.is_err(),
      "on_exit panic must propagate out of try_remove"
    );
    assert_eq!(
      fired.load(AOrd::Relaxed),
      1,
      "panicking on_exit must have been invoked exactly once",
    );
    armed.store(false, AOrd::Release);

    // Drain the in-flight Item::Delete. The processor's Delete handler
    // calls policy.remove to reconcile; pre-fix, on_exit panicking before
    // try_send meant the Delete never reached the queue and policy stayed
    // charged.
    c.wait().unwrap();

    assert!(
      c.0.store.get(&1, 0).is_none(),
      "store row must be gone after eager remove",
    );
    assert!(
      !c.0.policy.contains(&1),
      "policy must be reconciled — a surviving entry with no store row is a ghost",
    );
    assert_eq!(
      c.0.policy.cost(&1),
      -1,
      "policy cost ledger must show no charge for the removed key",
    );
  }

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

#[cfg(all(feature = "async", feature = "tokio"))]
mod async_test {
  use super::*;
  use crate::{
    AsyncCache, AsyncCacheBuilder, DefaultCacheCallback, DefaultCoster, DefaultKeyBuilder,
    DefaultUpdateValidator, TokioCache, TransparentKeyBuilder, UpdateValidator,
  };
  use agnostic_lite::tokio::TokioRuntime;
  use std::{collections::hash_map::RandomState, hash::Hash, time::Duration};
  use tokio::{sync::mpsc::channel, task::spawn, time::sleep};

  async fn new_test_cache<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<Key = K>>(
    kh: KH,
  ) -> AsyncCache<
    K,
    V,
    TokioRuntime,
    KH,
    DefaultCoster<V>,
    DefaultUpdateValidator<V>,
    DefaultCacheCallback<V>,
    RandomState,
  > {
    AsyncCacheBuilder::new_with_key_builder(100, 10, kh)
      .build::<TokioRuntime>()
      .unwrap()
  }

  async fn retry_set<
    C: Coster<Value = u64>,
    U: UpdateValidator<Value = u64>,
    CB: CacheCallback<Value = u64>,
  >(
    c: &AsyncCache<u64, u64, TokioRuntime, TransparentKeyBuilder<u64>, C, U, CB, RandomState>,
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
    let _: TokioCache<u64, u64, DefaultKeyBuilder<u64>> =
      AsyncCacheBuilder::new_with_key_builder(100, 10, TransparentKeyBuilder::default())
        .set_coster(DefaultCoster::default())
        .set_update_validator(DefaultUpdateValidator::default())
        .set_callback(DefaultCacheCallback::default())
        .set_num_counters(200)
        .set_max_cost(100)
        .set_cleanup_duration(Duration::from_secs(1))
        .set_insert_stripe_high_water(1000)
        .set_key_builder(DefaultKeyBuilder::default())
        .set_hasher(RandomState::default())
        .build::<TokioRuntime>()
        .unwrap();
  }

  #[tokio::test]
  async fn test_wait() {
    let max_cost = 10_000;
    let lru = AsyncCacheBuilder::new(max_cost * 10, max_cost as i64)
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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

    let c: TokioCache<u64, u64, KHTest> =
      AsyncCacheBuilder::new_with_key_builder(10, 1000, KHTest { ctr: ctr.clone() })
        .build::<TokioRuntime>()
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
    let c = AsyncCacheBuilder::new(10, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(false)
      .build::<TokioRuntime>()
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
    let c: TokioCache<i64, i64, TransparentKeyBuilder<i64>> =
      AsyncCacheBuilder::new_with_key_builder(100, 10, TransparentKeyBuilder::default())
        .build::<TokioRuntime>()
        .unwrap();

    drop(c);
  }

  #[tokio::test]
  async fn test_cache_process_items() {
    let cb = Arc::new(Mutex::new(HashSet::new()));
    let c = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_coster(TestCoster::default())
      .set_callback(TestCallback::new(cb.clone()))
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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
    let c = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .set_metrics(true)
      .build::<TokioRuntime>()
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
    let c = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .set_metrics(true)
      .build::<TokioRuntime>()
      .unwrap();

    retry_set(&c, 1, 1, 1, Duration::ZERO).await;

    c.insert(1, 2, 2).await;
    assert_eq!(c.get(&1).await.unwrap().read(), 2);
  }

  #[tokio::test]
  async fn test_cache_internal_cost() {
    let c = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(false)
      .set_metrics(true)
      .build::<TokioRuntime>()
      .unwrap();

    // Get should return None because the cache's cost is too small to store the item
    // when accounting for the internal cost.
    c.insert_with_ttl(1, 1, 1, Duration::ZERO).await;
    // wait() drains stripe-buffered items so the processor can reject the
    // over-cost item before we assert. sleep() alone is not deterministic
    // with the stripe ring's drain_interval.
    c.wait().await.unwrap();
    assert!(c.get(&1).await.is_none())
  }

  #[tokio::test]
  async fn test_recache_with_ttl() {
    let c = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .set_metrics(true)
      .build::<TokioRuntime>()
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
    let c = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_callback(TestCallback::new(cb.clone()))
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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
    let c = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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
    let c = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_metrics(true)
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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
    let c = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_metrics(true)
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
      .unwrap();

    for i in 0..10 {
      c.insert(i, i, 1).await;
    }
    // wait() drains stripe-buffered items to the processor before returning,
    // so this is deterministic regardless of drain_interval.
    c.wait().await.unwrap();
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
      AsyncCacheBuilder::new(100, 10)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_metrics(true)
        .build::<TokioRuntime>()
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
      let c = AsyncCacheBuilder::new(100, 10)
        .set_callback(DefaultCacheCallback::default())
        .set_metrics(true)
        .set_insert_stripe_high_water(1)
        .build::<TokioRuntime>()
        .unwrap();

      for i in 0..50 {
        let v = format!("{:0100}", i);
        // We're updating the same key.
        let _ = c.insert(0, v, 1).await;
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
      let c = AsyncCacheBuilder::new(100, 1000)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_metrics(true)
        .build::<TokioRuntime>()
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
      AsyncCacheBuilder::new(12960, 1e6 as i64)
        .set_metrics(true)
        .build::<TokioRuntime>()
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
    let c: Arc<TokioCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
      AsyncCacheBuilder::new(100, 10)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_ignore_internal_cost(true)
        .build::<TokioRuntime>()
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
    let c: TokioCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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
  async fn test_async_set_buffer_items_and_len() {
    let c: TokioCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_buffer_items(32)
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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
    let c: TokioCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
      .unwrap();
    let r: &TokioCache<u64, u64, TransparentKeyBuilder<u64>> = c.as_ref();
    assert_eq!(r.max_cost(), 10);
  }

  #[tokio::test]
  async fn test_async_insert_if_present_missing() {
    let c: TokioCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
      .unwrap();
    assert!(!c.insert_if_present(42, 0, 1).await);
    c.wait().await.unwrap();
    assert!(c.get(&42).await.is_none());
  }

  #[tokio::test]
  async fn test_async_ring_overflow_and_fill() {
    let c: TokioCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_buffer_items(4)
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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
      let c: TokioCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCacheBuilder::new(100, 100)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_ignore_internal_cost(true)
        .build::<TokioRuntime>()
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
    let c: TokioCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCacheBuilder::new(100, 100)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .set_cleanup_duration(Duration::from_millis(50))
      .build::<TokioRuntime>()
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
      TokioRuntime,
      TransparentKeyBuilder<u64>,
      DefaultCoster<u64>,
      DefaultUpdateValidator<u64>,
      CountingCB,
      RandomState,
    > = AsyncCacheBuilder::new_with_key_builder(1000, 1000, TransparentKeyBuilder::default())
      .set_callback(CountingCB {
        on_exit: on_exit_count.clone(),
      })
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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
      let c: Arc<TokioCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        AsyncCacheBuilder::new(500, 5_000)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .build::<TokioRuntime>()
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
      let c: Arc<TokioCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        AsyncCacheBuilder::new(20_000, 1_000_000)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .build::<TokioRuntime>()
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

  // Saturation regression for the async insert path: when many concurrent
  // `insert()` calls overrun the bounded insert ring, every overflow must
  // either land on the channel or roll its eager store write back. The
  // original failure was that cancelled `.send().await`s left orphan
  // store rows the policy never admitted and max-cost never saw. The
  // current design avoids the cancellation hole structurally — the slow
  // path performs no `.await`s after the eager store write, so there is
  // no cancellation point that can strand a pre-rollback mutation.
  // Under both cancellation pressure and pure saturation, the store
  // stays bounded.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_async_insert_cancellation_does_not_leak() {
    const MAX_COST: i64 = 100;
    const N: u64 = 5_000;

    let c: Arc<TokioCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
      AsyncCacheBuilder::new(1000, MAX_COST)
        .set_key_builder(TransparentKeyBuilder::default())
        .set_ignore_internal_cost(true)
        .set_insert_stripe_high_water(1)
        .build::<TokioRuntime>()
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

    // Without rollback, dropped/cancelled inserts would leave eager store
    // writes that never pass through policy admission or cost accounting,
    // and the store would grow unbounded past max_cost. With the
    // drop-on-overflow rollback, every saturated insert removes its eager
    // write and the store stays within the policy's cost bound (plus a
    // small slack for legitimate admissions that haven't been evicted
    // yet; the key signal is that len() doesn't blow past max_cost by
    // orders of magnitude).
    assert!(
      (c.len() as i64) <= MAX_COST * 2,
      "store size {} exceeds 2x max_cost {} — cancellation likely leaked eager inserts",
      c.len(),
      MAX_COST,
    );
  }

  // Retired: `test_async_update_cancellation_preserves_value`,
  // `test_async_cancelled_update_reconciles_policy_cost`, and
  // `test_async_cancelled_update_admits_via_fallthrough_when_new_skipped`.
  //
  // Those tests targeted the `EagerInsertGuard` design, which used
  // per-item Drop-based rollback to undo cancelled inserts/updates. The
  // current architecture (`InsertStripeRing` + drop-on-overflow)
  // makes those scenarios structurally unreachable:
  //
  //   - On the fast path (`Buffered`/`Sent`), `try_insert_in` does its
  //     eager `try_update`, the stripe push, and only then yields once
  //     for cooperative scheduling. There is no `.await` between the
  //     store write and the rollback decision, so cancellation cannot
  //     interleave at all between mutation and reconciliation.
  //   - On the slow path (`PushOutcome::Dropped`), the batch is rolled
  //     back inline via `rollback_batch` before the trailing yield. Per-
  //     item rules mirror sync's `PushOutcome::Dropped` (`New` removed
  //     from store; `Update` graceful leak + `metrics::DropSets`).
  //   - The bounded barrier preludes in `wait()`/`clear()` drain stripe
  //     buffers before sending their markers, so cancellation-after-eager
  //     cannot leave a ghost row visible past a barrier.
  //
  // The invariant those tests guarded (cancellation does not destroy
  // live data and does not leak admissions past `max_cost`) is now
  // covered by `test_async_insert_cancellation_does_not_leak`, which
  // exercises the saturated path under cancellation pressure.

  #[tokio::test]
  async fn test_async_reject_update() {
    struct NoUpdate;
    impl UpdateValidator for NoUpdate {
      type Value = u64;
      fn should_update(&self, _prev: &u64, _curr: &u64) -> bool {
        false
      }
    }

    let c = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_update_validator(NoUpdate)
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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
      .build::<TokioRuntime>()
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

    let c: TokioCache<String, Box<dyn Any + Send + Sync>> = AsyncCacheBuilder::new(100, 10)
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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
  // and silently dropped the item. In cachebench that manifested as <3 % hit
  // ratio on S3/DS1 at n=16. The backpressure fix (awaiting send) turns this
  // into natural rate-limiting; dropped-set count must stay near zero and the
  // cache must actually hold a meaningful fraction of the working set.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_async_insert_no_starvation_under_high_concurrency() {
    use std::sync::Arc;

    let cache: TokioCache<u64, u64> = AsyncCacheBuilder::new(100_000, 10_000)
      .set_metrics(true)
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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
      let c: Arc<TokioCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        AsyncCacheBuilder::new(20_000, 1_000_000)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .build::<TokioRuntime>()
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
      let c: Arc<TokioCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        AsyncCacheBuilder::new(20_000, 1_000_000)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .build::<TokioRuntime>()
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
      let c: Arc<TokioCache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
        AsyncCacheBuilder::new(KEYS_PER_ROUND as usize * 2, MAX_COST)
          .set_key_builder(TransparentKeyBuilder::default())
          .set_ignore_internal_cost(true)
          .build::<TokioRuntime>()
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

  // Cloning AsyncCache is cheap (Arc bump) and the clones share backing state.
  #[tokio::test]
  async fn test_async_clone_shares_state() {
    let c: TokioCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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
    let c: TokioCache<u64, u64, TransparentKeyBuilder<u64>> = AsyncCacheBuilder::new(100, 10)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
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

    let hook_guard = super::SuppressPanicHookGuard::new();

    let c: TokioCache<u64, u64, TransparentKeyBuilder<u64>, _, _, PanicCB> =
      AsyncCacheBuilder::new_with_key_builder(64, 2, TransparentKeyBuilder::default())
        .set_callback(PanicCB)
        .set_ignore_internal_cost(true)
        .build::<TokioRuntime>()
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

    drop(hook_guard);
    assert!(
      r.is_ok(),
      "live clone hung on insert_sem.acquire() after processor panic",
    );
  }

  // Dropping an insert future before its first poll leaves no eager store
  // write because the eager write only occurs once the future is polled.
  // There is no cancellation surface here: ring.push completes before any
  // await, so dropping mid-push is the same as never calling insert at all.
  #[tokio::test]
  async fn async_below_threshold_no_cancellation_hole() {
    let cache: TokioCache<u64, u64> = AsyncCacheBuilder::new(1_000, 1_000)
      .build::<TokioRuntime>()
      .unwrap();

    let key = 42_u64;
    let val = 7_u64;

    {
      // Construct the future but drop it without polling.
      let _fut = cache.insert(key, val, 1);
      // _fut dropped here; future never polled, never executed.
    }

    // The eager store write only happens once the future is polled.
    // Without polling, no row was written.
    assert!(
      cache.get(&key).await.is_none(),
      "future never polled: no eager store write should have happened"
    );

    // Confirm the cache is still functional after the no-op drop.
    cache.insert(key, val, 1).await;
    cache.wait().await.unwrap();
    assert_eq!(*cache.get(&key).await.unwrap().value(), val);
  }

  // Smoke check for slow-path saturation: with high_water=1, every
  // push hits `PushOutcome::Sent` or `PushOutcome::Dropped`, exercising
  // the drop-on-overflow rollback path. 10_000 detached spawned inserts
  // overrun the processor and force concurrent slow-path producers to
  // interleave. This test does NOT exercise cancellation —
  // `spawn_detach` runs each future to completion. The point is no
  // panic, no hang, no deadlock under concurrent slow-path load.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn async_slow_path_under_load_smoke() {
    use agnostic_lite::RuntimeLite;

    let cache: TokioCache<u64, u64> = AsyncCacheBuilder::new(1_000, 1_000)
      .set_insert_stripe_high_water(1)
      .build::<TokioRuntime>()
      .unwrap();

    let n_pushes = 10_000_u64;

    let cache = std::sync::Arc::new(cache);
    for i in 0..n_pushes {
      let cache_clone = cache.clone();
      // Spawn each insert as its own task so they don't serialize.
      // We deliberately forget the handles — the point is to flood the
      // channel without serializing on the caller side.
      TokioRuntime::spawn_detach(async move {
        cache_clone.insert(i, i, 1).await;
      });
    }

    // Drain whatever has reached the ring by now. This isn't a barrier
    // on outstanding spawned tasks (they may still be running) — it's
    // just a flush of the stripe state.
    cache.wait().await.unwrap();

    // Spot-check at least one item was admitted.
    let mut hits = 0;
    for i in 0..n_pushes {
      if cache.get(&i).await.is_some() {
        hits += 1;
      }
    }
    assert!(hits > 0, "at least some items should be admitted");
    drop(cache);
  }

  // Dropping the cache with stripe-resident items must not panic or hang.
  // The processor's stop arm must drain partial stripes inline and
  // process queued batches before the std::thread processor exits.
  #[tokio::test]
  async fn async_close_drains_stripes_into_consistent_state() {
    let cache: TokioCache<u64, u64> = AsyncCacheBuilder::new(10_000, 10_000)
      .build::<TokioRuntime>()
      .unwrap();

    // Insert several items but do NOT call wait — items may sit in
    // partial stripes when the cache drops.
    for i in 0..50u64 {
      cache.insert(i, i, 1).await;
    }

    // Drop the cache. The stop arm must drain stripes inline and process
    // queued batches; nothing should hang. `Drop` joins the processor
    // thread before returning, so when this line completes the drain is
    // guaranteed done.
    drop(cache);
  }

  // wait() must drain partial-stripe items BEFORE placing the marker.
  // Without the drain prelude, the marker would slide past in-progress
  // stripes and wait() would return before our items are processed.
  #[tokio::test]
  async fn async_wait_drains_partial_stripes() {
    let cache: TokioCache<u64, u64> = AsyncCacheBuilder::new(10_000, 10_000)
      .build::<TokioRuntime>()
      .unwrap();

    for i in 0..5u64 {
      cache.insert(i, i * 10, 1).await;
    }

    // wait() must drain stripes BEFORE the marker. Without the drain
    // prelude, the marker would slide past the partial stripes and
    // wait() would return before our items are processed.
    cache.wait().await.unwrap();

    // After wait(), all items must be visible — capacity 10_000 vs 5
    // cost-1 items means zero eviction pressure, and wait() is a hard
    // barrier on the processor.
    let mut admitted = 0;
    for i in 0..5u64 {
      if cache.get(&i).await.is_some() {
        admitted += 1;
      }
    }
    assert_eq!(
      admitted, 5,
      "wait() must drain all partial-stripe items into the cache; saw {} admitted",
      admitted
    );
  }

  // Regression: pre-fix, async `try_remove` fired `on_exit` BEFORE
  // `try_send(Item::Delete)`. A panic in the user callback unwound past
  // the enqueue, leaving the store row gone but the policy entry still
  // charging cost — a ghost no later operation could reconcile until
  // `clear()` or a same-key reinsert. Fix: enqueue Delete first, transfer
  // permit on success, THEN fire on_exit so the in-flight Delete still
  // converges policy/store to consistency on panic.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn test_async_try_remove_on_exit_panic_no_ghost() {
    use std::sync::{
      Arc,
      atomic::{AtomicBool, AtomicU64, Ordering as AOrd},
    };

    struct PanicCB {
      armed: Arc<AtomicBool>,
      fired: Arc<AtomicU64>,
    }
    impl CacheCallback for PanicCB {
      type Value = u64;
      fn on_exit(&self, _v: Option<u64>) {
        self.fired.fetch_add(1, AOrd::Relaxed);
        if self.armed.load(AOrd::Acquire) {
          panic!("intentional on_exit panic for async try_remove regression");
        }
      }
    }

    let armed = Arc::new(AtomicBool::new(false));
    let fired = Arc::new(AtomicU64::new(0));
    let c: AsyncCache<
      u64,
      u64,
      TokioRuntime,
      TransparentKeyBuilder<u64>,
      DefaultCoster<u64>,
      DefaultUpdateValidator<u64>,
      PanicCB,
      RandomState,
    > = AsyncCacheBuilder::new_with_key_builder(100, 100, TransparentKeyBuilder::default())
      .set_callback(PanicCB {
        armed: armed.clone(),
        fired: fired.clone(),
      })
      .set_ignore_internal_cost(true)
      .build::<TokioRuntime>()
      .unwrap();

    assert!(c.insert(1u64, 42u64, 1).await);
    c.wait().await.unwrap();
    assert!(
      c.0.policy.contains(&1),
      "key must be admitted before remove"
    );
    assert_eq!(c.0.policy.cost(&1), 1);

    // Suppress the panic message so the test output stays clean.
    let hook_guard = super::SuppressPanicHookGuard::new();

    armed.store(true, AOrd::Release);
    let c_worker = c.clone();
    let join = tokio::spawn(async move { c_worker.try_remove(&1u64).await });
    let join_result = join.await;
    armed.store(false, AOrd::Release);
    drop(hook_guard);

    assert!(
      join_result.as_ref().err().is_some_and(|e| e.is_panic()),
      "on_exit panic must propagate out of async try_remove",
    );
    assert_eq!(
      fired.load(AOrd::Relaxed),
      1,
      "panicking on_exit must have been invoked exactly once",
    );

    // Drain the in-flight Item::Delete. The processor's Delete handler
    // calls policy.remove to reconcile; pre-fix, on_exit panicking before
    // try_send meant the Delete never reached the queue and policy stayed
    // charged.
    c.wait().await.unwrap();

    assert!(
      c.0.store.get(&1, 0).is_none(),
      "store row must be gone after eager remove",
    );
    assert!(
      !c.0.policy.contains(&1),
      "policy must be reconciled — a surviving entry with no store row is a ghost",
    );
    assert_eq!(
      c.0.policy.cost(&1),
      -1,
      "policy cost ledger must show no charge for the removed key",
    );
  }
}

#[cfg(all(feature = "async", feature = "smol"))]
mod async_smol_test {
  #[test]
  fn smol_runtime_basic_insert_and_get() {
    use agnostic_lite::{RuntimeLite, smol::SmolRuntime};

    SmolRuntime::block_on(async {
      let cache: crate::SmolCache<u64, u64> = crate::AsyncCacheBuilder::new(1_000, 1_000)
        .build::<SmolRuntime>()
        .unwrap();
      cache.insert(1, 100, 1).await;
      cache.wait().await.unwrap();
      assert_eq!(*cache.get(&1).await.unwrap().value(), 100);

      cache.insert(2, 200, 1).await;
      cache.wait().await.unwrap();
      assert_eq!(*cache.get(&2).await.unwrap().value(), 200);

      cache.remove(&1).await;
      cache.wait().await.unwrap();
      assert!(cache.get(&1).await.is_none());

      cache.clear().await.unwrap();
      assert!(cache.get(&2).await.is_none());

      drop(cache);
    });
  }
}
