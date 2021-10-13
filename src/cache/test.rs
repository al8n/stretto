use crate::cache::{Cache, Item};
use crate::{
    CacheCallback, Coster, DefaultKeyBuilder, Item as CrateItem, KeyBuilder, TransparentKeyBuilder,
    UpdateValidator,
};
use parking_lot::Mutex;
use rand::rngs::OsRng;
use rand::Rng;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

static CHARSET: &'static [u8] = "abcdefghijklmnopqrstuvwxyz0123456789".as_bytes();

fn get_key() -> [u8; 2] {
    let mut rng = OsRng::default();
    let k1 = CHARSET[rng.gen::<usize>() % CHARSET.len()];
    let k2 = CHARSET[rng.gen::<usize>() % CHARSET.len()];
    [k1, k2]
}

struct TestCallback {
    evicted: Arc<Mutex<HashSet<u64>>>,
}

impl Default for TestCallback {
    fn default() -> Self {
        Self {
            evicted: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

impl TestCallback {
    fn new(map: Arc<Mutex<HashSet<u64>>>) -> Self {
        Self { evicted: map }
    }
}

struct KHTest {
    ctr: Arc<AtomicU64>,
}

impl KeyBuilder<u64> for KHTest {
    fn hash_index(&self, key: &u64) -> u64 {
        *key
    }

    fn hash_conflict(&self, _key: &u64) -> u64 {
        0
    }

    fn build_key(&self, k: &u64) -> (u64, u64) {
        self.ctr.fetch_add(1, Ordering::SeqCst);
        (self.hash_index(k), self.hash_conflict(k))
    }
}

#[derive(Default)]
struct TestCoster {}

impl Coster<u64> for TestCoster {
    fn cost(&self, val: &u64) -> i64 {
        *val as i64
    }
}

impl CacheCallback<u64> for TestCallback {
    fn on_exit(&self, _val: Option<u64>) {}

    fn on_evict(&self, item: CrateItem<u64>) {
        let mut evicted = self.evicted.lock();
        evicted.insert(item.index);
        self.on_exit(item.val)
    }
}

struct TestCallbackDropUpdates {
    set: Arc<Mutex<HashSet<u64>>>,
}

impl CacheCallback<String> for TestCallbackDropUpdates {
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
            self.set.lock().iter().map(|v| *v).collect::<Vec<u64>>()
        );
    }
}

cfg_not_async! {
    use crossbeam_channel::{bounded, select};
    use std::thread::{sleep, spawn};

    fn new_test_cache<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<K>>(
        kh: KH,
    ) -> Cache<K, V, KH> {
        Cache::new(100, 10, kh).unwrap()
    }

    fn retry_set<C: Coster<u64>, U: UpdateValidator<u64>, CB: CacheCallback<u64>>(
        c: Arc<Cache<u64, u64, TransparentKeyBuilder<u64>, C, U, CB>>,
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
    fn test_cache_key_to_hash() {
        let ctr = Arc::new(AtomicU64::new(0));

        let c: Cache<u64, u64, KHTest> = Cache::new(10, 1000, KHTest { ctr: ctr.clone() }).unwrap();

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
        let c = Cache::builder(12960, 1e6 as i64, DefaultKeyBuilder::default())
            .set_metrics(true)
            .finalize()
            .unwrap();

        let c = Arc::new(c);
        let (stop_tx, stop_rx) = bounded::<()>(8);

        for _ in 0..8 {
            let rx = stop_rx.clone();
            let tc = c.clone();

            spawn(move || loop {
                select! {
                    recv(rx) -> _ => return,
                    default => {
                        let k = get_key();
                        match tc.get(&k) {
                            None => {
                                let mut rng = OsRng::default();
                                let rv = rng.gen::<usize>() % 100;
                                let val: String;
                                if rv < 10 {
                                    val = "test".to_string();
                                } else {
                                    val = vec!["a"; 1000].join("");
                                }
                                let cost = val.len() + 2;
                                tc.insert(get_key(), val, cost as i64);
                            },
                            Some(_) => {},
                        }
                    }
                }
            });
        }

        for _ in 0..20 {
            sleep(Duration::from_millis(180));
            let (cost_added, cost_evicted) = (
                c.metrics.get_cost_added().unwrap(),
                c.metrics.get_cost_evicted().unwrap(),
            );
            let cost = cost_added - cost_evicted;
            eprintln!("{}", c.metrics);
            assert!(cost as f64 <= (1e6 * 1.05));
        }

        for _ in 0..8 {
            let _ = stop_tx.send(());
        }
    }

    #[test]
    fn test_cache_update_max_cost() {
        let c = Cache::builder(10, 10, TransparentKeyBuilder::default())
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
    fn test_cache_multiple_close() {
        let c: Cache<i64, i64, TransparentKeyBuilder<i64>> =
            Cache::new(100, 10, TransparentKeyBuilder::default()).unwrap();

        let _ = c.close();
        let _ = c.close();
    }

    #[test]
    fn test_cache_insert_after_close() {
        let c = new_test_cache::<u64, u64, TransparentKeyBuilder<u64>>(TransparentKeyBuilder::default());
        let _ = c.close();
        assert!(!c.insert(1, 1, 1));
    }

    #[test]
    fn test_cache_clear_after_close() {
        let c = new_test_cache::<u64, u64, TransparentKeyBuilder<u64>>(TransparentKeyBuilder::default());
        let _ = c.close();
        let _ = c.clear();
    }

    #[test]
    fn test_cache_get_after_close() {
        let c = new_test_cache::<u64, u64, TransparentKeyBuilder<u64>>(TransparentKeyBuilder::default());
        assert!(c.insert(1, 1, 1));
        let _ = c.close();

        assert!(c.get(&1).is_none());
    }

    #[test]
    fn test_cache_remove_after_close() {
        let c = new_test_cache::<u64, u64, TransparentKeyBuilder<u64>>(TransparentKeyBuilder::default());
        assert!(c.insert(1, 1, 1));
        let _ = c.close();

        c.remove(&1);
    }

    #[test]
    fn test_cache_process_items() {

        let cb = Arc::new(Mutex::new(HashSet::new()));
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_coster(TestCoster::default())
            .set_callback(TestCallback::new(cb.clone()))
            .set_ignore_internal_cost(true)
            .finalize()
            .unwrap();

        assert!(c.insert(1, 1, 0));

        sleep(Duration::from_secs(1));
        assert!(c.policy.contains(&1));
        assert_eq!(c.policy.cost(&1), 1);

        let _ = c.insert_if_present(1, 2, 0);
        sleep(Duration::from_secs(1));
        assert_eq!(c.policy.cost(&1), 2);

        c.remove(&1);
        sleep(Duration::from_secs(1));
        assert!(c.store.get(&1, 0).is_none());
        assert!(!c.policy.contains(&1));

        c.insert(2, 2, 3);
        c.insert(3, 3, 3);
        c.insert(4, 3, 3);
        c.insert(5, 3, 5);
        sleep(Duration::from_secs(1));
        assert_ne!(cb.lock().len(), 0);

        let _ = c.close();
        assert!(!c.insert(1, 1, 1));
    }

    #[test]
    fn test_cache_get() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
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
        assert_eq!(c.metrics.ratio().unwrap(), 0.5);

        assert_eq!(c.get_mut(&1).unwrap().read(), 10);
    }

    #[test]
    fn test_cache_set() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_ignore_internal_cost(true)
            .set_metrics(true)
            .finalize()
            .unwrap();
        let c = Arc::new(c);

        retry_set(c.clone(), 1, 1, 1, Duration::ZERO);

        c.insert(1, 2, 2);
        assert_eq!(c.get(&1).unwrap().read(), 2);

        let _ = c.stop_tx.send(());
        (0..32768).for_each(|_| {
            let _ = c.insert_buf_tx.send(Item::update(1, 1, 0));
        });

        assert!(!c.insert(2, 2, 1));
        assert_eq!(c.metrics.get_sets_dropped().unwrap(), 1);
    }

    #[test]
    fn test_cache_internal_cost() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
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
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
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
        let c = Arc::new(
            Cache::builder(100, 10, TransparentKeyBuilder::default())
                .set_callback(TestCallback::new(cb.clone()))
                .set_ignore_internal_cost(true)
                .finalize()
                .unwrap(),
        );

        retry_set(c.clone(), 1, 1, 1, Duration::from_secs(1));

        // Sleep to make sure the item has expired after execution resumes.
        sleep(Duration::from_secs(2));
        assert!(c.get(&1).is_none());

        // Sleep to ensure that the bucket where the item was stored has been cleared
        // from the expiration map.
        sleep(Duration::from_secs(5));
        assert_eq!(cb.lock().len(), 1);

        // Verify that expiration times are overwritten.
        retry_set(c.clone(), 2, 1, 1, Duration::from_secs(1));
        retry_set(c.clone(), 2, 2, 1, Duration::from_secs(100));
        sleep(Duration::from_secs(3));
        assert_eq!(c.get(&2).unwrap().read(), 2);

        // Verify that entries with no expiration are overwritten.
        retry_set(c.clone(), 3, 1, 1, Duration::ZERO);
        retry_set(c.clone(), 3, 1, 1, Duration::from_secs(1));
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
        let c = Arc::new(
            Cache::builder(100, 10, TransparentKeyBuilder::default())
                .set_ignore_internal_cost(true)
                .finalize()
                .unwrap(),
        );

        retry_set(c.clone(), 3, 1, 1, Duration::from_secs(10));
        sleep(Duration::from_secs(1));

        // remove the item
        c.remove(&3);

        // ensure the key is deleted
        assert!(c.get(&3).is_none());
    }

    #[test]
    fn test_cache_get_ttl() {
        let c = Arc::new(
            Cache::builder(100, 10, TransparentKeyBuilder::default())
                .set_metrics(true)
                .set_ignore_internal_cost(true)
                .finalize()
                .unwrap(),
        );

        // try expiration with valid ttl item
        {
            let expiration = Duration::from_secs(5);
            retry_set(c.clone(), 1, 1, 1, expiration);

            assert_eq!(c.get(&1).unwrap().read(), 1);
            assert!(c.get_ttl(&1).unwrap() < expiration);

            c.remove(&1);

            assert!(c.get_ttl(&1).is_none());
        }

        // try expiration with no ttl
        {
            retry_set(c.clone(), 2, 2, 1, Duration::ZERO);
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
            retry_set(c.clone(), 3, 3, 1, expiration);

            assert_eq!(c.get(&3).unwrap().read(), 3);
            sleep(Duration::from_secs(1));
            assert!(c.get_ttl(&3).is_none());
        }
    }

    #[test]
    fn test_cache_drop_clear() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_buffer_size(4096)
            .set_metrics(true)
            .set_ignore_internal_cost(true)
            .finalize()
            .unwrap();
        let c = Arc::new(c);
        let (stop_tx, stop_rx) = bounded(0);
        let tc = c.clone();

        let ctr = Arc::new(AtomicU64::new(0));

        let tctr = ctr.clone();
        spawn(move || {
            let mut rng = OsRng::default();
            loop {
                select! {
                    recv(stop_rx) -> _ => return,
                    default => {
                        let i = rng.gen::<u64>();
                        if tc.insert(i, i, 1) {
                            tctr.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
        });

        sleep(Duration::from_millis(100));
        let _ = stop_tx.send(());
        c.clear().unwrap();
        // sleep(Duration::from_millis(100));
        assert_eq!(c.metrics.get_keys_added(), Some(0));

        (0..10).for_each(|i| {
            assert!(c.get(&i).is_none());
        })
    }

    #[test]
    fn test_cache_clear() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_metrics(true)
            .set_ignore_internal_cost(true)
            .finalize()
            .unwrap();

        (0..10).for_each(|i| {
            c.insert(i, i, 1);
        });
        sleep(Duration::from_millis(100));
        assert_eq!(c.metrics.get_keys_added(), Some(10));
        c.clear().unwrap();
        assert_eq!(c.metrics.get_keys_added(), Some(0));

        (0..10).for_each(|i| {
            assert!(c.get(&i).is_none());
        })
    }

    #[test]
    fn test_cache_metrics_clear() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_metrics(true)
            .finalize()
            .unwrap();

        let c = Arc::new(c);
        c.insert(1, 1, 1);

        let (stop_tx, stop_rx) = bounded(0);
        let tc = c.clone();
        spawn(move || loop {
            select! {
                recv(stop_rx) -> _ => return,
                default => {
                    tc.get(&1);
                }
            }
        });

        sleep(Duration::from_millis(100));
        let _ = c.clear();
        stop_tx.send(()).unwrap();
        c.metrics.clear();
    }

    // Regression test for bug https://github.com/dgraph-io/ristretto/issues/167
    #[test]
    fn test_cache_drop_updates() {
        fn test() {
            let set = Arc::new(Mutex::new(HashSet::new()));
            let c = Cache::builder(100, 10, DefaultKeyBuilder::default())
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
            sleep(Duration::from_millis(1));
            // This will cause eviction from the cache.
            assert!(c.insert(1, "0".to_string(), 10));
            let _ = c.close();
        }

        // Run the test 100 times since it's not reliable.
        (0..100).for_each(|_| test())
    }

    #[test]
    fn test_cache_with_ttl() {
        let mut process_win = 0;
        let mut clean_win = 0;

        for _ in 0..10 {
            let c = Cache::builder(100, 1000, TransparentKeyBuilder::default())
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
}

cfg_async! {
    use tokio::sync::mpsc::{channel, error::TryRecvError};
    use tokio::task::spawn;
    use tokio::time::sleep;

    async fn new_test_cache<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<K>>(
        kh: KH,
    ) -> Cache<K, V, KH> {
        Cache::new(100, 10, kh).unwrap()
    }

    async fn retry_set<C: Coster<u64>, U: UpdateValidator<u64>, CB: CacheCallback<u64>>(
        c: Arc<Cache<u64, u64, TransparentKeyBuilder, C, U, CB>>,
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
            assert_eq!(c.get(&key).unwrap().read(), val);
            return;
        }
    }

    #[tokio::test]
    async fn test_cache_key_to_hash() {
        let ctr = Arc::new(AtomicU64::new(0));

        let c: Cache<u64, u64, KHTest> = Cache::new(10, 1000, KHTest { ctr: ctr.clone() }).unwrap();

        assert!(c.insert(1, 1, 1).await);
        sleep(Duration::from_millis(10)).await;

        loop {
            match c.get(&1) {
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
        let c = Cache::builder(10, 10, TransparentKeyBuilder::default())
            .finalize()
            .unwrap();

        assert_eq!(c.max_cost(), 10);
        assert!(c.insert(1, 1, 1).await);

        sleep(Duration::from_secs(1)).await;
        // Set is rejected because the cost of the entry is too high
        // when accounting for the internal cost of storing the entry.
        assert!(c.get(&1).is_none());

        // Update the max cost of the cache and retry.
        c.update_max_cost(1000);
        assert_eq!(c.max_cost(), 1000);
        assert!(c.insert(1, 1, 1).await);

        sleep(Duration::from_millis(200)).await;
        assert_eq!(c.get(&1).unwrap().read(), 1);
        c.remove(&1).await;
    }

    #[tokio::test]
    async fn test_cache_multiple_close() {
        let c: Cache<i64, i64, TransparentKeyBuilder> =
            Cache::new(100, 10, TransparentKeyBuilder::default()).unwrap();

        let _ = c.close().await;
        let _ = c.close().await;
    }

    #[tokio::test]
    async fn test_cache_insert_after_close() {
        let c =
            new_test_cache::<u64, u64, TransparentKeyBuilder>(TransparentKeyBuilder::default()).await;
        let _ = c.close().await;
        assert!(!c.insert(1, 1, 1).await);
    }

    #[tokio::test]
    async fn test_cache_clear_after_close() {
        let c =
            new_test_cache::<u64, u64, TransparentKeyBuilder>(TransparentKeyBuilder::default()).await;
        let _ = c.close().await;
        let _ = c.clear();
    }

    #[tokio::test]
    async fn test_cache_get_after_close() {
        let c =
            new_test_cache::<u64, u64, TransparentKeyBuilder>(TransparentKeyBuilder::default()).await;
        assert!(c.insert(1, 1, 1).await);
        let _ = c.close().await;

        assert!(c.get(&1).is_none());
    }

    #[tokio::test]
    async fn test_cache_remove_after_close() {
        let c =
            new_test_cache::<u64, u64, TransparentKeyBuilder>(TransparentKeyBuilder::default()).await;
        assert!(c.insert(1, 1, 1).await);
        let _ = c.close().await;

        c.remove(&1).await;
    }

    #[tokio::test]
    async fn test_cache_process_items() {
        let cb = Arc::new(Mutex::new(HashSet::new()));
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_coster(TestCoster::default())
            .set_callback(TestCallback::new(cb.clone()))
            .set_ignore_internal_cost(true)
            .finalize()
            .unwrap();

        assert!(c.insert(1, 1, 0).await);

        sleep(Duration::from_secs(1)).await;
        assert!(c.policy.contains(&1));
        assert_eq!(c.policy.cost(&1), 1);

        let _ = c.insert_if_present(1, 2, 0).await;
        sleep(Duration::from_secs(1)).await;
        assert_eq!(c.policy.cost(&1), 2);

        c.remove(&1).await;
        sleep(Duration::from_secs(1)).await;
        assert!(c.store.get(&1, 0).is_none());
        assert!(!c.policy.contains(&1));

        c.insert(2, 2, 3).await;
        c.insert(3, 3, 3).await;
        c.insert(4, 3, 3).await;
        c.insert(5, 3, 5).await;
        sleep(Duration::from_secs(1)).await;
        assert_ne!(cb.lock().len(), 0);

        let _ = c.close().await;
        assert!(!c.insert(1, 1, 1).await);
    }

    #[tokio::test]
    async fn test_cache_get() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_ignore_internal_cost(true)
            .set_metrics(true)
            .finalize()
            .unwrap();

        c.insert(1, 1, 0).await;
        sleep(Duration::from_secs(1)).await;
        match c.get_mut(&1) {
            None => {}
            Some(mut val) => {
                val.write(10);
            }
        }

        assert!(c.get_mut(&2).is_none());

        // 0.5 and not 1.0 because we tried Getting each item twice
        assert_eq!(c.metrics.ratio().unwrap(), 0.5);

        assert_eq!(c.get_mut(&1).unwrap().read(), 10);
    }

    #[tokio::test]
    async fn test_cache_set() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_ignore_internal_cost(true)
            .set_metrics(true)
            .finalize()
            .unwrap();
        let c = Arc::new(c);

        retry_set(c.clone(), 1, 1, 1, Duration::ZERO).await;

        c.insert(1, 2, 2).await;
        assert_eq!(c.get(&1).unwrap().read(), 2);

        assert!(c.stop_tx.send(()).await.is_ok());
        for _ in 0..32768 {
            let _ = c.insert_buf_tx.send(Item::update(1, 1, 0)).await;
        }

        assert!(!c.insert(2, 2, 1).await);
        assert_eq!(c.metrics.get_sets_dropped().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_cache_internal_cost() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_metrics(true)
            .finalize()
            .unwrap();

        // Get should return None because the cache's cost is too small to store the item
        // when accounting for the internal cost.
        c.insert_with_ttl(1, 1, 1, Duration::ZERO).await;
        sleep(Duration::from_millis(100)).await;
        assert!(c.get(&1).is_none())
    }

    #[tokio::test]
    async fn test_recache_with_ttl() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_ignore_internal_cost(true)
            .set_metrics(true)
            .finalize()
            .unwrap();

        // Set initial value for key = 1
        assert!(c.insert_with_ttl(1, 1, 1, Duration::from_secs(5)).await);

        sleep(Duration::from_secs(2)).await;

        // Get value from cache for key = 1
        assert_eq!(c.get(&1).unwrap().read(), 1);

        // wait for expiration
        sleep(Duration::from_secs(5)).await;

        // The cached value for key = 1 should be gone
        assert!(c.get(&1).is_none());

        // set new value for key = 1
        assert!(c.insert_with_ttl(1, 2, 1, Duration::from_secs(5)).await);

        sleep(Duration::from_secs(2)).await;
        // get value from cache for key = 1;
        assert_eq!(c.get(&1).unwrap().read(), 2);
    }

    #[tokio::test]
    async fn test_cache_set_with_ttl() {
        let cb = Arc::new(Mutex::new(HashSet::new()));
        let c = Arc::new(
            Cache::builder(100, 10, TransparentKeyBuilder::default())
                .set_callback(TestCallback::new(cb.clone()))
                .set_ignore_internal_cost(true)
                .finalize()
                .unwrap(),
        );

        retry_set(c.clone(), 1, 1, 1, Duration::from_secs(1)).await;

        // Sleep to make sure the item has expired after execution resumes.
        sleep(Duration::from_secs(2)).await;
        assert!(c.get(&1).is_none());

        // Sleep to ensure that the bucket where the item was stored has been cleared
        // from the expiration map.
        sleep(Duration::from_secs(5)).await;
        assert_eq!(cb.lock().len(), 1);

        // Verify that expiration times are overwritten.
        retry_set(c.clone(), 2, 1, 1, Duration::from_secs(1)).await;
        retry_set(c.clone(), 2, 2, 1, Duration::from_secs(100)).await;
        sleep(Duration::from_secs(3)).await;
        assert_eq!(c.get(&2).unwrap().read(), 2);

        // Verify that entries with no expiration are overwritten.
        retry_set(c.clone(), 3, 1, 1, Duration::ZERO).await;
        retry_set(c.clone(), 3, 1, 1, Duration::from_secs(1)).await;
        sleep(Duration::from_secs(3)).await;
        assert!(c.get(&3).is_none());
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
        assert!(c.get(&1).is_none());
    }

    #[tokio::test]
    async fn test_cache_remove_with_ttl() {
        let c = Arc::new(
            Cache::builder(100, 10, TransparentKeyBuilder::default())
                .set_ignore_internal_cost(true)
                .finalize()
                .unwrap(),
        );

        retry_set(c.clone(), 3, 1, 1, Duration::from_secs(10)).await;
        sleep(Duration::from_secs(1)).await;

        // remove the item
        c.remove(&3).await;

        // ensure the key is deleted
        assert!(c.get(&3).is_none());
    }

    #[tokio::test]
    async fn test_cache_get_ttl() {
        let c = Arc::new(
            Cache::builder(100, 10, TransparentKeyBuilder::default())
                .set_metrics(true)
                .set_ignore_internal_cost(true)
                .finalize()
                .unwrap(),
        );

        // try expiration with valid ttl item
        {
            let expiration = Duration::from_secs(5);
            retry_set(c.clone(), 1, 1, 1, expiration).await;

            assert_eq!(c.get(&1).unwrap().read(), 1);
            assert!(c.get_ttl(&1).unwrap() < expiration);

            c.remove(&1).await;

            assert!(c.get_ttl(&1).is_none());
        }

        // try expiration with no ttl
        {
            retry_set(c.clone(), 2, 2, 1, Duration::ZERO).await;
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
            retry_set(c.clone(), 3, 3, 1, expiration).await;

            assert_eq!(c.get(&3).unwrap().read(), 3);
            sleep(Duration::from_secs(1)).await;
            assert!(c.get_ttl(&3).is_none());
        }
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_metrics(true)
            .set_ignore_internal_cost(true)
            .finalize()
            .unwrap();

        for i in 0..10 {
            c.insert(i, i, 1).await;
        }
        sleep(Duration::from_millis(100)).await;
        assert_eq!(c.metrics.get_keys_added(), Some(10));
        c.clear().unwrap();
        assert_eq!(c.metrics.get_keys_added(), Some(0));

        (0..10).for_each(|i| {
            assert!(c.get(&i).is_none());
        })
    }

    #[tokio::test]
    async fn test_cache_metrics_clear() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_metrics(true)
            .finalize()
            .unwrap();

        let c = Arc::new(c);
        c.insert(1, 1, 1).await;

        let (stop_tx, mut stop_rx) = channel(1);
        let tc = c.clone();
        spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_rx.recv() => return,
                    else => {
                        tc.get(&1);
                    }
                }
            }
        });

        sleep(Duration::from_millis(100)).await;
        let _ = c.clear();
        stop_tx.send(()).await.unwrap();
        c.metrics.clear();
    }

    // Regression test for bug https://github.com/dgraph-io/ristretto/issues/167
    #[tokio::test]
    async fn test_cache_drop_updates() {

        async fn test() {
            let set = Arc::new(Mutex::new(HashSet::new()));
            let c = Cache::builder(100, 10, DefaultKeyBuilder::default())
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
            let _ = c.close().await;
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
            let c = Cache::builder(100, 1000, TransparentKeyBuilder::default())
                .set_metrics(true)
                .finalize()
                .unwrap();

            // Set initial value for key = 1
            assert!(c.insert_with_ttl(1, 1, 0, Duration::from_millis(800)).await);

            sleep(Duration::from_millis(100)).await;

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

            sleep(Duration::from_millis(1200)).await;
            assert!(c.get(&1).is_none());
        }
        eprintln!("process: {} cleanup: {}", process_win, clean_win);
    }

    #[tokio::test]
    async fn test_cache_max_cost() {
        let c = Cache::builder(12960, 1e6 as i64, DefaultKeyBuilder::default())
            .set_metrics(true)
            .finalize()
            .unwrap();

        let c = Arc::new(c);

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
                    tc.metrics.get_cost_added().unwrap(),
                    tc.metrics.get_cost_evicted().unwrap(),
                );
                let cost = cost_added - cost_evicted;
                eprintln!("{}", tc.metrics);
                assert!(cost as f64 <= (1e6 * 1.05));
            }

            for tx in txs {
                let _ = tx.send(()).await;
            }
        });

        for mut rx in rxs {
            loop {
                match rx.try_recv() {
                    Ok(_) => break,
                    Err(_) => {
                        let k = get_key();
                        match c.get(&k) {
                            None => {
                                let mut rng = OsRng::default();
                                let rv = rng.gen::<usize>() % 100;
                                let val: String;
                                if rv < 10 {
                                    val = "test".to_string();
                                } else {
                                    val = vec!["a"; 1000].join("");
                                }
                                let cost = val.len() + 2;
                                assert!(c.insert(get_key(), val, cost as i64).await);
                            },
                            Some(_) => {},
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_cache_drop_clear() {
        let c = Cache::builder(100, 10, TransparentKeyBuilder::default())
            .set_buffer_size(4096)
            .set_metrics(true)
            .set_ignore_internal_cost(true)
            .finalize()
            .unwrap();
        let c = Arc::new(c);
        let (stop_tx, mut stop_rx) = channel(1);
        let tc = c.clone();
        let ctr = Arc::new(AtomicU64::new(0));
        let tctr = ctr.clone();

        spawn(async move {
            sleep(Duration::from_millis(100)).await;
            let _ = stop_tx.send(()).await;
        });

        loop {
            match stop_rx.try_recv() {
                Ok(_) => {
                    break;
                },
                Err(_) => {
                    let mut rng = OsRng::default();
                    let i = rng.gen::<u64>();
                    drop(rng);
                    if tc.insert(i, i, 1).await {
                        tctr.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }
        c.clear().unwrap();
        assert_eq!(c.metrics.get_keys_added(), Some(0));

        (0..10).for_each(|i| {
            assert!(c.get(&i).is_none());
        })
    }
}
