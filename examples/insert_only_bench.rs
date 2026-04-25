// Pure-insert throughput, sync vs async, with eviction pressure.

use std::{sync::Arc, time::Instant};

use stretto::{AsyncCache, Cache, TokioRuntime};

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
  let cap: i64 = 100_000;
  let n_clients: usize = 16;
  let n_ops: usize = 100_000;

  // SYNC bench: each client inserts unique keys (so we get eviction)
  let sync_cache: Cache<u64, u64> = Cache::new(cap as usize * 10, cap).unwrap();
  let sync_cache = Arc::new(sync_cache);
  let start = Instant::now();
  let mut handles = Vec::with_capacity(n_clients);
  for c in 0..n_clients {
    let cache = Arc::clone(&sync_cache);
    handles.push(std::thread::spawn(move || {
      let base = (c as u64) * (n_ops as u64);
      for i in 0..n_ops {
        let k = base + i as u64;
        cache.insert(k, k, 1);
      }
    }));
  }
  for h in handles {
    h.join().unwrap();
  }
  let producers = start.elapsed();
  sync_cache.wait().unwrap();
  let total = start.elapsed();
  println!(
    "SYNC insert: producers={:.3}s wait={:.3}s total={:.3}s {:.0} ops/sec",
    producers.as_secs_f64(),
    (total - producers).as_secs_f64(),
    total.as_secs_f64(),
    (n_clients * n_ops) as f64 / total.as_secs_f64(),
  );
  drop(sync_cache);

  // ASYNC bench
  let async_cache: AsyncCache<u64, u64> =
    AsyncCache::new::<TokioRuntime>(cap as usize * 10, cap).unwrap();
  let async_cache = Arc::new(async_cache);
  let start = Instant::now();
  let mut handles = Vec::with_capacity(n_clients);
  for c in 0..n_clients {
    let cache = Arc::clone(&async_cache);
    handles.push(tokio::spawn(async move {
      let base = (c as u64) * (n_ops as u64);
      for i in 0..n_ops {
        let k = base + i as u64;
        cache.insert(k, k, 1).await;
        if i % 10_000 == 0 {
          tokio::task::yield_now().await;
        }
      }
    }));
  }
  for h in handles {
    h.await.unwrap();
  }
  let producers = start.elapsed();
  async_cache.wait().await.unwrap();
  let total = start.elapsed();
  println!(
    "ASYNC insert: producers={:.3}s wait={:.3}s total={:.3}s {:.0} ops/sec",
    producers.as_secs_f64(),
    (total - producers).as_secs_f64(),
    total.as_secs_f64(),
    (n_clients * n_ops) as f64 / total.as_secs_f64(),
  );
}
