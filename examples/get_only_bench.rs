// Pure-get throughput, sync vs async (no eviction, all hits).

use std::{sync::Arc, time::Instant};

use agnostic_lite::tokio::TokioRuntime;
use stretto::{AsyncCacheBuilder, Cache, TokioCache};

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
  let cap: i64 = 100_000;
  let n_clients: usize = 16;
  let n_ops: usize = 1_000_000;

  // Build sync cache and prepopulate.
  let sync_cache: Cache<u64, u64> = Cache::new(cap as usize * 10, cap).unwrap();
  for k in 0..(cap as u64) {
    sync_cache.insert(k, k, 1);
  }
  sync_cache.wait().unwrap();
  let sync_cache = Arc::new(sync_cache);

  // Build async cache and prepopulate.
  let async_cache: TokioCache<u64, u64> = AsyncCacheBuilder::new(cap as usize * 10, cap)
    .build::<TokioRuntime>()
    .unwrap();
  for k in 0..(cap as u64) {
    async_cache.insert(k, k, 1).await;
  }
  async_cache.wait().await.unwrap();
  let async_cache = Arc::new(async_cache);

  // SYNC bench
  let start = Instant::now();
  let mut handles = Vec::with_capacity(n_clients);
  for c in 0..n_clients {
    let cache = Arc::clone(&sync_cache);
    handles.push(std::thread::spawn(move || {
      let mut state: u64 = 0xdead_beef ^ ((c as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
      if state == 0 {
        state = 1;
      }
      let mut hits = 0u64;
      for _ in 0..n_ops {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let k = state % (cap as u64);
        if cache.get(&k).is_some() {
          hits += 1;
        }
      }
      hits
    }));
  }
  let mut total_hits = 0u64;
  for h in handles {
    total_hits += h.join().unwrap();
  }
  let sync_elapsed = start.elapsed();
  println!(
    "SYNC pure-get: {:.3}s {:.0} ops/sec hits={}",
    sync_elapsed.as_secs_f64(),
    (n_clients * n_ops) as f64 / sync_elapsed.as_secs_f64(),
    total_hits
  );

  // ASYNC bench
  let start = Instant::now();
  let mut handles = Vec::with_capacity(n_clients);
  for c in 0..n_clients {
    let cache = Arc::clone(&async_cache);
    handles.push(tokio::spawn(async move {
      let mut state: u64 = 0xdead_beef ^ ((c as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
      if state == 0 {
        state = 1;
      }
      let mut hits = 0u64;
      for i in 0..n_ops {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let k = state % (cap as u64);
        if cache.get(&k).await.is_some() {
          hits += 1;
        }
        if i % 10_000 == 0 {
          tokio::task::yield_now().await;
        }
      }
      hits
    }));
  }
  let mut total_hits = 0u64;
  for h in handles {
    total_hits += h.await.unwrap();
  }
  let async_elapsed = start.elapsed();
  println!(
    "ASYNC pure-get: {:.3}s {:.0} ops/sec hits={}",
    async_elapsed.as_secs_f64(),
    (n_clients * n_ops) as f64 / async_elapsed.as_secs_f64(),
    total_hits
  );
}
