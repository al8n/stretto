// Reproduction of the cachebench S3 workload pattern: get_or_insert against a
// keyspace slightly larger than capacity, with mostly-hit traffic. This is
// the workload that exposed the async/sync gap (sync ~1.4s vs async ~190s
// at cap=400k) — a get-heavy mixed loop where the sync cache amortizes
// ring-buffer flushes very cheaply.
//
// Build:
//   cargo build --release --features async,sync,tokio --example async_bench

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Instant,
};

use stretto::{AsyncCache, TokioRuntime};

#[inline]
fn xorshift(state: &mut u64) -> u64 {
  let mut x = *state;
  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;
  *state = x;
  x
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
  let cap: i64 = std::env::args()
    .nth(1)
    .as_deref()
    .and_then(|s| s.parse().ok())
    .unwrap_or(400_000);
  let n_clients: usize = std::env::args()
    .nth(2)
    .as_deref()
    .and_then(|s| s.parse().ok())
    .unwrap_or(16);
  let n_ops: usize = std::env::args()
    .nth(3)
    .as_deref()
    .and_then(|s| s.parse().ok())
    .unwrap_or(1_000_000);
  let key_ratio_pct: u64 = std::env::args()
    .nth(4)
    .as_deref()
    .and_then(|s| s.parse().ok())
    .unwrap_or(125);

  let key_space: u64 = ((cap as u64) * key_ratio_pct) / 100;

  let cache: AsyncCache<u64, u64> =
    AsyncCache::new::<TokioRuntime>(cap as usize * 10, cap).unwrap();
  let cache = Arc::new(cache);

  let hits = Arc::new(AtomicU64::new(0));
  let misses = Arc::new(AtomicU64::new(0));

  let start = Instant::now();
  let mut handles = Vec::with_capacity(n_clients);
  for c in 0..n_clients {
    let cache = Arc::clone(&cache);
    let hits = Arc::clone(&hits);
    let misses = Arc::clone(&misses);
    handles.push(tokio::spawn(async move {
      let mut state: u64 = 0xdead_beef ^ ((c as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
      if state == 0 {
        state = 1;
      }
      let mut local_hits = 0u64;
      let mut local_misses = 0u64;
      for i in 0..n_ops {
        let k = xorshift(&mut state) % key_space;
        if cache.get(&k).await.is_some() {
          local_hits += 1;
        } else {
          cache.insert(k, k, 1).await;
          local_misses += 1;
        }
        if i % 10_000 == 0 {
          tokio::task::yield_now().await;
        }
      }
      hits.fetch_add(local_hits, Ordering::Relaxed);
      misses.fetch_add(local_misses, Ordering::Relaxed);
    }));
  }
  for h in handles {
    h.await.unwrap();
  }
  let producer_elapsed = start.elapsed();
  cache.wait().await.unwrap();
  let elapsed = start.elapsed();

  let total_ops = (n_clients * n_ops) as f64;
  let h = hits.load(Ordering::Relaxed);
  let m = misses.load(Ordering::Relaxed);
  let hit_ratio = h as f64 / (h + m) as f64;
  println!(
    "ASYNC cap={} clients={} ops/client={} total={} key_space={} producers={:.3}s wait={:.3}s total={:.3}s {:.0} ops/sec hits={} misses={} hit_ratio={:.2}%",
    cap,
    n_clients,
    n_ops,
    total_ops as u64,
    key_space,
    producer_elapsed.as_secs_f64(),
    (elapsed - producer_elapsed).as_secs_f64(),
    elapsed.as_secs_f64(),
    total_ops / elapsed.as_secs_f64(),
    h,
    m,
    hit_ratio * 100.0,
  );
}
