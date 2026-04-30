// Sync equivalent of async_bench.rs — same get_or_insert pattern over a
// keyspace slightly larger than capacity. Used to localize the async/sync
// performance gap.

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Instant,
};

use stretto::Cache;

#[inline]
fn xorshift(state: &mut u64) -> u64 {
  let mut x = *state;
  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;
  *state = x;
  x
}

fn main() {
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

  let cache: Cache<u64, u64> = Cache::new(cap as usize * 10, cap).unwrap();
  let cache = Arc::new(cache);

  let hits = Arc::new(AtomicU64::new(0));
  let misses = Arc::new(AtomicU64::new(0));

  let start = Instant::now();
  let mut handles = Vec::with_capacity(n_clients);
  for c in 0..n_clients {
    let cache = Arc::clone(&cache);
    let hits = Arc::clone(&hits);
    let misses = Arc::clone(&misses);
    handles.push(std::thread::spawn(move || {
      let mut state: u64 = 0xdead_beef ^ ((c as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
      if state == 0 {
        state = 1;
      }
      let mut local_hits = 0u64;
      let mut local_misses = 0u64;
      for _ in 0..n_ops {
        let k = xorshift(&mut state) % key_space;
        if cache.get(&k).is_some() {
          local_hits += 1;
        } else {
          cache.insert(k, k, 1);
          local_misses += 1;
        }
      }
      hits.fetch_add(local_hits, Ordering::Relaxed);
      misses.fetch_add(local_misses, Ordering::Relaxed);
    }));
  }
  for h in handles {
    h.join().unwrap();
  }
  let producer_elapsed = start.elapsed();
  cache.wait().unwrap();
  let elapsed = start.elapsed();

  let total_ops = (n_clients * n_ops) as f64;
  let h = hits.load(Ordering::Relaxed);
  let m = misses.load(Ordering::Relaxed);
  let hit_ratio = h as f64 / (h + m) as f64;
  println!(
    "SYNC cap={} clients={} ops/client={} total={} key_space={} producers={:.3}s wait={:.3}s total={:.3}s {:.0} ops/sec hits={} misses={} hit_ratio={:.2}%",
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
