//! Insert-throughput microbenchmarks. Establishes a regression floor
//! for the striped insert buffer's hot path. Run with:
//!     cargo bench --features sync --bench insert
//!
//! Two scenarios:
//!   - Single-thread baseline: pure producer-side cost, processor
//!     mostly idle.
//!   - 8-thread fan-in: stripe contention, channel batching, and
//!     processor under sustained load.

use std::{sync::Arc, thread};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use stretto::{Cache, TransparentKeyBuilder};

fn insert_throughput_single_thread(c: &mut Criterion) {
  let cache: Cache<u64, u64, TransparentKeyBuilder<u64>> = Cache::builder(1_000_000, 100_000)
    .set_key_builder(TransparentKeyBuilder::default())
    .set_ignore_internal_cost(true)
    .finalize()
    .unwrap();
  let mut k = 0u64;
  c.bench_function("insert_single_thread", |b| {
    b.iter(|| {
      cache.insert(black_box(k), k, 1);
      k = k.wrapping_add(1);
    });
  });
}

fn insert_throughput_8_threads(c: &mut Criterion) {
  let cache: Arc<Cache<u64, u64, TransparentKeyBuilder<u64>>> = Arc::new(
    Cache::builder(1_000_000, 100_000)
      .set_key_builder(TransparentKeyBuilder::default())
      .set_ignore_internal_cost(true)
      .finalize()
      .unwrap(),
  );
  c.bench_function("insert_8_threads_per_iter_8k_inserts", |b| {
    b.iter_custom(|iters| {
      let start = std::time::Instant::now();
      for _ in 0..iters {
        let mut handles = Vec::new();
        for t in 0..8u64 {
          let cache = cache.clone();
          handles.push(thread::spawn(move || {
            for i in 0..1024u64 {
              cache.insert(black_box(t * 1024 + i), t * 1024 + i, 1);
            }
          }));
        }
        for h in handles {
          h.join().unwrap();
        }
      }
      start.elapsed()
    });
  });
}

criterion_group!(
  insert,
  insert_throughput_single_thread,
  insert_throughput_8_threads
);
criterion_main!(insert);
