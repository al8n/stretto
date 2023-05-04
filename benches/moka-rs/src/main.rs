#[macro_use]
extern crate serde;

use std::{
    fmt,
    hash::{BuildHasher, Hasher},
    path::Path,
};

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Serialize, Deserialize)]
struct Dataset {
    data: Vec<KV>,
}

#[derive(Serialize, Deserialize)]
struct KV {
    key: String,
    val: String,
    hash: u64,
    conflict: u64,
    cost: i64,
}

#[derive(Default)]
struct DumbHasher(u64);

impl Hasher for DumbHasher {
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }

    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!()
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

#[derive(Clone, Default)]
struct DumbBuildHasher;

impl BuildHasher for DumbBuildHasher {
    type Hasher = DumbHasher;

    fn build_hasher(&self) -> Self::Hasher {
        DumbHasher::default()
    }
}

#[derive(Default)]
struct OpMetrics {
    hit: u64,
    miss: u64,
    gets_total: u64,
}

impl OpMetrics {
    fn hit_ratio(&self) -> f64 {
        if self.gets_total == 0 {
            0.0
        } else {
            let gt = self.gets_total as f64;
            self.hit as f64 / gt
        }
    }
}

impl fmt::Debug for OpMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hit_ratio = (self.hit_ratio() * 100.0).round() / 100.0;
        f.debug_struct("OpMetrics")
            .field("hit", &self.hit)
            .field("miss", &self.miss)
            .field("gets_total", &self.gets_total)
            .field("hit_ratio", &hit_ratio)
            .finish()
    }
}

#[cfg(feature = "sync")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use moka::sync::CacheBuilder;
    use std::fs;
    use std::time::Instant;

    let content = fs::read(Path::new("mock.json"))?;
    let mut dataset: Dataset = serde_json::from_slice(content.as_slice())?;
    recalc_hash(&mut dataset);
    let dataset = dataset;

    let mut metrics = OpMetrics::default();

    let c = CacheBuilder::new(12960).build_with_hasher(DumbBuildHasher::default());
    let time = Instant::now();

    for kv in dataset.data {
        metrics.gets_total += 1;
        if c.get(&kv.hash).is_none() {
            c.insert(kv.hash, kv.val);
            metrics.miss += 1;
        } else {
            metrics.hit += 1;
        }
    }
    let elapsed = time.elapsed();
    println!("---Sync Moka Finished in {}ms---", elapsed.as_millis());
    println!("{:?}", metrics);

    Ok(())
}

#[cfg(not(feature = "sync"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use moka::future::CacheBuilder;
    use std::fs;
    use std::time::Instant;

    let content = fs::read(Path::new("mock.json"))?;
    let mut dataset: Dataset = serde_json::from_slice(content.as_slice())?;
    recalc_hash(&mut dataset);
    let dataset = dataset;

    let mut metrics = OpMetrics::default();

    let c = CacheBuilder::new(12960).build_with_hasher(DumbBuildHasher::default());
    let time = Instant::now();

    for kv in dataset.data {
        metrics.gets_total += 1;
        if c.get(&kv.hash).is_none() {
            c.insert(kv.hash, kv.val).await;
            metrics.miss += 1;
        } else {
            metrics.hit += 1;
        }
    }
    let elapsed = time.elapsed();
    println!("---Async Moka Finished in {}ms---", elapsed.as_millis());
    println!("{:?}", metrics);

    Ok(())
}

fn recalc_hash(dataset: &mut Dataset) {
    use std::hash::Hash;
    let build_hasher = std::collections::hash_map::RandomState::default();
    for kv in &mut dataset.data {
        let key = &kv.key;
        let mut hasher = build_hasher.build_hasher();
        key.hash(&mut hasher);
        kv.hash = hasher.finish();
        kv.conflict = 0;
    }
}
