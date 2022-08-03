#[macro_use]
extern crate serde;

use std::path::Path;
use stretto::KeyBuilder;

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

#[derive(Hash, Eq, PartialEq)]
struct KC {
    hash: u64,
    conflict: u64,
}

#[derive(Default)]
struct KH;

impl KeyBuilder for KH {
    type Key = KC;

    fn hash_index(&self, key: &KC) -> u64 {
        key.hash
    }

    fn hash_conflict(&self, key: &KC) -> u64 {
        key.conflict
    }
}

#[cfg(feature = "sync")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::fs;
    use std::time::Instant;
    use stretto::Cache;

    let content = fs::read(Path::new("mock.json"))?;
    let dataset: Dataset = serde_json::from_slice(content.as_slice())?;

    let c = Cache::builder(12960, 1e6 as i64)
        .set_key_builder(KH::default())
        .set_metrics(true)
        .finalize()
        .unwrap();

    let time = Instant::now();
    for kv in dataset.data {
        let kc = KC {
            hash: kv.hash,
            conflict: kv.conflict,
        };
        if c.get(&kc).is_none() {
            c.insert(kc, kv.val, kv.cost);
        }
    }
    c.wait().unwrap();
    let elapsed = time.elapsed();
    println!("---Sync Stretto Finished in {}ms---", elapsed.as_millis());
    println!("{}", c.metrics);

    Ok(())
}

#[cfg(feature = "async")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use tokio::fs;
    use tokio::time::Instant;
    use stretto::AsyncCache;

    let content = fs::read(Path::new("mock.json")).await?;
    let dataset: Dataset = serde_json::from_slice(content.as_slice())?;

    let c = AsyncCache::builder(12960, 1e6 as i64)
        .set_key_builder(KH::default())
        .set_metrics(true)
        .finalize(tokio::spawn)
        .unwrap();

    let time = Instant::now();
    for kv in dataset.data {
        let kc = KC {
            hash: kv.hash,
            conflict: kv.conflict,
        };
        if c.get(&kc).is_none() {
            c.insert(kc, kv.val, kv.cost).await;
        }
    }
    c.wait().await.unwrap();
    let elapsed = time.elapsed();
    println!("---Async Stretto Finished in {}ms---", elapsed.as_millis());
    println!("{}", c.metrics);

    Ok(())
}
