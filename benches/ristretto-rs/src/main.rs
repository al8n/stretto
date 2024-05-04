#[macro_use]
extern crate serde;

use std::path::Path;

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

#[cfg(feature = "sync")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::fs;
    use std::time::Instant;
    use stretto::Cache;

    let content = fs::read(Path::new("mock.json"))?;
    let dataset: Dataset = serde_json::from_slice(content.as_slice())?;

    let c = Cache::builder(12960, 1e6 as i64)
        .set_metrics(true)
        .finalize()
        .unwrap();

    let time = Instant::now();
    for kv in dataset.data {
        if c.get(&kv.key).is_none() {
            c.insert(kv.key, kv.val, kv.cost);
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
    use stretto::AsyncCache;
    use tokio::fs;
    use tokio::time::Instant;

    let content = fs::read(Path::new("mock.json")).await?;
    let dataset: Dataset = serde_json::from_slice(content.as_slice())?;

    let c = AsyncCache::builder(12960, 1e6 as i64)
        .set_metrics(true)
        .finalize(tokio::spawn)
        .unwrap();

    let time = Instant::now();
    for kv in dataset.data {
        if c.get(&kv.key).await.is_none() {
            c.insert(kv.key, kv.val, kv.cost).await;
        }
    }
    c.wait().await.unwrap();
    let elapsed = time.elapsed();
    println!("---Async Stretto Finished in {}ms---", elapsed.as_millis());
    println!("{}", c.metrics);

    Ok(())
}
