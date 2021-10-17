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
    use moka::sync::Cache;
    use std::fs;
    use std::time::Instant;

    let content = fs::read(Path::new("mock.json"))?;
    let dataset: Dataset = serde_json::from_slice(content.as_slice())?;

    let c = Cache::new(12960);
    let time = Instant::now();

    for kv in dataset.data {
        if let None = c.get(&kv.key) {
            c.insert(kv.key, kv.val);
        }
    }
    let elapsed = time.elapsed();
    println!("---Sync Moka Finished in {}ms---", elapsed.as_millis());

    Ok(())
}

#[cfg(not(feature = "sync"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use moka::future::Cache;
    use std::fs;
    use std::time::Instant;

    let content = fs::read(Path::new("mock.json"))?;
    let dataset: Dataset = serde_json::from_slice(content.as_slice())?;

    let c = Cache::new(12960);
    let time = Instant::now();

    for kv in dataset.data {
        if let None = c.get(&kv.key) {
            c.insert(kv.key, kv.val).await;
        }
    }
    let elapsed = time.elapsed();
    println!("---Async Moka Finished in {}ms---", elapsed.as_millis());

    Ok(())
}

