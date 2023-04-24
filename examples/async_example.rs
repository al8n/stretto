use std::time::Duration;
use stretto::AsyncCache;

#[tokio::main]
async fn main() {
    // cache is intended to take ownership of key and value
    let c: AsyncCache<String, String> = AsyncCache::new(12960, 1e6 as i64, tokio::spawn).unwrap();

    // set a value with a cost of 1
    c.insert("key1".to_string(), "value1".to_string(), 1).await;

    // set a value with a cost of 1 and ttl
    c.insert_with_ttl(
        "key2".to_string(),
        "value2".to_string(),
        1,
        Duration::from_secs(3),
    )
    .await;

    // wait for value to pass through buffers
    c.wait().await.unwrap();

    // Create a search key
    let key1 = "key1".to_string();
    // when we get the value, we will get a ValueRef, which contains a RwLockReadGuard
    // so when we finish use this value, we must release the ValueRef
    let v = c.get(&key1).await.unwrap();
    assert_eq!(v.value(), &"value1");
    // release the value
    v.release(); // or drop(v)

    // lock will be auto released when out of scope
    {
        // when we get the value, we will get a ValueRef, which contains a RwLockWriteGuard
        // so when we finish use this value, we must release the ValueRefMut
        let mut v = c.get_mut(&key1).await.unwrap();
        v.write("value2".to_string());
        assert_eq!(v.value(), &"value2");
        // release the value
    }

    // if you just want to do one operation
    let v = c.get_mut(&key1).await.unwrap();
    v.write_once("value3".to_string());

    let v = c.get(&key1).await.unwrap();
    assert_eq!(v.value(), &"value3");
    v.release();

    // clear the cache
    c.clear().await.unwrap();
    // wait all the operations are finished
    c.wait().await.unwrap();

    assert!(c.get(&key1).await.is_none());
}
