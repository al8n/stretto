use std::time::Duration;
use stretto::{AsyncCache, DefaultKeyBuilder};

#[tokio::main]
async fn main() {
    let c: AsyncCache<&str, &str, DefaultKeyBuilder> =
        AsyncCache::new(12960, 1e6 as i64, DefaultKeyBuilder::default()).unwrap();

    // set a value with a cost of 1
    c.insert("a", "a", 1).await;

    // set a value with a cost of 1 and ttl
    c.insert_with_ttl("b", "b", 1, Duration::from_secs(3)).await;

    // wait for value to pass through buffers
    c.wait().await.unwrap();

    // when we get the value, we will get a ValueRef, which contains a RwLockReadGuard
    // so when we finish use this value, we must release the ValueRef
    let v = c.get(&"a").unwrap();
    assert_eq!(v.value(), &"a");
    // release the value
    v.release(); // or drop(v)

    // when we get the value, we will get a ValueRef, which contains a RwLockWriteGuard
    // so when we finish use this value, we must release the ValueRefMut
    let mut v = c.get_mut(&"a").unwrap();
    v.write("aa");
    assert_eq!(v.value(), &"aa");
    // release the value
    v.release(); // or use drop(v);

    // if you just want to do one operation
    let v = c.get_mut(&"a").unwrap();
    v.write_once("aaa");

    let v = c.get(&"a").unwrap();
    println!("{}", v);
    assert_eq!(v.value(), &"aaa");
    v.release();

    // clear the cache
    c.clear().unwrap();
    // wait all the operations are finished
    c.wait().await.unwrap();

    assert!(c.get(&"a").is_none());
}
