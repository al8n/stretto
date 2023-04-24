use std::time::Duration;
use stretto::Cache;

fn main() {
    // cache is intended to take ownership of key and value
    let c = Cache::new(12960, 1e6 as i64).unwrap();

    // set a value with a cost of 1
    c.insert("key1".to_string(), "value1".to_string(), 1);
    // set a value with a cost of 1 and ttl
    c.insert_with_ttl(
        "key2".to_string(),
        "value2".to_string(),
        1,
        Duration::from_secs(3),
    );

    // wait for value to pass through buffers
    c.wait().unwrap();

    // Create a search key
    let key1 = "key1".to_string();
    // when we get the value, we will get a ValueRef, which contains a RwLockReadGuard
    // so when we finish use this value, we must release the ValueRef
    let v = c.get(&key1).unwrap();
    assert_eq!(v.value(), &"value1");
    v.release();

    // lock will be auto released when out of scope
    {
        // when we get the value, we will get a ValueRef, which contains a RwLockWriteGuard
        // so when we finish use this value, we must release the ValueRefMut
        let mut v = c.get_mut(&key1).unwrap();
        v.write("value3".to_string());
        assert_eq!(v.value(), &"value3");
        // release the value
    }

    // if you just want to do one operation
    let v = c.get_mut(&key1).unwrap();
    v.write_once("value4".to_string());

    let v = c.get(&key1).unwrap();
    assert_eq!(v.value(), &"value4");
    v.release();

    // clear the cache
    c.clear().unwrap();
    // wait all the operations are finished
    c.wait().unwrap();
    assert!(c.get(&key1).is_none());
}
