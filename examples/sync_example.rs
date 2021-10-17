use stretto::{Cache, DefaultKeyBuilder};

fn main() {
    let c = Cache::new(12960, 1e6 as i64, DefaultKeyBuilder::default()).unwrap();

    // set a value with a cost of 1
    c.insert("a", "a", 1);

    // wait for value to pass through buffers
    c.wait().unwrap();

    // when we get the value, we will get a ValueRef, which contains a RwLockReadGuard
    // so when we finish use this value, we must release the ValueRef
    let v = c.get(&"a").unwrap();
    assert_eq!(v.value(), &"a");
    v.release();

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
    assert_eq!(v.value(), &"aaa");
    v.release();

    // clear the cache
    c.clear().unwrap();
    // wait all the operations are finished
    c.wait().unwrap();
    assert!(c.get(&"a").is_none());
}
