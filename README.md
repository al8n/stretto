<div align="center">
<h1>Stretto</h1>
</div>
<div align="center">

Stretto is a Rust implementation for https://github.com/dgraph-io/ristretto. A high performance thread-safe memory-bound Rust cache.

English | [简体中文](README-zh_CN.md)

[<img alt="github" src="https://img.shields.io/badge/GITHUB-Stretto-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/workflow/status/al8n/stretto/CI/main?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/stretto?style=for-the-badge&token=P175Q03Q1L&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-stretto-66c2a5?style=for-the-badge&labelColor=555555&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/stretto?style=for-the-badge&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMiA1MTIiIHhtbDpzcGFjZT0icHJlc2VydmUiPg0KPGc+DQoJPGc+DQoJCTxwYXRoIGQ9Ik0yNTYsMEwzMS41MjgsMTEyLjIzNnYyODcuNTI4TDI1Niw1MTJsMjI0LjQ3Mi0xMTIuMjM2VjExMi4yMzZMMjU2LDB6IE0yMzQuMjc3LDQ1Mi41NjRMNzQuOTc0LDM3Mi45MTNWMTYwLjgxDQoJCQlsMTU5LjMwMyw3OS42NTFWNDUyLjU2NHogTTEwMS44MjYsMTI1LjY2MkwyNTYsNDguNTc2bDE1NC4xNzQsNzcuMDg3TDI1NiwyMDIuNzQ5TDEwMS44MjYsMTI1LjY2MnogTTQzNy4wMjYsMzcyLjkxMw0KCQkJbC0xNTkuMzAzLDc5LjY1MVYyNDAuNDYxbDE1OS4zMDMtNzkuNjUxVjM3Mi45MTN6IiBmaWxsPSIjRkZGIi8+DQoJPC9nPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPC9zdmc+DQo=" height="22">][crates-url]
[<img alt="rustc" src="https://img.shields.io/badge/MSRV-1.55.0-fc8d62.svg?style=for-the-badge&logo=Rust" height="22">][rustc-url]

[<img alt="license-apache" src="https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=for-the-badge&logo=Apache" height="22">][license-apache-url]
[<img alt="license-mit" src="https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge&fontColor=white&logoColor=f5c076&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGhlaWdodD0iMzZweCIgdmlld0JveD0iMCAwIDI0IDI0IiB3aWR0aD0iMzZweCIgZmlsbD0iI2Y1YzA3NiI+PHBhdGggZD0iTTAgMGgyNHYyNEgwVjB6IiBmaWxsPSJub25lIi8+PHBhdGggZD0iTTEwLjA4IDEwLjg2Yy4wNS0uMzMuMTYtLjYyLjMtLjg3cy4zNC0uNDYuNTktLjYyYy4yNC0uMTUuNTQtLjIyLjkxLS4yMy4yMy4wMS40NC4wNS42My4xMy4yLjA5LjM4LjIxLjUyLjM2cy4yNS4zMy4zNC41My4xMy40Mi4xNC42NGgxLjc5Yy0uMDItLjQ3LS4xMS0uOS0uMjgtMS4yOXMtLjQtLjczLS43LTEuMDEtLjY2LS41LTEuMDgtLjY2LS44OC0uMjMtMS4zOS0uMjNjLS42NSAwLTEuMjIuMTEtMS43LjM0cy0uODguNTMtMS4yLjkyLS41Ni44NC0uNzEgMS4zNlM4IDExLjI5IDggMTEuODd2LjI3YzAgLjU4LjA4IDEuMTIuMjMgMS42NHMuMzkuOTcuNzEgMS4zNS43Mi42OSAxLjIuOTFjLjQ4LjIyIDEuMDUuMzQgMS43LjM0LjQ3IDAgLjkxLS4wOCAxLjMyLS4yM3MuNzctLjM2IDEuMDgtLjYzLjU2LS41OC43NC0uOTQuMjktLjc0LjMtMS4xNWgtMS43OWMtLjAxLjIxLS4wNi40LS4xNS41OHMtLjIxLjMzLS4zNi40Ni0uMzIuMjMtLjUyLjNjLS4xOS4wNy0uMzkuMDktLjYuMS0uMzYtLjAxLS42Ni0uMDgtLjg5LS4yMy0uMjUtLjE2LS40NS0uMzctLjU5LS42MnMtLjI1LS41NS0uMy0uODgtLjA4LS42Ny0uMDgtMXYtLjI3YzAtLjM1LjAzLS42OC4wOC0xLjAxek0xMiAyQzYuNDggMiAyIDYuNDggMiAxMnM0LjQ4IDEwIDEwIDEwIDEwLTQuNDggMTAtMTBTMTcuNTIgMiAxMiAyem0wIDE4Yy00LjQxIDAtOC0zLjU5LTgtOHMzLjU5LTggOC04IDggMy41OSA4IDgtMy41OSA4LTggOHoiLz48L3N2Zz4=" height="22">][license-mit-url]

</div>

## Features
* **Internal Mutability** - Do not need to use `Arc<RwLock<Cache<...>>` for concurrent code, you just need `Arc<Cache<...>`
* **Sync and Async** - Stretto support async by `tokio` and sync by `crossbeam`.
  * In sync, Cache starts two extra OS level threads. One is policy thread, the other is writing thread.
  * In async, Cache starts two extra green threads. One is policy thread, the other is writing thread.
* **Store policy** Stretto only store the value, which means the cache does not store the key. 
* **High Hit Ratios** - with Dgraph's developers unique admission/eviction policy pairing, Ristretto's performance is best in class.
    * **Eviction: SampledLFU** - on par with exact LRU and better performance on Search and Database traces.
    * **Admission: TinyLFU** - extra performance with little memory overhead (12 bits per counter).
* **Fast Throughput** - use a variety of techniques for managing contention and the result is excellent throughput.
* **Cost-Based Eviction** - any large new item deemed valuable can evict multiple smaller items (cost could be anything).
* **Fully Concurrent** - you can use as many threads as you want with little throughput degradation.
* **Metrics** - optional performance metrics for throughput, hit ratios, and other stats.
* **Simple API** - just figure out your ideal `CacheBuilder` values and you're off and running.

## Table of Contents

* [Usage](#Usage)
    * [Example](#Example)
      * [Sync](#Sync)
      * [Async](#Async)
    * [Config](#Config)
        * [num_counters](#num_counters)
        * [max_cost](#max_cost)
        * [key_builder](#key_builder)
        * [buffer_size](#buffer_size)
        * [metrics](#metrics)
        * [ignore_internal_cost](#ignore_internal_cost)
        * [cleanup_duration](#cleanup_duration)
        * [update_validator](#update_validator)
        * [callback](#callback)
        * [coster](#coster)
        * [hasher](#hasher)

## Usage
### Example
#### Sync
```rust
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
```

#### Async
```rust
use stretto::{Cache, DefaultKeyBuilder};

#[tokio::main]
async fn main() {
    let c = Cache::new(12960, 1e6 as i64, DefaultKeyBuilder::default()).unwrap();

    // set a value with a cost of 1
    c.insert("a", "a", 1).await;

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
```
### Config 
The `CacheBuilder` struct is used when creating Cache instances if you want to customize the Cache settings.

#### num_counters

`num_counters` is the number of 4-bit access counters to keep for admission and eviction. Dgraph's developers have seen good performance in setting this to 10x the number of items you expect to keep in the cache when full.

For example, if you expect each item to have a cost of 1 and `max_cost` is 100, set `num_counters` to 1,000. Or, if you use variable cost values but expect the cache to hold around 10,000 items when full, set num_counters to 100,000. The important thing is the *number of unique items* in the full cache, not necessarily the `max_cost` value.

#### max_cost

`max_cost` is how eviction decisions are made. For example, if max_cost is 100 and a new item with a cost of 1 increases total cache cost to 101, 1 item will be evicted.

`max_cost` can also be used to denote the max size in bytes. For example, if max_cost is 1,000,000 (1MB) and the cache is full with 1,000 1KB items, a new item (that's accepted) would cause 5 1KB items to be evicted.

`max_cost` could be anything as long as it matches how you're using the cost values when calling `insert`.

#### key_builder

```rust
pub trait KeyBuilder<K: Hash + Eq + ?Sized> {
    /// hash_index is used to hash the key to u64
    fn hash_index(&self, key: &K) -> u64;

    /// if you want a 128bit hashes, you should implement this method,
    /// or leave this method return 0
    fn hash_conflict(&self, key: &K) -> u64 { 0 }

    /// build the key to 128bit hashes.
    fn build_key(&self, k: &K) -> (u64, u64) {
        (self.hash_index(k), self.hash_conflict(k))
    }
}
```

KeyBuilder is the hashing algorithm used for every key. In Stretto, the Cache will never store the real key.
The key will be processed by `KeyBuilder`. Stretto has two default built-in key builder,
one is `TransparentKeyBuilder`, the other is `DefaultKeyBuilder`. If your key implements `TransparentKey` trait,
you can use `TransparentKeyBuilder` which is faster than `DefaultKeyBuilder`. Otherwise, you should use `DefaultKeyBuilder`
You can also write your own key builder for the Cache, by implementing `KeyBuilder` trait.

Note that if you want 128bit hashes you should use the full `(u64, u64)`,
otherwise just fill the `u64` at the `0` position, and it will behave like
any 64bit hash.

#### buffer_size

`buffer_size` is the size of the insert buffers. The Dgraph's developers find that 32 * 1024 gives a good performance.

If for some reason you see insert performance decreasing with lots of contention (you shouldn't), try increasing this value in increments of 32 * 1024. This is a fine-tuning mechanism and you probably won't have to touch this.

#### metrics

Metrics is true when you want real-time logging of a variety of stats. The reason this is a CacheBuilder flag is because there's a 10% throughput performance overhead.

#### ignore_internal_cost

Set to true indicates to the cache that the cost of internally storing the value should be ignored. This is useful when the 
cost passed to set is not using bytes as units. Keep in mind that setting this to true will increase the memory usage.

#### cleanup_duration

The Cache will cleanup the expired values every 500ms by default.

#### update_validator

```rust
pub trait UpdateValidator<V>: Send + Sync + 'static {
    /// should_update is called when a value already exists in cache and is being updated.
    fn should_update(&self, prev: &V, curr: &V) -> bool;
}
```

By default, the Cache will always update the value if the value already exists in the cache,
this trait is for you to check if the value should be updated.

#### callback

```rust
pub trait CacheCallback<V: Send + Sync>: Send + Sync + 'static {
    /// on_exit is called whenever a value is removed from cache. This can be
    /// used to do manual memory deallocation. Would also be called on eviction
    /// and rejection of the value.
    fn on_exit(&self, val: Option<V>);

    /// on_evict is called for every eviction and passes the hashed key, value,
    /// and cost to the function.
    fn on_evict(&self, item: Item<V>) {
        self.on_exit(item.val)
    }

    /// on_reject is called for every rejection done via the policy.
    fn on_reject(&self, item: Item<V>) {
        self.on_exit(item.val)
    }
}
```

CacheCallback is for customize some extra operations on values when related event happens.

#### coster

```rust
pub trait Coster<V>: Send + Sync + 'static {
    /// cost evaluates a value and outputs a corresponding cost. This function
    /// is ran after insert is called for a new item or an item update with a cost
    /// param of 0.
    fn cost(&self, val: &V) -> i64;
}
```

Cost is a trait you can pass to the CacheBuilder in order to evaluate
item cost at runtime, and only for the `insert` calls that aren't dropped (this is
useful if calculating item cost is particularly expensive, and you don't want to
waste time on items that will be dropped anyways).

To signal to Stretto that you'd like to use this Coster trait:

1. Set the Coster field to your own Coster implementation.
2. When calling `insert` for new items or item updates, use a `cost` of 0.

#### hasher

The hasher for the Cache, default is SipHasher.

## Acknowledgements
- Thanks Dgraph's developers for providing amazing Go version [Ristretto](https://github.com/dgraph-io/ristretto) implementation.

## License

<sup>
Licensed under either of <a href="https://opensource.org/licenses/Apache-2.0">Apache License, Version
2.0</a> or <a href="https://opensource.org/licenses/MIT">MIT license</a> at your option.
</sup>

<br>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.
</sub>

[Github-url]: https://github.com/al8n/stretto/
[CI-url]: https://github.com/al8n/stretto/actions/workflows/ci.yml
[doc-url]: https://docs.rs/stretto
[crates-url]: https://crates.io/crates/stretto
[codecov-url]: https://app.codecov.io/gh/al8n/stretto/
[license-url]: https://opensource.org/licenses/Apache-2.0
[rustc-url]: https://github.com/rust-lang/rust/blob/master/RELEASES.md
[license-apache-url]: https://opensource.org/licenses/Apache-2.0
[license-mit-url]: https://opensource.org/licenses/MIT
[rustc-image]: https://img.shields.io/badge/rustc-1.52.0--nightly%2B-orange.svg?style=for-the-badge&logo=Rust
