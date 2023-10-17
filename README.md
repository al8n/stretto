<div align="center">
<h1>Stretto</h1>
</div>
<div align="center">

Stretto is a pure Rust implementation for https://github.com/dgraph-io/ristretto. 

A high performance thread-safe memory-bound Rust cache.

English | [简体中文](README-zh_hans.md)

[<img alt="github" src="https://img.shields.io/badge/GITHUB-al8n/Stretto-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/stretto/ci.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/stretto?style=for-the-badge&token=P175Q03Q1L&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-stretto-66c2a5?style=for-the-badge&labelColor=555555&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/stretto?style=for-the-badge&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMiA1MTIiIHhtbDpzcGFjZT0icHJlc2VydmUiPg0KPGc+DQoJPGc+DQoJCTxwYXRoIGQ9Ik0yNTYsMEwzMS41MjgsMTEyLjIzNnYyODcuNTI4TDI1Niw1MTJsMjI0LjQ3Mi0xMTIuMjM2VjExMi4yMzZMMjU2LDB6IE0yMzQuMjc3LDQ1Mi41NjRMNzQuOTc0LDM3Mi45MTNWMTYwLjgxDQoJCQlsMTU5LjMwMyw3OS42NTFWNDUyLjU2NHogTTEwMS44MjYsMTI1LjY2MkwyNTYsNDguNTc2bDE1NC4xNzQsNzcuMDg3TDI1NiwyMDIuNzQ5TDEwMS44MjYsMTI1LjY2MnogTTQzNy4wMjYsMzcyLjkxMw0KCQkJbC0xNTkuMzAzLDc5LjY1MVYyNDAuNDYxbDE1OS4zMDMtNzkuNjUxVjM3Mi45MTN6IiBmaWxsPSIjRkZGIi8+DQoJPC9nPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPC9zdmc+DQo=" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/stretto?color=critical&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBzdGFuZGFsb25lPSJubyI/PjwhRE9DVFlQRSBzdmcgUFVCTElDICItLy9XM0MvL0RURCBTVkcgMS4xLy9FTiIgImh0dHA6Ly93d3cudzMub3JnL0dyYXBoaWNzL1NWRy8xLjEvRFREL3N2ZzExLmR0ZCI+PHN2ZyB0PSIxNjQ1MTE3MzMyOTU5IiBjbGFzcz0iaWNvbiIgdmlld0JveD0iMCAwIDEwMjQgMTAyNCIgdmVyc2lvbj0iMS4xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHAtaWQ9IjM0MjEiIGRhdGEtc3BtLWFuY2hvci1pZD0iYTMxM3guNzc4MTA2OS4wLmkzIiB3aWR0aD0iNDgiIGhlaWdodD0iNDgiIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIj48ZGVmcz48c3R5bGUgdHlwZT0idGV4dC9jc3MiPjwvc3R5bGU+PC9kZWZzPjxwYXRoIGQ9Ik00NjkuMzEyIDU3MC4yNHYtMjU2aDg1LjM3NnYyNTZoMTI4TDUxMiA3NTYuMjg4IDM0MS4zMTIgNTcwLjI0aDEyOHpNMTAyNCA2NDAuMTI4QzEwMjQgNzgyLjkxMiA5MTkuODcyIDg5NiA3ODcuNjQ4IDg5NmgtNTEyQzEyMy45MDQgODk2IDAgNzYxLjYgMCA1OTcuNTA0IDAgNDUxLjk2OCA5NC42NTYgMzMxLjUyIDIyNi40MzIgMzAyLjk3NiAyODQuMTYgMTk1LjQ1NiAzOTEuODA4IDEyOCA1MTIgMTI4YzE1Mi4zMiAwIDI4Mi4xMTIgMTA4LjQxNiAzMjMuMzkyIDI2MS4xMkM5NDEuODg4IDQxMy40NCAxMDI0IDUxOS4wNCAxMDI0IDY0MC4xOTJ6IG0tMjU5LjItMjA1LjMxMmMtMjQuNDQ4LTEyOS4wMjQtMTI4Ljg5Ni0yMjIuNzItMjUyLjgtMjIyLjcyLTk3LjI4IDAtMTgzLjA0IDU3LjM0NC0yMjQuNjQgMTQ3LjQ1NmwtOS4yOCAyMC4yMjQtMjAuOTI4IDIuOTQ0Yy0xMDMuMzYgMTQuNC0xNzguMzY4IDEwNC4zMi0xNzguMzY4IDIxNC43MiAwIDExNy45NTIgODguODMyIDIxNC40IDE5Ni45MjggMjE0LjRoNTEyYzg4LjMyIDAgMTU3LjUwNC03NS4xMzYgMTU3LjUwNC0xNzEuNzEyIDAtODguMDY0LTY1LjkyLTE2NC45MjgtMTQ0Ljk2LTE3MS43NzZsLTI5LjUwNC0yLjU2LTUuODg4LTMwLjk3NnoiIGZpbGw9IiNmZmZmZmYiIHAtaWQ9IjM0MjIiIGRhdGEtc3BtLWFuY2hvci1pZD0iYTMxM3guNzc4MTA2OS4wLmkwIiBjbGFzcz0iIj48L3BhdGg+PC9zdmc+&style=for-the-badge" height="22">][crates-url]

<img alt="license" src="https://img.shields.io/badge/License-Apache%202.0/MIT-blue.svg?style=for-the-badge&fontColor=white&logoColor=f5c076&logo=data:image/svg+xml;base64,PCFET0NUWVBFIHN2ZyBQVUJMSUMgIi0vL1czQy8vRFREIFNWRyAxLjEvL0VOIiAiaHR0cDovL3d3dy53My5vcmcvR3JhcGhpY3MvU1ZHLzEuMS9EVEQvc3ZnMTEuZHRkIj4KDTwhLS0gVXBsb2FkZWQgdG86IFNWRyBSZXBvLCB3d3cuc3ZncmVwby5jb20sIFRyYW5zZm9ybWVkIGJ5OiBTVkcgUmVwbyBNaXhlciBUb29scyAtLT4KPHN2ZyBmaWxsPSIjZmZmZmZmIiBoZWlnaHQ9IjgwMHB4IiB3aWR0aD0iODAwcHgiIHZlcnNpb249IjEuMSIgaWQ9IkNhcGFfMSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIiB4bWxuczp4bGluaz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94bGluayIgdmlld0JveD0iMCAwIDI3Ni43MTUgMjc2LjcxNSIgeG1sOnNwYWNlPSJwcmVzZXJ2ZSIgc3Ryb2tlPSIjZmZmZmZmIj4KDTxnIGlkPSJTVkdSZXBvX2JnQ2FycmllciIgc3Ryb2tlLXdpZHRoPSIwIi8+Cg08ZyBpZD0iU1ZHUmVwb190cmFjZXJDYXJyaWVyIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiLz4KDTxnIGlkPSJTVkdSZXBvX2ljb25DYXJyaWVyIj4gPGc+IDxwYXRoIGQ9Ik0xMzguMzU3LDBDNjIuMDY2LDAsMCw2Mi4wNjYsMCwxMzguMzU3czYyLjA2NiwxMzguMzU3LDEzOC4zNTcsMTM4LjM1N3MxMzguMzU3LTYyLjA2NiwxMzguMzU3LTEzOC4zNTcgUzIxNC42NDgsMCwxMzguMzU3LDB6IE0xMzguMzU3LDI1OC43MTVDNzEuOTkyLDI1OC43MTUsMTgsMjA0LjcyMywxOCwxMzguMzU3UzcxLjk5MiwxOCwxMzguMzU3LDE4IHMxMjAuMzU3LDUzLjk5MiwxMjAuMzU3LDEyMC4zNTdTMjA0LjcyMywyNTguNzE1LDEzOC4zNTcsMjU4LjcxNXoiLz4gPHBhdGggZD0iTTE5NC43OTgsMTYwLjkwM2MtNC4xODgtMi42NzctOS43NTMtMS40NTQtMTIuNDMyLDIuNzMyYy04LjY5NCwxMy41OTMtMjMuNTAzLDIxLjcwOC0zOS42MTQsMjEuNzA4IGMtMjUuOTA4LDAtNDYuOTg1LTIxLjA3OC00Ni45ODUtNDYuOTg2czIxLjA3Ny00Ni45ODYsNDYuOTg1LTQ2Ljk4NmMxNS42MzMsMCwzMC4yLDcuNzQ3LDM4Ljk2OCwyMC43MjMgYzIuNzgyLDQuMTE3LDguMzc1LDUuMjAxLDEyLjQ5NiwyLjQxOGM0LjExOC0yLjc4Miw1LjIwMS04LjM3NywyLjQxOC0xMi40OTZjLTEyLjExOC0xNy45MzctMzIuMjYyLTI4LjY0NS01My44ODItMjguNjQ1IGMtMzUuODMzLDAtNjQuOTg1LDI5LjE1Mi02NC45ODUsNjQuOTg2czI5LjE1Miw2NC45ODYsNjQuOTg1LDY0Ljk4NmMyMi4yODEsMCw0Mi43NTktMTEuMjE4LDU0Ljc3OC0zMC4wMDkgQzIwMC4yMDgsMTY5LjE0NywxOTguOTg1LDE2My41ODIsMTk0Ljc5OCwxNjAuOTAzeiIvPiA8L2c+IDwvZz4KDTwvc3ZnPg==" height="22">


</div>

## Features

* **Internal Mutability** - Do not need to use `Arc<RwLock<Cache<...>>` for concurrent code, you just need `Cache<...>` or `AsyncCache<...>`
* **Sync and Async** - Stretto support sync and runtime agnostic async.
  * In sync, Cache starts two extra OS level threads. One is policy thread, the other is writing thread.
  * In async, AsyncCache starts two extra green threads. One is policy thread, the other is writing thread.
* **Store policy** Stretto only store the value, which means the cache does not store the key. 
* **High Hit Ratios** - with Dgraph's developers unique admission/eviction policy pairing, Ristretto's performance is best in class.
    * **Eviction: SampledLFU** - on par with exact LRU and better performance on Search and Database traces.
    * **Admission: TinyLFU** - extra performance with little memory overhead (12 bits per counter).
* **Fast Throughput** - use a variety of techniques for managing contention and the result is excellent throughput.
* **Cost-Based Eviction** - any large new item deemed valuable can evict multiple smaller items (cost could be anything).
* **Fully Concurrent** - you can use as many threads as you want with little throughput degradation.
* **Metrics** - optional performance metrics for throughput, hit ratios, and other stats.
* **Simple API** - just figure out your ideal `CacheBuilder`/`AsyncCacheBuilder` values and you're off and running.

## Table of Contents

- [Features](#features)
- [Table of Contents](#table-of-contents)
- [Installation](#installation)
- [Related](#related)
- [Usage](#usage)
  - [Example](#example)
    - [Sync](#sync)
    - [Async](#async)
  - [Config](#config)
    - [num\_counters](#num_counters)
    - [max\_cost](#max_cost)
    - [key\_builder](#key_builder)
    - [buffer\_size](#buffer_size)
    - [metrics](#metrics)
    - [ignore\_internal\_cost](#ignore_internal_cost)
    - [cleanup\_duration](#cleanup_duration)
    - [update\_validator](#update_validator)
    - [callback](#callback)
    - [coster](#coster)
    - [hasher](#hasher)
- [Acknowledgements](#acknowledgements)
- [License](#license)

## Installation
- Use Cache.
```toml
[dependencies]
stretto = "0.8"
```
or
```toml 
[dependencies]
stretto = { version = "0.8", features = ["sync"] }
```


- Use AsyncCache
```toml 
[dependencies]
stretto = { version = "0.8", features = ["async"] }
```

- Use both Cache and AsyncCache
```toml 
[dependencies]
stretto = { version = "0.8", features = ["full"] }
```

## Related
If you want some basic caches implementation(no_std), please see https://crates.io/crates/caches.

## Usage
### Example
#### Sync
```rust
use std::time::Duration;
use stretto::Cache;

fn main() {
    let c = Cache::new(12960, 1e6 as i64).unwrap();

    // set a value with a cost of 1
    c.insert("a", "a", 1);
    // set a value with a cost of 1 and ttl
    c.insert_with_ttl("b", "b", 1, Duration::from_secs(3));

    // wait for value to pass through buffers
    c.wait().unwrap();

    // when we get the value, we will get a ValueRef, which contains a RwLockReadGuard
    // so when we finish use this value, we must release the ValueRef
    let v = c.get(&"a").unwrap();
    assert_eq!(v.value(), &"a");
    v.release();

    // lock will be auto released when out of scope
    {
        // when we get the value, we will get a ValueRef, which contains a RwLockWriteGuard
        // so when we finish use this value, we must release the ValueRefMut
        let mut v = c.get_mut(&"a").unwrap();
        v.write("aa");
        assert_eq!(v.value(), &"aa");
        // release the value
    }

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
Stretto support runtime agnostic `AsyncCache`, the only thing you need to do is passing a `spawner` when building the `AsyncCache`.

```rust
use std::time::Duration;
use stretto::AsyncCache;

#[tokio::main]
async fn main() {
    // In this example, we use tokio runtime, so we pass tokio::spawn when constructing AsyncCache
    let c: AsyncCache<&str, &str> = AsyncCache::new(12960, 1e6 as i64, tokio::spawn).unwrap();

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

    // lock will be auto released when out of scope
    {
        // when we get the value, we will get a ValueRef, which contains a RwLockWriteGuard
        // so when we finish use this value, we must release the ValueRefMut
        let mut v = c.get_mut(&"a").unwrap();
        v.write("aa");
        assert_eq!(v.value(), &"aa");
        // release the value
    }

    // if you just want to do one operation
    let v = c.get_mut(&"a").unwrap();
    v.write_once("aaa");

    let v = c.get(&"a").unwrap();
    println!("{}", v);
    assert_eq!(v.value(), &"aaa");
    v.release();

    // clear the cache
    c.clear().await.unwrap();
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
pub trait KeyBuilder {
    type Key: Hash + Eq + ?Sized;

    /// hash_index is used to hash the key to u64
    fn hash_index<Q>(&self, key: &Q) -> u64
        where 
            Self::Key: core::borrow::Borrow<Q>,
            Q: Hash + Eq + ?Sized;

    /// if you want a 128bit hashes, you should implement this method,
    /// or leave this method return 0
    fn hash_conflict<Q>(&self, key: &Q) -> u64
        where 
            Self::Key: core::borrow::Borrow<Q>,
            Q: Hash + Eq + ?Sized;
    { 0 }

    /// build the key to 128bit hashes.
    fn build_key<Q>(&self, k: &Q) -> (u64, u64) 
        where 
            Self::Key: core::borrow::Borrow<Q>,
            Q: Hash + Eq + ?Sized;
    {
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
pub trait UpdateValidator: Send + Sync + 'static {
    type Value: Send + Sync + 'static;

    /// should_update is called when a value already exists in cache and is being updated.
    fn should_update(&self, prev: &Self::Value, curr: &Self::Value) -> bool;
}
```

By default, the Cache will always update the value if the value already exists in the cache,
this trait is for you to check if the value should be updated.

#### callback

```rust
pub trait CacheCallback: Send + Sync + 'static {
    type Value: Send + Sync + 'static;

    /// on_exit is called whenever a value is removed from cache. This can be
    /// used to do manual memory deallocation. Would also be called on eviction
    /// and rejection of the value.
    fn on_exit(&self, val: Option<Self::Value>);

    /// on_evict is called for every eviction and passes the hashed key, value,
    /// and cost to the function.
    fn on_evict(&self, item: Item<Self::Value>) {
        self.on_exit(item.val)
    }

    /// on_reject is called for every rejection done via the policy.
    fn on_reject(&self, item: Item<Self::Value>) {
        self.on_exit(item.val)
    }
}
```

CacheCallback is for customize some extra operations on values when related event happens.

#### coster

```rust
pub trait Coster: Send + Sync + 'static {
    type Value: Send + Sync + 'static;

    /// cost evaluates a value and outputs a corresponding cost. This function
    /// is ran after insert is called for a new item or an item update with a cost
    /// param of 0.
    fn cost(&self, val: &Self::Value) -> i64;
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

The hasher for the Cache, default is SeaHasher.

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
