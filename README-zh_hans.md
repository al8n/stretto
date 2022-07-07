<div align="center">
<h1>Stretto</h1>
</div>
<div align="center">

[Ristretto](https://github.com/dgraph-io/ristretto) 项目的纯 Rust 实现. 

高性能、线程安全、内存绑定的 Rust 缓存。

[English](README.md) | 简体中文

[<img alt="github" src="https://img.shields.io/badge/GITHUB-Stretto-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/workflow/status/al8n/stretto/CI/main?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/stretto?style=for-the-badge&token=P175Q03Q1L&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-stretto-66c2a5?style=for-the-badge&labelColor=555555&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/stretto?style=for-the-badge&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMiA1MTIiIHhtbDpzcGFjZT0icHJlc2VydmUiPg0KPGc+DQoJPGc+DQoJCTxwYXRoIGQ9Ik0yNTYsMEwzMS41MjgsMTEyLjIzNnYyODcuNTI4TDI1Niw1MTJsMjI0LjQ3Mi0xMTIuMjM2VjExMi4yMzZMMjU2LDB6IE0yMzQuMjc3LDQ1Mi41NjRMNzQuOTc0LDM3Mi45MTNWMTYwLjgxDQoJCQlsMTU5LjMwMyw3OS42NTFWNDUyLjU2NHogTTEwMS44MjYsMTI1LjY2MkwyNTYsNDguNTc2bDE1NC4xNzQsNzcuMDg3TDI1NiwyMDIuNzQ5TDEwMS44MjYsMTI1LjY2MnogTTQzNy4wMjYsMzcyLjkxMw0KCQkJbC0xNTkuMzAzLDc5LjY1MVYyNDAuNDYxbDE1OS4zMDMtNzkuNjUxVjM3Mi45MTN6IiBmaWxsPSIjRkZGIi8+DQoJPC9nPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPC9zdmc+DQo=" height="22">][crates-url]
[<img alt="rustc" src="https://img.shields.io/badge/MSRV-1.55.0-fc8d62.svg?style=for-the-badge&logo=Rust" height="22">][rustc-url]

[<img alt="license-apache" src="https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=for-the-badge&logo=Apache" height="22">][license-apache-url]
[<img alt="license-mit" src="https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge&fontColor=white&logoColor=f5c076&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGhlaWdodD0iMzZweCIgdmlld0JveD0iMCAwIDI0IDI0IiB3aWR0aD0iMzZweCIgZmlsbD0iI2Y1YzA3NiI+PHBhdGggZD0iTTAgMGgyNHYyNEgwVjB6IiBmaWxsPSJub25lIi8+PHBhdGggZD0iTTEwLjA4IDEwLjg2Yy4wNS0uMzMuMTYtLjYyLjMtLjg3cy4zNC0uNDYuNTktLjYyYy4yNC0uMTUuNTQtLjIyLjkxLS4yMy4yMy4wMS40NC4wNS42My4xMy4yLjA5LjM4LjIxLjUyLjM2cy4yNS4zMy4zNC41My4xMy40Mi4xNC42NGgxLjc5Yy0uMDItLjQ3LS4xMS0uOS0uMjgtMS4yOXMtLjQtLjczLS43LTEuMDEtLjY2LS41LTEuMDgtLjY2LS44OC0uMjMtMS4zOS0uMjNjLS42NSAwLTEuMjIuMTEtMS43LjM0cy0uODguNTMtMS4yLjkyLS41Ni44NC0uNzEgMS4zNlM4IDExLjI5IDggMTEuODd2LjI3YzAgLjU4LjA4IDEuMTIuMjMgMS42NHMuMzkuOTcuNzEgMS4zNS43Mi42OSAxLjIuOTFjLjQ4LjIyIDEuMDUuMzQgMS43LjM0LjQ3IDAgLjkxLS4wOCAxLjMyLS4yM3MuNzctLjM2IDEuMDgtLjYzLjU2LS41OC43NC0uOTQuMjktLjc0LjMtMS4xNWgtMS43OWMtLjAxLjIxLS4wNi40LS4xNS41OHMtLjIxLjMzLS4zNi40Ni0uMzIuMjMtLjUyLjNjLS4xOS4wNy0uMzkuMDktLjYuMS0uMzYtLjAxLS42Ni0uMDgtLjg5LS4yMy0uMjUtLjE2LS40NS0uMzctLjU5LS42MnMtLjI1LS41NS0uMy0uODgtLjA4LS42Ny0uMDgtMXYtLjI3YzAtLjM1LjAzLS42OC4wOC0xLjAxek0xMiAyQzYuNDggMiAyIDYuNDggMiAxMnM0LjQ4IDEwIDEwIDEwIDEwLTQuNDggMTAtMTBTMTcuNTIgMiAxMiAyem0wIDE4Yy00LjQxIDAtOC0zLjU5LTgtOHMzLjU5LTggOC04IDggMy41OSA4IDgtMy41OSA4LTggOHoiLz48L3N2Zz4=" height="22">][license-mit-url]

</div>

## 特性
* **内部可变** - 毋须为并发编程而使用 `Arc<RwLock<Cache<...>>`，用 `Cache<...>` 或者 `AsyncCache<...>` 就够了。
* **异同两制** - `stretto` 通过 `crossbeam` 实现同步版本, 使用 `tokio` 实现异步支持。但是本质是统一的。
    * 在同步版本中，缓存会开启两个额外的操作系统线程。一个是策略线程，另一个为写入线程；
    * 在异步版本中，缓存会开启两个额外的 `tokio` 协程。一个为策略协程，另一个为写入协程。
* **写入策略** - `stretto` 仅会存储键值对中的值，并不会存储键。
* **高命中率** - 在 `Dgraph` 开发者独树一帜的录入/撤除策略的加持下，Ristretto 的性能在同级下是坠吼的，跑得比谁都快。
    * **录入：TinyLFU 算法** - 更高的性能，仅需为每个计数器额外 +12bits。
    * **撤除：SampledLFU 算法** - 性能比肩 LRU，但在搜索与数据库追踪上更胜一筹。
* **高吞吐量** - 多种操作处理冲突，带来催人跑的高带宽。
* **基于权重** - 插入大权重的新缓存项可以淘汰多个低权重的缓存项。（权重可以是任何属性）
* **完全并行** - 在并行中性能仅会略微降低。新线程？开，都可以开。
* **可选度量** - 可选的吞吐量、命中率或者其他统计指标的度量衡。
* **Simple API** - 考察、设定您理想的 `CacheBuilder`/`AsyncCacheBuilder` 参数，然后起飞！🚀

## 目录

- [特性](#特性)
- [目录](#目录)
- [安装](#安装)
- [操作方法](#操作方法)
  - [示例](#示例)
    - [同步](#同步)
    - [异步](#异步)
  - [配置](#配置)
    - [num_counters](#num_counters)
    - [max_cost](#max_cost)
    - [key_builder](#key_builder)
    - [buffer_size](#buffer_size)
    - [metrics](#metrics)
    - [ignore_internal_cost](#ignore_internal_cost)
    - [cleanup_duration](#cleanup_duration)
    - [update_validator](#update_validator)
    - [callback](#callback)
    - [coster](#coster)
    - [hasher](#hasher)
- [鸣谢](#鸣谢)
- [许可](#许可)

## 安装
- 使用同步缓存
```toml
[dependencies]
stretto = "0.4"
```
或
```toml 
[dependencies]
stretto = { version = "0.4", features = ["sync"] }
```


- 使用异步缓存
```toml 
[dependencies]
stretto = { version = "0.4", features = ["async"] }
```

- 同步异步同时使用
```toml 
[dependencies]
stretto = { version = "0.4", features = ["full"] }
```

## 操作方法
### 示例
#### 同步
```rust
use stretto::{Cache, DefaultKeyBuilder};
use std::time::Duration;

fn main() {
    let c = Cache::new(12960, 1e6 as i64, DefaultKeyBuilder::default()).unwrap();

    // 设定一个键为 "a", 权为 1 的值
    c.insert("a", "a", 1);

    // 设定一个键为 "a"，权为 1 的带生存期的值
    c.insert_with_ttl("b", "b", 1, Duration::from_secs(3));
    
    // 等待值存入缓存中
    c.wait().unwrap();

    // 当尝试访问值时，会返回一个包含了 RwLockReadGuard 的 ValueRef
    // 当完成使用这个值时，ValueRef 需要释放
    let v = c.get(&"a").unwrap();
    assert_eq!(v.value(), &"a");
    // 手动释放
    v.release(); // 或者析构 v

    // 离开作用域后锁会被自动释放
    {
        // 当尝试访问值时，会返回一个包含了 RwLockReadGuard 的 ValueRef
        // 当完成使用这个值时，ValueRef 需要释放

        let mut v = c.get_mut(&"a").unwrap();
        v.write("aa");
        assert_eq!(v.value(), &"aa");
        // 释放值
    }

    // 如果只对 v 操作一次
    let v = c.get_mut(&"a").unwrap();
    v.write_once("aaa");

    let v = c.get(&"a").unwrap();
    assert_eq!(v.value(), &"aaa");
    v.release();

    // 缓存清零
    c.clear().unwrap();
    // 等待所有操作完成
    c.wait().unwrap();
    assert!(c.get(&"a").is_none());
}
```

#### 异步
```rust
use stretto::{AsyncCache, DefaultKeyBuilder};
use std::time::Duration;

#[tokio::main]
async fn main() {
    // 在这个例子中, 我们使用tokio运行时, 所以需要将tokio::spawn作为spawner在构建缓存的时候
    let c = AsyncCache::new(12960, 1e6 as i64, DefaultKeyBuilder::default(), tokio::spawn).unwrap();

    // 设定一个键为 "a" 权为 1 的值
    c.insert("a", "a", 1).await;

    // 设定一个键为 "a"，权为 1，生存期为 1s 的值
    c.insert_with_ttl("b", "b", 1, Duration::from_secs(1)).await;
    
    // 等待值存入缓存中
    c.wait().await.unwrap();

    
    // 当尝试访问值时，会返回一个包含了 RwLockReadGuard 的 ValueRef
    // 当完成使用这个值时，ValueRef 需要释放
    let v = c.get(&"a").unwrap();
    assert_eq!(v.value(), &"a");
    // 释放值
    v.release(); // 或者直接析构 v

    // 离开作用域时锁会自动释放
    {
        // 当尝试访问值时，会返回一个包含了 RwLockReadGuard 的 ValueRef
        // 当完成使用这个值时，ValueRef 需要释放
        let mut v = c.get_mut(&"a").unwrap();
        v.write("aa");
        assert_eq!(v.value(), &"aa");
        // 释放值
    }
    

    // 如果只对 v 操作一次
    let v = c.get_mut(&"a").unwrap();
    v.write_once("aaa");

    let v = c.get(&"a").unwrap();
    println!("{}", v);
    assert_eq!(v.value(), &"aaa");
    v.release();

    // 缓存清空
    c.clear().await.unwrap();
    // 等待操作完成
    c.wait().await.unwrap();

    assert!(c.get(&"a").is_none());
}
```
### 配置
如果希望定制缓存，请使用 `CacheBuilder` 来创建 `Cache` 对象。

#### num_counters

`num_counters` （计数器数）是用于保存录入与淘汰信息的 4 位访问计数器的数目。Dgraph 的开发者们在将其设为约 10 倍于缓存容量的时候获得了不错的性能。

比如，在每个缓存项的权为 1 且 `max_cost` 设定为 100 时，应将 `num_counters` 设为 1,000；或者如果缓存项权值不等，而期望缓存可以容纳约 10,000 项时，应将 `num_counter` 设为 100,000——应当考虑的是可以装满缓存的**唯一键值数量**而非 `max_cost` 的值。

#### max_cost

`max_cost` （最大权值和）是缓存是否进行撤除操作的参考。在 `max_cost` 为 100 时，如果插入一个权为 1 的项使得缓存内总权值之和为 101，那么一个缓存项会被淘汰。

`max_cost` 可以被用于表示缓存的最大体积（字节）。举个例子，如果 `max_cost` 为 1,000,000 (1 MB，1 兆字节) 而缓存已经装入 1,000 个 1 KB 的项，一个被接收的新缓存项会导致 5 个 1KB 的缓存项被撤除。

权值可以是任意属性，亦即 `max_cost` 也可以指代任何属性的权值的和的最大值。

#### key_builder

```rust
pub trait KeyBuilder<K: Hash + Eq + ?Sized> {
    /// hash_index 用于将键哈希运算成一个 u64 值
    fn hash_index(&self, key: &K) -> u64;

    /// 如果希望使用一个 128 位哈希，需要实现此方法。
    /// 默认返回 0
    fn hash_conflict(&self, key: &K) -> u64 { 0 }

    /// 将键进行哈希运算，返回 128 位哈希结果。
    fn build_key(&self, k: &K) -> (u64, u64) {
        (self.hash_index(k), self.hash_conflict(k))
    }
}
```

`KeyBuilder`（键生成器）是使用于所有的键的哈希算法。`Stretto` 并不会存储键的真正的值，
而是会将其使用 `KeyBuilder` 处理。
`Stretto` 内建了两套默认的键生成器，
一套为 `TransparentKeyBuilder`（透明键生成器），另一套为 `DefaultKeyBuilder`（默认键生成器）。
只有当键类型实现了 `TransparentKey` 特性时，才可以使用相比 `DefaultKeyBuider` 更快的 `TransparentKeyBuilder`。

用户可以通过实现 `KeyBuilder` 特质另起炉灶，自己实现一套键生成器。

注意当希望使用 128 位哈希时请将 `(u64, u64)` 中的两项都用到。如果只想使用 64 位哈希可以将元组中第一个（索引为 0）的值置 0。

#### buffer_size

`buffer_size`（缓存大小）是插入缓存的大小。Dgraph 的开发者们发现设为 32 × 1024 （的整倍数？）时性能很好。

如果偶然发现插入性能大幅下降，同时出现较多冲突（通常并不会），请尝试将该值设定为更高的 32 × 1024 的整倍数。缓存的内部机制调教得当，用户一般不会需要修改该值。

#### metrics

Metrics（度量）应当在需要实时日志记录多种状态信息的时候设置为 `true`。之所以并未设定成默认启用，是因为可能会降低 10% 的吞吐量。
#### ignore_internal_cost

设定为 `true` 时缓存将会忽略存储值的内部开销，这在开销不以比特为单位时很有用。不过谨记这会导致更高的内存占用。

#### cleanup_duration

默认情况下缓存会每 500 毫秒清理一次过期的值

#### update_validator

```rust
pub trait UpdateValidator<V>: Send + Sync + 'static {
    /// should_update 在一个已经存在于缓存中的值被更新时调用
    fn should_update(&self, prev: &V, curr: &V) -> bool;
}
```

默认状态下，缓存总是会更新已经在缓存中的值。
该特性用于确认该值是否被更新。
#### callback

```rust
pub trait CacheCallback<V: Send + Sync>: Send + Sync + 'static {
    /// on_exit 在一个值被移除 (remove) 出缓存的时候调用。
    /// 可以用于实现手动内存释放。
    /// 在撤除 (evict) 或者拒绝 (reject) 值的时候亦会被调用
    fn on_exit(&self, val: Option<V>);

    /// on_evict 在撤除值的时候会被调用，同时会将哈希键、值和权传给函数。
    fn on_evict(&self, item: Item<V>) {
        self.on_exit(item.val)
    }

    /// on_reject 会被 policy 为每个所拒绝的值调用
    fn on_reject(&self, item: Item<V>) {
        self.on_exit(item.val)
    }
}
```

CacheCallBack（缓存回调）被用于定制在事件发生时对值的额外操作。

#### coster

```rust
pub trait Coster<V>: Send + Sync + 'static {
    /// cost 函数对值进行求值并返回对应的权重，该函数
    /// 会在一个新值插入或一个值更新为 0 权值时被调用
    fn cost(&self, val: &V) -> i64;
}
```

`Cost` 是一个可以传给 `CacheBuilder` 进行运行时权重求值的特征，并且仅仅对未丢弃的 `insert` 函数调用使用——这在计算权值相当耗时或者耗资源时非常有用，尤其是当用户不想在迟早被析构的值上浪费时间时。

用户可以通过如下方法使得 Stretto 使用自己定制的 Coster 特征：

1. 将 `Coster` 值设定为自己的 `Coster` 实现；
2. 在插入新缓存项或更新缓存项，调用 `insert`时，将 `cost` 设为 0。

#### hasher

缓存的哈希器，默认为 `SipHasher`。

## 鸣谢
- 感谢 Dgraph 的开发者们，提供了如此亦可赛艇的 [Ristretto](https://github.com/dgraph-io/ristretto) Go 语言实现。

## 许可

<sup>
根据您的选择，在 <a href="https://opensource.org/licenses/Apache-2.0">Apache 许可证
2.0 版</a> 或 <a href="https://opensource.org/licenses/MIT">MIT 许可证</a> 下进行授权。

</sup>

<br>

<sub>
除非您明确说明，任何由您有意提交以纳入本项目的贡献，如Apache-2.0许可证所定义的，应按上述规定进行双重许可，没有任何附加条款或条件。
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
