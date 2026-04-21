# agnostic-lite Async Runtime Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `async-io` and the raw spawner-closure API with `agnostic-lite`'s `RuntimeLite` trait, making the `async` feature truly runtime-agnostic.

**Architecture:** A single `RT: RuntimeLite` type parameter replaces both the `Fn(BoxFuture)` spawner closure and `async-io::Timer`. `RuntimeLite` is a zero-sized `Copy` type whose associated functions `RT::spawn_detach` and `RT::interval` are called directly inside async blocks via static dispatch — no values captured, no boxing.

**Tech Stack:** Rust, `agnostic-lite 0.6` (`time` + `tokio` features), `async-channel`, `futures`, `wg`

---

## File Map

| File | What changes |
|------|-------------|
| `Cargo.toml` | Remove `async-io`, add `agnostic-lite`, update `async`/`full` features, remove `async-std` dev-dep, add `agnostic-lite/tokio` dev-dep |
| `src/policy/async.rs` | `PolicyProcessor::spawn` and `AsyncLFUPolicy::with_hasher`/`new`: replace `Box<dyn Fn(BoxFuture)>` with `RT: RuntimeLite`; remove `BoxFuture` import |
| `src/cache/async.rs` | `CacheProcessor::spawn`: replace `Box<dyn Fn(BoxFuture)>` + `Timer` with `RT: RuntimeLite`; `AsyncCacheBuilder::finalize`, `AsyncCache::new`, `AsyncCache::new_with_key_builder`: same; remove `async_io::Timer` and `BoxFuture` imports |
| `src/cache/test.rs` | Replace every `tokio::spawn` / `finalize(tokio::spawn)` with `TokioRuntime` equivalents |
| `examples/async_example.rs` | Update `AsyncCache::new` call |
| `README.md` | Update async installation and usage sections |

---

### Task 1: Update Cargo.toml

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Apply all dependency and feature changes**

Replace the entire `Cargo.toml` with the following (only the `[features]`, `[dependencies]`, and `[dev-dependencies]` sections change; everything else stays identical):

```toml
[package]
name = "stretto"
version = "0.9.0"
authors = ["Al Liu <scygliu1@gmail.com>"]
description = "Stretto is a high performance thread-safe memory-bound Rust cache."
homepage = "https://github.com/al8n/stretto"
repository = "https://github.com/al8n/stretto.git"
documentation = "https://docs.rs/stretto/"
readme = "README.md"
license = "MIT OR Apache-2.0"
keywords = ["cache", "caching", "concurrent", "tinylfu", "async"]
categories = ["caching", "concurrency", "asynchronous", "data-structures"]
exclude = [
    "**/*.json",
    "**/*.go",
    "**/*.mod",
    "**/*.sum",
    "benches/*"
]
edition = "2021"

[[example]]
path = "examples/async_example.rs"
name = "async_example"
required-features = ["async-tokio"]
edition = "2021"

[[example]]
path = "examples/sync_example.rs"
name = "sync_example"
required-features = ["sync"]
edition = "2021"

[target.'cfg(target_family = "wasm")'.dependencies]
getrandom = { version = "0.2", features = ["js"] }

[features]
default = ["sync"]
full = ["sync", "async-tokio", "serde"]
async = ["async-channel", "agnostic-lite/time", "futures/default", "wg/future"]
async-tokio = ["async", "agnostic-lite/tokio"]
async-smol  = ["async", "agnostic-lite/smol"]
async-wasm  = ["async", "agnostic-lite/wasm"]
sync = ["crossbeam-channel"]
serde = ["dep:serde", "dep:serde_json"]

[dependencies]
atomic = "0.6"
agnostic-lite = { version = "0.6", optional = true }
async-channel = { version = "2", optional = true }
crossbeam-channel = { version = "0.5", optional = true }
futures = { version = "0.3", optional = true }
parking_lot = "0.12"
rand = "0.8"
serde = { version = "1", optional = true, features = ["derive"] }
serde_json = { version = "1", optional = true }
seahash = "4.1"
wg = "0.9"
thiserror = "1"
tracing = "0.1"
xxhash-rust = { version = "0.8", features = ["xxh64"] }

[dev-dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }
agnostic-lite = { version = "0.6", features = ["tokio"] }

[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```

- [ ] **Step 2: Verify Cargo.toml is parseable**

```bash
cargo metadata --no-deps --quiet 2>&1 | head -5
```

Expected: no errors (JSON output or silence). If you see a parse error, fix the TOML syntax.

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "chore: replace async-io with agnostic-lite, update feature flags"
```

---

### Task 2: Update `src/policy/async.rs`

**Files:**
- Modify: `src/policy/async.rs`

- [ ] **Step 1: Replace the entire file**

```rust
use crate::axync::{select, stop_channel, unbounded, Receiver, RecvError, Sender};
use crate::policy::PolicyInner;
use crate::{CacheError, MetricType, Metrics};
use agnostic_lite::RuntimeLite;
use futures::future::FutureExt;
use parking_lot::Mutex;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(crate) struct AsyncLFUPolicy<S = RandomState> {
    pub(crate) inner: Arc<Mutex<PolicyInner<S>>>,
    pub(crate) items_tx: Sender<Vec<u64>>,
    pub(crate) stop_tx: Sender<()>,
    pub(crate) is_closed: AtomicBool,
    pub(crate) metrics: Arc<Metrics>,
}

impl AsyncLFUPolicy {
    #[inline]
    pub(crate) fn new<RT: RuntimeLite>(ctrs: usize, max_cost: i64) -> Result<Self, CacheError> {
        Self::with_hasher::<RT>(ctrs, max_cost, RandomState::new())
    }
}

impl<S: BuildHasher + Clone + 'static + Send> AsyncLFUPolicy<S> {
    #[inline]
    pub fn with_hasher<RT: RuntimeLite>(
        ctrs: usize,
        max_cost: i64,
        hasher: S,
    ) -> Result<Self, CacheError> {
        let inner = PolicyInner::with_hasher(ctrs, max_cost, hasher)?;

        let (items_tx, items_rx) = unbounded();
        let (stop_tx, stop_rx) = stop_channel();

        PolicyProcessor::new(inner.clone(), items_rx, stop_rx).spawn::<RT>();

        let this = Self {
            inner,
            items_tx,
            stop_tx,
            is_closed: AtomicBool::new(false),
            metrics: Arc::new(Metrics::new()),
        };

        Ok(this)
    }

    pub async fn push(&self, keys: Vec<u64>) -> Result<bool, CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let num_of_keys = keys.len() as u64;
        if num_of_keys == 0 {
            return Ok(true);
        }
        let first = keys[0];

        select! {
            rst = self.items_tx.send(keys).fuse() => rst.map(|_| {
                self.metrics.add(MetricType::KeepGets, first, num_of_keys);
                true
            })
            .map_err(|e| {
                self.metrics.add(MetricType::DropGets, first, num_of_keys);
                CacheError::SendError(format!("sending on a disconnected channel, msg: {:?}", e))
            }),
            default => {
                self.metrics.add(MetricType::DropGets, first, num_of_keys);
                Ok(false)
            }
        }
    }

    #[inline]
    pub async fn close(&self) -> Result<(), CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        self.stop_tx
            .send(())
            .await
            .map_err(|e| CacheError::SendError(format!("{}", e)))?;
        self.is_closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

pub(crate) struct PolicyProcessor<S> {
    inner: Arc<Mutex<PolicyInner<S>>>,
    items_rx: Receiver<Vec<u64>>,
    stop_rx: Receiver<()>,
}

impl<S: BuildHasher + Clone + 'static + Send> PolicyProcessor<S> {
    #[inline]
    fn new(
        inner: Arc<Mutex<PolicyInner<S>>>,
        items_rx: Receiver<Vec<u64>>,
        stop_rx: Receiver<()>,
    ) -> Self {
        Self {
            inner,
            items_rx,
            stop_rx,
        }
    }

    #[inline]
    fn spawn<RT: RuntimeLite>(self) {
        RT::spawn_detach(async move {
            loop {
                select! {
                    items = self.items_rx.recv().fuse() => self.handle_items(items),
                    _ = self.stop_rx.recv().fuse() => {
                        drop(self);
                        return;
                    },
                }
            }
        });
    }

    #[inline]
    fn handle_items(&self, items: Result<Vec<u64>, RecvError>) {
        match items {
            Ok(items) => {
                let mut inner = self.inner.lock();
                inner.admit.increments(items);
            }
            Err(_) => {}
        }
    }
}

unsafe impl<S: BuildHasher + Clone + 'static + Send> Send for PolicyProcessor<S> {}
unsafe impl<S: BuildHasher + Clone + 'static + Send + Sync> Sync for PolicyProcessor<S> {}

impl_policy!(AsyncLFUPolicy);
```

- [ ] **Step 2: Check this file compiles in isolation**

```bash
cargo check --features async-tokio 2>&1 | grep "policy"
```

Expected: no errors mentioning `policy/async.rs`. (Other files will still fail — that's fine.)

- [ ] **Step 3: Commit**

```bash
git add src/policy/async.rs
git commit -m "refactor(async): replace BoxFuture spawner with RuntimeLite in PolicyProcessor"
```

---

### Task 3: Update `src/cache/async.rs` — imports and `CacheProcessor::spawn`

**Files:**
- Modify: `src/cache/async.rs`

- [ ] **Step 1: Replace the imports block at the top of the file**

Find the existing imports (lines 1–24) and replace with:

```rust
use crate::axync::{
    bounded, select, stop_channel, unbounded, Receiver, RecvError, Sender, WaitGroup,
};
use crate::cache::builder::CacheBuilderCore;
use crate::policy::AsyncLFUPolicy;
use crate::ring::AsyncRingStripe;
use crate::store::ShardedMap;
use crate::ttl::{ExpirationMap, Time};
use crate::{
    metrics::MetricType, CacheCallback, CacheError, Coster, DefaultCacheCallback, DefaultCoster,
    DefaultKeyBuilder, DefaultUpdateValidator, KeyBuilder, Metrics, UpdateValidator,
};
use agnostic_lite::RuntimeLite;
use futures::{future::FutureExt, stream::StreamExt};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
```

- [ ] **Step 2: Replace `CacheProcessor::spawn`**

Find the `spawn` method on `CacheProcessor` (currently takes `spawner: Box<dyn Fn(BoxFuture<'static, ()>) + Send + Sync>`) and replace the entire method:

```rust
#[inline]
pub(crate) fn spawn<RT: RuntimeLite>(mut self) {
    RT::spawn_detach(async move {
        let mut cleanup_timer = RT::interval(self.cleanup_duration);

        loop {
            select! {
                item = self.insert_buf_rx.recv().fuse() => {
                    if let Err(e) = self.handle_insert_event(item) {
                        tracing::error!("fail to handle insert event, error: {}", e);
                    }
                }
                _ = cleanup_timer.next().fuse() => {
                    if let Err(e) = self.handle_cleanup_event() {
                        tracing::error!("fail to handle cleanup event, error: {}", e);
                    }
                },
                _ = self.clear_rx.recv().fuse() => {
                    if let Err(e) = CacheCleaner::new(&mut self).clean().await {
                        tracing::error!("fail to handle clear event, error: {}", e);
                    }
                },
                _ = self.stop_rx.recv().fuse() => {
                    _ = self.handle_close_event();
                    return;
                },
            }
        }
    });
}
```

- [ ] **Step 3: Commit this partial change**

```bash
git add src/cache/async.rs
git commit -m "refactor(async): replace Timer+BoxFuture with RuntimeLite in CacheProcessor"
```

---

### Task 4: Update `src/cache/async.rs` — public API (`finalize`, `new`, `new_with_key_builder`)

**Files:**
- Modify: `src/cache/async.rs`

- [ ] **Step 1: Replace `AsyncCacheBuilder::finalize`**

Find the `finalize` method in `impl<K, V, KH, C, U, CB, S> AsyncCacheBuilder<...>` and replace it entirely:

```rust
/// Build Cache and start all threads needed by the Cache.
///
/// `RT` is the async runtime to use. For example, if you use `tokio`,
/// pass `TokioRuntime` from `agnostic-lite`.
///
/// ```no_run
/// use stretto::AsyncCacheBuilder;
/// use agnostic_lite::TokioRuntime;
///
/// AsyncCacheBuilder::<u64, u64>::new(100, 10)
///     .finalize::<TokioRuntime>()
///     .unwrap();
/// ```
#[inline]
pub fn finalize<RT: RuntimeLite>(
    self,
) -> Result<AsyncCache<K, V, KH, C, U, CB, S>, CacheError> {
    let num_counters = self.inner.num_counters;

    if num_counters == 0 {
        return Err(CacheError::InvalidNumCounters);
    }

    let max_cost = self.inner.max_cost;
    if max_cost == 0 {
        return Err(CacheError::InvalidMaxCost);
    }

    let insert_buffer_size = self.inner.insert_buffer_size;
    if insert_buffer_size == 0 {
        return Err(CacheError::InvalidBufferSize);
    }

    let (buf_tx, buf_rx) = bounded(insert_buffer_size);
    let (stop_tx, stop_rx) = stop_channel();
    let (clear_tx, clear_rx) = unbounded();

    let hasher = self.inner.hasher.unwrap();
    let expiration_map = ExpirationMap::with_hasher(hasher.clone());

    let store = Arc::new(ShardedMap::with_validator_and_hasher(
        expiration_map,
        self.inner.update_validator.unwrap(),
        hasher.clone(),
    ));
    let mut policy = AsyncLFUPolicy::with_hasher::<RT>(num_counters, max_cost, hasher)?;

    let coster = Arc::new(self.inner.coster.unwrap());
    let callback = Arc::new(self.inner.callback.unwrap());
    let metrics = if self.inner.metrics {
        let m = Arc::new(Metrics::new_op());
        policy.collect_metrics(m.clone());
        m
    } else {
        Arc::new(Metrics::new())
    };

    let policy = Arc::new(policy);
    CacheProcessor::new(
        100000,
        self.inner.ignore_internal_cost,
        self.inner.cleanup_duration,
        store.clone(),
        policy.clone(),
        buf_rx,
        stop_rx,
        clear_rx,
        metrics.clone(),
        callback.clone(),
    )
    .spawn::<RT>();

    let buffer_items = self.inner.buffer_items;
    let get_buf = AsyncRingStripe::new(policy.clone(), buffer_items);
    let this = AsyncCache {
        store,
        policy,
        get_buf: Arc::new(get_buf),
        insert_buf_tx: buf_tx,
        callback,
        key_to_hash: Arc::new(self.inner.key_to_hash),
        stop_tx,
        clear_tx,
        is_closed: Arc::new(AtomicBool::new(false)),
        coster,
        metrics,
        _marker: Default::default(),
    };

    Ok(this)
}
```

- [ ] **Step 2: Replace `AsyncCache::new`**

Find the `new` method in `impl<K: Hash + Eq, V: Send + Sync + 'static> AsyncCache<K, V>` and replace it:

```rust
/// Returns a Cache instance with default configurations.
///
/// `RT` is the async runtime to use. For example:
///
/// ```no_run
/// use stretto::AsyncCache;
/// use agnostic_lite::TokioRuntime;
///
/// AsyncCache::<u64, u64>::new::<TokioRuntime>(100, 10).unwrap();
/// ```
#[inline]
pub fn new<RT: RuntimeLite>(
    num_counters: usize,
    max_cost: i64,
) -> Result<Self, CacheError> {
    AsyncCacheBuilder::new(num_counters, max_cost).finalize::<RT>()
}
```

- [ ] **Step 3: Replace `AsyncCache::new_with_key_builder`**

Find the `new_with_key_builder` method in `impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<Key = K>> AsyncCache<K, V, KH>` and replace it:

```rust
/// Returns a Cache instance with a custom key builder.
///
/// ```no_run
/// use stretto::{AsyncCache, TransparentKeyBuilder};
/// use agnostic_lite::TokioRuntime;
///
/// AsyncCache::<u64, u64, TransparentKeyBuilder<_>>::new_with_key_builder::<TokioRuntime>(
///     100, 10, TransparentKeyBuilder::<u64>::default(),
/// ).unwrap();
/// ```
#[inline]
pub fn new_with_key_builder<RT: RuntimeLite>(
    num_counters: usize,
    max_cost: i64,
    index: KH,
) -> Result<Self, CacheError> {
    AsyncCacheBuilder::new_with_key_builder(num_counters, max_cost, index).finalize::<RT>()
}
```

- [ ] **Step 4: Attempt to compile**

```bash
cargo check --features async-tokio 2>&1
```

Expected: zero errors. If there are errors related to `RT::Interval` not being `Unpin` or `Send`, add the relevant where clause to `CacheProcessor::spawn`:

```rust
pub(crate) fn spawn<RT: RuntimeLite>(mut self)
where
    <RT as RuntimeLite>::Interval: Send,
{
```

- [ ] **Step 5: Run the async tests**

```bash
cargo test --features async-tokio 2>&1
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/cache/async.rs
git commit -m "refactor(async): replace BoxFuture spawner with RuntimeLite in AsyncCacheBuilder and AsyncCache"
```

---

### Task 5: Update `src/cache/test.rs`

**Files:**
- Modify: `src/cache/test.rs`

- [ ] **Step 1: Replace the async test module imports**

Find the imports block at the start of the async test module (around line 735–744):

```rust
// old
use tokio::sync::mpsc::channel;
use tokio::task::spawn;
use tokio::time::sleep;
```

Replace the entire import block at the top of `mod async_test` (or whatever the module is named) with:

```rust
use crate::{
    AsyncCache, AsyncCacheBuilder, DefaultCacheCallback, DefaultCoster, DefaultKeyBuilder,
    DefaultUpdateValidator, TransparentKeyBuilder, UpdateValidator,
};
use agnostic_lite::TokioRuntime;
use std::collections::hash_map::RandomState;
use std::hash::Hash;
use std::time::Duration;
use tokio::sync::mpsc::channel;
use tokio::time::sleep;
```

(Remove `use tokio::task::spawn;` — it is no longer used.)

- [ ] **Step 2: Update `new_test_cache`**

Find:
```rust
async fn new_test_cache<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<Key = K>>(
    kh: KH,
) -> AsyncCache<K, V, KH> {
    AsyncCache::new_with_key_builder(100, 10, kh, tokio::spawn).unwrap()
}
```

Replace with:
```rust
async fn new_test_cache<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<Key = K>>(
    kh: KH,
) -> AsyncCache<K, V, KH> {
    AsyncCache::new_with_key_builder::<TokioRuntime>(100, 10, kh).unwrap()
}
```

- [ ] **Step 3: Replace all remaining `.finalize(tokio::spawn)` calls**

Run a search to find every occurrence:

```bash
grep -n "finalize(tokio::spawn)\|tokio::spawn)" src/cache/test.rs
```

For each occurrence, replace `finalize(tokio::spawn)` with `finalize::<TokioRuntime>()`.

For example:
```rust
// old
.finalize(tokio::spawn)
// new
.finalize::<TokioRuntime>()
```

And for any direct `AsyncCache::new(..., tokio::spawn)` calls:
```rust
// old
AsyncCache::new_with_key_builder(100, 10, kh, tokio::spawn)
// new
AsyncCache::new_with_key_builder::<TokioRuntime>(100, 10, kh)
```

- [ ] **Step 4: Run async tests**

```bash
cargo test --features async-tokio 2>&1
```

Expected: all tests pass with no compilation errors.

- [ ] **Step 5: Commit**

```bash
git add src/cache/test.rs
git commit -m "test(async): migrate tokio::spawn to TokioRuntime"
```

---

### Task 6: Update `examples/async_example.rs`

**Files:**
- Modify: `examples/async_example.rs`

- [ ] **Step 1: Replace the file**

```rust
use agnostic_lite::TokioRuntime;
use std::time::Duration;
use stretto::AsyncCache;

#[tokio::main]
async fn main() {
    // In this example we use tokio runtime via TokioRuntime from agnostic-lite.
    // For smol, use SmolRuntime; for wasm, use WasmRuntime.
    let c: AsyncCache<String, String> =
        AsyncCache::new::<TokioRuntime>(12960, 1e6 as i64).unwrap();

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

    let key1 = "key1".to_string();
    // when we get the value, we will get a ValueRef, which contains a RwLockReadGuard
    // so when we finish use this value, we must release the ValueRef
    let v = c.get(&key1).await.unwrap();
    assert_eq!(v.value(), &"value1");
    v.release(); // or drop(v)

    {
        // when we get the value, we will get a ValueRef, which contains a RwLockWriteGuard
        // so when we finish use this value, we must release the ValueRefMut
        let mut v = c.get_mut(&key1).await.unwrap();
        v.write("value2".to_string());
        assert_eq!(v.value(), &"value2");
    }

    let v = c.get_mut(&key1).await.unwrap();
    v.write_once("value3".to_string());

    let v = c.get(&key1).await.unwrap();
    assert_eq!(v.value(), &"value3");
    v.release();

    c.clear().await.unwrap();
    c.wait().await.unwrap();

    assert!(c.get(&key1).await.is_none());
}
```

- [ ] **Step 2: Verify the example compiles**

```bash
cargo build --example async_example --features async-tokio 2>&1
```

Expected: `Compiling stretto ...` then `Finished`.

- [ ] **Step 3: Commit**

```bash
git add examples/async_example.rs
git commit -m "example(async): migrate to TokioRuntime"
```

---

### Task 7: Update `README.md`

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update the Installation section**

Find the async installation block:
```markdown
- Use AsyncCache
\`\`\`toml
[dependencies]
stretto = { version = "0.8", features = ["async"] }
\`\`\`
```

Replace with:
```markdown
- Use AsyncCache with tokio
\`\`\`toml
[dependencies]
stretto = { version = "0.9", features = ["async-tokio"] }
agnostic-lite = { version = "0.6", features = ["tokio"] }
\`\`\`

- Use AsyncCache with smol
\`\`\`toml
[dependencies]
stretto = { version = "0.9", features = ["async-smol"] }
agnostic-lite = { version = "0.6", features = ["smol"] }
\`\`\`
```

Also update the `full` feature description:
```markdown
- Use both Cache and AsyncCache
\`\`\`toml
[dependencies]
stretto = { version = "0.9", features = ["full"] }
agnostic-lite = { version = "0.6", features = ["tokio"] }
\`\`\`
```

- [ ] **Step 2: Update the Async example in the Usage section**

Find the `#### Async` section and replace the `AsyncCache::new` call:

```markdown
Stretto supports runtime agnostic `AsyncCache`. Pass the `RuntimeLite` implementor
for your async runtime as a type parameter.

\`\`\`rust
use std::time::Duration;
use stretto::AsyncCache;
use agnostic_lite::TokioRuntime;

#[tokio::main]
async fn main() {
    let c: AsyncCache<&str, &str> =
        AsyncCache::new::<TokioRuntime>(12960, 1e6 as i64).unwrap();
    // ... rest of example unchanged
}
\`\`\`
```

- [ ] **Step 3: Verify tests still pass after README-only change**

```bash
cargo test --features async-tokio 2>&1 | tail -5
```

Expected: `test result: ok`.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: update README for agnostic-lite AsyncCache API"
```

---

### Task 8: Final verification

- [ ] **Step 1: Run the full test suite**

```bash
cargo test --features async-tokio 2>&1
cargo test --features sync 2>&1
cargo test --features full 2>&1
```

Expected: all three pass with zero failures.

- [ ] **Step 2: Check `--all-features` compiles (docs.rs gate)**

```bash
cargo check --all-features 2>&1
```

Expected: no errors.

- [ ] **Step 3: Run the sync example to confirm no sync regressions**

```bash
cargo run --example sync_example --features sync 2>&1
```

Expected: runs and exits cleanly (no panics).

- [ ] **Step 4: Final commit**

```bash
git add -A
git commit -m "chore: agnostic-lite migration complete — remove async-io, add RuntimeLite API"
```
