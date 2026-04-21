# Design: Runtime-Agnostic Async via agnostic-lite

**Date:** 2026-04-21  
**Branch:** 0.9.0  
**Type:** Breaking change — requires semver bump

---

## Goal

Replace the smol-ecosystem-specific `async-io` dependency with `agnostic-lite`, making
the `async` feature truly runtime-agnostic. Also remove the deprecated `async-std`
dev-dependency and its associated spawner closure API.

---

## Current State

The `async` feature has two non-agnostic elements:

1. **`async-io::Timer`** — used in `CacheProcessor::spawn` for the cleanup interval.
   `async-io` is from the smol ecosystem and brings in its own reactor, which can
   conflict with tokio's reactor.

2. **Raw spawner closure** — `SP: Fn(BoxFuture<'static, ()>) -> R + Send + Sync + 'static + Copy`
   — an awkward public API that requires users to pass `tokio::spawn` or similar as a
   function pointer.

`async-std` is listed as a dev-dependency but is unused anywhere in the codebase.

---

## Approach: `RT: RuntimeLite` Type Parameter (Option A)

Replace both the spawner closure and `async-io::Timer` with a single `RT: RuntimeLite`
type parameter from `agnostic-lite`. `RuntimeLite` exposes:

- `RT::spawn_detach(future)` — spawns a future without a join handle
- `RT::interval(duration)` — returns `RT::Interval` which implements `Stream<Item = Instant>`

Since `RuntimeLite` implementations are zero-sized `Copy` types and all methods are
associated functions, no runtime value needs to be captured or stored. The type
parameter is resolved at compile time with zero overhead.

---

## Dependency Changes

**`Cargo.toml` — remove:**
```toml
async-io = { version = "2.3", optional = true }
```

**`Cargo.toml` — add:**
```toml
agnostic-lite = { version = "0.6", optional = true }
```

**Dev-dependencies — remove:**
```toml
async-std = { version = "1.12" }
```

---

## Feature Flag Changes

```toml
[features]
# Before
async = ["async-channel", "async-io", "futures/default", "wg/future"]

# After
async       = ["async-channel", "agnostic-lite/time", "futures/default", "wg/future"]
async-tokio = ["async", "agnostic-lite/tokio"]
async-smol  = ["async", "agnostic-lite/smol"]
async-wasm  = ["async", "agnostic-lite/wasm"]
```

The core `async` feature enables only the abstract traits (`agnostic-lite/time`).
Users pick a concrete runtime by enabling `async-tokio`, `async-smol`, or `async-wasm`.
The `full` feature becomes `["sync", "async-tokio", "serde"]` — it uses tokio as its
concrete async backend, consistent with the existing dev-dependency on tokio.

---

## Public API Changes (Breaking)

### `AsyncCache::new`

```rust
// Before
pub fn new<SP, R>(num_counters: usize, max_cost: i64, spawner: SP) -> Result<Self, CacheError>
where
    SP: Fn(BoxFuture<'static, ()>) -> R + Send + Sync + 'static + Copy

// After
pub fn new<RT: RuntimeLite>(num_counters: usize, max_cost: i64) -> Result<Self, CacheError>
```

### `AsyncCache::new_with_key_builder`

```rust
// Before
pub fn new_with_key_builder<SP, R>(num_counters, max_cost, index: KH, spawner: SP) -> ...

// After
pub fn new_with_key_builder<RT: RuntimeLite>(num_counters, max_cost, index: KH) -> ...
```

### `AsyncCacheBuilder::finalize`

```rust
// Before
pub fn finalize<SP, R>(self, spawner: SP) -> Result<AsyncCache<...>, CacheError>
where
    SP: Fn(BoxFuture<'static, ()>) -> R + Send + Sync + 'static + Copy

// After
pub fn finalize<RT: RuntimeLite>(self) -> Result<AsyncCache<...>, CacheError>
```

### User migration

```rust
// Before — tokio
AsyncCache::new(12960, 1e6 as i64, tokio::spawn)
AsyncCacheBuilder::new(100, 10).finalize(tokio::spawn)

// After — tokio (with agnostic-lite/tokio feature)
use agnostic_lite::TokioRuntime;
AsyncCache::new::<TokioRuntime>(12960, 1e6 as i64)
AsyncCacheBuilder::new(100, 10).finalize::<TokioRuntime>()

// Before — smol / async-std
AsyncCache::new(12960, 1e6 as i64, async_std::task::spawn)

// After — smol (with agnostic-lite/smol feature)
use agnostic_lite::SmolRuntime;
AsyncCache::new::<SmolRuntime>(12960, 1e6 as i64)
```

---

## Internal Changes

Three call sites need updating. The `RT` type parameter threads down from `finalize`
through each internal constructor.

### 1. `src/cache/async.rs` — `CacheProcessor::spawn`

```rust
// Before
pub(crate) fn spawn(mut self, spawner: Box<dyn Fn(BoxFuture<'static, ()>) + Send + Sync>) {
    (spawner)(Box::pin(async move {
        let mut cleanup_timer = Timer::interval(self.cleanup_duration);
        loop {
            select! {
                _ = cleanup_timer.next().fuse() => { ... }
                // ...
            }
        }
    }))
}

// After
pub(crate) fn spawn<RT: RuntimeLite>(mut self) {
    RT::spawn_detach(async move {
        let mut cleanup_timer = RT::interval(self.cleanup_duration);
        loop {
            select! {
                _ = cleanup_timer.next().fuse() => { ... }
                // ...
            }
        }
    });
}
```

Remove `use async_io::Timer` import.

### 2. `src/policy/async.rs` — `PolicyProcessor::spawn`

```rust
// Before
PolicyProcessor::new(inner.clone(), items_rx, stop_rx).spawn(Box::new(move |fut| {
    spawner(fut);
}));

// After
PolicyProcessor::new(inner.clone(), items_rx, stop_rx).spawn::<RT>();
```

The `spawn` method changes from taking a `Box<dyn Fn(BoxFuture)>` to using
`RT::spawn_detach` directly.

### 3. `src/cache/async.rs` — `AsyncCacheBuilder::finalize` internal wiring

The spawner closure passed to `CacheProcessor` and `AsyncLFUPolicy` is replaced by
threading `RT` as a type parameter through both calls.

---

## `src/lib.rs` — `axync` module

Remove `futures::future::BoxFuture` — it was only used for the spawner closure type.
`RT::spawn_detach<F: Future + Send + 'static>` accepts any future directly with no
boxing, so the `BoxFuture` import can be dropped from `axync` and from the policy/cache
async files. The `async-io` reference is removed entirely.

---

## Test Changes

All tests in `src/cache/test.rs` use `tokio::spawn`. Replace with `TokioRuntime`:

```rust
// Before
.finalize(tokio::spawn)
AsyncCache::new_with_key_builder(100, 10, kh, tokio::spawn)

// After
use agnostic_lite::TokioRuntime;
.finalize::<TokioRuntime>()
AsyncCache::new_with_key_builder::<TokioRuntime>(100, 10, kh)
```

Add `agnostic-lite = { version = "0.6", features = ["tokio"] }` to dev-dependencies.
Remove `async-std = { version = "1.12" }` from dev-dependencies.

Update `examples/async_example.rs` to use `TokioRuntime` instead of `tokio::spawn`.

---

## Example Update

```rust
// Before
let c: AsyncCache<&str, &str> = AsyncCache::new(12960, 1e6 as i64, tokio::spawn).unwrap();

// After
use agnostic_lite::TokioRuntime;
let c: AsyncCache<&str, &str> = AsyncCache::new::<TokioRuntime>(12960, 1e6 as i64).unwrap();
```

---

## Files to Change

| File | Change |
|------|--------|
| `Cargo.toml` | Remove `async-io`, add `agnostic-lite`, new feature flags, remove `async-std` dev-dep |
| `src/lib.rs` | Remove `async-io` from `axync` module imports |
| `src/cache/async.rs` | `finalize<RT>`, `CacheProcessor::spawn<RT>`, remove `BoxFuture` spawner |
| `src/policy/async.rs` | `with_hasher<RT>`, `PolicyProcessor::spawn<RT>` |
| `src/cache/test.rs` | Replace `tokio::spawn` with `TokioRuntime` at all call sites |
| `examples/async_example.rs` | Update to `TokioRuntime` |
| `README.md` | Update async installation and usage sections |

---

## Non-Goals

- No changes to the sync feature or `Cache` / `CacheBuilder`
- No changes to `async-channel` (already runtime-agnostic)
- No changes to `wg/future` (already runtime-agnostic)
- No `SmolRuntime` tests added (tokio tests are sufficient; smol users verify via type system)
