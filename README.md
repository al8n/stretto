<div align="center">
<h1>Stretto</h1>
</div>
<div align="center">

A high performance thread-safe memory-bound Rust cache.

Stretto is a pure Rust implementation of <https://github.com/dgraph-io/ristretto>.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/stretto-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
<img alt="LoC" src="https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fal8n%2F327b2a8aef9003246e45c6e47fe63937%2Fraw%2Fstretto" height="22">
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/stretto/ci.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/stretto?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

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
* **Designed for Database Workloads** - on OLTP-style traces with small working sets and strong frequency skew, Stretto keeps the hot set far better than general-purpose caches (see [Benchmarks](#benchmarks)).
* **Fast Throughput** - use a variety of techniques for managing contention and the result is excellent throughput.
* **Cost-Based Eviction** - any large new item deemed valuable can evict multiple smaller items (cost could be anything).
* **Fully Concurrent** - you can use as many threads as you want with little throughput degradation.
* **Metrics** - optional performance metrics for throughput, hit ratios, and other stats.
* **Simple API** - just figure out your ideal `CacheBuilder`/`AsyncCacheBuilder` values and you're off and running.

## Table of Contents

- [Features](#features)
- [Table of Contents](#table-of-contents)
- [Benchmarks](#benchmarks)
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

## Benchmarks

Hit ratios produced by [cachebench](https://github.com/al8n/cachebench) against the [ARC trace](https://github.com/moka-rs/cache-trace) suite, 16 concurrent clients. Compared against [QuickCache](https://crates.io/crates/quick_cache) 0.6, [TinyUFO](https://crates.io/crates/TinyUFO) 0.8 and [Moka](https://crates.io/crates/moka) 0.12. Higher is better; **bold** marks the leader for each row. The "Stretto" column reports the sync `Cache`; `AsyncCache` tracks sync closely on the same traces (see [Sync vs async](#sync-vs-async-cache)).

**Where Stretto fits.** TinyLFU admission compares each candidate's recent access frequency against the current eviction victim's and only admits when the candidate is "hotter." Two conditions together make this win:

1. **Capacity is small relative to the working set** (typically ≤ ~25%). The admission filter then has a real choice on every insert; rejecting a one-off scan beats evicting a hot tail entry.
2. **Access frequencies are skewed** (Zipf-like). The frequency sketch needs signal to distinguish hot from cold; uniform or scan-heavy traffic gives it none.

When both hold, Stretto leads by **7–47 percentage points** — OLTP at every capacity, the 20K rows of P1–P13, the 100K rows of S1–S3. When either fails (DS1 at 4M+, S1/S2 at 800K, P14, ConCat, MergeP, MergeS, the 160K rows of P4/P6/P7/P11/P12), admission rejects items LRU/W-TinyLFU would have kept and Stretto trails by up to 57 points. **Decision rule:** if your working set is several times your cache size *and* hot keys repeat, pick Stretto; if traffic is bursty scans or your cache is sized to hold most of the data, pick Moka or QuickCache.

### S3 trace (Search)

S3 traces an entire workload from small to large capacity in one section. At 100K (cold tail still being rejected) Stretto wins by ~8 points; at 400K (Stretto's "frontier") it edges past Moka Segmented by 1.6; at 800K (capacity ≈ working set) admission filtering hurts and Moka Segmented pulls ahead by 13.6. The 400K row is the cleanest single illustration of where Stretto's regime ends.

| Capacity | QuickCache | Stretto | TinyUFO | Moka Sync | Moka Async | Moka Segmented(8) |
|---:|---:|---:|---:|---:|---:|---:|
|   100,000 | 12.83% | **20.72%** |  2.80% | 10.40% | 10.49% | 10.08% |
|   400,000 | 42.29% | **43.92%** | 20.85% | 40.98% | 41.59% | 42.30% |
|   800,000 | 68.35% | 56.52% | 62.28% | 66.35% | 67.37% | **70.10%** |

### DS1 trace (Database server)

DS1 is a "database" trace in name only — its access pattern has weak frequency skew across a huge key space and rewards LRU-style retention. Even at 1M (≈5% of interesting working set) Stretto trails QuickCache by 2 points; the gap widens to 20 points at 4M and **45 points** at 8M, as LRU policies keep recently-touched items that admission filtering rejects. DS1 is the canonical "do not pick Stretto" workload.

| Capacity | QuickCache | Stretto | TinyUFO | Moka Sync | Moka Async | Moka Segmented(8) |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000,000 | **15.09%** | 12.98% |  3.36% | 12.51% | 13.19% | 14.74% |
| 4,000,000 | 44.42% | 24.92% | 24.71% | 41.48% | 39.22% | **44.65%** |
| 8,000,000 | **69.31%** | 24.57% | 53.42% | 64.38% | 64.85% | 66.43% |

### OLTP trace

OLTP is the workload Ristretto was designed for: a tight, repeatedly-accessed working set with strong frequency skew. Stretto's hit ratio is **33–47 percentage points** above the next-best cache and stays nearly flat across capacity (75.7% at cap=256, 76.4% at cap=2000) — TinyLFU pins the hot tail regardless of slack. Other caches' ratios climb with capacity (QuickCache: 22% → 42%, Moka Sync: 26% → 43%) because they have to physically retain the working set first. v0.9.0's 64-stripe insert buffer keeps producer contention out of the picture under 16 concurrent clients, so the result reflects the policy itself, not buffer-overflow drops.

| Capacity | QuickCache | Stretto | TinyUFO | Moka Sync | Moka Async | Moka Segmented(8) |
|---:|---:|---:|---:|---:|---:|---:|
|    256 | 22.28% | **75.74%** | 16.35% | 26.40% | 25.55% | 28.80% |
|    512 | 28.79% | **76.02%** | 19.89% | 32.71% | 32.00% | 33.53% |
|  1,000 | 35.12% | **76.13%** | 27.08% | 38.02% | 37.48% | 37.80% |
|  2,000 | 41.68% | **76.39%** | 35.05% | 42.99% | 42.47% | 42.85% |

### ARC P-series traces (Workstation)

The P-series captures workstation block-IO over a few hours per machine. Two distinct regimes:

- **At 20K capacity (~12.5% of working set)** — Stretto wins all 13 of P1–P13, by 13–40 points. P14 (a ~5×-larger trace, 80K is already a tighter ratio) sits past Stretto's regime and QuickCache leads.
- **At 160K capacity (~100% of working set)** — the picture flattens. Stretto still wins clearly on P2 (+6), P3 (+5), P8 (+9), P10 (+7); ties P1, P5, P9, P13 within ~1 point; trails P4, P6, P7, P11, P12, P14 where W-TinyLFU's recency-and-frequency signal extracts more from a saturated cache than admission filtering can.

| Trace | Capacity | QuickCache | Stretto | TinyUFO | Moka Sync | Moka Async | Moka Segmented(8) |
|---|---:|---:|---:|---:|---:|---:|---:|
| P1  |  20,000 | 22.70% | **54.97%** | 17.39% | 21.37% | 21.25% | 21.86% |
| P1  | 160,000 | 68.61% | **68.94%** | 64.30% | 63.98% | 64.84% | 66.96% |
| P2  |  20,000 | 21.02% | **60.94%** | 16.20% | 19.39% | 19.44% | 20.16% |
| P2  | 160,000 | 67.15% | **73.48%** | 63.91% | 63.38% | 63.85% | 66.83% |
| P3  |  20,000 |  8.92% | **44.17%** |  3.82% | 11.22% | 11.25% | 11.02% |
| P3  | 160,000 | 54.16% | **59.44%** | 50.02% | 48.23% | 48.89% | 50.91% |
| P4  |  20,000 |  4.79% | **18.61%** |  4.52% |  4.79% |  4.74% |  4.66% |
| P4  | 160,000 | 30.62% | 22.46% | 26.98% | 28.81% | 29.04% | **30.71%** |
| P5  |  20,000 |  8.98% | **29.91%** |  6.26% |  8.55% |  8.53% |  8.38% |
| P5  | 160,000 | 46.58% | **47.39%** | 38.57% | 43.14% | 43.28% | 45.16% |
| P6  |  20,000 | 17.22% | **37.05%** |  8.89% | 16.29% | 15.89% | 14.89% |
| P6  | 160,000 | **84.41%** | 64.87% | 58.88% | 77.27% | 78.01% | 81.28% |
| P7  |  20,000 | 10.46% | **43.19%** |  5.12% |  8.22% |  8.10% |  7.46% |
| P7  | 160,000 | **57.35%** | 53.52% | 44.31% | 53.20% | 54.30% | 56.63% |
| P8  |  20,000 | 22.64% | **62.46%** | 18.43% | 21.38% | 21.63% | 21.73% |
| P8  | 160,000 | 76.54% | **85.26%** | 73.72% | 72.72% | 73.19% | 76.03% |
| P9  |  20,000 | 12.49% | **40.13%** |  9.88% | 13.63% | 13.65% | 13.87% |
| P9  | 160,000 | 55.31% | **56.59%** | 51.33% | 50.28% | 51.42% | 53.15% |
| P10 |  20,000 |  6.56% | **19.93%** |  3.36% |  4.58% |  4.61% |  4.53% |
| P10 | 160,000 | 28.72% | **37.08%** | 21.48% | 30.38% | 29.57% | 29.99% |
| P11 |  20,000 | 19.53% | **42.40%** | 17.57% | 18.81% | 18.69% | 18.91% |
| P11 | 160,000 | **68.28%** | 56.77% | 66.50% | 65.71% | 65.91% | 68.26% |
| P12 |  20,000 | 10.09% | **32.71%** |  8.96% |  9.23% |  9.26% |  9.11% |
| P12 | 160,000 | **45.06%** | 41.79% | 40.61% | 41.94% | 42.58% | 44.66% |
| P13 |  20,000 | 12.60% | **43.26%** |  7.32% | 11.36% | 11.16% | 10.99% |
| P13 | 160,000 | 55.44% | **56.74%** | 47.44% | 50.05% | 50.76% | 53.95% |
| P14 |  80,000 | **29.80%** | 24.99% | 19.53% | 28.13% | 28.22% | 29.11% |
| P14 | 640,000 | **57.93%** | 33.05% | 53.13% | 51.93% | 52.15% | 54.30% |

### ARC S-series traces (Search)

S1 and S2 are search-engine traces with weak frequency skew. The crossover between Stretto's regime and LRU-style policies' regime sits between 100K and 800K capacity:

- At 100K (working set still doesn't fit), Stretto's filter rejects the long cold tail and wins by 8–9 points.
- At 800K (capacity nears the working set), LRU/W-TinyLFU keep recently-touched items that admission rejects, and Stretto trails by 14–27 points.

| Trace | Capacity | QuickCache | Stretto | TinyUFO | Moka Sync | Moka Async | Moka Segmented(8) |
|---|---:|---:|---:|---:|---:|---:|---:|
| S1 | 100,000 |  9.59% | **18.78%** |  2.64% |  8.88% |  8.89% |  8.69% |
| S1 | 800,000 | **56.86%** | 29.81% | 49.44% | 55.32% | 55.38% | 56.08% |
| S2 | 100,000 | 13.11% | **21.24%** |  2.78% | 10.53% | 10.61% | 10.24% |
| S2 | 800,000 | 68.36% | 56.55% | 62.57% | 66.78% | 67.67% | **70.33%** |

### Merged ARC traces (scan-heavy)

ConCat (DS1 ∥ S3), MergeP (P1–P14 interleaved), and MergeS (S1–S3 interleaved) splice ARC traces into long sequences with weak frequency skew over very large key spaces. TinyLFU's frequency sketch is built for a single stable workload, not concatenated phases, so admission rejects items LRU-style policies would retain — Stretto trails by **30–60 points** across every measured capacity. **These traces document the regime where Stretto is the wrong tool**; do not use them to size a Stretto cache.

ConCat (DS1 ∥ S3):

| Capacity | QuickCache | Stretto | TinyUFO | Moka Sync | Moka Async | Moka Segmented(8) |
|---:|---:|---:|---:|---:|---:|---:|
|   200,000 | **60.43%** | 14.31% | 40.81% | 54.80% | 55.38% | 57.86% |
|   400,000 | **72.44%** | 15.31% | 57.37% | 63.41% | 64.51% | 67.96% |
| 3,200,000 | **87.74%** | 43.26% | 86.87% | 73.09% | 76.99% | 82.22% |

MergeP (P1–P14 interleaved):

| Capacity | QuickCache | Stretto | TinyUFO | Moka Sync | Moka Async | Moka Segmented(8) |
|---:|---:|---:|---:|---:|---:|---:|
|   400,000 | **44.31%** | 10.71% | 38.40% | 38.09% | 38.48% | 40.31% |
| 1,000,000 | **57.98%** | 22.83% | 53.41% | 50.66% | 51.46% | 53.77% |
| 3,200,000 | **75.23%** | 39.62% | 71.23% | 65.02% | 66.43% | 70.42% |

MergeS (S1–S3 interleaved):

| Capacity | QuickCache | Stretto | TinyUFO | Moka Sync | Moka Async | Moka Segmented(8) |
|---:|---:|---:|---:|---:|---:|---:|
|   400,000 | 19.96% | 14.67% |  7.29% | 20.62% | 20.68% | **21.14%** |
| 1,000,000 | **44.81%** | 28.25% | 30.15% | 41.67% | 42.01% | 44.19% |
| 3,200,000 | **83.02%** | 45.29% | 82.03% | 77.54% | 78.38% | 80.38% |

### Sync vs async cache

`AsyncCache` since v0.9.0 uses the same TinyLFU policy and the same 64-stripe insert buffer as sync `Cache`. Hit ratios track within ~0.4 percentage points across OLTP and most of S3:

| Trace, Capacity | Stretto sync | Stretto async |
|---|---:|---:|
| OLTP, 256        | 75.74% | 75.78% |
| OLTP, 512        | 76.02% | 75.66% |
| OLTP, 1,000      | 76.13% | 75.87% |
| OLTP, 2,000      | 76.39% | 76.15% |
| S3,   100,000    | 20.72% | 20.90% |
| S3,   400,000    | 43.92% | 43.21% |
| S3,   800,000    | 56.52% | 58.50% |

The S3 800K row swings ~2 points run-to-run (async ahead here, sync ahead in others) because of drop-on-overflow during scan bursts, not policy divergence. Throughput is comparable: at S3 cap=400K sync finishes in ~2.9s vs async ~4.2s; at OLTP cap=256 both complete in under 0.07s. Pick `AsyncCache` for runtime ergonomics — hit ratio is not the tradeoff.

### Reproducing

```bash
git clone --recursive https://github.com/al8n/cachebench.git
cd cachebench/cache-trace/arc && for f in *.lis.zst; do zstd -d "$f"; done && cd ../..
cargo run --release --features "stretto,quick_cache,tiny-ufo,moka-v012" -- \
    -f oltp,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,s1,s2,s3,ds1,concat,merge-p,merge-s -n 16
```

`spc1likeread` is omitted from the tables above; with a 30,000,000-entry default capacity it takes ≈30 minutes per cache implementation and adds little signal beyond what DS1 already shows.

## Installation

- Use Cache.

    ```toml
    [dependencies]
    stretto = "0.9"
    ```

- Use AsyncCache with tokio

    ```toml
    [dependencies]
    stretto = { version = "0.9", features = ["tokio"] }
    ```

- Use AsyncCache with smol

    ```toml
    [dependencies]
    stretto = { version = "0.9", features = ["smol"] }
    ```

## Usage

### Example

See [examples](./examples/) folder for more details:

- [sync](./examples/sync.rs): Use stretto's cache in sync environment
- [tokio](./examples/tokio.rs): Use stretto's cache with tokio async runtime
- [smol](./examples/smol.rs): Use stretto's cache with smol async runtime

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

```rust,ignore
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

Both `Cache` and `AsyncCache` buffer admission requests in a 64-stripe ring that hashes each thread (or task) to its own `Mutex<Vec<_>>` so producers rarely contend. When a stripe accumulates `insert_stripe_high_water` items (default `64`) the full batch is sent to the policy processor; the processor also drains every stripe inline every `drain_interval` (default `500ms`) as a safety net.

Use `set_insert_stripe_high_water(items)` to tune the per-stripe batch size. Larger values amortize channel sends but delay admission decisions; values around `64–256` work well for caches under ~10K capacity. The stripe count and per-batch channel capacity are not user-tunable.

#### metrics

Metrics is true when you want real-time logging of a variety of stats. The reason this is a CacheBuilder flag is because there's a 10% throughput performance overhead.

#### ignore_internal_cost

Defaults to `true`: each insert is charged only the caller-supplied cost, so `max_cost` behaves as an entry budget when you pass `1` per insert.

Set to `false` when `max_cost` represents a byte budget and you need each stored item to also account for ~56 bytes of per-entry bookkeeping (key, conflict, version, value wrapper, time).

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

```rust,ignore
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
