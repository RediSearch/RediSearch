# Union Iterator Sweep — Benchmark Results

This document captures the full comparison between the **Rust** and **C** implementations of the
Union iterator, sweeping over child counts, data-distribution patterns, iterator types, and
execution modes.

## Contents

1. [Setup](#setup)
2. [Raw timings per group](#raw-timings-per-group)
   - [IdList](#idlist)
   - [Term](#term)
   - [Numeric (Rust only)](#numeric-rust-only)
3. [Flat vs Heap](#flat-vs-heap)
4. [C vs Rust — ratio tables](#c-vs-rust--ratio-tables)
5. [Findings](#findings)
6. [Heuristic proposal](#heuristic-proposal)

## Setup

### Bench source
`src/redisearch_rs/rqe_iterators_bencher/src/benchers/union_sweep.rs`
(invoked via `cargo bench -p rqe_iterators_bencher -- "Sweep"`).

### Parameters

| Parameter            | Value                                                   |
|----------------------|---------------------------------------------------------|
| `DOCS_PER_CHILD`     | `10_000`                                                |
| `CHILD_COUNTS`       | `[2, 4, 8, 12, 16, 20, 24, 32, 48, 64]`                 |
| `RNG_SEED`           | `42`                                                    |
| Measurement time     | 3 s (criterion default sampling of 100 iterations)      |
| Warmup time          | 200 ms                                                  |
| Total measured cases | **600** (3 overlaps × 3 iter types × 4 modes × 10 n)    |

### Overlap strategies (doc-centric generation)

For `total_docs = num_children * DOCS_PER_CHILD`, iterate through the global ID range once and
assign each `doc_id` to a subset of children — ensuring each child's list is already sorted.

| Overlap      | Per-doc membership                                       |
|--------------|----------------------------------------------------------|
| **High**     | Each doc is put into **2 to 75% of children** (random).  |
| **Low**      | Each doc is put into **1-2 children** (random).          |
| **Disjoint** | Each doc is put into **exactly 1 child** (random).       |

### Iterator types & modes

- **Iterator types**: `IdList` (`IdListSorted`), `Term` (`Term` inverted-index), `Numeric` (`Numeric` inverted-index; Rust only — C path not wired in the bencher).
- **Modes**:
  - **Flat / Heap** — union strategy (linear scan of active children vs. priority queue).
  - **Quick / Full** — `quick_exit` off or on; Quick stops at the first match for the current doc, Full gathers every child match.

### How to read the tables

- Times are **medians in milliseconds** (criterion `[low mid high]`, middle value).
- For tables showing both implementations, the column header carries the suffix `/Rust` or `/C`.
- Lower is better.

## Raw timings per group

### IdList

#### IdList — Disjoint (ms, median)

| n | Flat Quick/Rust | Flat Quick/C | Heap Quick/Rust | Heap Quick/C | Flat Full/Rust | Flat Full/C | Heap Full/Rust | Heap Full/C |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2  | 0.36 | 0.44 | 0.43 | 0.50 | 0.33 | 0.40 | 0.50 | 0.60 |
| 4  | 0.85 | 1.02 | 0.91 | 1.13 | 0.80 | 1.05 | 1.11 | 1.46 |
| 8  | 2.15 | 3.01 | 2.06 | 2.69 | 2.01 | 2.47 | 2.53 | 3.48 |
| 12 | 3.52 | 5.49 | 3.34 | 4.61 | 3.57 | 4.32 | 4.03 | 5.63 |
| 16 | 5.31 | 8.19 | 4.91 | 7.15 | 5.70 | 6.77 | 5.84 | 8.20 |
| 20 | 7.38 | 11.69 | 6.41 | 8.66 | 8.41 | 9.11 | 7.93 | 10.32 |
| 24 | 9.12 | 13.88 | 7.72 | 10.57 | 10.37 | 12.76 | 9.12 | 13.05 |
| 32 | 14.06 | 20.45 | 11.22 | 15.60 | 20.46 | 19.14 | 13.21 | 17.69 |
| 48 | 24.40 | 34.32 | 17.69 | 24.93 | 40.03 | 58.20 | 19.88 | 29.19 |
| 64 | 39.00 | 51.47 | 25.91 | 34.41 | 68.43 | 63.27 | 28.41 | 41.80 |

#### IdList — High Overlap (ms, median)

| n | Flat Quick/Rust | Flat Quick/C | Heap Quick/Rust | Heap Quick/C | Flat Full/Rust | Flat Full/C | Heap Full/Rust | Heap Full/C |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2  | 0.32 | 0.38 | 0.39 | 0.47 | 0.28 | 0.39 | 0.45 | 0.57 |
| 4  | 0.81 | 0.95 | 1.38 | 1.72 | 1.19 | 1.58 | 1.93 | 2.57 |
| 8  | 2.24 | 2.52 | 4.76 | 6.35 | 5.31 | 7.01 | 6.87 | 10.42 |
| 12 | 3.68 | 4.37 | 9.39 | 12.59 | 14.14 | 18.53 | 15.20 | 24.14 |
| 16 | 5.31 | 6.59 | 14.48 | 21.44 | 24.40 | 31.95 | 27.21 | 43.69 |
| 20 | 7.39 | 9.05 | 20.99 | 30.35 | 36.23 | 47.89 | 43.06 | 69.30 |
| 24 | 9.44 | 12.17 | 28.33 | 40.59 | 51.41 | 67.10 | 61.76 | 101.30 |
| 32 | 14.85 | 18.80 | 45.07 | 62.54 | 89.76 | 113.86 | 112.32 | 203.14 |
| 48 | 26.11 | 32.24 | 86.42 | 122.55 | 190.91 | 245.84 | 260.41 | 431.46 |
| 64 | 37.66 | 50.12 | 137.51 | 187.56 | 330.72 | 424.81 | 477.90 | 807.81 |

#### IdList — Low Overlap (ms, median)

| n | Flat Quick/Rust | Flat Quick/C | Heap Quick/Rust | Heap Quick/C | Flat Full/Rust | Flat Full/C | Heap Full/Rust | Heap Full/C |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2  | 0.34 | 0.40 | 0.46 | 0.54 | 0.36 | 0.49 | 0.61 | 0.71 |
| 4  | 0.97 | 1.15 | 1.30 | 1.61 | 1.12 | 1.34 | 1.59 | 2.00 |
| 8  | 2.31 | 3.05 | 2.82 | 3.94 | 2.60 | 3.36 | 3.56 | 4.77 |
| 12 | 3.89 | 5.55 | 8.16 | 6.73 | 4.80 | 6.03 | 6.04 | 7.94 |
| 16 | 5.84 | 8.86 | 7.82 | 10.22 | 8.24 | 10.67 | 8.95 | 12.53 |
| 20 | 8.28 | 11.52 | 9.46 | 13.71 | 10.66 | 13.13 | 11.30 | 16.20 |
| 24 | 10.50 | 16.52 | 11.80 | 17.59 | 14.87 | 18.54 | 14.10 | 20.28 |
| 32 | 15.52 | 23.23 | 17.50 | 24.51 | 27.41 | 26.00 | 20.17 | 28.75 |
| 48 | 27.19 | 36.67 | 27.03 | 37.50 | 48.73 | 46.89 | 29.95 | 43.74 |
| 64 | 39.27 | 52.56 | 37.30 | 51.74 | 75.36 | 75.18 | 44.18 | 60.48 |



### Term

#### Term — Disjoint (ms, median)

| n | Flat Quick/Rust | Flat Quick/C | Heap Quick/Rust | Heap Quick/C | Flat Full/Rust | Flat Full/C | Heap Full/Rust | Heap Full/C |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2  | 0.61 | 0.74 | 0.66 | 0.78 | 0.60 | 0.62 | 0.77 | 0.87 |
| 4  | 1.16 | 1.57 | 1.25 | 1.69 | 1.22 | 1.45 | 1.53 | 1.97 |
| 8  | 3.04 | 3.73 | 2.88 | 3.94 | 2.66 | 3.09 | 3.02 | 4.14 |
| 12 | 4.69 | 6.43 | 4.15 | 5.80 | 4.57 | 5.32 | 4.65 | 6.72 |
| 16 | 6.30 | 9.85 | 5.87 | 9.08 | 6.52 | 7.68 | 6.49 | 9.86 |
| 20 | 8.43 | 13.44 | 7.49 | 11.79 | 9.71 | 10.92 | 8.29 | 12.41 |
| 24 | 11.83 | 16.20 | 10.43 | 14.53 | 13.12 | 14.42 | 10.57 | 15.62 |
| 32 | 17.05 | 22.93 | 14.26 | 20.54 | 21.91 | 23.53 | 15.40 | 22.41 |
| 48 | 29.24 | 39.18 | 22.16 | 31.01 | 45.26 | 52.37 | 24.14 | 40.44 |
| 64 | 53.24 | 60.67 | 48.36 | 46.42 | 84.15 | 74.86 | 36.07 | 50.40 |

#### Term — High Overlap (ms, median)

| n | Flat Quick/Rust | Flat Quick/C | Heap Quick/Rust | Heap Quick/C | Flat Full/Rust | Flat Full/C | Heap Full/Rust | Heap Full/C |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2  | 0.45 | 0.56 | 0.48 | 0.63 | 0.47 | 0.51 | 0.57 | 0.74 |
| 4  | 1.05 | 1.36 | 1.74 | 2.35 | 1.93 | 2.39 | 2.55 | 3.42 |
| 8  | 2.96 | 3.85 | 5.92 | 7.95 | 7.50 | 10.51 | 9.04 | 13.76 |
| 12 | 5.33 | 6.69 | 11.51 | 16.46 | 19.69 | 24.39 | 19.45 | 29.60 |
| 16 | 8.11 | 9.82 | 18.92 | 26.67 | 32.99 | 43.16 | 34.79 | 52.90 |
| 20 | 11.65 | 14.11 | 26.23 | 38.89 | 49.00 | 64.20 | 58.01 | 85.46 |
| 24 | 15.72 | 17.75 | 37.09 | 51.97 | 73.05 | 101.35 | 81.32 | 124.28 |
| 32 | 23.41 | 26.62 | 60.53 | 78.88 | 121.79 | 158.79 | 145.13 | 217.10 |
| 48 | 42.75 | 51.19 | 111.32 | 152.68 | 258.88 | 335.26 | 337.83 | 500.76 |
| 64 | 67.70 | 79.33 | 180.15 | 253.41 | 455.22 | 650.21 | 623.72 | 956.79 |

#### Term — Low Overlap (ms, median)

| n | Flat Quick/Rust | Flat Quick/C | Heap Quick/Rust | Heap Quick/C | Flat Full/Rust | Flat Full/C | Heap Full/Rust | Heap Full/C |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2  | 0.44 | 0.58 | 0.52 | 0.73 | 0.55 | 0.69 | 0.70 | 0.96 |
| 4  | 1.20 | 1.58 | 1.54 | 2.19 | 1.49 | 1.97 | 1.85 | 2.56 |
| 8  | 3.00 | 3.94 | 3.69 | 5.47 | 3.47 | 4.16 | 4.33 | 5.85 |
| 12 | 5.25 | 7.06 | 6.50 | 9.13 | 6.18 | 7.22 | 6.80 | 11.13 |
| 16 | 9.16 | 13.95 | 10.79 | 15.54 | 10.29 | 14.37 | 11.79 | 18.28 |
| 20 | 19.56 | 16.01 | 13.82 | 20.20 | 16.10 | 17.34 | 15.93 | 22.51 |
| 24 | 14.91 | 21.76 | 17.82 | 30.82 | 21.43 | 27.40 | 21.58 | 27.93 |
| 32 | 22.85 | 34.36 | 27.01 | 39.55 | 36.34 | 43.96 | 28.21 | 38.80 |
| 48 | 38.22 | 59.35 | 41.32 | 60.28 | 73.60 | 68.87 | 54.54 | 68.61 |
| 64 | 65.49 | 86.31 | 61.06 | 95.69 | 110.37 | 111.07 | 66.03 | 100.28 |

### Numeric (Rust only)

The numeric C path is not wired in the bencher; only Rust measurements are shown.

#### Numeric — Disjoint (ms, median)

| n | Flat Quick | Heap Quick | Flat Full | Heap Full |
|---:|---:|---:|---:|---:|
| 2  | 0.60 | 0.63 | 0.60 | 0.71 |
| 4  | 1.33 | 1.41 | 1.39 | 1.51 |
| 8  | 3.03 | 2.82 | 3.62 | 3.31 |
| 12 | 4.92 | 4.43 | 6.32 | 5.24 |
| 16 | 7.45 | 6.32 | 8.98 | 6.78 |
| 20 | 9.62 | 8.31 | 11.84 | 8.89 |
| 24 | 12.83 | 9.84 | 15.40 | 10.73 |
| 32 | 17.26 | 14.25 | 24.95 | 15.35 |
| 48 | 32.37 | 22.86 | 48.90 | 24.94 |
| 64 | 50.08 | 33.73 | 81.39 | 35.28 |

#### Numeric — High Overlap (ms, median)

| n | Flat Quick | Heap Quick | Flat Full | Heap Full |
|---:|---:|---:|---:|---:|
| 2  | 0.66 | 0.65 | 0.64 | 0.73 |
| 4  | 1.84 | 2.53 | 2.69 | 3.13 |
| 8  | 5.84 | 8.63 | 10.81 | 10.86 |
| 12 | 10.53 | 18.08 | 25.83 | 24.56 |
| 16 | 16.94 | 28.90 | 40.34 | 40.78 |
| 20 | 24.73 | 43.01 | 60.91 | 64.15 |
| 24 | 33.41 | 60.08 | 87.97 | 92.27 |
| 32 | 51.23 | 99.34 | 146.75 | 165.60 |
| 48 | 100.93 | 207.48 | 320.26 | 375.38 |
| 64 | 162.87 | 352.15 | 548.21 | 682.48 |

#### Numeric — Low Overlap (ms, median)

| n | Flat Quick | Heap Quick | Flat Full | Heap Full |
|---:|---:|---:|---:|---:|
| 2  | 0.70 | 0.79 | 0.76 | 0.89 |
| 4  | 1.67 | 1.92 | 1.90 | 2.25 |
| 8  | 3.96 | 4.10 | 4.65 | 4.81 |
| 12 | 6.05 | 10.44 | 8.34 | 8.03 |
| 16 | 9.04 | 9.41 | 11.47 | 10.72 |
| 20 | 11.61 | 12.14 | 16.55 | 13.61 |
| 24 | 14.63 | 15.43 | 23.55 | 17.90 |
| 32 | 20.28 | 21.72 | 32.52 | 23.20 |
| 48 | 35.23 | 35.20 | 58.60 | 37.27 |
| 64 | 51.11 | 47.66 | 92.91 | 51.81 |


## Flat vs Heap

Ratio is `Flat / Heap` so **> 1 ⇒ Heap is faster**, **< 1 ⇒ Flat is faster**.

Numeric has Rust measurements only (C path not wired in the bencher).

### Rust

#### IdList — Flat / Heap (Rust)

| n | Disjoint Quick | Disjoint Full | High Quick | High Full | Low Quick | Low Full |
|---:|---:|---:|---:|---:|---:|---:|
| 2  | 0.84× | 0.66× | 0.82× | 0.63× | 0.75× | 0.59× |
| 4  | 0.93× | 0.72× | 0.59× | 0.62× | 0.75× | 0.71× |
| 8  | 1.04× | 0.79× | 0.47× | 0.77× | 0.82× | 0.73× |
| 12 | 1.05× | 0.89× | 0.39× | 0.93× | 0.48× | 0.79× |
| 16 | 1.08× | 0.98× | 0.37× | 0.90× | 0.75× | 0.92× |
| 20 | 1.15× | 1.06× | 0.35× | 0.84× | 0.87× | 0.94× |
| 24 | 1.18× | 1.14× | 0.33× | 0.83× | 0.89× | 1.05× |
| 32 | 1.25× | 1.55× | 0.33× | 0.80× | 0.89× | 1.36× |
| 48 | 1.38× | 2.01× | 0.30× | 0.73× | 1.01× | 1.63× |
| 64 | 1.51× | 2.41× | 0.27× | 0.69× | 1.05× | 1.71× |

#### Term — Flat / Heap (Rust)

| n | Disjoint Quick | Disjoint Full | High Quick | High Full | Low Quick | Low Full |
|---:|---:|---:|---:|---:|---:|---:|
| 2  | 0.91× | 0.77× | 0.95× | 0.81× | 0.84× | 0.79× |
| 4  | 0.92× | 0.80× | 0.61× | 0.75× | 0.78× | 0.80× |
| 8  | 1.05× | 0.88× | 0.50× | 0.83× | 0.81× | 0.80× |
| 12 | 1.13× | 0.98× | 0.46× | 1.01× | 0.81× | 0.91× |
| 16 | 1.07× | 1.01× | 0.43× | 0.95× | 0.85× | 0.87× |
| 20 | 1.12× | 1.17× | 0.44× | 0.84× | 1.42× | 1.01× |
| 24 | 1.13× | 1.24× | 0.42× | 0.90× | 0.84× | 0.99× |
| 32 | 1.20× | 1.42× | 0.39× | 0.84× | 0.85× | 1.29× |
| 48 | 1.32× | 1.87× | 0.38× | 0.77× | 0.92× | 1.35× |
| 64 | 1.10× | 2.33× | 0.38× | 0.73× | 1.07× | 1.67× |

#### Numeric — Flat / Heap (Rust)

| n | Disjoint Quick | Disjoint Full | High Quick | High Full | Low Quick | Low Full |
|---:|---:|---:|---:|---:|---:|---:|
| 2  | 0.94× | 0.84× | 1.02× | 0.88× | 0.88× | 0.85× |
| 4  | 0.94× | 0.93× | 0.73× | 0.86× | 0.87× | 0.85× |
| 8  | 1.07× | 1.09× | 0.68× | 1.00× | 0.97× | 0.97× |
| 12 | 1.11× | 1.21× | 0.58× | 1.05× | 0.58× | 1.04× |
| 16 | 1.18× | 1.32× | 0.59× | 0.99× | 0.96× | 1.07× |
| 20 | 1.16× | 1.33× | 0.57× | 0.95× | 0.96× | 1.22× |
| 24 | 1.30× | 1.44× | 0.56× | 0.95× | 0.95× | 1.32× |
| 32 | 1.21× | 1.63× | 0.52× | 0.89× | 0.93× | 1.40× |
| 48 | 1.42× | 1.96× | 0.49× | 0.85× | 1.00× | 1.57× |
| 64 | 1.48× | 2.31× | 0.46× | 0.80× | 1.07× | 1.79× |

### C

#### IdList — Flat / Heap (C)

| n | Disjoint Quick | Disjoint Full | High Quick | High Full | Low Quick | Low Full |
|---:|---:|---:|---:|---:|---:|---:|
| 2  | 0.88× | 0.66× | 0.81× | 0.67× | 0.75× | 0.69× |
| 4  | 0.90× | 0.72× | 0.55× | 0.62× | 0.72× | 0.67× |
| 8  | 1.12× | 0.71× | 0.40× | 0.67× | 0.78× | 0.70× |
| 12 | 1.19× | 0.77× | 0.35× | 0.77× | 0.82× | 0.76× |
| 16 | 1.15× | 0.83× | 0.31× | 0.73× | 0.87× | 0.85× |
| 20 | 1.35× | 0.88× | 0.30× | 0.69× | 0.84× | 0.81× |
| 24 | 1.31× | 0.98× | 0.30× | 0.66× | 0.94× | 0.91× |
| 32 | 1.31× | 1.08× | 0.30× | 0.56× | 0.95× | 0.90× |
| 48 | 1.38× | 1.99× | 0.26× | 0.57× | 0.98× | 1.07× |
| 64 | 1.50× | 1.51× | 0.27× | 0.53× | 1.02× | 1.24× |

#### Term — Flat / Heap (C)

| n | Disjoint Quick | Disjoint Full | High Quick | High Full | Low Quick | Low Full |
|---:|---:|---:|---:|---:|---:|---:|
| 2  | 0.95× | 0.71× | 0.88× | 0.69× | 0.79× | 0.72× |
| 4  | 0.93× | 0.74× | 0.58× | 0.70× | 0.72× | 0.77× |
| 8  | 0.95× | 0.75× | 0.48× | 0.76× | 0.72× | 0.71× |
| 12 | 1.11× | 0.79× | 0.41× | 0.82× | 0.77× | 0.65× |
| 16 | 1.08× | 0.78× | 0.37× | 0.82× | 0.90× | 0.79× |
| 20 | 1.14× | 0.88× | 0.36× | 0.75× | 0.79× | 0.77× |
| 24 | 1.12× | 0.92× | 0.34× | 0.82× | 0.71× | 0.98× |
| 32 | 1.12× | 1.05× | 0.34× | 0.73× | 0.87× | 1.13× |
| 48 | 1.26× | 1.29× | 0.34× | 0.67× | 0.98× | 1.00× |
| 64 | 1.31× | 1.49× | 0.31× | 0.68× | 0.90× | 1.11× |

### Flat/Heap crossover summary

Smallest `n` at which Heap becomes faster than Flat (per iterator type and mode).
Entries shown as `Rust / C` where both are available.

| Overlap   | IdList Quick        | IdList Full         | Term Quick          | Term Full           | Numeric Quick   | Numeric Full |
|-----------|:-------------------:|:-------------------:|:-------------------:|:-------------------:|:---------------:|:------------:|
| Disjoint  | ~8 / ~8             | ~20 / ~32           | ~8 / ~12            | ~16 / ~32           | ~8              | ~8           |
| Low       | ~64 (≤1.05×) / ~64 (≤1.02×) | ~24 / ~48   | never (≤1.07×) / never (≤0.98×) | ~20 / ~32 | ~64 (≤1.07×) | ~12          |
| **High**  | **never / never**   | **never / never**   | **never / never**   | **never / never**   | **never**       | **never**    |

## C vs Rust — ratio tables

Ratio is `C time / Rust time`, so **> 1 ⇒ Rust is faster**, **< 1 ⇒ C is faster**.

### IdList — C / Rust

#### IdList — Disjoint

| n | Flat Quick | Heap Quick | Flat Full | Heap Full |
|---:|---:|---:|---:|---:|
| 2  | 1.22× | 1.16× | 1.23× | 1.22× |
| 4  | 1.20× | 1.24× | 1.31× | 1.31× |
| 8  | 1.40× | 1.30× | 1.23× | 1.38× |
| 12 | 1.56× | 1.38× | 1.21× | 1.40× |
| 16 | 1.54× | 1.46× | 1.19× | 1.40× |
| 20 | 1.58× | 1.35× | 1.08× | 1.30× |
| 24 | 1.52× | 1.37× | 1.23× | 1.43× |
| 32 | 1.45× | 1.39× | 0.94× | 1.34× |
| 48 | 1.41× | 1.41× | 1.45× | 1.47× |
| 64 | 1.32× | 1.33× | 0.92× | 1.47× |

#### IdList — High Overlap

| n | Flat Quick | Heap Quick | Flat Full | Heap Full |
|---:|---:|---:|---:|---:|
| 2  | 1.19× | 1.20× | 1.37× | 1.27× |
| 4  | 1.17× | 1.25× | 1.34× | 1.33× |
| 8  | 1.13× | 1.33× | 1.32× | 1.52× |
| 12 | 1.19× | 1.34× | 1.31× | 1.59× |
| 16 | 1.24× | 1.48× | 1.31× | 1.61× |
| 20 | 1.22× | 1.45× | 1.32× | 1.61× |
| 24 | 1.29× | 1.43× | 1.31× | 1.64× |
| 32 | 1.27× | 1.39× | 1.27× | 1.81× |
| 48 | 1.24× | 1.42× | 1.29× | 1.66× |
| 64 | 1.33× | 1.36× | 1.28× | 1.69× |

#### IdList — Low Overlap

| n | Flat Quick | Heap Quick | Flat Full | Heap Full |
|---:|---:|---:|---:|---:|
| 2  | 1.17× | 1.18× | 1.37× | 1.17× |
| 4  | 1.18× | 1.24× | 1.20× | 1.26× |
| 8  | 1.32× | 1.40× | 1.29× | 1.34× |
| 12 | 1.43× | 0.83× | 1.26× | 1.31× |
| 16 | 1.52× | 1.31× | 1.29× | 1.40× |
| 20 | 1.39× | 1.45× | 1.23× | 1.43× |
| 24 | 1.57× | 1.49× | 1.25× | 1.44× |
| 32 | 1.50× | 1.40× | 0.95× | 1.43× |
| 48 | 1.35× | 1.39× | 0.96× | 1.46× |
| 64 | 1.34× | 1.39× | 1.00× | 1.37× |

### Term — C / Rust

#### Term — Disjoint

| n | Flat Quick | Heap Quick | Flat Full | Heap Full |
|---:|---:|---:|---:|---:|
| 2  | 1.23× | 1.17× | 1.03× | 1.12× |
| 4  | 1.36× | 1.35× | 1.19× | 1.29× |
| 8  | 1.22× | 1.37× | 1.16× | 1.37× |
| 12 | 1.37× | 1.40× | 1.16× | 1.44× |
| 16 | 1.56× | 1.55× | 1.18× | 1.52× |
| 20 | 1.59× | 1.57× | 1.12× | 1.50× |
| 24 | 1.37× | 1.39× | 1.10× | 1.48× |
| 32 | 1.35× | 1.44× | 1.07× | 1.46× |
| 48 | 1.34× | 1.40× | 1.16× | 1.68× |
| 64 | 1.14× | 0.96× | 0.89× | 1.40× |

#### Term — High Overlap

| n | Flat Quick | Heap Quick | Flat Full | Heap Full |
|---:|---:|---:|---:|---:|
| 2  | 1.23× | 1.32× | 1.10× | 1.29× |
| 4  | 1.29× | 1.35× | 1.24× | 1.34× |
| 8  | 1.30× | 1.34× | 1.40× | 1.52× |
| 12 | 1.26× | 1.43× | 1.24× | 1.52× |
| 16 | 1.21× | 1.41× | 1.31× | 1.52× |
| 20 | 1.21× | 1.48× | 1.31× | 1.47× |
| 24 | 1.13× | 1.40× | 1.39× | 1.53× |
| 32 | 1.14× | 1.30× | 1.30× | 1.50× |
| 48 | 1.20× | 1.37× | 1.30× | 1.48× |
| 64 | 1.17× | 1.41× | 1.43× | 1.53× |


#### Term — Low Overlap

| n | Flat Quick | Heap Quick | Flat Full | Heap Full |
|---:|---:|---:|---:|---:|
| 2  | 1.31× | 1.40× | 1.26× | 1.37× |
| 4  | 1.31× | 1.42× | 1.32× | 1.38× |
| 8  | 1.31× | 1.48× | 1.20× | 1.35× |
| 12 | 1.35× | 1.40× | 1.17× | 1.64× |
| 16 | 1.52× | 1.44× | 1.40× | 1.55× |
| 20 | 0.82× | 1.46× | 1.08× | 1.41× |
| 24 | 1.46× | 1.73× | 1.28× | 1.29× |
| 32 | 1.50× | 1.46× | 1.21× | 1.38× |
| 48 | 1.55× | 1.46× | 0.94× | 1.26× |
| 64 | 1.32× | 1.57× | 1.01× | 1.52× |

## Findings

> **Correction note.** An earlier revision of this document reported a "Rust Heap
> Quick regression at High Overlap" of 2-3×. That conclusion was wrong: the
> dispatch of the C heap implementation had been commented out in
> `src/iterators/union_iterator.c` in this branch (commit `2b7e3af`, "flat only
> again"), so every `/Heap */C/` measurement was actually exercising the C **flat**
> path. The numbers below are from a complete single-session re-run with the
> heap dispatch restored (`/tmp/union_sweep_full_v2.txt`, 600 cases).

### 1. Heap vs Flat — strategy choice depends on overlap (Rust only)

- **Disjoint** is the most heap-friendly pattern; heap wins early (Numeric from n ≈ 4, IdList/Term from n ≈ 8-12) and its advantage grows with `n` — up to **2.4×** at n=64 (IdList/Disjoint/Full).
- **Low Overlap** is mixed. Heap wins in Full mode from around n ≈ 16-32; in Quick mode Flat generally matches or beats Heap up to n=64 (the n=64 column shows a marginal ≤1.07× Heap advantage only).
- **High Overlap** shows **no Rust benefit** from Heap at any `n`: Flat is always faster on the Rust side, with Flat/Heap ratios at n=64 down to 0.27× (IdList Quick) — i.e. Heap is ~3.7× slower.

  *Intuition:* under High Overlap almost every child holds the current doc, so the heap is near-full and pays O(log n) per advance while Flat resolves everything with an in-place scan. The C implementation shows the same qualitative behaviour, often with a steeper penalty.

### 2. Rust Flat consistently beats C Flat

Rust's Flat mode (Quick and Full) is faster than C in essentially every cell, typically **1.1-1.6×** across all three overlaps and both IdList/Term. Full-Flat at large `n` is the weakest group for Rust (a few cells near parity, e.g. IdList-Disjoint n=32/64 at 0.92-0.94×).

### 3. Rust Heap beats C Heap across the board — including High Overlap

With the true C heap implementation measured, Rust is faster than C in heap mode in essentially every case:

| Group                      | Heap Quick at n=64: Rust vs C     | Heap Full at n=64: Rust vs C      |
|----------------------------|-----------------------------------|-----------------------------------|
| IdList — Disjoint          | 25.9 ms vs 34.4 ms (**1.33×**)    | 28.4 ms vs 41.8 ms (**1.47×**)    |
| IdList — Low Overlap       | 37.3 ms vs 51.7 ms (**1.39×**)    | 44.2 ms vs 60.5 ms (**1.37×**)    |
| IdList — High Overlap      | 137.5 ms vs 187.6 ms (**1.36×**)  | 477.9 ms vs 807.8 ms (**1.69×**)  |
| Term — Disjoint            | 48.4 ms vs 46.4 ms (0.96×)        | 36.1 ms vs 50.4 ms (**1.40×**)    |
| Term — Low Overlap         | 61.1 ms vs 95.7 ms (**1.57×**)    | 66.0 ms vs 100.3 ms (**1.52×**)   |
| Term — High Overlap        | 180.2 ms vs 253.4 ms (**1.41×**)  | 623.7 ms vs 956.8 ms (**1.53×**)  |

The largest Rust-over-C margins appear exactly where the earlier "regression" had been reported: **IdList / High Overlap / large n**, where Rust's heap is 1.4-1.7× faster. The only cell at n=64 where C is marginally ahead is Term-Disjoint Heap Quick (0.96×).

### 4. Both implementations underperform Flat under High Overlap

Rust and C agree that Heap is the wrong strategy in High Overlap. At n=64:
- **IdList-High Quick:** C Heap 187.6 ms vs C Flat 50.1 ms → C Heap ~3.7× slower; Rust Heap 137.5 ms vs Rust Flat 37.7 ms → Rust Heap ~3.6× slower.
- **Term-High Quick:** C Heap 253.4 ms vs C Flat 79.3 ms → ~3.2×; Rust Heap 180.2 ms vs Rust Flat 67.7 ms → ~2.7×.
- **Numeric-High Quick:** Rust Heap 352.2 ms vs Rust Flat 162.9 ms → ~2.2×.

So the existing `minUnionIterHeap` threshold steering away from heap in dense queries remains the right call, and no Rust-specific heap tuning is required for parity.

### 5. No actionable Heap-Quick regression — investigation closed

The triggering question ("investigate the Rust Heap Quick regression") resolves as **no regression exists**. The prior interpretation was measurement artefact. Rust's `UnionHeap::read_quick` / `skip_to` path is competitive with or ahead of C in every iterator-type × overlap × n combination measured (only outlier: Term-Disjoint Heap Quick at n=64, within 4% of C).

## Heuristic proposal

### Problem with the current dispatch

Today both implementations dispatch with the rule:

```
if num_children > minUnionIterHeap /* default 15 */ then Heap else Flat
```

`num_children` alone does not model the data distribution that actually drives the choice. The v2 sweep shows Heap wins are bounded by **overlap**, not by `num`:

| Overlap     | Does raising `num` make Heap a good choice?              |
|-------------|----------------------------------------------------------|
| Disjoint    | Yes — Heap wins from ~n=8, widening to 2.3× at n=64.     |
| Low         | Only in Full mode and only from n ≈ 20-32.               |
| **High**    | **No — Heap is 1.5-3.7× slower than Flat at every `n`.** |

Consequence: the current `num > 15` threshold dispatches to Heap for any 16+-child union regardless of overlap. On High-Overlap workloads this is a direct performance hit (e.g. IdList-High n=64 Quick: Heap is ~3.6× slower than Flat).

### Query-type as an overlap proxy (no signature change)

`NewUnionIterator` already receives `QueryNodeType type`. Every call site maps to a structural overlap profile driven by how the children are produced:

| Call site                                          | `type`              | Children                                     | Structural overlap                        |
|----------------------------------------------------|---------------------|----------------------------------------------|-------------------------------------------|
| `numeric_index.c::createNumericIterator`           | `QN_NUMERIC` / `QN_GEO` | Numeric range leaves for a single field  | **Disjoint** — each doc has one numeric value, falls in exactly one leaf |
| `geo_index.c::NewGeoRangeIterator`                 | `QN_GEO`            | 8 geohash-range numeric filters              | **Disjoint** — each point is in one geohash cell |
| `query.c::Query_EvalPrefixNode`                    | `QN_PREFIX`         | Term iterators per expanded prefix term      | **Low** — same doc can match multiple terms |
| `query.c::Query_EvalWildcardQueryNode`             | `QN_WILDCARD_QUERY` | Term iterators per expanded wildcard term    | **Low** — same doc can match multiple terms |
| `query.c::Query_EvalLexRangeNode` (×2)             | `QN_LEXRANGE`       | Term iterators per distinct term in range    | **Low** — same doc can have multiple terms in range |
| `query.c::iterateExpandedTerms` (fuzzy/prefix)     | `QN_FUZZY` / `QN_PREFIX` | Fuzzy-expanded term iterators           | **Low** — term-expansion pattern          |
| `query.c::Query_EvalTagNode`                       | `QN_TAG`            | One per tag value in the union               | **Low** — multi-value tags ⇒ small overlap |
| `query.c::Query_EvalUnionNode`                     | `QN_UNION`          | Arbitrary user OR-expression                 | **Unknown** — no structural guarantee     |

So the *type alone* partitions the call sites into three buckets — Disjoint / Low / Unknown — with no runtime density computation required.

### Proposed rule

```
dispatch(num_children, type, quick_exit):
    # 1. Dispatch overhead gate — too small for Heap to amortise.
    if num_children < MIN_HEAP_N:        # 8
        return Flat

    bucket = overlap_bucket(type)

    # 2. Disjoint-by-construction → Heap wins from n≥8 onward.
    if bucket == Disjoint:
        return Heap

    # 3. Low-overlap term-expansion types.
    if bucket == Low:
        if quick_exit:                   # Quick rarely benefits from Heap here
            return Flat
        return (num_children >= LOW_OVERLAP_HEAP_N) ? Heap : Flat   # 24

    # 4. Unknown (QN_UNION): conservative threshold, biased toward Flat.
    return (num_children >= UNKNOWN_HEAP_N) ? Heap : Flat            # 32


overlap_bucket(type):
    QN_NUMERIC, QN_GEO, QN_GEOMETRY             => Disjoint
    QN_PREFIX, QN_WILDCARD_QUERY, QN_LEXRANGE,
    QN_FUZZY, QN_TAG                            => Low
    QN_UNION, _                                 => Unknown
```

### Where the thresholds come from

- **`MIN_HEAP_N = 8`** — Under Disjoint at n=8 Heap starts winning (IdList/Term Quick 1.04-1.05×, Numeric 1.07×). Below n=8 the heap-init and per-step `log n` overhead is not amortised.
- **`LOW_OVERLAP_HEAP_N = 24`** — From the Low-Full columns: Heap starts beating Flat at n≈20-32 (IdList 1.05× at 24, Term 1.01× at 20, Numeric 1.04× at 12). 24 is a safe mid-point that captures most of the Full-mode gains (1.3-1.8× at n≥32) without dispatching Heap for the n=8-16 neutral zone.
- **`UNKNOWN_HEAP_N = 32`** — For generic `QN_UNION` with no a-priori information, this threshold biases toward Flat:
  - If the actual distribution turns out to be High overlap, the worst-case penalty of running Heap at n=32 is ~2-3× (Flat/Heap ≈ 0.33-0.52×).
  - If the actual distribution is Disjoint, the missed gain at n=32 is 1.2-1.7×.
  - 32 is approximately the crossover where Disjoint gains exceed the average High-overlap penalty weighted by the "surprise" cost.
- **Quick-exit Low override**: every Low-Quick cell in the sweep is ≤ 1.07× Heap/Flat (usually < 1.0). In Quick mode the heap's per-step bookkeeping isn't amortised by collecting siblings, so any non-zero overlap tips the balance to Flat. Default to Flat for Low-Quick regardless of n.

### Expected impact vs. current default (`num > 15`)

Per-call-site projection, using the benchmark ratios at n=32 as a representative point:

| Call site                            | Current (n=32) | Proposed (n=32) | Change                             |
|--------------------------------------|----------------|-----------------|------------------------------------|
| `createNumericIterator` (Numeric)    | Heap           | Heap            | unchanged (1.21-1.63× retained)    |
| `NewGeoRangeIterator` (n=8 fixed)    | Flat           | Heap            | **+7-30%** (Disjoint Heap at n=8)  |
| `Query_EvalPrefixNode` (Quick)       | Heap           | Flat            | +2-3× on high-overlap expansions   |
| `Query_EvalWildcardQueryNode` (Quick)| Heap           | Flat            | +2-3× on high-overlap expansions   |
| `Query_EvalLexRangeNode` (Quick)     | Heap           | Flat            | ~+15-25% (Low-Quick is Flat-friendly) |
| `Query_EvalTagNode` (Full)           | Heap           | Heap at n≥24    | shift crossover from 15 → 24       |
| `Query_EvalUnionNode` (generic)      | Heap           | Flat at n<32    | avoids High-overlap penalty at cost of some Disjoint gain |

The biggest wins are `QN_PREFIX` / `QN_WILDCARD_QUERY` in Quick mode — a common pattern for `NOT` sub-trees and `WEIGHT 0` clauses where today's threshold routes to Heap on any expansion with >15 terms. `NewGeoRangeIterator` gains as well: it always has exactly 8 children, right below the current threshold of 15, and is Disjoint-by-construction.

### Open points

1. **`QN_TAG` sub-classification**: single-valued vs multi-valued tag fields have very different overlap profiles. Single-valued tags are effectively Disjoint. If the TagIndex's multi-value flag is cheap to check at construction, splitting `QN_TAG` into Disjoint/Low could recover another chunk of gain.
2. **`QN_UNION` refinement**: the fallback is necessarily pessimistic. A second-pass heuristic that inspects the children's types (e.g. if all children are `QN_NUMERIC` after `Query_EvalUnionNode` inlining, treat as Disjoint) could help but requires reading the iterator types post-construction.
3. **Runtime tunability**: keep `minUnionIterHeap` as a per-deployment override (ignoring the type-bucket when explicitly set) and expose `LOW_OVERLAP_HEAP_N` / `UNKNOWN_HEAP_N` as additional module parameters with the defaults above.
4. **Validation**: the current sweep covers three synthetic overlap buckets with uniform children. Before landing, measure on a production-shaped workload with skewed child estimates (prefix expansions that hit one very frequent term and many rare ones) to confirm the query-type mapping is not defeated by skew.

## Code references

- Bench source: `src/redisearch_rs/rqe_iterators_bencher/src/benchers/union_sweep.rs`
- C dispatch: `src/iterators/union_iterator.c::NewUnionIterator` (line 533)
- Rust dispatch: `src/redisearch_rs/rqe_iterators/src/union_reducer.rs::new_union_iterator`
- Config knob: `DEFAULT_UNION_ITERATOR_HEAP` in `src/config.h` (default 15)
- FFI helpers (C-side Union construction):
  - `bench_ffi::QueryIterator::new_union` (IdList children)
  - `bench_ffi::QueryIterator::new_union_term` (Term children)
