---
name: run-rust-benchmarks
description: Run Rust benchmarks and compare performance with the C implementation. Use this when you work on migrating C code to Rust and want to ensure performance is not regressed.
---

# Rust Benchmarks Skill

Run Rust benchmarks and compare performance with the C implementation.

## Arguments
- `<crate>`: Run the given benchmark crate (e.g., `/run-rust-benchmarks rqe_iterators_bencher`)
- `<crate> <bench>`: Run specific bench in a benchmakr crate (e.g., `/run-rust-benchmarks rqe_iterators_bencher "Iterator - InvertedIndex - Numeric - Read Dense"`)

Arguments provided: `$ARGUMENTS`

## Instructions

1. Check the arguments provided above:
   - If a crate name is provided, run benchmarks for that crate:
     ```bash
     cd src/redisearch_rs && cargo bench -p <crate_name>
     ```
   - If both crate and bench name are provided, run the specific bench:
     ```bash
     cd src/redisearch_rs && cargo bench -p <crate_name> <bench_name>
     ```
2. **Run benchmarks only once.** If the output is too large or truncated, extract the timing data from the saved output file rather than re-running the benchmarks.
3. Once the benchmarks are complete, generate a summary comparing the average run times between the Rust and C implementations.

## Common Benchmark Commands

```bash
# Bench given crate
cd src/redisearch_rs && cargo bench -p rqe_iterators_bencher
cd src/redisearch_rs && cargo bench -p inverted_index_bencher

# Run a specific benchmark
cd src/redisearch_rs && cargo bench -p rqe_iterators_bencher "Iterator - InvertedIndex - Numeric - Read Dense"
```
