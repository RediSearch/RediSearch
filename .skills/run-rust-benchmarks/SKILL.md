---
name: run-rust-benchmarks
description: Run Rust benchmarks against the implementation being replaced. Use for C-to-Rust migrations or performance comparisons of alternative Rust implementations.
---

# Rust Benchmarks Skill

Run Rust benchmarks against the relevant C or Rust baseline. Use the
[performance comparison guidance](../port-c-module/references/validation.md#define-acceptance-before-evaluating-results)
to select equivalent workloads, include caller adaptation costs, and distinguish
microbenchmark evidence from production performance.

## Arguments
- `<crate>`: Run the given benchmark crate (e.g., `/run-rust-benchmarks rqe_iterators_bencher`)
- `<crate> <bench>`: Run specific bench in a benchmark crate (e.g., `/run-rust-benchmarks rqe_iterators_bencher "Iterator - InvertedIndex - Numeric - Read Dense"`)

Arguments provided: `$ARGUMENTS`

## Instructions

1. Check the arguments provided above:
   - If a crate name is provided, run benchmarks for that crate:
     ```bash
     cargo bench --manifest-path src/redisearch_rs/Cargo.toml -p <crate_name>
     ```
   - If both crate and bench name are provided, run the specific bench:
     ```bash
     cargo bench --manifest-path src/redisearch_rs/Cargo.toml -p <crate_name> <bench_name>
     ```
2. Run the planned repetitions and save their output. If displayed output is truncated, read the saved log rather than rerunning merely to recover output.
3. Summarize timings and variability by operation and workload against the pinned baseline. Include regressions and inconclusive results; do not infer an overall winner from an unweighted average.
4. Ask the user: "Would you like a copyable markdown version for your PR description?" If yes, re-emit the summary table inside a fenced ` ```markdown ` code block so it can be pasted directly.

## Common Benchmark Commands

```bash
# Bench given crate
cargo bench --manifest-path src/redisearch_rs/Cargo.toml -p rqe_iterators_bencher
cargo bench --manifest-path src/redisearch_rs/Cargo.toml -p inverted_index_bencher

# Run a specific benchmark
cargo bench --manifest-path src/redisearch_rs/Cargo.toml -p rqe_iterators_bencher "Iterator - InvertedIndex - Numeric - Read Dense"
```
