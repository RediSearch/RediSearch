# Trie Benchmarks

A set of microbenchmarks for the trie data structure.
It currently compares:
- the C implementation from `deps/triemap.c`
- the Rust implementation from `trie_rs`
- the Rust radix trie implementation from the `radix_trie` crate (Rust baseline)

## Building

In order to benchmark the C implementation, you need to build the `libtrie.a`
static library first.
It's enough to run `make build` in the root directory of the repository.

## Memory Usage

The binary entrypoint can be used to measure memory usage for a set of representative documents.

Execute it via:

```bash
cargo run --release
```

You should see output similar to the following:

```text
Statistics:
- Raw text size: 0.558 MBs
- Number of words (with duplicates): 103366
- Number of unique words: 15520
- Rust -> 1.070 MBs
          23130 nodes
- C    -> 0.640 MBs
          23129 nodes
```

## Performance

Run

```bash
cargo bench
```

to execute all micro-benchmarks.
To run a subset of benchmarks, pass the name of the benchmark as an argument after `--`:

```bash
# Run all microbenchmarks that include "Remove" in their names
cargo bench -- Remove
```

On top of the terminal output, you can also explore the more detailed HTML report in your browser:

```bash
open ../../../bin/redisearch_rs/criterion/report/index.html
```
