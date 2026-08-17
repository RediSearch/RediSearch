# Trie Benchmarks

A set of microbenchmarks for the Rust trie map implementation in `trie_rs`.

Originally these benchmarks compared the Rust port against the C implementation
in `deps/triemap.c`. The C implementation was removed in
[#6087](https://github.com/RediSearch/RediSearch/pull/6087); the suite is now
kept as a regression gate for `trie_rs`.

## Memory Usage

The binary entrypoint can be used to measure memory usage for a set of representative documents.

Execute it via:

```bash
cargo run --release
```

You should see output similar to the following:

```text
Statistics:
- Raw text size: 0.114 MBs
- Number of unique words: 15524
- Memory 0.469 MBs
- 18944 nodes
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
