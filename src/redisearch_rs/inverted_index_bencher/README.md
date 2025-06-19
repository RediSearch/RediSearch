# Inverted Index Benchmarks

A set of microbenchmarks for the inverted index.
It currently compares:
- the C implementation from `src/inverted_index/inverted_index.c`
- the Rust implementation from `inverted_index`

## Building

In order to benchmark the C implementation, you need to build the `libinverted_index.a`
static library first.
It's enough to run `./build.sh` in the root directory of the repository.

## Performance

Run

```bash
cargo bench
```

to execute all micro-benchmarks.
To run a subset of benchmarks, pass the name of the benchmark as an argument after `--`:

```bash
# Run all microbenchmarks that include "PosInt" in their names
cargo bench -- PosInt
```

On top of the terminal output, you can also explore the more detailed HTML report in your browser:

```bash
open ../../../bin/redisearch_rs/criterion/report/index.html
```
