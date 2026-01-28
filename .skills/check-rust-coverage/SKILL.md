---
name: check-rust-coverage
description: Check which Rust lines are not covered by Rust tests.
---

# Check Rust Coverage

Determine which Rust lines are not covered by Rust tests.

## Arguments
- `<path>`: Path to the Rust crate or file.
- `<path 1> <path 2>`: Multiple crate/file paths.

If a path doesn't include `src/`, assume it to be in the `src/redisearch_rs` directory. E.g. `numeric_range_tree` becomes `src/redisearch_rs/numeric_range_tree`.
If a path points to a directory, consider all Rust files in that directory.

## Instructions 

Run

```bash
cd <crate directory> && cargo llvm-cov test --quiet --json --output-path coverage.json --show-missing-lines
```

E.g. 

```bash
cd src/redisearch_rs/trie_rs && cargo llvm-cov test --quiet --json --output-path coverage.json --show-missing-lines
```

Don't leave `coverage.json` around after the analysis.
