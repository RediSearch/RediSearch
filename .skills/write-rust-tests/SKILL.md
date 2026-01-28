---
name: write-rust-tests
description: Write Rust tests to verify correctness of Rust code.
---

# Write Rust Tests

Write new Rust tests for Rust code.

## Arguments
- `<path>`: Path to the Rust crate or file.
- `<path 1> <path 2>`: Multiple crate/file paths.

If a path doesn't include `src/`, assume it to be in the `src/redisearch_rs` directory. E.g. `numeric_range_tree` becomes `src/redisearch_rs/numeric_range_tree`.
If a path points to a directory, consider all Rust files in that directory.

## Guidelines

The generated tests must follow the guidelines outlined in [/rust-tests-guidelines](../rust-tests-guidelines/SKILL.md).

## What to test

Ensure that all public APIs are tested thoroughly, including edge cases, error conditions and branches.
Use [`/check-rust-coverage`](../check-rust-coverage/SKILL.md) to determine which lines are not covered by tests.
