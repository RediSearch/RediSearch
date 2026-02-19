---
name: write-rust-tests
description: Write Rust tests to verify correctness of Rust code. Use this when you want to write Rust tests.
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

## Avoiding redundant tests

Before writing each test, explicitly identify which branch or code path it will cover that no existing test already covers. An uncovered line is not sufficient justification — ask *why* it is uncovered and whether it is reachable through an already-tested entry point.

Two tests are redundant if they exercise the same set of branches in the code under test. Differing only in input values that don't change control flow is not a distinct scenario.

Do not write standalone tests for:
- **Trivial trait delegations** — `Default`, `From`, or similar trait impls that are single-line delegations to an already-tested constructor, since they will be covered transitively.

After adding tests, double check that every new test covers at least one branch that no other test (existing or new) covers. Remove any that don't.
