---
name: run-rust-tests
description: Run Rust tests after making changes to verify correctness. Use this when you want to verify your changes to Rust code.
---

# Rust Test Skill

Run Rust tests after making changes to verify correctness.

## Arguments
- No arguments: Analyze changes and run tests for affected crates only
- `all`: Run all Rust tests
- `<crate>`: Run tests for specific crate (e.g., `/run-rust-tests hyperloglog`)
- `<crate> <test>`: Run specific test in crate (e.g., `/run-rust-tests hyperloglog test_merge`)

Arguments provided: `$ARGUMENTS`

## Usage
Run this skill after modifying Rust code to ensure tests pass.

## Instructions

1. Check the arguments provided above:
   - If arguments are empty, determine affected crates:
     1. Check which files were modified in `src/redisearch_rs/` using `git status` and `git diff --name-only`
     2. Map each modified file to its crate (the directory name directly under `src/redisearch_rs/`, e.g., `src/redisearch_rs/hyperloglog/src/lib.rs` → `hyperloglog`)
     3. Run tests for each affected crate:
        ```bash
        cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml -p <crate1> -p <crate2> ...
        ```
     4. If no Rust files were modified in `src/redisearch_rs/`, or if you cannot determine affected crates, run all tests
   - If `all` is provided, run all Rust tests:
     ```bash
     cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml
     ```
   - If a crate name is provided, run tests for that crate:
     ```bash
     cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml -p <crate_name>
     ```
   - If both crate and test name are provided, run the specific test:
     ```bash
     cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml -p <crate_name> <test_name>
     ```
2. If tests fail:
   - Read the error output carefully
   - Fix the failing tests or the code causing failures
   - Re-run tests to verify the fix

## Common Test Commands

```bash
# Test specific crate
cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml -p hyperloglog
cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml -p inverted_index
cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml -p trie_rs

# Run a specific test
cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml -p <crate_name> <test_name>

# Run tests under miri (for undefined behavior detection)
cargo +nightly miri test --manifest-path src/redisearch_rs/Cargo.toml
```
