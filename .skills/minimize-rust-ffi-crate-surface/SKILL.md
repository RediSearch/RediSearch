---
name: minimize-rust-ffi-crate-surface
description: Remove Rust-defined C symbols that are either unused or only used in C/C++ unit tests.
---

# Minimize Rust FFI Crate Surface

Remove C symbols defined in a Rust FFI crate or file that are either unused or only used in C/C++ unit tests.

## Arguments
- `<path>`: Path to the Rust crate or file.
- `<path 1> <path 2>`: Multiple Rust crates/files.

If the path doesn't start with `src/`, assume it to be in the `src/redisearch_rs/c_entrypoint` directory. E.g. `numeric_range_tree_ffi` becomes `src/redisearch_rs/numeric_range_tree_ffi`.
If the path points to a directory, review the documentation of all Rust files in that directory.

## Instructions

- Use [analyze-rust-ffi-crate-surface](../analyze-rust-ffi-crate-surface/SKILL.md) to enumerate and analyze the usage of all the FFI symbols exposed by the Rust crate or file (e.g. `extern "C" fn` annotated with `#[unsafe(no_mangle)]` or type definitions).
- For each unused symbol:
  - Delete its Rust definition.
  - Run C/C++ unit tests to ensure the symbol was indeed unused (via `./build.sh RUN_UNIT_TESTS`)
- For each symbol that is only used in C/C++ unit tests, elaborate a plan to either:
  - Refactor the C/C++ unit tests not to use it.
  - Remove the C/C++ unit tests (or assertions) that rely on it, since they are prying into the implementation details of the Rust crate.
  - Keep the symbol, but mark it as "test only" in the Rust documentation.
