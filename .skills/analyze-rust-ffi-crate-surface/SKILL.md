---
name: analyze-rust-ffi-crate-surface
description: Determine which parts of the C codebase use Rust-defined C symbols.
---

# Analyze Rust FFI Crate Surface

Compile a list of all C-visible symbols defined in a given Rust FFI crate or file (e.g. an `extern "C" fn` annotated with `#[unsafe(no_mangle)]` or a type definition).
Then determine which parts of the C codebase use these symbols.

## Arguments
- `<path>`: Path to the Rust crate or file.
- `<path 1> <path 2>`: Multiple Rust crates/files.

If the path doesn't start with `src/`, assume it to be in the `src/redisearch_rs/c_entrypoint` directory. E.g. `numeric_range_tree_ffi` becomes `src/redisearch_rs/numeric_range_tree_ffi`.
If the path points to a directory, review the documentation of all Rust files in that directory.

## Instructions

- Read the relevant Rust source files.
- Compile a list of all the FFI symbols defined they expose (e.g. `extern "C" fn` annotated with `#[unsafe(no_mangle)]` or type definitions).
  You can use the corresponding auto-generated header file in `src/redisearch_rs/headers`, if it helps.
- For each symbol, determine which modules in the C codebase use it:
  - For functions, look for calls to the function in the C codebase.
  - For types, check out if they are used as function arguments, field types, or in type casts.

Emit a report that lists, for each symbol, the following information:
- The symbol name.
- The module(s) in the C codebase that use it.
- The type(s) of the symbol (function, type, etc.).
- If it's only used in C/C++ unit tests (i.e. under `tests/)

## Auto-generated header files

Each `*_ffi` Rust crate has a corresponding auto-generated header file in `src/redisearch_rs/headers`, created by the `build.rs` script via `cbindgen`.
The auto-generated header file includes all the FFI symbols defined by the Rust crate, no matter the sub-module they are defined in.
