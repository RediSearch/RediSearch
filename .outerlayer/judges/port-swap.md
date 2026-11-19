---
name: Port swap completeness
description: A swap exports every C symbol from Rust under its own name, deletes the C module and leaves no test-only C ABI.
when:
  paths: ["src/**"]
required: true
---
Read `issue.md`. Unless it has the line `Port stage: swap`, pass with the reason "not a swap issue" and stop.

Judge the pushed commit in `tree/` against the issue's `Symbols` and `Module`. Fail, with one item per problem and refs, when:

1. A `Symbols` entry's Rust item is not defined in `tree/`: for `crate::Type::method`, the crate whose `Cargo.toml` has `name = "crate"` must define `Type` and an `impl` block of it that defines `fn method`; for `crate::function`, a file of that crate must define it.
2. No file under `tree/src/redisearch_rs/headers/` declares the `Symbols` entry's C function under its exact name.
3. A `Module` file still exists in `tree/`.
4. A function marked `#[unsafe(no_mangle)]` that this diff adds, or whose last caller this diff deletes, is called by no C or Rust file outside tests (`tests/`, `benches/`, `#[cfg(test)]`), generated headers and comments. Such an export keeps C ABI alive only for tests; its tests belong on the Rust API.
5. The diff adds Rust outside `src/redisearch_rs/c_entrypoint/`, `ffi/`, `c_wrappers/` and `redis_mock/`, other than tests, and the issue's `Port checks` has no `mutants` check.
