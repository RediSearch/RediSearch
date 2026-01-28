---
name: write-rust-tests
description: Write Rust tests to verify correctness of Rust code.
---

# Write Rust Tests

Guidelines for writing new tests for Rust code.

## Guidelines

1. Test against the public API of the code under test.
2. Test private APIs if and only if the private component is highly complex and difficult to test through the public API.
3. Use `insta` whenever you are testing output that is difficult to predict or compare.
4. Where appropriate, use `proptest` to add property-based tests for key invariants.
5. Testing code should be written with the same care reserved to production code.
   Avoid unnecessary duplication, introduce helpers to reduce boilerplate and ensure readability.
   The intent of a test should be obvious or, if not possible, clearly documented.

## Code organization

1. Put tests under the `tests` directory of the relevant crate if they don't rely on private APIs.
2. The `tests` directory should be organized as a crate, with a `main.rs` file and all tests in modules. 
   Refer to the `trie_rs/tests` directory as an example.
3. If the test *must* rely on private APIs, co-locate them with the code they test, using a `#[cfg(test)]` module.

## What to test

Ensure that all public APIs are tested thoroughly, including edge cases, error conditions and branches.
Use test coverage to determine which parts of the code are not covered by tests:

```bash
cd src/redisearch_rs/<my_crate> && cargo llvm-cov test --quiet --json --output-path coverage.json --show-missing-lines
```

Don't leave `coverage.json` around after the analysis.

## Dealing with extern C symbols

Check out [CONTRIBUTING.md](../../src/redisearch_rs/CONTRIBUTING.md) for instructions on how to deal with
undefined C symbols in Rust tests.
