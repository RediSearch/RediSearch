---
name: rust-tests-guidelines
description: Guidelines for writing Rust tests. Use this when you want to write Rust tests.
---

# Guidelines for Writing Rust Tests

Guidelines for writing new tests for Rust code.

## Guidelines

1. Test against the public API of the code under test.
2. Test private APIs if and only if the private component is highly complex and difficult to test through the public API.
3. Use `insta` whenever you are testing output that is difficult to predict or compare.
4. Where appropriate, use `proptest` to add property-based tests for key invariants.
5. Testing code should be written with the same care reserved to production code.
   Avoid unnecessary duplication, introduce helpers to reduce boilerplate and ensure readability.
   The intent of a test should be obvious or, if not possible, clearly documented.
6. Do not reference exact line numbers in comments, as they may change over time.
7. Test each fact once. A test named for one behavior asserts that behavior, not a mix of
   parsing, defaults and edge cases.
8. For tables of cases, name each case: a slice of structs or `(name, input, expected)`
   tuples, not parallel arrays, bare booleans, or `if` branches inside the shared loop.
9. Test the code, not the language or its dependencies: not `thiserror`'s `Display`, enum
   discriminants, or float casts.
10. Do not pin a bug or a panic on user input with a test. Fix it, or leave it out and say so
    in the PR.
11. Name a test for the behavior it asserts (`nan_observations_are_ignored`), never for the
    artifact it belongs to (`contract`, `boundary`, `compat`). Let the name and the code say
    what is tested; add one short comment only where neither makes the behavior plain. A
    test whose behavior needs more than a sentence to describe tests several things: split it.
12. Do not write leak- or double-free-counting allocators or similar instrumentation for tests
    Miri runs: it already catches leaks, double frees and use-after-free in them. Only a test
    that cannot run under Miri, such as one calling into C, may count allocations to check
    ownership across the boundary.

## Code organization

1. Put tests for public (`pub`) items under the crate's `tests` directory. Two layouts are
   in use and both are fine — **match whichever the crate already has**:
   - **`tests/integration/`** — one test crate with its own `main.rs` and a module per
     area (`trie_rs`, `geo`, `query_eval`, …). Prefer this for a new crate: it compiles as
     a single unit instead of one binary per file.
   - **Cargo's default layout** — one integration binary per `tests/*.rs` file (`varint`,
     `fork_gc`, `rlookup`, …).
2. If the test *must* rely on private APIs, co-locate it with the code it tests, using a
   `#[cfg(test)]` module. Integration tests cannot reach `pub(crate)` or private items,
   so this is the only option for them — but prefer exercising the behavior through the
   public API where you can, per guideline 2 above.
3. Making an item `pub` solely so integration tests can reach it is acceptable. We own
   both the crate and its callers, so visibility hygiene is not worth a review round.

## Dealing with extern C symbols

Check out [CONTRIBUTING.md](../../src/redisearch_rs/CONTRIBUTING.md) for instructions on how to deal with
undefined C symbols in Rust tests.
