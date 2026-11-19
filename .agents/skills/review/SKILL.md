---
name: review
description: Adds this repository's own rules to the code review of a pull request - what is also a problem here, and what never is. Use when asked to review a change in this repository, or to change what the code review reports.
---
<!-- yours to edit. Copied once from the default template by outerlayer init 0.42.0; never overwritten. -->

Read ~/.agents/skills/outerlayer-review/SKILL.md and follow it from start to finish. Apply the rules below for this repository.

The code review of a pull request reads the two sections below, and nothing else in this file. It reads them from the branch the pull request merges into, so a changed rule applies after the change merges. The built-in review wins a conflict with a rule here.

Write one rule on each line, as a sentence a reviewer can check against a change. A rule under the second heading never hides a security problem or data loss.

## Also a problem here

A change to Rust breaks a house rule of `.skills/rust-review/SKILL.md`, `.skills/rust-tests-guidelines/SKILL.md` or `.skills/rust-docs-guidelines/SKILL.md`; the finding cites the rule's line.
A port changes C behavior that its issue's Contract pins, or behaves differently from the C without a Divergences entry in its issue.
A contract item that holds over a range of inputs, such as a conversion, a bound or an ordering, is pinned only by hand-picked values instead of a property test over generated inputs.
A property test restates the implementation's expression instead of an invariant such as a round trip, an ordering or agreement with a simpler model.
A panic is reachable from the safe API with a value a constructor or parser accepts: an `assert!`, `expect`, `unwrap`, indexing, or a call into a helper that panics.
At the FFI boundary, a pointer is not handled correctly for each of initialization (a reference made to memory that may be uninitialized), aliasing (a reference that overlaps another live one or outlives its target) and ownership (who frees it, with which allocator, exactly once).
A port adds code the stage does not need: an abstraction, indirection, file or test a smaller port would not have; the finding names what to delete.
A test is named for its artifact (contract, ABI, boundary, compat) instead of the behavior it asserts.
A Rust crate allocates through `RedisModule_Alloc`, `RedisModule_Calloc` or `RedisModule_Realloc` instead of the global allocator, or adds a `#[global_allocator]`.
A new crate's `lib.rs` lacks the lint block `.skills/port-c-module/SKILL.md` gives.
A change breaks a build script, a lint exclusion or a CI path that the changed files are part of.

## Never a problem here

A port deletes C code, or C tests that only exercised the deleted C ABI, once their behavior is covered by Rust tests named in the issue.
A port's generated header under `src/redisearch_rs/headers/` changes as `make generate-rust-headers` writes it.
