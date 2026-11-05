---
name: port-c-module
description: Guide for porting a C module to Rust. Use this when starting to port a C module to Rust.
disable-model-invocation: true
---

# Port Module Skill

Guide for porting a C module to Rust.

## Arguments
The module name to port should be provided as an argument (e.g., `/port-module triemap`).

Module to port: `$ARGUMENTS`

## Usage
Use this skill when starting to port a C module to Rust.

## Instructions

### 1. Analyze the C Code
First, understand the C module you're porting (look for `$ARGUMENTS.c` and `$ARGUMENTS.h` in `src/`):
- Read the `.c` and `.h` files in `src/`
- Identify what is exposed by the header file:
  - Does the rest of the codebase have access to the inner fields of the data structures defined in this module?
  - Are types always passed by value or by reference? A mix?
  - Can the corresponding Rust types be passed over the FFI boundary?
- Understand data structures and their lifetimes
- Identify which types and functions this module imports from other modules:
  - Determine if those types are implemented in Rust or C
  - If those types are implemented in Rust, identify the relevant Rust crate
  - If those types are implemented in C, understand if it makes sense to port them first to Rust
    or if it's preferable to invoke the C implementation from Rust via FFI
- Note any global state or Redis module interactions
- Identify which tests under `tests/` are relevant to this module
- Measure how much of the module the Python flow tests cover with
  [`/check-flow-coverage`](../check-flow-coverage/SKILL.md), and close the gaps with
  [`/write-flow-tests`](../write-flow-tests/SKILL.md) before porting. The port is only as safe as
  the tests that pin the C, and writing them often surfaces bugs in the C itself

### 2. Define A Porting Plan

Outline the steps and decisions for porting the module. The plan is working material, not part
of the change: keep it in the conversation or your tooling, and never commit a plan, notes, or
PR-draft file.
Determine if the C code should be modified, at this stage, to ease the porting process.
For example:
- Introduce getters and setters to avoid exposing inner fields of data structures defined in this module.
- Split the module into smaller, more manageable parts.

#### Contract and divergences

The most important part of the plan separates what must stay the same from what may change.

- **Contract:** the behavior callers rely on — replies and error texts users see, persisted
  formats, and the C API semantics other modules depend on. List each item with the test that
  pins it, existing or to be written. Contract tests go through the production path (Python
  flow tests or existing C/C++ tests), and are written while C still serves production, so
  they first pin the C and later prove the Rust.
- **Divergences:** every place the Rust will behave differently from the C — what C does, what
  Rust does, and why. Behavior outside the contract is incidental: the state of output
  arguments after an error, how out-of-range enum values or sentinels are handled, a panic
  that becomes an error. Port it idiomatically rather than replicating it, and list each such
  change so a human reviewer can accept or reject it. The divergences belong in the PR
  description and commit message.

Map every contract item to tests named for the behavior they assert, never for the contract
they belong to, as [`/rust-tests-guidelines`](../rust-tests-guidelines/SKILL.md) describes: in
Rust, and in the flow tests wherever users can see the behavior. The test names then read as the
contract, such as `invalid_unit_is_reported_before_coordinates_and_radius`: a reviewer can tell
from the names alone what each item guarantees, and which test fails when it breaks.

A contract item that holds over a range of inputs, such as a conversion, a bound, an ordering or
a round trip, is pinned by a property test over generated inputs, not by a few hand-picked
values. Keep example tests for the boundaries and for regressions.

#### Two PRs

A port ships as two stacked PRs, each small enough to review:

1. **Rust:** tests pinning the C behavior the existing tests miss, the pure Rust implementation
   and its tests, and the benchmark against C. Production still runs the C.
2. **Swap:** the FFI crate exporting the Rust under the C functions' names, production callers
   switched to it, and the module's C files deleted, headers included. The first PR's tests now
   exercise the Rust. Whatever else the headers define that other code shares (structs, enums,
   constants) moves to Rust in this PR, exported through a generated header; plan that move,
   because every user of those types changes with it.

When closing the coverage gaps of step 1 takes more than a few tests, or they surface C bugs
worth fixing first, land them as a PR of their own ahead of the two.

#### Who the port affects

Each port is one step in a migration carried out module by module, by a team that maintains the
result for years. Before each design choice, ask who else it affects:

- **Reviewers** read every line, and their attention is the migration's scarcest resource:
  every file touched is one more to open, understand and approve.
- **Others work in the same code:** changes spread across many files and crates collide with
  work in flight. A narrow change lands sooner.
- **Redis users depend on the behavior:** a divergence changes what applications see, and a
  memory-safety mistake in the FFI crashes the server and can lose data.
- **Later ports copy this one:** an abstraction, test layout or workaround becomes precedent.

Prefer the change the codebase's own maintainers would make: the smallest one that does the job
completely.

### 3. Create the Rust Crate
```bash
cargo new src/redisearch_rs/$ARGUMENTS --lib
```

Every crate a port creates, this one and the FFI crate, opens its `lib.rs` with the lints the
workspace does not enable yet, so `make lint` enforces them for the new code:

```rust
#![warn(
    clippy::unwrap_used,
    clippy::dbg_macro,
    clippy::todo,
    clippy::unimplemented,
    clippy::print_stdout,
    clippy::panic,
    clippy::unnecessary_safety_comment,
    clippy::ptr_as_ptr,
    clippy::borrow_as_ptr,
    clippy::ref_as_ptr,
    clippy::indexing_slicing
)]
```

`src/redisearch_rs/clippy.toml` allows `unwrap`, `panic` and indexing in tests.

### 4. Implement Pure Rust Logic
- Create idiomatic Rust code: model the domain in Rust types rather than mirroring the C
  structs, enums and sentinels one to one
- Keep the crate pure: no C ABI types, `#[repr(C)]` layouts for C, `extern "C"` items or
  `#[no_mangle]` exports. They belong to the FFI crate (step 6), and so do Rust tests of the
  C boundary
- Porting is also cleanup: legacy C carries cruft, and a port may simplify where the result is
  smaller and no caller relies on the old shape, such as a null-pointer fallback no caller
  uses or a length limit that only existed for a C stack buffer. Keep it measured: a
  simplification that grows the diff noticeably or changes user-visible behavior beyond a
  listed divergence is out of scope
- Test each fact once, at the lowest layer that can observe it:
  - logic through the safe Rust API, following [`/rust-tests-guidelines`](../rust-tests-guidelines/SKILL.md);
  - each FFI function at most once, and only where it does more than forward;
  - the contract end to end, through the production path.

  Do not add a C/C++ test suite that re-checks what the Rust tests already check. One overlap
  is expected: until the swap, the flow tests exercise C, so in the Rust PR a Rust test of a
  contract behavior is the only coverage of the new implementation.
- Document public APIs with doc comments
- For performance sensitive code, benchmark Rust against C with `criterion` in a
  `<module>_bencher` crate, written while the C still exists: its build script links C through
  `build_utils::bind_foreign_c_symbols`, and each benchmark is a group with a `c` function
  calling the C through the generated `ffi` bindings and a `rust` function calling the Rust
  API. Never copy C sources into the benchmark or rebuild C from git history.
- Pin contract items that hold over a range of inputs with property tests, as _Contract and
  divergences_ requires
- Testing code should be written with the same care reserved to production code

### 5. Compare Rust API with C API
- Review the public API of the new Rust module against the C API in the header file
- Ensure that differences can be bridged by adding appropriate wrappers or adapters
- Go back to step 1 if discovered differences cannot be bridged without a re-design

### 6. Create FFI Wrapper
Create an FFI crate to expose the new Rust module to the C codebase:
```bash
cargo new src/redisearch_rs/c_entrypoint/${ARGUMENTS}_ffi --lib
```

FFI crate should:
- Expose `#[unsafe(no_mangle)] pub extern "C" fn` functions under the C functions' own names,
  never prefixed with a language: the FFI crate is linked in the same change that deletes the
  C definitions, so the two never meet in one binary and C callers keep their calls
- State preconditions as a numbered `# Safety` list and check the cheap ones with named
  assertion helpers, as [`/rust-review`](../rust-review/SKILL.md) § 3a and § 3c describe; do
  not add runtime checks for states the contract rules out
- Convert between C and Rust types safely, in one place per type
- Document all unsafe blocks with `// SAFETY:` comments that discharge the preconditions by
  number (§ 3b)
- Follow the house rules in [`/rust-review`](../rust-review/SKILL.md) § 3k: `expect` over
  `unwrap`, pointer methods over `*p`, `Box` over `RedisModule_Alloc`, the `c_wrappers/` crates
  over raw `ffi::` calls
- Export only what production C calls: a symbol only C/C++ tests use is not exported, and those
  tests change with it
- Ask three separate questions of every pointer, because one answer does not imply another.
  Initialization: an output parameter may point at uninitialized memory, so write it with
  `ptr.write`, never through a `&mut` made from it. Aliasing: may it overlap another argument?
  Ownership: who frees it, with which allocator, exactly once?
- Keep Rust tests of the C boundary in this crate, not in the pure crate or another crate's
  tests

### 7. Wire Up C Code
- Delete the C header file and its implementation
- Move whatever else the header defined that other code shares (structs, enums, constants) to
  Rust, exported through the generated header; every user of those types changes with it, so
  plan the move
- Update the rest of the C codebase to import the new Rust header wherever the old C header was used

C header files for Rust FFI crates are auto-generated. No need to use their full path in imports,
use just their name (e.g. `#include $ARGUMENTS.h;` for `${ARGUMENTS}_ffi`)

### 8. Test The Integration
```bash
./build.sh RUN_UNIT_TESTS               # C/C++ unit tests
./build.sh RUN_PYTEST                   # Integration tests
```

### 9. Verify as CI Does

A CI round takes nearly an hour, so catch locally what it would:

- **Miri:** run the tests of every crate the port adds or changes (`-p` each one), because CI runs
  Miri over the whole workspace (`make rust-tests RUN_MIRI=1`, which builds the C side first).
  Tests that link the C bundle need it built and `BINDIR` pointing at it, so run `./build.sh`
  first and export the `BINDIR` it reports, then
  `cd src/redisearch_rs && cargo +nightly miri nextest run -p <crate> -p <crate>_ffi`. Proptest
  and tests that call into C cannot run under Miri: mark them `#[cfg(not(miri))]` as the
  codebase does, and compare floats with a tolerance where Miri's rounding may differ. A test
  nextest reports as `SLOW` locally will time out on CI's slower runners: shrink its inputs.
  Never change CI's Miri configuration or timeouts to make a test pass.
- **Mutation testing:** run `cargo mutants` over the port's diff, so code no test exercises shows
  up. Run it in place, because the `ffi` crate's build script reads headers outside the Cargo
  workspace, and keep cargo's own test runner (with `--test-tool nextest` the baseline fails on
  a crate whose tests live in another package):

  ```sh
  work=$(mktemp -d) && cd src/redisearch_rs \
    && git diff --relative=src/redisearch_rs <base> HEAD > "$work/port.diff" \
    && cargo mutants --in-place -p <crate> --test-package <crate with its tests> --in-diff "$work/port.diff" --output "$work"
  ```

  Tests that link the C bundle need `BINDIR` pointing at it. A mutant no test can catch because
  it changes nothing observable goes into the command as `--exclude-re '<mutant>'`. cargo
  mutants skips `unsafe extern` functions, so FFI exports need Miri and boundary tests instead.
- **Linking:** once C calls the Rust exports, every Rust test binary that links the C bundle needs
  the FFI crate that defines them. Those binaries name the FFI crates they need as
  `extern crate …_ffi as _;` lines (`git grep 'extern crate .*_ffi as _'` finds them); add yours
  beside the others. macOS resolves symbols lazily, so a missing one links locally and fails only
  on CI's Linux linker: check those lists by reading them, not by a passing local build.
- **Lint:** `make lint`, then `cargo hakari generate --diff` and
  `cargo hakari manage-deps --dry-run` from `src/redisearch_rs`, and `make license-check`, as
  CI's lint job does: a new crate needs its `workspace_hack` dependency, and the files
  `cargo new` creates lack the license header (`cargo license-fix` from `src/redisearch_rs` adds
  it). Only a `<module>_bencher` crate belongs in `build.sh`'s
  `EXCLUDE_RUST_BENCHING_CRATES_LINKING_C`, which also drops it from `make lint` and coverage; a
  crate whose tests link C uses the `unittest` feature pattern in
  `src/redisearch_rs/CONTRIBUTING.md` instead.

### 10. Keep the Diff Small

Before opening each PR, read the diff as its reviewer would and delete what neither the contract
nor a listed divergence needs; touch as few files as the port allows. Never buy size by cramming
code onto fewer lines (`make lint` formats it), by dropping documentation the lints require, or by
deleting tests.

## Example: Well-Ported Module
See `src/redisearch_rs/trie_rs/` for a high-quality example:
- Pure Rust implementation with comprehensive docs
- Extensive test coverage
- Clean FFI boundary in `c_entrypoint/trie_ffi/`
