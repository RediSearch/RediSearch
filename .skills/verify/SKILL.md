---
name: verify
description: Run full verification before committing or creating a PR. Use this when you want to create a PR.
---

# Verify Skill

Run full verification before committing or creating a PR.

## Usage
Use this skill to run comprehensive checks before finalizing changes.

## Prefer the swamp workflows

`verify` runs everything below except the two conditional branches, in one command,
building once and reusing that build for every suite — plus miri, which CI gates on:

swamp is optional here: check `command -v swamp` first, and follow the by-hand
path below if it is not installed. Its files live under `swamp/`, and swamp only
looks *upward* for them, so these commands need `--repo-dir swamp` from the
repository root — or the export below, once per shell. See *Where swamp lives in
this repository* in `AGENTS.md`.

```bash
export SWAMP_REPO_DIR="$PWD/swamp"
swamp workflow run verify
swamp report get @gdesmott/failure-digest --workflow verify --markdown   # what to fix
```

The cluster branch is its own workflow, because it is a second full run of the suite and
swamp cannot skip a step on a condition:

```bash
swamp workflow run verify-cluster   # changes under src/coord/
```

The AddressSanitizer branch is also a workflow, but it is not part of the gate. CI covers
asan on every pull request, so run it only to reproduce a failure that job reported —
narrowed to the case it named, since the sanitizer needs its own full build:

```bash
swamp workflow run verify-asan --input '{"cTestFilter":"<binary or gtest>"}'
```

For the quick paths at the bottom of this file, use `swamp workflow run rust-quick`
(tests then clippy, debug profile only).

Each workflow asserts that its suites actually ran, so a filter that matched nothing or a
module that failed to load reads as a failure rather than a pass. Follow the steps below
by hand only when swamp is unavailable.

One check cannot live inside the workflow, and has to be run beside it whenever the
change touches `swamp/`:

```bash
make swamp-definitions-check    # validates swamp/models/ and swamp/workflows/
```

`verify` runs the extension tests, but not this. The definition check shells out to
`swamp` to validate and evaluate every checked-in model instance and workflow, and a
swamp workflow cannot invoke swamp — which is why `verify`'s step deliberately runs
`make swamp-extension-tests` rather than `make swamp-tests`. So a change that breaks
only a definition — a guard naming an input that does not exist, a step naming a model
that does not — passes `verify` and fails the `swamp-tests` job in CI, which is the one
outcome a pre-PR gate exists to rule out. `make swamp-tests` runs both halves, and is the
single command to use when swamp/ changed.

## Instructions

Determine which code was modified (C, Rust, or both) and run the appropriate checks.

### If C code was modified

Run the following checks in order:

#### 1. Build
```bash
./build.sh
```
Ensure the full project compiles without warnings promoted to errors.

#### 2. C/C++ Unit Tests
```bash
./build.sh RUN_UNIT_TESTS ENABLE_ASSERT=1
```
All unit tests must pass. Use [/run-c-unit-tests](../run-c-unit-tests/SKILL.md) for details
on running specific tests.

#### 3. Behavioral Tests
```bash
./build.sh RUN_PYTEST ENABLE_ASSERT=1
```
Required for changes to command handlers, query execution, indexing pipeline, or RDB serialization.

#### 4. AddressSanitizer (recommended for memory-related changes)
```bash
./build.sh RUN_UNIT_TESTS SAN=address
```

If the change claims to fix a memory-safety bug, also follow
*Memory-safety fixes (either language)* below.

#### 5. Coordinator Tests (if `coord/` code was modified)

Changes to the coordinator (`src/coord/`), distributed hybrid (`src/coord/hybrid/`), or
the Map-Reduce layer (`src/coord/rmr/`) must be tested in a clustered environment:

```bash
./build.sh RUN_PYTEST ENABLE_ASSERT=1 REDIS_STANDALONE=0 SHARDS=3
```

This spins up a 3-shard cluster and runs the full test suite against it.

### If Rust code was modified

#### 1. Format Check
```bash
make fmt CHECK=1
```
If it fails, run `make fmt` to fix formatting.

#### 2. Lint Check
```bash
make lint
```
Fix any clippy warnings or errors.

#### 3. Build
```bash
./build.sh
```
Ensure the full project compiles.

#### 4. Rust Tests
```bash
cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml
```
All Rust tests must pass.

If the change claims to fix a memory-safety bug, also follow
*Memory-safety fixes (either language)* below.

### If both C and Rust were modified

Run all checks from both sections above.

### Memory-safety fixes (either language)

When the change claims to fix a memory-safety bug (out-of-bounds access, use-after-free),
demonstrate the bug once on the pre-fix code with an *executed* sanitizer repro —
a predicted report is not verification. Expect the sanitizer report on the pre-fix code
and a clean run on the fixed one.

For C, the cheap form needs no ASan build tree: extract the affected file at the pre-fix
revision and compile a standalone repro against it, e.g.

```bash
git show HEAD:src/<file>.c > /tmp/before_fix.c           # fix not yet committed (the usual case)
git show <fix-commit>^:src/<file>.c > /tmp/before_fix.c  # fix already committed
clang -fsanitize=address -g -O0 <repro.c> /tmp/before_fix.c -o repro && ./repro
```

Most `src/` files won't compile standalone as-is: expect to add `-I` paths and stub out
heavyweight includes (`rm_malloc`, `RS_ABORT`, module headers) with minimal definitions
in the repro's own directory. That stubbing is usually minutes of work, not hours.

The repro must build against a consistent pre-fix baseline. If the fix also touches a
header, macro, or inline helper the extracted file depends on, extract those at the same
pre-fix revision — compiling a pre-fix `.c` against post-fix headers tests a mixed
program that can hide the bug or fail for an unrelated reason. When the dependency set
grows past a few files, a worktree checked out at the pre-fix revision is simpler.

For Rust, run the failing case on the pre-fix code under Miri
(`cargo +nightly miri test --manifest-path src/redisearch_rs/Cargo.toml -p <crate>`);
for bugs Miri cannot reach (FFI, foreign memory), use an ASan build of the affected test.
Single-file extraction does not work here — cargo builds the whole crate — and the
current checkout already contains the fix, so a Miri run in it only shows the *fixed*
code is clean. Check out a pre-fix tree first:

```bash
git worktree add --detach /tmp/prefix_tree HEAD           # fix not yet committed (the usual case)
git worktree add --detach /tmp/prefix_tree <fix-commit>^  # fix already committed
```

If the failing test was added alongside the fix, copy the test (and only the test) into
that tree. Run Miri or ASan with `--manifest-path` pointing at the worktree, expect the
failure there and a clean run in the fixed checkout, then clean up with
`git worktree remove --force /tmp/prefix_tree`.

### Behavioral Tests (for significant changes in either language)
```bash
./build.sh RUN_PYTEST ENABLE_ASSERT=1
```

## Quick Verification

For minor Rust-only changes (subshell keeps the chain readable — one cwd hop, three commands):
```bash
(cd src/redisearch_rs && cargo fmt --check && cargo clippy --all-targets && cargo nextest run)
```

For minor C-only changes:
```bash
./build.sh && ./build.sh RUN_UNIT_TESTS ENABLE_ASSERT=1
```
