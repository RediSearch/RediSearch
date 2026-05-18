# RediSearch Development Guide

RediSearch is a Redis module providing full-text search, secondary indexing, and vector similarity search.
The codebase is primarily C, with an ongoing effort to port modules to Rust in `src/redisearch_rs/`.

## Build Commands

```bash
./build.sh                    # Full build (C + Rust)
./build.sh DEBUG=1            # Debug build (recommended for development)
./build.sh FORCE              # Rebuild discarding previous artifacts
```

## Testing

```bash
./build.sh RUN_UNIT_TESTS                     # C/C++ unit tests
./build.sh RUN_UNIT_TESTS TEST=unit_test_name # Specific C/C++ unit tests
./build.sh RUN_UNIT_TESTS SAN=address         # C/C++ unit tests with AddressSanitizer
./build.sh RUN_PYTEST                         # Python behavioral tests
./build.sh RUN_PYTEST TEST=test_name          # Specific Python test
cargo nextest run                             # Rust tests, from `src/redisearch_rs/`
cargo +nightly miri test                      # Rust tests under `miri`, from `src/redisearch_rs/`
```

Run Rust tests from workspace root (`src/redisearch_rs/`):
```bash
cd src/redisearch_rs && cargo nextest run
cd src/redisearch_rs && cargo nextest run -p <crate_name>
```

## Linting & Formatting

```bash
make lint                                 # Run clippy and cargo doc checks
make fmt                                  # Format all code
make fmt CHECK=1                          # Check formatting without changes
cd src/redisearch_rs && cargo license-fix # Add missing license headers
```

C code formatting is governed by `.clang-format` at the repo root (LLVM-derived, 100-column limit, 2-space indent). Apply with `clang-format -i <file>`.

## Code Style

### C

- `.clang-format` is the authoritative formatting spec; run `clang-format` before committing C changes
- 2-space indentation, 100-character line limit, attached braces (`BreakBeforeBraces: Attach`)
- Pointer alignment: left (`int* p;`)
- No trailing spaces, no tabs (`UseTab: Never`)
- **Memory management**: use `rm_malloc` / `rm_free` / `rm_calloc` / `rm_realloc` (wrappers around `RedisModule_Alloc/Free/Realloc`). Never use raw `malloc`/`free` in module code.
- **Error handling**: functions return `int` status codes (`REDISMODULE_OK` / `REDISMODULE_ERR`). Use `goto cleanup` pattern for resource cleanup on error paths.
- **Naming**: `ModuleName_FunctionName` for public functions (e.g., `DocTable_GetById`), `static` helper functions use lowercase or camelCase. Struct types use `PascalCase` or `t_typeName`.
- **Header guards**: `#ifndef MODULENAME_H__` / `#define MODULENAME_H__` / `#endif`
- **Logging**: use `RedisModule_Log(ctx, level, fmt, ...)` with levels `"debug"`, `"verbose"`, `"notice"`, `"warning"`.
- **Assertions**: use `RS_LOG_ASSERT` from `deps/rmutil/rm_assert.h` for debug-only assertions.

### Rust
- Edition 2024
- Document all `unsafe` blocks with `// SAFETY:` comments
- Use `#[expect(...)]` over `#[allow(...)]` for lint suppressions
- Use `tracing` macros for logging (debug!, info!, warn!, error!)

## C Code Architecture

### Module Entry and Command Dispatch
- `src/module-init/module-init.c` — `RedisModule_OnLoad`, calls `RediSearch_InitModuleInternal`
- `src/module.c` — command registration and top-level handlers for `FT.CREATE`, `FT.SEARCH`, `FT.AGGREGATE`, `FT.INFO`, etc.

### Indexing Pipeline
- `src/indexer.c` — background indexing queue
- `src/forward_index.c` — per-document forward index built during indexing
- `src/doc_table.c` — document metadata table (id mapping, flags, scores)
- `src/redis_index.c` — Redis keyspace integration for index storage
- `src/field_spec.c` — field type definitions and schema
- `src/spec.c` — index spec lifecycle (create, drop, alter)
- `src/document.c`, `src/document_add.c` — document add/update/delete pipeline
- `src/rdb.c` — RDB serialization/deserialization for all index types
- `src/notifications.c` — keyspace notification callbacks (index/update documents on hash/JSON writes)

### Query Engine
- `src/query.c` — query execution entry point
- `src/query_optimizer.c` — query plan optimization
- `src/query_parser/v2/` — Ragel lexer (`lexer.rl`) + Lemon parser (`parser.y`), used by DIALECT 2 onwards (v1 is legacy)
- `src/iterators/` — iterator implementations (hybrid_reader, optimizer_reader)
- `src/result_processor.c` — result processing pipeline
- `src/numeric_filter.c` — numeric range filter iterators
- `src/cursor.c` — cursor-based result pagination

### Aggregation
- `src/aggregate/aggregate_request.c` — aggregate command parsing
- `src/aggregate/aggregate_plan.c` — execution plan construction
- `src/aggregate/aggregate_exec.c` — pipeline execution
- `src/aggregate/group_by.c`, `src/aggregate/reducer.c` — GROUP BY and reducers
- `src/aggregate/expr/` — expression evaluation
- `src/aggregate/functions/` — built-in aggregate functions

### Hybrid (Vector + Text) Search
- `src/hybrid/hybrid_exec.c` — hybrid query execution
- `src/hybrid/hybrid_request.c` — hybrid query parsing
- `src/hybrid/hybrid_scoring.c` — combined scoring

### Garbage Collection
- `src/fork_gc/fork_gc.c` — fork-based GC (main orchestrator, also triggers tiered vector index GC)
- `src/fork_gc/terms.c`, `tags.c`, `numeric.c` — per-index-type GC for inverted indexes
- `src/fork_gc/existing_docs.c`, `missing_docs.c` — document-level GC
- `src/gc.c`, `src/gc.h` — GC interface and scheduling
- Vector (tiered) indexes use VecSim's own GC, called from the fork GC cycle
- Geometry indexes remove entries inline on document deletion (no deferred GC)

### Specialized Indexes
- `src/geo_index.c` — geographic index
- `src/tag_index.c` — tag (exact-match) index
- `src/vector_index.c` — vector similarity index (wraps VectorSimilarity lib)
- `src/geometry/` — GEOSHAPE index type for WKT points and polygons (C++ API, R-tree)

### Config, Debug, Profile
- `src/config.c` / `src/config.h` — runtime configuration (`FT.CONFIG SET/GET`)
- `src/debug_commands.c` — `FT.DEBUG` subcommands for introspection
- `src/profile/` — `FT.PROFILE` query profiling
- `src/info/` — `FT.INFO` implementation and field stats

### Coordinator (Cluster)
- `src/coord/` — distributed search (separate CMake sub-project)
- `src/coord/rmr/` — Redis Map-Reduce layer (fan-out commands to shards, reduce replies)
- `src/coord/dist_aggregate.c` — distributed aggregate execution

### Utilities
- `src/util/` — logging, memory helpers, arrays, hash, workers, misc
- `src/concurrent_ctx.c` — concurrent search context (thread handoff)
- `src/buffer/buffer.c` — Redis String DMA buffer implementation

### Key Dependencies
- `deps/VectorSimilarity/` — vector index backends (HNSW, flat, etc.)
- `deps/snowball/` — stemming algorithms (git submodule)
- `deps/friso/` — Chinese tokenization
- `deps/phonetics/` — phonetic matching
- `deps/rmutil/` — Redis module utility helpers
- `deps/googletest/` — Google Test/Mock library (used by `tests/cpptests/`)

### Test Organization
- `tests/pytests/` — Python integration tests (RLTest framework)
- `tests/cpptests/` — C++ unit tests (Google Test → `rstest` binary)
- `tests/ctests/` — C unit tests (standalone binaries)
- `tests/benchmarks/` — YAML-driven benchmark configs

## Build System

- The top-level `CMakeLists.txt` promotes specific warnings to errors with compiler-specific flags (gcc vs clang) guarded by `check_c_compiler_flag()`. These propagate to all subdirectories including deps.
- When overriding a compiler flag (e.g. `-Wno-error=X` for a dep), always use the same compiler guard as the original flag, or a `$<C_COMPILER_ID:...>` generator expression. Never add bare `-W*` flags without a compiler check.
- Core C sources are collected via `file(GLOB SOURCES ...)` in root `CMakeLists.txt`.
- The coordinator build (`src/coord/CMakeLists.txt`) is a standalone CMake project that reuses core sources.

## Project Structure

```
src/                          # C source code
├── aggregate/                # FT.AGGREGATE pipeline
├── fork_gc/                  # Fork-based garbage collection
├── hybrid/                   # Hybrid (vector+text) search
├── iterators/                # Query iterator implementations
├── info/                     # FT.INFO implementation
├── profile/                  # FT.PROFILE implementation
├── module-init/              # RedisModule_OnLoad entry point
├── query_parser/v2/          # Ragel lexer + Lemon parser
├── geometry/                 # Geometry index (C++)
├── util/                     # Shared utilities
└── redisearch_rs/            # Rust codebase
    ├── ffi/                  # Rust bindings for C types and functions
    ├── headers/              # Autogenerated C headers for *_ffi crates
    ├── c_entrypoint/         # FFI layer (C bindings for Rust types)
    │   └── *_ffi/            # Per-module FFI crates
    ├── c_wrappers/           # Idiomatic Rust APIs on top of C types
    └── Cargo.toml            # Workspace root

src/coord/                    # Coordinator (cluster) build
tests/                        # All tests (pytests, cpptests, ctests, benchmarks)
deps/                         # Vendored dependencies
docs/                         # User-facing and internal documentation
```

## C to Rust Porting Patterns

### FFI Bridge Pattern
Each ported module has a corresponding `*_ffi` crate in `c_entrypoint/`:
```
src/redisearch_rs/
├── trie_rs/              # Pure Rust implementation
└── c_entrypoint/
    └── triemap_ffi/      # C-callable wrapper
```

## Common Workflows

### C Code
Invoke [/code-review](.skills/code-review/SKILL.md) to review C code changes or PRs.
Invoke [/run-c-unit-tests](.skills/run-c-unit-tests/SKILL.md) to run C/C++ unit tests.
Invoke [/pr-backport](.skills/pr-backport/SKILL.md) to backport a PR to a release branch.
Invoke [/run-python-tests](.skills/run-python-tests/SKILL.md) to run end-to-end behavioral tests.

### Rust Code
Follow [/rust-docs-guidelines](.skills/rust-docs-guidelines/SKILL.md) when writing documentation for Rust code.
Invoke [/port-c-module](.skills/port-c-module/SKILL.md) to plan the porting of a C module.
Invoke [/write-rust-tests](.skills/write-rust-tests/SKILL.md) to add tests to Rust code.
Invoke [/rust-review](.skills/rust-review/SKILL.md) to review Rust code changes.

### General
Invoke [/report-flaky-test](.skills/report-flaky-test/SKILL.md) to report a flaky CI test to Jira or update an existing flaky-test ticket.
Invoke [/investigate-flaky-test](.skills/investigate-flaky-test/SKILL.md) to investigate a flaky-test report and propose an evidence-backed fix.
Invoke [/verify](.skills/verify/SKILL.md) to verify the correctness of your work before wrapping up.
Invoke [/build](.skills/build/SKILL.md) to compile and verify the build.
Invoke [/lint](.skills/lint/SKILL.md) to check code quality and formatting.
Invoke [/jj-fix-conflicts](.skills/jj-fix-conflicts/SKILL.md) to resolve conflicts in jj changes.

## Pull Request Description (Required)

When creating a PR, include the following checkboxes from the PR template
(exactly one must be checked — CI enforces this):

```
- [x] This PR requires release notes
- [ ] This PR does not require release notes
```

Check "requires" for user-facing changes (new commands, behavior changes, bug fixes,
performance improvements). Check "does not require" for internal-only changes
(refactoring, CI, tests, documentation).

## Pull Request Workflow

- Once a branch has an open pull request, do not amend, rebase, squash, or force-push it unless the user explicitly asks for history rewriting.
- Address review feedback with normal follow-up commits and regular pushes.
- Before opening a pull request, history cleanup is acceptable when it is useful and does not discard user work.
- When opening a pull request, use `.github/PULL_REQUEST_TEMPLATE.md` for the description and keep all template sections.
- For normal PRs to `master` or another primary target branch, use the title format `[MOD-xyz] concise user-facing summary` when a Jira ticket exists. If no ticket is known, ask the user whether one should be opened before choosing the title.
- For backport PRs, use the title format `[x.y] original title`, where `x.y` is the target branch. In the PR description, link back to the original PR.
- If release notes are required, make sure the title describes the user impact as requested by the PR template.

## License Header (Required)
```
/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
```
