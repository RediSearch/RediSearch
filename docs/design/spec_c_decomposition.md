# Proposal: Decompose `spec.c` into `spec` + `indexes` + `indexes_scan`

**Status:** proposal
**Author:** Joan Fontanals Martinez
**Branch:** `joan-spec-refactor-2`

## Summary

`src/spec.c` is ~4986 lines and tangles three distinct concerns. This proposal
splits it into three translation units with a **strictly one-directional
dependency graph** — no cyclic dependencies, no compatibility umbrella header.

This is the *bold* variant: we do **not** keep `spec.h` as an umbrella that
re-includes the new headers. Every consumer is updated to include the header it
actually needs, and the Rust FFI header generation is updated alongside. If the
FFI layer breaks, we fix it as part of the work rather than hiding the move
behind transitive includes.

The change is **almost entirely code movement** plus one behavior-preserving
hoist. No new behavior, no format changes.

## Motivation

`spec.c` currently mixes:

1. **`IndexSpec` lifecycle** — create / alter / free a single index spec, field
   wiring, GC and cursor setup.
2. **The indexes registry + keyspace dispatch** — everything that operates over
   the global `specDict_g`: RDB load/save, propagation, and the
   `Indexes_*MatchingWithSchemaRules` keyspace-notification dispatch.
3. **The background scanner / reindex subsystem** — `IndexesScanner`, the debug
   scanner, the scan procs, the reindex thread pool, and the full-rescan entry
   points.

These are independently reasoned-about subsystems with their own globals
(`specDict_g`, `global_spec_scanner`, `reindexPool`). Keeping them in one file
forces every reader to hold all three in their head and makes the
scanner ↔ spec call relationship circular.

## Target file layout

| File | Owns | Key globals |
|------|------|-------------|
| `src/spec.{c,h}` | `IndexSpec` lifecycle: `IndexSpec_CreateNew`, `IndexSpec_AddFields`, field/GC/cursor wiring, single-spec ops | — |
| `src/indexes.{c,h}` | Registry + keyspace dispatch over the global dict: `Indexes_RdbLoad/Save/Save2`, `Indexes_Propagate`, `Indexes_FindMatchingSchemaRules`, `Indexes_Update/Delete/ReplaceMatchingWithSchemaRules`, `Indexes_UpdateMatchingDocExpiration`, `Indexes_UpdateMatchingHashFieldExpiration`, `Indexes_Init/Count/List/Free`, RDB-loading-event + SST-replication functions | `specDict_g` |
| `src/indexes_scan.{c,h}` | Background scanner + reindex: `IndexesScanner_*`, `DebugIndexesScanner_*`, `Indexes_ScanProc`, `Indexes_ScanAndReindexTask`, `IndexSpec_ScanAndReindex`/`...Async`, `Indexes_ScanAndReindex`, `Indexes_UpgradeLegacyIndexes`, `ReindexPool_ThreadPoolDestroy`, OOM/sleep helpers, scanner structs | `global_spec_scanner`, `reindexPool` |

## The dependency graph (target)

```
                 spec.{c,h}            (IndexSpec core — depends on neither)
                  ▲        ▲
                  │        │
      indexes_scan.{c,h}   │           (reads specDict_g extern; calls IndexSpec_*)
                  ▲        │
                  │        │
              indexes.{c,h}            (owns specDict_g; calls scan triggers)
```

Allowed edges, all one-directional:

- `indexes_scan.c` → `spec.h` (uses `IndexSpec`, `StrongRef`, reindex helpers)
- `indexes_scan.c` → reads `extern dict *specDict_g` (declared in `indexes.h`)
- `indexes.c` → `spec.h`
- `indexes.c` → `indexes_scan.h` (RDB-load events trigger full rescans)
- `spec.c` → **nothing new** (this is what the hoist below buys us)

No `*.h` includes a header that (transitively) includes it back. No `.c` pair
calls into each other.

## The one cycle, and how we break it

There is exactly one function-call cycle today:

```
spec.c  IndexSpec_CreateNew  (src/spec.c:614)  ─┐
spec.c  IndexSpec_AddFields  (src/spec.c:1703) ─┼─► IndexSpec_ScanAndReindex ──► (scanner) ──► IndexSpec_* (back into spec.c)
                                                ─┘
```

**Break it by hoisting the scan trigger to the callers.** Both call sites live in
`module.c` and already hold the context and the spec, so the guard moves with
the call cleanly:

- `src/spec.c:614` — `IndexSpec_CreateNew` calls
  `IndexSpec_ScanAndReindex` when `!(sp->flags & Index_SkipInitialScan)`.
  Move this to the caller at `src/module.c:626`.
- `src/spec.c:1703` — `IndexSpec_AddFields` calls
  `IndexSpec_ScanAndReindex` when `rc && initialScan`.
  Move this to the caller at `src/module.c:975`.
- `src/module.c:844` already calls `IndexSpec_ScanAndReindex` directly — no
  change; this is the existing precedent that makes the hoist idiomatic.

After the hoist, `IndexSpec_CreateNew` / `IndexSpec_AddFields` no longer know
about scanning, so `spec.c` has no edge into the scanner.

The apparent `indexes.c` ↔ `indexes_scan.c` cycle is **not** a function-call
cycle: `Indexes_ScanAndReindex` / `Indexes_UpgradeLegacyIndexes` only *read*
`specDict_g` (a shared extern owned by `indexes.c`), while the only function
calls go `indexes.c` → `indexes_scan.c`. A data dependency on an extern is not a
layering violation.

## No umbrella header — explicit consumer fix-up

`spec.h` will **not** re-include `indexes.h` / `indexes_scan.h`. Every file that
referenced a moved symbol gets its include list updated. Known consumers to
touch (from current `grep`):

- Scanner symbols (`IndexesScanner`, `global_spec_scanner`,
  `IndexesScanner_IndexedPercent`, `DebugIndexScannerCode`,
  `ReindexPool_ThreadPoolDestroy`): `src/module.c`, `src/debug_commands.c`,
  `src/info/info_command.c` → add `#include "indexes_scan.h"`.
- `specDict_g` and `Indexes_*` registry functions: `src/module.c`,
  `src/rdb.c`, `src/search_disk.c`, `src/notifications.c`, `src/config.c`,
  `src/info/info_command.c`, `src/info/indexes_info.c`,
  `src/module-init/module-init.c` → add `#include "indexes.h"`.

### FFI impact — expected, and in scope

`specDict_g` and the scanner types are reachable by the Rust FFI build
(`src/redisearch_rs/ffi/build.rs`,
`src/redisearch_rs/rqe_iterators_test_utils/src/test_context.rs`). Moving them
out of `spec.h` **may** break bindgen / `cheadergen` input paths.

We accept this risk: the FFI build configuration will be updated to point at the
new headers as needed, and `make generate-rust-headers` + a Rust build will be
run to confirm. We do **not** hide the move behind transitive includes to avoid
this — if it breaks, we see it and fix it.

## Commit sequence

Each commit builds, lints, and passes tests on its own.

1. **Hoist `IndexSpec_ScanAndReindex` to callers.** Move the two calls into
   `module.c` with their guards; update any test callers of
   `IndexSpec_CreateNew` / `IndexSpec_AddFields`. No new files. This is the
   keystone — it removes the spec → scanner edge and is verifiable in isolation.
2. **Extract `indexes_scan.{c,h}`.** Move the scanner family + `reindexPool` +
   scanner struct definitions. Forward-declare `IndexSpec` / `StrongRef` in the
   header; include `spec.h` from the `.c`. Update scanner-symbol consumers.
3. **Extract `indexes.{c,h}`.** Move the `Indexes_*` registry/dispatch family +
   the `specDict_g` definition. `indexes.c` includes `indexes_scan.h` for the
   rescan triggers. Update registry-symbol consumers.
4. **Fix the FFI.** Update `ffi/build.rs` / header-gen inputs; regenerate
   headers; build Rust + run affected Rust tests.

Use `git`'s rename/move detection (`-M`/`-C`, `git log --follow`) so reviewers
see the diffs as moves rather than delete+add.

## Verification

- `./build.sh DEBUG=1` after each commit.
- `./build.sh RUN_UNIT_TESTS` and a targeted `RUN_PYTEST` over index
  create/alter/drop, RDB save/load, and reindex flows.
- `make lint` / `make fmt CHECK=1`.
- `make generate-rust-headers` then a Rust build + `cargo nextest run` for FFI.

## Risks & non-goals

- **Risk:** FFI header inputs break. *Mitigation:* explicit in scope (commit 4).
- **Risk:** a hidden caller of `CreateNew`/`AddFields` relied on the implicit
  scan. *Mitigation:* `grep` shows only `module.c`; sweep tests in commit 1.
- **Non-goal:** changing scanner behavior, RDB format, or the keyspace-dispatch
  logic. This is movement + one hoist only.
