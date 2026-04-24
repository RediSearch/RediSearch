# Porting Fork GC to Rust

| | |
|---|---|
| **Author** | Dax Huiberts |
| **Date** | 2026-04-18 |
| **Status** | Draft |

## TL;DR

Port RediSearch's Fork GC subsystem (~1500 LoC of C across `src/fork_gc/`) to Rust in 8 incremental phases. Each phase lands as a group of PRs; the module remains buildable and tested throughout. Final deliverable: all runtime logic in a new `src/redisearch_rs/fork_gc/` library crate behind a thin FFI trampoline in `src/redisearch_rs/c_entrypoint/fork_gc_ffi/`. **By the end of Phase 8 the files under `src/fork_gc/` are deleted** — no fork-GC logic remains in C. Dependencies that fork_gc *calls into* (Redis module APIs, inverted-index GC, numeric range tree, dict, trie/triemap, HiddenString, VecSim, etc.) stay in C and are invoked from Rust via `ffi`/cbindgen bindings, but the orchestrator, pipe protocol, scanners, struct definition, and stats helpers all move to Rust. All existing `FGC_*` C symbols remain exported at the same names so no caller in the rest of the C codebase has to change.

## Background

Fork GC is the garbage collector for RediSearch's inverted-index-based data structures. When documents are deleted or updated, their entries in the inverted indexes aren't removed eagerly — tombstones are left behind. Fork GC periodically reclaims that space by scanning the indexes in a forked child process (using `fork(2)` + copy-on-write for a consistent snapshot without blocking queries), streaming deltas to the parent over a pipe, which applies them to the live indexes.

Three roughly-independent layers:

1. **Orchestrator** (`fork_gc.c`) — the `periodicCb` timer callback that forks the child, manages the fork/pipe lifecycle, and the pause state machine used by tests.
2. **Pipe protocol** (`fork_gc/pipe.c`) — send/recv primitives for variable-length frames and sentinels. The narrow waist between child and parent.
3. **Per-kind scanners** (`fork_gc/{terms,numeric,tags,missing_docs,existing_docs}.c`) — delta collection (child-side) and application (parent-side) for each index kind.

## Goals

- Port **all** of Fork GC's runtime logic to Rust behind the existing `FGC_*` FFI symbols.
- Preserve observable behavior: error codes, `FT.INFO` statistics, existing pytest expectations.
- Unit-test the protocol primitives with in-memory writers/readers, not real pipes.

## Non-goals

- Not changing the overall architecture (fork-based vs. cleanup thread — separate discussion).
- Not refactoring per-kind scanner semantics (what gets collected, how deltas are encoded).

## Strategy: direct incremental replacement

This project uses a **direct-replacement** approach rather than the parallel-implementation / big-bang-swap pattern used on some earlier migrations.

**What that means concretely:**

- Each change ports a small piece at a time. Gradually replacing C implementations with Rust implementations.
- At no point does a parallel "Rust-flavour" of a piece of functionality which is not activecoexist with the live C version which is not active.
- There is no final migration / swap-over step. The module is always fully wired up; it just has progressively more Rust and less C with each merged PR.

**Why prefer this over parallel-then-swap for Fork GC:**

- **No parallel implementation.** In the off case that changes or bugfixes are being made to the C version, no extra care have to be taken to also make sure that this change is also applied in the parallel rust implementation. With direct replacement, each port is the last definition of that function; there's nothing to keep in sync.
- **Continuous validation.** Because the module is never in a "half-built" state with dormant Rust code, the full test suite (C/C++ unit tests + pytests + Rust tests) runs against the real shipping module after every merge, not just at the swap.
- **Clear blast radius on rollback.** If a PR introduces a regression, reverting that one PR puts that piece of code back in C without disturbing any other port.

**Constraints this imposes:**

- Compatibility with existing C structs and function interfaces dursing phased rollout. With the help of the FFI layer crates and Rust wrapper structs around C structs this is quite workable.
- Preserve Wire-format compatibility between C and Rust at any intermediate point during phased rollout.

## Proposed architecture

Mirrors the established `*_ffi` pattern already used by `inverted_index_ffi`, `numeric_range_tree_ffi`, etc.

### `src/redisearch_rs/fork_gc/` — library crate

Pure Rust. Owns all algorithmic logic:

- `ForkGC` — the struct itself. During the port it starts as a `#[repr(transparent)]` wrapper around `ffi::ForkGC` (so the same struct is accessible from both C and Rust while scanners are being migrated one at a time); once all fields are Rust-managed, the struct body moves to Rust and the C side retains only a forward declaration (`typedef struct ForkGC ForkGC;`) — see Phase 7. External callers only hold `ForkGC *` as an opaque handle returned by `FGC_Create`, so they're unaffected by the layout migration.
- Pipe protocol — used for communication between main process and gc process.
- Gradually, scanners for each kind.

### `src/redisearch_rs/c_entrypoint/fork_gc_ffi/` — FFI trampoline

Thin layer exposing `FGC_*` C symbols via `#[no_mangle] extern "C"`. Each trampoline:

1. Converts raw C pointer → `&mut ForkGC`.
2. Builds safe `&[u8]` / `&mut [u8]` from `(ptr, len)` pairs.
3. Calls into `fork_gc`.
4. Maps `Result` → `REDISMODULE_OK` / `REDISMODULE_ERR` where applicable.

No business logic in this layer. cbindgen generates `src/redisearch_rs/headers/fork_gc_rs.h`, included from `src/fork_gc.h` so C callers pick up the prototypes without changes.

### Bindgen (transitional)

During Phases 1–6, add `src/fork_gc.h` to the `ffi` crate's header list so Rust sees `struct ForkGC` and can read/write its fields directly while C code still does the same. Transitive includes (`gc.h`, `util/references.h`) are modest; no fragile types. Once the struct is fully Rust-native (Phase 7), this entry is removed and `fork_gc.h` holds only the forward declaration.

## Phasing

### Phase 1 — Scaffolding + pipe protocol

Groups together the initial setup and all pipe-level primitives into one phase. These pieces are tightly related (everything touches the same `PipeWriter` / `PipeReader` surface) and the pipe protocol has no cross-phase risks on its own — the wire format is determined entirely by the existing C, and the unit tests here don't need any of the scanner-side machinery to be meaningful. Shipping them together lets reviews focus on the protocol API shape once rather than across four mini-PRs.

This phase can still be broken into reviewable sub-commits along the old seams (scaffolding → send side → receive side → protocol helpers), but they merge as a single phase before scanner work starts.

**Scaffolding:**

- Create `fork_gc` and `fork_gc_ffi` crates with the minimum structure (Cargo.toml, lib.rs, cbindgen.toml).
- Add `fork_gc_ffi` as a dep of `c_entrypoint/redisearch_rs`.
- Add `src/fork_gc.h` to the `ffi` bindgen headers.
- Stand up the `ForkGC` wrapper with `from_ptr_mut` + `pipe_write` / `pipe_read` accessors (see *Pipe handle design*).

**Send-side primitives:**

- `FGC_sendFixed`, `FGC_sendBuffer`, `FGC_sendTerminator`.
- Introduce `PipeWriter` with `send_fixed`/`send_buffer`/`send_terminator` methods and `_or_exit` variants.
- Unit tests with `Vec<u8>` sinks asserting the wire format.

**Receive-side primitives:**

- `FGC_recvFixed`, `FGC_recvBuffer`.
- Introduce `PipeReader` with `recv_fixed`/`recv_buffer` methods, `RecvFrame` enum.
- `fn read_with_timeout<R: Read + AsRawFd>(…)` encapsulates the `poll(2)` + `EINTR` loop.
- Unit tests with `Cursor<Vec<u8>>` sources; round-trip tests against the send side.

**Protocol helpers:**

- `sendHeaderString` (uses `iovec` + `CTX_II_GC_Callback`; calls `FGC_sendBuffer` under the hood).
- `recvFieldHeader` (reads field name + `u64` id; decodes `RECV_BUFFER_EMPTY` → `FGCError::FGC_DONE`).
- `pipe_write_cb` / `pipe_read_cb` (II-GC callback glue).

**Dependency status:** No external Rust dependencies — this phase builds the foundation. Both new crates (`fork_gc`, `fork_gc_ffi`) are greenfield. Bindgen already covers the C types Phase 1 needs once `src/fork_gc.h` is added to the `ffi` crate's header list: `ForkGC` struct (pipe fds, stats, etc.), `FGCError` enum, and the `RECV_BUFFER_EMPTY` extern. No other subsystem has to move first. All C code currently in `src/fork_gc/pipe.c` ports to Rust in this phase — including `sendHeaderString`, `recvFieldHeader`, and the two II-GC callback trampolines `pipe_write_cb` / `pipe_read_cb`. The II-GC callback protocol (the `void (*)(void*, const void*, size_t)` shape and the `CTX_II_GC_Callback` context struct) is defined from the II side, so those signatures are fixed; the Rust trampolines register matching `extern "C"` functions and interpret the `void*` context via `ForkGC::from_ptr_mut`.

### Phases 2–6 — Per-kind scanners (preamble)

Each index kind has a child-side collector and a parent-side applier. The port handles one kind per phase.

### Phase 2 — Scanner: `existing_docs`

- `existing_docs.c`: `FGC_childCollectExistingDocs` + `FGC_parentHandleExistingDocs`.
- Simplest kind — tracks per-field doc-id sets, no term/value dimension.
- Serves as the template for the remaining scanner phases; extra care here on the scanner-module layout (one module per kind in `fork_gc/src/scanners/`) pays back in Phases 3–6.

**Dependency status:** Scanner walks `IndexSpec::existingDocs` (an `InvertedIndex*`) and calls the II GC protocol. `InvertedIndex_GcDelta_Scan` / `_Read` / `InvertedIndex_ApplyGcDelta` / `_NumDocs` / `_MemUsage` / `_Free` are **all already exposed** via `inverted_index_ffi`, together with the `II_GCWriter` / `II_GCReader` / `II_GCCallback` bridging structs (`inverted_index_ffi::fork_gc`). `IndexSpec::existingDocs` itself stays in C (it's `IndexSpec`'s field, not fork_gc's) but is read and nulled from Rust via the `ffi` bindgen crate. No API gaps — Phase 2 is purely a translation of the scanner's control flow into Rust.

### Phase 3 — Scanner: `missing_docs`

- `missing_docs.c`: `FGC_childCollectMissingDocs` + `FGC_parentHandleMissingDocs`.
- Structurally mirrors `existing_docs`; the two share most of the doc-id-tracking plumbing.
- Good second phase: if a shared helper for doc-set diffing wants to exist, this is where it emerges, by extraction from Phase 2's code rather than speculative abstraction up front.

**Dependency status:** Same II GC APIs as Phase 2 — already Rust-reachable. Additionally walks `IndexSpec::missingFieldDict` (a Redis `dict*`) and builds per-field `HiddenString` keys. The backing implementations (`dictIterator`, `dictNext`, `dictGetKey`, `dictGetVal`, `dictDelete`, `NewHiddenString`) live outside fork_gc and remain in C; the scanner calls them from Rust via `ffi`/bindgen as `unsafe extern "C"`. `c_wrappers/hidden_string` already wraps reads; it doesn't need to grow a `new` constructor for this port — direct bindgen call is fine. Not blocking; the Rust scanner code has a few more unsafe blocks around dict iteration than Phase 2 did.

### Phase 4 — Scanner: `numeric`

- `numeric.c`: `FGC_childCollectNumeric` + `FGC_parentHandleNumeric`.
- Backing structure (numeric range tree) already has a Rust wrapper in `numeric_range_tree_ffi` — the most Rust-reachable of the three index-backed kinds.

**Dependency status:** The complete scanner API is already available — `numeric_range_tree_ffi/src/gc.rs` exposes `NumericGcScanner_New` / `_Next` / `_Free` (streaming child-side walk yielding `NumericGcNodeEntry`), plus `NumericRangeTree_ApplyGcEntry` (parent-side apply with `ApplyGcEntryResult` enum) and `NumericRangeTree_CompactIfSparse` (conditional leaf trimming). `getFieldsByType` / `openNumericOrGeoIndex` / `IndexSpec_GetFieldWithLength` / spec locking all live in other subsystems (still C) and are called from the Rust scanner via `ffi`/bindgen. No blockers; the expected "FFI gap" risk for this phase has not materialized.

### Phase 5 — Scanner: `terms`

- `terms.c`: `FGC_childCollectTerms` + `FGC_parentHandleTerms`.
- Backing structure: per-term inverted indexes. Central to full-text search; the scanner's hot path.

**Dependency status:** The II GC side is already Rust-reachable (same APIs as Phase 2). The gap is the **trie** — the child walks all terms via `Trie_Iterate` / `TrieIterator_Next` / `TrieIterator_Free` and the parent calls `Trie_Delete` + `deleteSuffixTrie` to drop exhausted terms. None of these have Rust wrappers. A `trie_rs` crate exists but is a **separate Rust implementation** not bridged to the C trie owned by `IndexSpec`, so it doesn't help. The trie itself lives outside fork_gc and stays in C; the open question is how the Rust scanner calls it: (a) add a small `c_trie_ffi` wrapper crate exposing safe iteration / deletion — idiomatic, one-time cost amortized across Phase 6; (b) declare the C trie functions as `unsafe extern "C"` directly inside `fork_gc` via bindgen — minimal, more unsafe blocks in the scanner. Recommend (a); budget it into Phase 5's estimate.

### Phase 6 — Scanner: `tags`

- `tags.c`: `FGC_childCollectTags` + `FGC_parentHandleTags`.
- Backing structure: tag index (trie of tag values → per-value inverted indexes). Composes per-value II GC with a `TrieMap` walk.
- Deliberately last among the scanners: it inherits Phase 5's trie-wrapping pattern and extends it to `TrieMap`.

**Dependency status:** The per-value II GC side is Rust-reachable. The gap is the **tag index's `TrieMap`** — `TrieMap_Iterate` / `TrieMapIterator_Next` for the child walk and `TrieMap_Delete` (with a value-free callback) + `deleteSuffixTrieMap` on the parent. `triemap_ffi` exists but only exposes opaque-pointer handling, not an iteration API. `TagIndex_Open` / `TagIndex_OpenIndex` are callable from Rust (test code already does) but not officially wrapped. Same two options as Phase 5: extend `triemap_ffi` with safe iteration / deletion, or declare the C functions `unsafe extern "C"` inline. If Phase 5 went with option (a), the same crate-extension pattern extends here naturally.

### Phase 7 — Orchestrator + struct migration

- `periodicCb`, fork/pipe setup, pause state machine (`FGCPauseFlags`, `FGCState`).
- Most FFI-coupled piece: `RedisModule_Fork`, `RedisModule_KillForkChild`, GC callback registration.
- **Migrate `struct ForkGC`'s definition to Rust**. After all `FGC_*` functions are Rust-native, no in-crate C code reads the struct's fields anymore. Remove `fork_gc.h` from bindgen; replace the full struct definition in `fork_gc.h` with a forward declaration (`typedef struct ForkGC ForkGC;`). The Rust definition becomes authoritative. `FGC_Create` returns `*mut ForkGC` as before; the fields are no longer accessible from C translation units.

**Dependency status — struct fields:** every field migrates into the Rust-side struct. Equivalents:

| Field | C type | Rust-side target |
|---|---|---|
| `index` | `WeakRef` (util/references.h) | Raw `ffi::WeakRef` or a light newtype; promote/release via bindgen calls. |
| `ctx` | `RedisModuleCtx*` | Raw `*mut ffi::RedisModuleCtx`. Not owned by `ForkGC`. |
| `stats` | `ForkGCStats` (POD) | Rust struct with identical POD fields; cbindgen re-exports for stats callbacks. |
| `pipe_read_fd`, `pipe_write_fd` | `int` | `RawFd`. |
| `pollfd_read` | `struct pollfd[1]` | Dropped — the Rust `poll(2)` path constructs the `pollfd` on the stack per call (already true for `read_with_timeout`). |
| `pauseState`, `execState` | `volatile uint32_t` | `std::sync::atomic::AtomicU32`. |
| `retryInterval` | `struct timespec` | `std::time::Duration`. |
| `deletedOrUpdatedDocsFromLastRun` | `_Atomic(size_t)` | `std::sync::atomic::AtomicUsize`. |
| `cleanNumericEmptyNodes` | `int` | `bool`. |

The C `struct ForkGC` definition in `fork_gc.h` is replaced with a forward declaration (`typedef struct ForkGC ForkGC;`); the Rust definition is authoritative. Memory orderings on the atomic transitions match the C semantics (`Acquire` / `Release` around pause/exec, `Relaxed` for the counter bump) — document and unit-test them on the Rust side since the C comments are the only existing spec.

**Dependency status — orchestrator:** `periodicCb` ports to Rust. It calls `RedisModule_Fork` / `RedisModule_KillForkChild` / `RedisModule_ThreadSafeContextLock` / `_Unlock`, `isOutOfMemory`, `VecSim_CallTieredIndexesGC`, and `IndexsGlobalStats_DecreaseLogicallyDeleted`. None are wrapped in `redis_module` (the Rust crate) today, but all are visible in the `ffi` bindgen output — the Rust orchestrator invokes them via `unsafe extern "C"` directly. This isn't a new-wrapper effort, just usage. Safe helpers on top can be added incrementally (e.g. an RAII guard for `ThreadSafeContextLock` / `_Unlock`) but aren't gating.

**Accessors for the test harness.** `tests/cpptests/test_cpp_forkgc.cpp` reads `fgc->pauseState` and `fgc->stats.{totalCollected, gcBlocksDenied}` directly. After the struct is Rust-native, those field accesses won't compile. Export small `#[no_mangle] extern "C"` accessor functions from `fork_gc_ffi` (`FGC_getPauseState`, `FGC_getStats`) via cbindgen; update the C++ test to use them. This is the only concession to C-side consumers; it's a one-line-per-accessor change and doesn't leak any fork_gc logic back to C.

### Phase 8 — Cleanup

- `FGC_updateStats` (mutates `RedisSearchCtx` / `IndexSpec` stats).
- `ForkGCStats` reporting (`statsCb`, `statsForInfoCb`).
- Delete now-unused C files.

**Dependency status:** `FGC_updateStats` is a ~10-line helper that mutates `spec->stats.{numRecords, invertedSize}` (IndexSpec, external — called via bindgen) and `gc->stats.{totalCollected, gcBlocksDenied}` (now a Rust struct post-Phase-7). Ports to a Rust method on `ForkGC`. `statsCb` / `statsForInfoCb` become `#[no_mangle] extern "C" fn` in `fork_gc_ffi`, registered with the `GCCallbacks` vtable at `FGC_Create` time. `RECV_BUFFER_EMPTY` moves to a Rust `pub static` re-exported via cbindgen so remaining C callers (if any — `recvFieldHeader` was its only user and is ported in Phase 1) still see the same symbol.

After this phase, the files under `src/fork_gc/` (`fork_gc.c`, `pipe.c`, `terms.c`, `numeric.c`, `tags.c`, `missing_docs.c`, `existing_docs.c`) are **deleted**, and `fork_gc.h` keeps only the forward declaration plus the `#include "fork_gc_rs.h"` line. The `CMakeLists.txt` target drops those source entries.

## Key design decisions

### Pipe handle design: `ForkGC`, `PipeWriter`, `PipeReader`

Three layers cooperate to turn the C-owned pipe fds into testable, leak-safe Rust protocol code. Most of the Fork GC port threads through this seam, so it's worth understanding how the pieces compose.

**Layer 1 — `ForkGC` wrapper.** `#[repr(transparent)]` newtype around `ffi::ForkGC`, borrowed from a raw C pointer via `ForkGC::from_ptr_mut(ptr: *mut ffi::ForkGC) -> &'a mut Self`. The unsafe pointer-to-reference conversion lives at exactly one place — every FFI trampoline does `let fgc = unsafe { ForkGC::from_ptr_mut(fgc) }` on entry and from then on works in safe Rust. The wrapper adds no state; its only job is to own the two accessors below.

**Layer 2 — accessors `pipe_write` / `pipe_read`.** Both take `&mut self`, so the borrow checker rules out two writers, two readers, or a writer and reader coexisting on the same `ForkGC`. Return types are `PipeWriter<impl Write + '_>` and `PipeReader<impl Read + '_>` — the `'_` ties the returned handle to `self` (it cannot outlive the `ForkGC`), and `impl Trait` hides the locally-defined adapter type (see Layer 3) from the public API.

**Layer 3 — protocol (`PipeWriter<W: Write>` / `PipeReader<R: Read>`).** Pure algorithm over a generic `Write` / `Read`: frame format, length prefix, terminator sentinel, `RecvFrame` decoding. Nothing in this layer knows about file descriptors, `poll`, or Redis. Unit tests instantiate it with `Vec<u8>` (writer: assert wire bytes) and `Cursor<Vec<u8>>` (reader: round-trip against writer output). That's what makes the protocol testable without real pipes.

**The adapters — `FdWriter` / `FdReader`.** Nested inside `pipe_write` / `pipe_read` as local types (not exported). Each holds two things:

- `ManuallyDrop<File>` around the pipe fd pulled via `File::from_raw_fd(self.0.pipe_{write,read}_fd)`. `File` provides idiomatic `Write` / `Read` impls; `ManuallyDrop` suppresses `File::drop` so the fd — still owned by the C-side state machine — isn't closed when the adapter is dropped.
- `PhantomData<&'a mut ForkGC>` so the adapter carries the exclusive borrow of the `ForkGC`. `PipeWriter<W>` / `PipeReader<R>` are generic over the adapter, so the lifetime flows through: a `PipeWriter<FdWriter<'a>>` is transitively tied to `&'a mut ForkGC` even though `PipeWriter` itself has no lifetime parameter. Net effect: the borrow checker prevents two concurrent writers interleaving frames on the pipe and prevents a writer/reader from outliving the `ForkGC` — without `PipeWriter` / `PipeReader` needing to know about it.

`FdReader::read` additionally routes through `read_with_timeout(&mut *self.file, buf, POLL_TIMEOUT_MS)` (see *Poll loop* below) and logs timeout-vs-error on the way out. `FdWriter::write` just delegates to `File::write`.

**End-to-end example (send path):**

```
FGC_sendBuffer(fgc, buf, len)                          // FFI trampoline
  ├─ ForkGC::from_ptr_mut(fgc)         → &mut ForkGC
  ├─ fgc.pipe_write()                  → PipeWriter<FdWriter<'_>>  (borrows fgc)
  └─ writer.send_buffer_or_exit(slice)
       └─ PipeWriter::send_buffer      → self.writer.write_all(...)
            └─ FdWriter::write          → ManuallyDrop<File>::write → write(2)
```

The receive path is symmetric, with `read_with_timeout` handling `poll(2)` + `EINTR` retry inside `FdReader::read`, and `PipeReader::recv_buffer` returning a `RecvFrame` that the trampoline converts back into a `(ptr, len)` pair for C.

**Net effect:** the protocol layer is reusable and unit-testable; fd ownership, timeout, and borrow-discipline concerns are localized to adapters that only exist inside the accessor methods; and the unsafe surface shrinks to two raw-pointer conversions (`from_ptr_mut`, `File::from_raw_fd`), both documented and narrow.

### Wire format — native endian

Parent and child share the same process image post-fork (identical architecture, identical byte layout). `len.to_ne_bytes()` for `size_t` prefixes matches the C `FGC_SEND_VAR(fgc, len)` macro exactly. No endianness conversion required.

### Error handling: child vs. parent

- **Child side**: a broken pipe is unrecoverable. Log a warning, call `RedisModule_ExitFromChild(1)`. Modeled as `_or_exit(&mut self, …)` methods that internally log + exit on failure. Private `die_on_pipe_error(err: io::Error) -> !` helper.
- **Parent side**: log, return error, let upstream decide. Modeled as `-> io::Result<_>`. Detailed logging (timeout vs. error) happens inside the `fork_gc` crate; the FFI trampoline just maps to `REDISMODULE_ERR`.

Both use `redis_module::logging::log_warning` for log output. Minor format difference from C's `RedisModule_Log(ctx, ...)`: the Rust path uses a NULL context, which Redis renders without the module name prefix. Small visible diff; acceptable.

### Sentinel interop

`RECV_BUFFER_EMPTY` (defined in `pipe.c` as `(void *)0xdeadbeef`) is compared by **pointer identity** in C callers like `recvFieldHeader`. Rust must not redefine it — access via `unsafe extern "C" { static RECV_BUFFER_EMPTY: *mut c_void; }`. On decode, this becomes `RecvFrame::Terminator`; on encode for FFI, the trampoline reads the C-side static and passes it into `RecvFrame::into_c_buffer`.

### Poll loop

No "blocking read with timeout" syscall exists for pipe fds on Unix. `SO_RCVTIMEO` requires a socket; `SIGALRM` is racy. `poll(2)` with a single fd + 3-minute timeout is the idiomatic answer, encapsulated in a standalone `fn read_with_timeout<R: Read + AsRawFd>(…)` with internal `EINTR` retry.

Potential future simplification (**not** in scope): switch `pipe(2)` to `socketpair(AF_UNIX, SOCK_STREAM, …)` on the C side. Then `UnixStream::set_read_timeout` replaces all the manual poll plumbing. Large enough cross-language change to defer.

## Rollout

- All `FGC_*` C symbols stay exported at the same names throughout — external callers need no edits.
- After Phase 8, the C files under `src/fork_gc/` are deleted entirely; the CMake target's source list shrinks accordingly. `fork_gc.h` retains only the forward declaration and the cbindgen include.

## Estimated effort

Estimates assume the "no C left in fork_gc" constraint — i.e. Phase 7 ports `periodicCb` in full, not a partial struct migration. Cost drivers: scanner control-flow translation (all scanner phases), trie/triemap wrapping (Phases 5–6), and the orchestrator port (Phase 7).

- Phase 1 (scaffolding + pipe protocol): ~2 weeks — New `fork_gc` and `fork_gc_ffi` crates from scratch, plus the five pipe primitives and the protocol helpers. No external dependencies to unblock; pace is set by review cycles and nailing the `PipeWriter` / `PipeReader` API shape once so the scanner phases inherit it.
- Phase 2 (`existing_docs` scanner): ~1 week — All APIs already exposed; pure translation work.
- Phase 3 (`missing_docs` scanner): ~1 week — faster than Phase 2 once the first establishes the scanner template. Dict iteration and `HiddenString` construction stay as C callthroughs.
- Phase 4 (`numeric` scanner): ~1.5 week — `numeric_range_tree_ffi::gc` has the complete scanner surface; no FFI extension needed.
- Phase 5 (`terms` scanner): ~2 weeks — dominated by the trie wrapper work (not by II, which is already ready). Add ~0.5 week if we go with the `c_trie_ffi` option.
- Phase 6 (`tags` scanner): ~1.5 weeks — reuses Phase 5's trie-wrapping pattern, plus a small equivalent for `TrieMap`.
- Phase 7 (full orchestrator port + struct migration with Rust atomics + test-harness accessors): ~3 weeks. Covers `periodicCb`, the fork/pipe setup, the pause/exec state machine on Rust atomics, the struct itself, and the `FGC_getPauseState` / `FGC_getStats` accessors for `test_cpp_forkgc.cpp`. Invocations of `RedisModule_Fork` etc. go through `unsafe extern "C"` directly from bindgen; no new wrapper crate work.
- Phase 8 (cleanup): ~1 week.

Total: roughly 13 weeks of focused work — everything inside `src/fork_gc/` ported, no C residue.

## Risks & open questions

### General

- **Wire-format drift during phased port.** *Mitigation*: ship child+parent for each kind in one changeset; round-trip unit tests at the protocol layer; CI runs the full pytest suite after each phase.
- **Async-signal safety post-fork.** Between `fork()` and child-side Rust code running, only async-signal-safe operations are guaranteed safe. Our pipe primitives only call `write(2)` / `poll(2)` / `_exit(2)`-equivalent. No heap allocations in the child's hot path. *Mitigation*: explicit audit in Phase 2 (first scanner kind) when scanners start allocating.
- **GIL discipline.** Orchestrator holds the Redis GIL when calling `RedisModule_Fork`; post-fork the child doesn't need it. *Mitigation*: the orchestrator port (Phase 7) preserves the existing `RedisModule_ThreadSafeContextLock`/`Unlock` pairs.

### Phase-specific difficulties

- **Phases 2–4: no API gaps, but scanner control-flow porting is still non-trivial.** The survey confirms II GC (`InvertedIndex_GcDelta_Scan` / `_Read` / `_Apply*`) and numeric (`NumericGcScanner_*`, `ApplyGcEntry`, `CompactIfSparse`) are already wrapped in `inverted_index_ffi` / `numeric_range_tree_ffi`. The remaining risk is translating the scanner's branching / error-handling / logging faithfully — a mis-translated `FGC_DONE` or early return can drop deltas silently without tripping any unit test. *Mitigation*: diff the Rust and C scanners line-by-line on PR review; rely on the existing pytest suite to catch behavior drift end-to-end.

- **Phases 5–6: trie / triemap iteration is C-only.** The II GC APIs the scanners need are already Rust-reachable, but the **trie walks** the scanners drive (`Trie_Iterate` / `TrieIterator_Next` / `Trie_Delete` for terms; `TrieMap_Iterate` / `TrieMap_Delete` for tags) have no Rust wrappers. `trie_rs` exists but is a separate Rust impl, not bridged to the C trie owned by `IndexSpec`. `triemap_ffi` only exposes opaque-pointer handling. *Mitigation*: budget a small sub-task in Phase 5 for a `c_trie`-style wrapper around `Trie_Iterate` / `Trie_Delete`; reuse the pattern for `TrieMap` in Phase 6. Alternative: leave iteration in C and have the C walker push per-term work down into a Rust callback — less idiomatic but smaller.

- **Phases 5–6: inverted-index block format in flux.** If the inverted-index crate is itself mid-migration during the terms/tags work, the block-compaction logic on both sides of the pipe must agree on exactly the same format. A version skew where the C child writes one block layout and a Rust parent decodes another corrupts indexes silently. *Mitigation*: synchronize scanner phases with the II migration timeline; if both are moving, freeze the block format for the duration of the scanner phase, or block the scanner phase on the II one.

- **Phase 7: atomic memory orderings aren't documented in the C.** `pauseState` / `execState` / `deletedOrUpdatedDocsFromLastRun` become Rust `AtomicU32` / `AtomicUsize`, but the C code uses specific orderings (acquire on pause-entry, release on pause-exit, relaxed on the counter bump — implied by the pause handshake protocol, not spelled out). Getting these wrong risks subtle child/parent synchronization bugs that won't trip any unit test. *Mitigation*: derive orderings from first principles of the pause handshake before porting; unit-test the pause transitions with threads (not fork) in Rust; keep the C orderings as reference during review.

- **Phase 7: orchestrator calls ~six unwrapped Redis / subsystem APIs.** `periodicCb` invokes `RedisModule_Fork`, `RedisModule_KillForkChild`, `RedisModule_ThreadSafeContextLock`/`Unlock`, `isOutOfMemory`, `VecSim_CallTieredIndexesGC`, and `IndexsGlobalStats_DecreaseLogicallyDeleted`. None are wrapped in the Rust `redis-module` crate, so the Rust orchestrator uses them as raw `unsafe extern "C"` calls (bindgen already generates the declarations in `ffi`). Not a blocker, but every call site is an unsafe block and a potential foot-gun around ordering (lock / unlock pairs, fork-child vs fork-parent dispatch). *Mitigation*: add a small set of RAII / typed helpers inside `fork_gc` (e.g. `ThreadSafeContextGuard`, `ForkOutcome` enum wrapping the fork result) so the `periodicCb` body doesn't accumulate raw unsafe calls; keep the mapping from C to Rust line-for-line during the initial port and refactor afterwards.

- **Phase 7: post-fork Rust runtime state.** `fork(2)` duplicates the child's address space, including the global allocator's internal state, any live mutexes, and any spawned threads. `std` used single-threaded is fork-safe, but transitive dependencies brought into the `fork_gc` crate (connection pools, async runtimes, lazy globals) can deadlock on poisoned mutexes post-fork. *Mitigation*: audit `fork_gc`'s transitive dependency graph before Phase 7; keep the dep list minimal; forbid async runtimes in this crate's dependency tree via a CI check if feasible.

- **Phase 7: test harness reaches into `ForkGC` fields.** `tests/cpptests/test_cpp_forkgc.cpp` reads `fgc->pauseState` and `fgc->stats.*` directly. The struct migration can't land until the test harness stops touching the fields. *Mitigation*: decide the approach (C accessors vs. porting the harness to Rust) one phase ahead so it's not on the critical path when Phase 7 starts.

### Open questions

1. **Trie / triemap wrapping strategy for Phases 5–6.** (a) Add a small `c_trie_ffi` wrapper crate (and companion `TrieMap` extension to `triemap_ffi`) before Phase 5 starts, so the scanner code is idiomatic safe Rust; (b) declare the C trie functions as `unsafe extern "C"` directly in `fork_gc` via bindgen and accept more unsafe blocks in the scanner. (a) is a one-time investment Phases 5 and 6 share; (b) is smaller up-front but scatters unsafe throughout the scanners.
2. **Should the pipe transport move to `socketpair(AF_UNIX, SOCK_STREAM, …)` as part of this project?** Doing so lets `UnixStream::set_read_timeout` replace all the manual `poll(2)` + `EINTR` plumbing in the pipe handle design. But it's a cross-language change touching the C-side fd creation. Defer as a follow-up, or bundle into Phase 7 where the orchestrator already changes?
3. **Test-harness migration.** (a) Keep `test_cpp_forkgc.cpp` as C++ and add `FGC_getPauseState` / `FGC_getStats` accessors exported from `fork_gc_ffi` via cbindgen — smallest change, fits the current Phase 7 plan. (b) Port the test harness to Rust entirely — larger, matches the broader migration direction. Decide before Phase 7 starts; (a) is the current default.
4. **`repr(C)` on the Rust-native `ForkGC`.** Post-Phase-7, nothing outside the crate reads the struct's fields — `repr(C)` isn't strictly needed. But the accessor functions in question 3(a) operate on `*mut ForkGC` and would benefit from a stable layout if anything else ever wants to inspect the struct from C. Default `repr(Rust)` unless a concrete need arises.
5. **Fork-vs-cleanup-thread revisit.** Explicitly a non-goal for this port, but once Fork GC is Rust, a thread-based collector becomes much more viable (no address-space duplication, no async-signal constraints, no GIL dance). Worth filing as a follow-up design question rather than leaving implicit.
6. **`RECV_BUFFER_EMPTY` ownership at end state.** Move the definition to a Rust `pub static` and re-export via cbindgen in Phase 8, or keep a one-line C translation unit alive purely to host the sentinel? The former is cleaner given the "no C left in fork_gc" goal; the latter avoids any new cbindgen gymnastics for a `*mut c_void` static. Default to the Rust definition.
