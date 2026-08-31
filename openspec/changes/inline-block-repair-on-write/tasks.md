# Tasks: inline block repair on the write path

Each item is intended to be one reviewable commit or PR. Items 0 and 1 are gates — if either
fails, stop and take the result back to the issue rather than continuing down the list.

## 0. Confirm the locking precondition (gate)

- [ ] Trace every call site of `InvertedIndex_WriteForwardIndexEntry` and
      `InvertedIndex_WriteEntryGeneric` (`src/indexer.c`, `src/tag_index.c`,
      `src/forward_index.c`) up to the lock acquisition, including the background indexer queue
      and the RDB-load path.
- [ ] Record, per call site, whether `RedisSearchCtx_LockSpecWrite` is held.
- [ ] **Gate:** if any writer runs without the write lock, document which one and stop.
      Open question A in [`design.md`](design.md) is unresolved and the design needs revisiting.

## 1. Measure the cost of the primitive (gate) — DONE, GATE NOT PASSED

- [x] Add a Rust benchmark for `IndexBlock::repair` on a full tail block at 0%, 5%, 25%, and
      50% dead entries, with a realistic `doc_exist` predicate (not a constant closure —
      `DocTable_Exists` does real work and dominates a cheap decode).
- [x] Compare against the cost of one `add_record` on the same block.
- [x] **Gate:** derive the stride needed to keep amortized repair cost under the write-path
      budget in the proposal. If no stride satisfies it, stop.

**Result: no stride satisfies it.** Repairing every tail block costs +39–52% on the write path;
a 5%-budget stride fires once per 8–10 block rotations, landing on blocks that are unlikely to
hold dead entries. Full numbers and reasoning in [`benchmarks.md`](benchmarks.md).

Root cause: `IndexBlock::repair` re-encodes every survivor unconditionally and discards the
result when nothing was dead, so the zero-benefit case is the most expensive one.

**Tasks 2–10 below are on hold** pending the re-scoped work in task 1a. They are kept because
most survive a redesign, but the mechanism in `design.md` that they implement does not.

## 1a. Lazy re-encode in `IndexBlock::repair` (re-scoped, do this first)

Independent of inline repair — this is a fork-GC improvement on its own, since every scan
currently pays full re-encode on every clean block it visits.

- [x] Start the temporary block only when the first dead entry is found, replaying the
      already-read prefix at that point; a block with no dead entries then costs a decode pass
      and no allocation.
- [x] Existing `src/redisearch_rs/inverted_index/src/tests/gc.rs` must pass unchanged —
      this is an optimization, not a behavior change. (188/188 pass.)
- [x] Cover the new path: no existing test had a survivor *before* the first dead entry, so
      the prefix replay was never exercised. Added `index_block_repair_replays_surviving_prefix`,
      `index_block_repair_replays_prefix_with_duplicate_doc_ids`,
      `index_block_repair_invokes_callback_once_per_survivor` (the replay must not re-deliver
      records to the callback — `fork_gc`'s numeric collector folds each survivor into an HLL),
      and `expiration_bit_survives_prefix_replay` (bits are addressed by ordinal, so an
      off-by-one in the replay shifts them).
- [x] Re-run `tail_block_repair`; report the new 0%-dead column against the old.
- [x] Re-run the existing `garbage_collection` bench to quantify the fork-GC win.
- [x] **Decision point.** Clean blocks are 3–7× cheaper and a whole-index scan is ~3× cheaper
      wherever most blocks are clean, so the change stands on its own. Full results and the
      one regression case in [`benchmarks.md`](benchmarks.md).

Remaining before this ships as its own PR:

- [ ] Run the C/C++ unit tests and the Python flow suite — both need a full `./build.sh`, which
      has not been done in this checkout (the submodules were uninitialized until now).
- [ ] `cargo +nightly miri test -p inverted_index`.
- [ ] Confirm `fork_gc` and `numeric_range_tree` tests pass; both need the C bundle static lib.

## 2. Extract shared GC accounting — DONE

- [x] Factored the `n_unique_docs` / `bytes_freed` / `bytes_allocated` / `entries_removed` /
      `block_count_delta` arithmetic out of `InvertedIndex::apply_gc` into
      `absorb_block_repair` and `finish_block_repair`, which both the fork path and the
      inline path call.
- [x] No behavior change; existing tests pass untouched.

## 3. `repair_tail_block` — DONE

Named `repair_tail_block`, not `repair_last_block_if_dirty`: the gating moved to a separate
wrapper (task 4), leaving this as the unconditional primitive.

- [x] Implemented on `InvertedIndex<E>` in `src/redisearch_rs/inverted_index/src/gc.rs`.
      Returns `std::io::Result<Option<GcApplyInfo>>` rather than swallowing a decode error —
      a corrupt block should surface, not silently skip.
- [x] Bumps `gc_marker` on mutation only; a no-op leaves it alone so readers positioned in
      the block are not made to revalidate for nothing.
- [x] Rejects a `RepairType::Replace` yielding more than one block (a delta-driven split
      would grow the index).
- [x] `min_reclaim_pct` filters the result rather than the trigger, so the decode is paid
      once.
- [x] Tests: empty index, clean block, tail-only reclaim (non-tail dead entries must
      survive), wholly-dead tail, minimum-reclaim accept/reject, and equivalence with
      `scan_gc` + `apply_gc` on unique-doc count, block count, and decoded contents.

## 4. Trigger — DONE, redesigned

The write counter in the original plan was replaced by gating on block rotation. Rationale in
[`design.md`](design.md) § *The trigger*: it needs no per-index state (there is one
`InvertedIndex` per term, so a counter field costs memory on every term), it is the last
moment a writer can reach the block, and the cadence is self-limiting without tuning.

- [x] `repair_full_tail_block` gates `repair_tail_block` on
      `num_entries >= RECOMMENDED_BLOCK_ENTRIES`.
- [x] Tests: no repair until the block fills; a clean full block is checked exactly once and
      then rotates away.
- [ ] Confirm the self-limiting cadence claim with a benchmark rather than by construction —
      that checks scale with garbage produced, and a clean index settles at one check per
      block.

## 4b. Wrapper index types — DONE (not in the original plan)

`InvertedIndex` is reached from C through an opaque enum that wraps two tracking types, both
of which had to forward the new call. `EntriesTrackingIndex` in particular keeps its own
`number_of_entries`, which `apply_gc` decrements — the inline path must too, or the two
reclaim paths disagree about how many entries the index holds.

- [x] `InvertedIndex::repair_full_tail_block` dispatch wrapper in `index/opaque.rs`.
- [x] `EntriesTrackingIndex::repair_full_tail_block`, adjusting `number_of_entries`.
- [x] `FieldMaskTrackingIndex::repair_full_tail_block` (plain forward — the tracked mask is a
      union over every entry ever added and is never narrowed by a removal).

## 5. FFI entry point — DONE

Named `InvertedIndex_RepairFullTailBlock`, and it takes the `IndexSpec` rather than a doc
table or a `RedisSearchCtx`: the spec is what the write sites already hold, and the doc table
comes off it exactly as in `InvertedIndex_GcDelta_Scan`.

- [x] Added in `inverted_index_ffi`, reusing the existing `DocTable_Exists` closure shape.
- [x] `make generate-rust-headers` (needed `cargo install --locked cheadergen_cli@0.3.2` and
      `rustup toolchain install nightly-2026-05-01 -c rust-docs-json` first — neither was
      present in this checkout).
- [x] Returns the reclaim through an out-param `II_GCScanStats`, which already carries signed
      block deltas, plus a `bool` for "did anything happen".

## 6. Wire up the C write path — DONE for text; tags deferred

- [x] `maybeRepairTailBlock` called from `writeIndexEntry` (`src/indexer.c`), gated on the
      config being non-zero.
- [x] Applies the reclaim to `spec->stats`: `invertedSize` (freed minus allocated),
      `numRecords`, `IndexStats_BlockCountAdd` (signed).
- [x] Missing-docs / existing-docs writes and RDB load deliberately not wired.
- [ ] **Tags not wired.** `tagIndex_Put` and `TagIndex_WritePostings` receive an `IndexStats*`
      but no `IndexSpec*`, so the spec has to be threaded through two functions first. Split
      out rather than widened into this change.

## 7. Configuration — DONE

- [x] `INLINE_GC_BLOCK_REPAIR_THRESHOLD` in `src/config.c`, default `0` (disabled), rejecting
      values above 100 with `QUERY_ERROR_CODE_LIMIT`.
- [x] Registered in `__configPairs` with an empty native alias, so it is `FT.CONFIG`-only. A
      `search-` native alias would need `RedisModule_RegisterNumericConfig` plumbing; worth
      adding before this ships, but it is a separate decision from the mechanism.
- [ ] Document the interaction with `FORK_GC_CLEAN_THRESHOLD` in the config reference.

## 8. Observability — DONE, relocated

Reported at `FT.INFO` top level next to `total_inverted_index_blocks`, **not** under
`gc_stats`: that section is rendered from the fork GC's own context (`statsCb` /
`statsForInfoCb` in `fork_gc.c`), which cannot reach `spec->stats`.

- [x] `inline_gc_repairs` and `inline_gc_bytes_collected` in `FT.INFO`
      (`src/info/info_command.c`) and the module `INFO` section (`src/spec.c`).
- [x] Reported separately from the fork GC's `bytes_collected` rather than summed into it, so
      the two reclaim paths can be compared. Recorded in the spec delta.

## 9. End-to-end tests

- [ ] Python flow test: repeated updates to a bounded key set with fork GC effectively
      disabled (`FORK_GC_RUN_INTERVAL` very high); assert memory and
      `FT.INFO` `inline_repairs` move, and that query results stay correct throughout.
- [ ] Flow test asserting a concurrent query running across an inline repair returns the same
      result set as one running without concurrent writes.
- [ ] Flow test: RDB save/load round-trip after inline repairs produces an identical index.
- [ ] Follow [/write-flow-tests](../../../.skills/write-flow-tests/SKILL.md).

## 10. Validation — partly done

Done:

- [x] `./build.sh DEBUG=1` — full build, 0 errors.
- [x] `cargo nextest run -p inverted_index -p fork_gc -p numeric_range_tree` — 400/400.
      Needs `BINDIR=<repo>/bin/macos-aarch64-debug/search-community`, otherwise the build
      script looks for a *release* C bundle.
- [x] `./build.sh DEBUG=1 RUN_UNIT_TESTS` — 981 passed, 6 failed. The failures are three
      `IORuntimeCtxCommonTest.UpdateNodes*` segfaults in coordinator connection auth
      (`MRConn_SendAuth` on a side thread), confirmed pre-existing by re-running them with
      every functional change stashed.
- [x] `cargo fmt --check` and clippy clean on all changed crates.

Done in a Linux container (see § *Container setup* below), which is what unblocked the rest:

- [x] Full Linux build — 0 errors.
- [x] `test_gc.py` — **passed** (31 tests). The suite that exercises fork GC end-to-end
      through the real module, so it covers the lazy re-encode and the accounting refactor.
- [x] `test_info.py` — **passed** (7 tests), with the two new `FT.INFO` fields present.
- [x] `test_config.py` — 122 passed, 47 failed, against a baseline of 120 passed / 46 failed
      with every functional change stashed. The change adds exactly three tests:
      - `testConfigAPILoadTimeNumericParams_INLINE_GC_BLOCK_REPAIR_THRESHOLD` — passes
      - `testModuleLoadexNumericParamsLastWins_INLINE_GC_BLOCK_REPAIR_THRESHOLD` — passes
      - `testModuleLoadexNumericParams_INLINE_GC_BLOCK_REPAIR_THRESHOLD` — fails

      The one failure is not a defect in the config: **all 32** `testModuleLoadexNumericParams_*`
      tests that run in this environment fail at baseline, on
      `env.envRunner.moduleArgs` being `None`/empty — an RLTest harness problem with
      module-load arguments, unrelated to config registration. The new config adds a 33rd
      member of a family that is already 100% failing. The two passing tests are the ones that
      actually validate its default, range and last-wins semantics.

- [x] `cargo +nightly miri test -p inverted_index gc::` — 21 passed, 0 failed, 1 ignored
      (`test_refresh_buffer_pointers_after_reallocation`, pre-existing: its memory hack is
      rejected by miri). No undefined behaviour in the lazy re-encode or the tail repair.

Still open:

- [ ] Macro benchmark of the actual write-path cost with the real `DocTable_Exists`. Every
      cost figure quoted so far comes from a microbenchmark with a stand-in predicate, so the
      +8.5–17% estimate is unconfirmed.
- [ ] Live end-to-end smoke test driving `INLINE_GC_BLOCK_REPAIR_THRESHOLD` against a running
      server and asserting `inline_gc_repairs` advances — task 9 covers this as flow tests,
      which are not written yet.

## Container setup (how to reproduce the Linux runs)

The macOS checkout cannot build (see § 11). Everything above was run in a container built
from the repo's own `Dockerfile`, which needs two adjustments to be usable interactively:

```bash
docker build --build-arg BASE_IMAGE=ubuntu:24.04 -t rs-dev:ubuntu24 .
```

1. **Redis is not in the image.** The `.install` scripts provision build dependencies only,
   and Ubuntu 24.04 ships Redis 7.x, which segfaults in `RedisModule_OnLoad` against current
   master. Graft the Redis 8 binaries in rather than building from source:

   ```dockerfile
   FROM redis:8 AS redis
   FROM rs-dev:ubuntu24
   COPY --from=redis /usr/local/bin/redis-server /usr/local/bin/redis-server
   COPY --from=redis /usr/local/bin/redis-cli /usr/local/bin/redis-cli
   ```

2. **`bin/` must be shadowed by a volume.** `build.sh` sets its own Rust target directory
   (`bin/redisearch_rs`) and so overrides `CARGO_TARGET_DIR`. Bind-mounting the repo without
   shadowing `bin/` makes the Linux linker consume the macOS object files sitting there, which
   fails as `unresolvable R_AARCH64_ADR_GOT_PAGE relocation against 'environ@@GLIBC_2.17'`.

```bash
docker run --rm -v "$PWD":/project -v rs-bin:/project/bin -w /project rs-dev:redis8 \
  bash -lc 'bash .install/test_deps/install_python_deps.sh && \
            REJSON=0 ./build.sh DEBUG=1 RUN_PYTEST TEST=test_gc.py'
```

`REJSON=0` skips building RedisJSON (`runtests.sh:552`). Python test deps are installed at
run time because the image build passes `SKIP_PYTHON_TEST_DEPS=1` — `uv.lock` is not in the
Docker build context.

**The harness exit code cannot be trusted.** A run that aborted before executing a single test
("Cannot find redis-server. Aborting.") reported the same failure text as a genuine test
failure, and the wrapper still exited 0. Grep the per-file result markers, not `$?`.

## 11. Pre-existing build breakage found on the way

Three separate macOS build failures, all reproduced on a clean `master`, all unrelated to this
change. The first two are fixed here because nothing builds without them; the third is worked
around locally only.

- [x] `deps/fast_float` and `src/coord` (`coordinator-core`) never set a C++ standard, so they
      took the compiler default. That is C++17 on gcc but **C++98 on Apple clang**
      (`__cplusplus == 199711`), and both use C++11-or-later features. Fixed with
      `target_compile_features(... cxx_std_20)` on the two targets. The root cause is that
      `set(CMAKE_CXX_STANDARD 20)` in the top-level `CMakeLists.txt` is function-local with no
      `PARENT_SCOPE`, so it never escapes — worth fixing properly, separately.
- [ ] VectorSimilarity's `SVE.cpp`/`SVE2.cpp` **crash** Apple clang (`Abort trap: 6`).
      `CHECK_CXX_COMPILER_FLAG` accepts `-march=armv8-a+sve` on Apple Silicon even though the
      hardware has no SVE, so the guard passes and the intrinsics are compiled. Worked around
      by clearing `CXX_SVE*` in the local `CMakeCache.txt`; the real fix belongs in the
      VectorSimilarity submodule.
- [ ] `tests/cpptests/test_cpp_dict_pause_rehash.cpp` uses `std::jthread`, absent from Apple
      libc++, which blocks the whole `rstest` binary. Excluded locally to run the suite; the
      exclusion was reverted and is **not** part of this change.
- [ ] `cargo +nightly miri test` for the new inverted-index code.
- [ ] `make lint`, `make fmt CHECK=1`.
- [ ] Macro benchmark before/after on a write-heavy workload — report both the write-path cost
      and the reclaim, and confirm the success criteria in [`proposal.md`](proposal.md).
- [ ] Measure `gc_blocks_denied` per fork-GC cycle with the feature on and off.
