# Benchmark plan: cost of the inverted-index copy-on-write snapshot

Status: **plan for review**. Goal: produce defensible numbers for the risk
questions raised about moving the Rust inverted index to a copy-on-write (COW)
snapshot / lock-free-read architecture.

## Scope: what is being measured

The four PRs under review are the **inverted-index lock-free-read** stack
(Epic 1 of the FT.HYBRID workers-pool consolidation, `design.md`) — **not** the
VecSim/HNSW COW work in `vecsim-hnsw-snapshot.md`, which is a separate track.

| PR | Step | Change |
|---|---|---|
| #10009 | Epic 1 prep | `InvertedIndexSnapshot` scaffolding, `RepairContext`, append-aware revalidation, `add_record` microbench |
| #10025 (MOD-16144) | step A "arc-sealed" | Reader owns snapshot; `sealed` → `Arc<ThinVec<IndexBlock>>` |
| #10026 (MOD-16157) | step B "arc-pending" | `pending` region: `Vec<Arc<IndexBlock>>` for in-flight writes |
| #10010 | step 05 "lock-free reads" | Full lock-free read path + `in_progress` tail region |

### The COW model (from `index/snapshot.rs`)

A reader snapshot captures three regions under the spec read lock, then walks
them lock-free. Their copy costs differ by an order of magnitude, so they must
be measured **separately**, not as one "snapshot cost":

1. `sealed` → `Arc::clone` — refcount bump, **no data copy**.
2. `pending` → shallow `Vec` clone — copies pointer slots; block buffers shared by Arc.
3. `in_progress` (tail) → **deep clone of one block's encoded buffer**, every snapshot.

### Where copies actually happen (pr-10010, verified)

There is **no `Arc::make_mut`** and **ordinary writes never copy**:

- `add_record` mutates `in_progress` **in place**; on rollover the old block is
  *moved* into a fresh `Arc` and pushed to `pending` (`core.rs`). A concurrent
  reader holding a snapshot imposes **zero penalty on the writer** — sealed/pending
  blocks are immutable once published, and the reader already deep-copied
  `in_progress` at snapshot time.
- The only copy-if-shared is in **GC** (`gc.rs` `apply_gc`):
  `Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone())`. When GC rebuilds
  `sealed` (`self.sealed = Arc::new(new_sealed)`) and drains `pending`, any block
  still pinned by a live snapshot (refcount > 1) is **deep-cloned** instead of moved.

So this is **copy-on-snapshot (reader side) + copy-on-GC-if-pinned**, not
copy-on-write on the write path.

### The three cost components to quantify

- **C1 — per-snapshot reader copy.** At snapshot time: deep-copy the `in_progress`
  tail buffer (~1 block) + shallow-clone `pending` (O(#pending) refcount bumps) +
  `Arc::clone(sealed)`. Reader-side, paid by *every* reader at construction and on
  each revalidation/reset. Scales with #pending blocks, **not** with concurrent
  write rate. Expected small.
- **C2 — GC copy-of-pinned-blocks.** When fork-GC applies while a snapshot is live,
  each block the snapshot pins is deep-cloned rather than reclaimed. Scales with
  (#blocks pinned by live snapshots) × (GC frequency) — **not** with per-write.
  A workload only exercises C2 if **GC runs while a snapshot/cursor is open**.
- **C3 — retention amplification.** A live snapshot pins the old `sealed` ThinVec
  + its `pending` Arc blocks; after GC swaps in a fresh `sealed`, the old versions
  stay alive until the snapshot drops → up to ~2× the pinned working set. Driven by
  **long-lived** readers × GC churn.

## Prerequisite: capture-then-release the lock (cursors especially)

**Releasing the lock is the mechanism under test, not an optimization.** COW (C2)
and retention (C3) only occur when a snapshot is held *and* the lock is released so
GC can run concurrently. Fork-GC applies under the spec **write** lock; if a
reader/cursor holds the **read** lock for its lifetime (today's behavior), GC is
blocked → `apply_gc` never runs → `Arc::try_unwrap` always succeeds (refcount 1) →
**zero COW, zero retention, and zero concurrency benefit.** The cost cannot be
observed without the lock release that also delivers the benefit.

None of the 4 PRs wire the C query pipeline to capture-then-release: the Rust
reader *has* snapshot capability, but the product still holds the read lock the old
way. So the snapshot machinery is currently dormant end-to-end.

**Cursors are the sharpest driver.** A single `FT.SEARCH` is sub-second and rarely
overlaps a GC cycle, so even a lock-releasing single query barely triggers C2/C3. A
cursor (`FT.AGGREGATE WITHCURSOR` across many `FT.CURSOR READ` calls) lives
seconds-to-minutes → reliably overlaps GC → the natural C2/C3 driver. For a long
cursor, releasing the lock between reads is doubly required: otherwise it starves
GC and writers for its whole lifetime.

**Locking model (corrected — do NOT release the lock around `Read`).** The read
lock guards more than II blocks, and crucially the **TTL/expiration table**: the
per-round work touches `spec->docs.ttl` in two places —
`getDocumentMetadata → DocTable_IsDocExpired` (result_processor.c) *and*
`it->Read → DocTable_CheckFieldExpirationPredicate` for field-expiration filtering
(hybrid_reader.c). So `Read` itself races the TTL table if run unlocked. The correct
structure holds the read lock around the **whole round (`Read` + dmd)** and releases
it **between** rounds:

```
loop {
  lock_read();
  revalidate();               // spec may have changed during the release window
  Read();                     // may touch spec->docs.ttl — must be locked
  getDocumentMetadata();      // doc + field expiration — must be locked
  unlock();                   // writers/GC interleave here; snapshot keeps our view stable
}
```

The snapshot's role is **not** intra-round memory safety (the lock covers that) — it
is surviving the **release windows**: because its blocks are immutable (COW), a
writer/GC running while the lock is released cannot invalidate the reader's iterator
state, so no per-round block-pointer refresh is needed and results stay point-in-time
consistent. COW fires exactly when GC runs during a release window while the snapshot
pins blocks. Per-round acquire/release is the safe baseline; releasing every N rounds
is the tuning knob if rwlock churn shows up.

**No SWAP / id gating.** `InvertedIndexSnapshot` has no internal ids, and doc-ids are
monotonic (`DocTable_Put: ++maxDocId`, never recycled); stale/deleted ids resolve to a
NULL dmd and are skipped. The SWAP slot-reuse gating in the HNSW/VecSim design does
**not** apply to this inverted-index track.

**Benchmark selection:** avoid the expiration/HFE configs (`search-expire-*`,
`search-hfe-*`) — their Read exercises the TTL path and muddies the lock measurement.
`search-msmarco-6M-documents-mixed-50-50.yml` has no TTL and is clean.

**Decision:** gate the per-round lock-release behind a **hidden config/debug flag**,
enabled for the benchmark runs.

## Instrumentation to add (both layers depend on it)

Inferring retention from process RSS is noisy. Add direct accounting:

- A counter of COW copies performed (backbone `make_mut` + block-buffer deep copies).
- Bytes currently retained by live snapshots (sum of superseded-but-pinned buffers).
- Active-snapshot count.

Expose via `FT.DEBUG` (and/or `FT.INFO`). This turns C2/C3 into exact numbers.

## Layer A — Rust microbenchmarks (criterion)

Isolates COW cost with no Redis noise; CI-gateable with the `microbenchmark`
label. Extend `inverted_index_bencher` (added by #10009). **Two categories, with
different branch placement:**

**Mechanism-cost benches — feature-branch only, absolute numbers.** These call the
Arc/three-region API (`snapshot()`, `Arc<ThinVec>`, `InvertedIndexSnapshot`) that
**does not exist on master and will not compile there.** There is no master
comparison point — master's snapshot cost is *definitionally zero* (it holds the
lock instead of copying). Measure absolute cost:
- **`snapshot()` cost** vs sealed / pending / in_progress sizes → **C1**.
- **`apply_gc` cost with N live snapshots** pinning blocks — the
  `Arc::try_unwrap → clone` deep-copy path (blocks cloned vs moved) → **C2**.
- **Allocator bytes retained** while N snapshots are held across a GC cycle → **C3**.

**Regression benches — must also exist on master (baseline compare).** These
measure APIs present on *both* branches (`add_record` write path, reader
iteration) to prove the Arc wrapping did **not** regress the flat-`ThinVec`
baseline. Criterion baseline-compare needs the bench compiled in both checkouts
(`--save-baseline master` on master, `--baseline master` on feature), and the
bencher crate is new — so land the bencher crate + these benches on **master
first** (same logic as the mixed YAML), then the feature branch inherits them.
- **`add_record` throughput** (master flat blocks vs feature in-place `in_progress`).
- **Reader full-iteration throughput** over a fixed index (traversal hot path).

Answers "memory cost of static COW" broken down per component, in isolation.

### Preliminary Layer-A results (Numeric index, `examples/snapshot_cow_memory`)

First data point from the counting-allocator report (bytes):

| records | index_bytes | C1 snapshot | C2 gc move (k=0) | C2 gc cow (k=8) | C3 retained (8 pins) |
|--:|--:|--:|--:|--:|--:|
| 10 000  | 99 580  | 1 691 | 5 008  | 67 039  | 111 707   |
| 100 000 | 993 248 | 8 891 | 49 648 | 678 049 | 1 055 807 |

Reading:
- **C1** ≈ 1.7 KB / 8.9 KB per snapshot — small, scales with block count (pending Vec
  clone + one `in_progress` buffer copy).
- **C2** COW cost = `cow(k=8) − move(k=0)` ≈ **62 KB / 628 KB** — the block deep-clones GC
  performs when 8 snapshots pin every surviving block.
- **C3** retained ≈ the **full index** (≈ 2× working set) while 8 snapshots are held across
  a GC cycle; freed on drop.
- **Finding:** the index's own `GcApplyInfo.bytes_allocated` reports **0** for the COW
  clones (measured via allocator instead) — so `FT.INFO` index memory would *undercount*
  COW/retention. This is direct motivation for the dedicated `FT.DEBUG` accounting below.

## Layer B — end-to-end macrobenchmarks (redisbench-admin)

Answers query runtime / throughput / memory under real load, on master vs the stack.

**Branch matrix:** `master` (lock-based baseline) vs `pr-10010` (stack tip).
Add `pr-10025` / `pr-10026` only if a regression needs attributing to a step.

**Workloads:**
- **Read-only** → C1 as pure query-latency/throughput delta. Base config:
  `search-ftsb-1M-enwiki_abstract-hashes-fulltext-simple-1word-query`.
- **Mixed 50/50 read/write** → the headline case: lock-free reads should win
  throughput/tail-latency (writers no longer blocked for a read's duration).
  Note C2/C3 only materialize when **GC runs while snapshots are live** — so this
  run must keep fork-GC active (tune `FORK_GC_RUN_INTERVAL` down) and hold readers
  long enough to overlap a GC cycle. Adapt
  `search-ftsb-10K-enwiki_pages-...-mixed_simple-...write_1_to_read_20` (ratio →
  50/50) or `search-expire-write-read-concurrent`.
- **Long-cursor + active GC** → the real C2/C3 driver once cursors hold the
  snapshot: FT.AGGREGATE WITHCURSOR held open across GC cycles while a writer churns
  the index, so pinned blocks are deep-cloned by GC and old `sealed` is retained.

**Dataset — "memark" = MS MARCO** (`6M-msmarco-documents`): 6M HASH docs,
`ms_marco_idx` over `url/title/headings/body` TEXT + `tags` TAG — a rich full-text
inverted index, the intended macrobenchmark corpus. In-repo configs:
- `search-msmarco-6M-documents-load` — ingestion/indexing baseline.
- `search-msmarco-6M-documents-single-word-query` — read-only, single-word (C1 read impact).
- `search-msmarco-6M-documents-and-query` — read-only, multi-term intersection.
- **Mixed 50/50** — *no msmarco mixed config exists yet*; author one reusing the
  `ms_marco_idx` create + 6M load, then memtier at 50% `FT.SEARCH` / 50% `HSET doc:*`
  with fork-GC active. (This is the run that exercises C2/C3.)

Secondary (faster iteration): `ftsb-1M-enwiki_abstract` (1M docs, 3 TEXT fields).

**Metrics per run:**
- Query latency p50 / p99 / p999
- Throughput (read ops/sec; write ops/sec in mixed)
- Memory time-series: `used_memory`, process RSS, `FT.INFO` index memory,
  and the new `FT.DEBUG` retained-bytes counter. Retention is a **time-series
  divergence** under sustained writes, not a single snapshot value.

## What each experiment answers

| Question | Experiment |
|---|---|
| Memory cost of static COW | Layer A (C1/C2/C3 isolated) + `FT.DEBUG` retained bytes in Layer B |
| Read-only impact | Layer B read-only: latency/throughput vs master (expect ~flat) |
| Mixed 50/50 impact | Layer B mixed: throughput/tail win vs memory cost |
| Existing dataset, master + feature | MS MARCO 6M (primary; enwiki-1M for fast iteration), both branches |
| Query runtime / memory / throughput | captured metrics above |

## Benchmark-config placement (land on master first)

The new msmarco 50/50 mixed YAML (and any GC-interval tuning it needs) is **pure
test infrastructure** under `tests/benchmarks/` — it has no dependency on the
snapshot code. To get a master comparison point, the config **must exist in both
checkouts**, so:

- Land the mixed-workload YAML as its own **small, product-neutral PR to master**
  first (touches only `tests/benchmarks/`).
- The feature branch (pr-10010) then merges master and inherits the identical
  config → byte-for-byte comparable runs.
- This matches the tooling: `defaults.yml` already sets `baseline-branch: master`
  for redisbench-admin comparison mode.

The msmarco load config and `ms_marco_idx` schema already exist on master, so only
the mixed-query YAML is new.

## Execution notes

- All `./build.sh` / `cargo` / `make` runs **sequentially** — shared `target/`
  build-directory lock (CLAUDE.md). No parallel bench + build.
- Capture long runs to a log file via `tee`; `set -o pipefail`.
- Apply the `microbenchmark` CI label on the Layer A PR.
- Pin CPU/memory per `defaults.yml` setups (`oss-standalone`,
  `oss-standalone-threads-6`) so master vs feature runs are comparable.

## Implementation status

Worktrees (branched from the relevant base, submodules initialized):
- `jk-snapshot-bench-infra` (from `origin/master`) — the master test-infra PR.
- `jk-snapshot-bench-feature` (from PR #10010) — feature-branch benches + instrumentation.

Done:
- **master infra:** `tests/benchmarks/search-msmarco-6M-documents-mixed-50-50.yml`;
  `reader_iteration` regression bench (compiles). `add_record`/`garbage_collection`
  benches already existed on master.
- **feature benches:** `benches/snapshot_cow.rs` (C1 capture + C2 gc-apply-pinned timing);
  `examples/snapshot_cow_memory.rs` (byte-level C1/C2/C3 via counting allocator — numbers
  above); `reader_iteration` copied over for the master-vs-feature compare.
- **feature instrumentation:** `COW_CLONED_BLOCKS` / `COW_CLONED_BYTES` counters at the
  `apply_gc` clone site + `unwrap_or_cow` helper; unit test
  `apply_gc_cow_clones_only_pinned_blocks` (0 clones unshared, exactly the pinned survivor
  cloned when a snapshot is held) — passing.

Remaining (coupled, correctness-sensitive):
- **Cursor lock-release behind a hidden flag** — capture snapshot under the lock, release
  it, paginate lock-free, free on close. This is what makes C2/C3 happen in a running
  server. Needs design decisions (query path, flag mechanism, holding the Rust snapshot
  alive across `FT.CURSOR READ` in the C cursor struct).
- **FFI + `FT.DEBUG` surface for the COW counters** — mechanical, but the counters stay 0
  in a live server until the cursor work above holds snapshots, so it is sequenced after.

## Sequencing

1. Land on **master** as a standalone test-infra PR: the msmarco 50/50 mixed YAML
   **and** the `inverted_index_bencher` crate + regression benches (`add_record`,
   reader iteration). Merge master into the feature branch so both share identical
   configs/benches. (Mechanism-cost benches stay feature-only — they can't compile
   on master.)
2. Add COW/retention accounting to `FT.DEBUG` (small).
3. Wire cursors to hold + release the snapshot (small).
4. Layer A: regression benches master-vs-feature (baseline compare); mechanism-cost
   benches feature-only (C1/C2/C3 absolute). Fast, no cluster needed.
5. Layer B read-only (existing msmarco configs) + mixed 50/50, master vs pr-10010.
6. Layer B long-cursor + active-GC run for C2/C3.
7. Write up numbers against the four questions.
