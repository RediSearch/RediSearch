# Inline block repair on the write path

## Problem

Deleting or updating a document in RediSearch is a *logical* delete: the doc is flagged
`Document_Deleted` in the doc table and `ForkGC.deletedOrUpdatedDocsFromLastRun` is
incremented, but every inverted-index posting for that document stays encoded in its block.
Reclaiming those bytes is the fork GC's job, and the fork GC pays for it in a way that hurts
exactly when the system is busiest:

1. **The trigger correlates with load.** A cycle fires when
   `deletedOrUpdatedDocsFromLastRun >= FORK_GC_CLEAN_THRESHOLD`. A traffic spike is largely
   updates, an update is a delete plus an insert, so a spike drives the counter across the
   threshold and forks *during* the spike.
2. **`fork()` stalls the main thread.** `forkGCChild` is called with the GIL held. The stall
   scales with page-table size, so with RSS.
3. **Post-fork COW faults tax every writer** that touches a page the child shares, for the
   whole lifetime of the child.
4. **The work is then partly discarded.** The child's snapshot is stale by the time the parent
   applies it. `InvertedIndex::apply_gc` drops any delta touching the last block if its
   `num_entries` changed since the scan, reporting `ignored_last_block`
   (surfaced as `gc_blocks_denied` in `FT.INFO`). Under a write-heavy spike this is the common
   case for hot terms, so the expensive cycle reclaims least where garbage accumulates fastest.

There is also a standing structural gap independent of latency: **the last block of an index
is never repaired by design**, because a writer may have appended to it since the fork. On an
append-heavy index with repeated updates to recent documents, dead entries in the tail can
survive many cycles.

## What changes

Add **inline block repair**: when a writer appends a posting to an inverted index and the block
it just wrote to has accumulated a configurable proportion of entries for documents that no
longer exist, repair that one block in place, immediately, inside the write that is already
in progress.

The writer inspects the tail block when it fills, and periodically before then. The periodic
check is what reaches posting lists shorter than one block — those are entirely tail, so the
fork GC never repairs them either, and they are most of the vocabulary in a natural-language
index.

The writer already holds the index and the block, already holds whatever lock the write path
holds, and the block is already in cache. The repair is bounded by one block, so the added
per-write cost is bounded and predictable rather than a periodic spike.

User-visible surface:

- One new runtime config, `INLINE_GC_BLOCK_REPAIR_THRESHOLD` (percent of dead entries in a
  block, `0` disables the feature). Default off for the first release.
- New counters in `FT.INFO` `gc_stats`: `inline_repairs`, `inline_bytes_collected`.
- No new command, no change to query results, no change to the persistence format.

## What does not change

Inline repair **does not replace the fork GC**, and this proposal does not remove or disable
it. A writer only ever touches the last block of terms that appear in the document being
written. Dead entries in cold terms, and in non-tail blocks of hot terms, are never visited by
a writer and remain the fork GC's responsibility. The two are complements:

| | Reclaims | Left to the other |
|---|---|---|
| Inline repair | Tail blocks of actively-written terms | Everything not written to |
| Fork GC | All blocks except the tail | The tail |

Inline repair closes the fork GC's structural last-block gap, and the fork GC keeps covering
the cold body of the index. Neither alone is sufficient.

## Why this shape

- **No fork, no COW, no stale snapshot.** Repair happens against the live index with no
  intervening window, so `ignored_last_block` cannot apply and no work is discarded.
- **The primitive already exists.** `IndexBlock::repair` in
  `src/redisearch_rs/inverted_index/src/gc.rs` already repairs exactly one block given a
  `doc_exist` predicate, and `InvertedIndex::scan_gc` is a loop over it. Inline repair is that
  loop body applied to a single block, plus the accounting that `apply_gc` already performs.
- **The reader-invalidation contract already exists.** `apply_gc` calls `gc_marker_inc`, and
  `RawIndexReaderCore` compares its cached marker against the index's to detect that a GC moved
  the blocks underneath it. Inline repair reuses that contract unchanged.

## Success criteria

1. On a workload of repeated updates to a bounded key set, peak RSS and steady-state
   `logically deleted` docs are lower with inline repair enabled than without, at equal
   fork-GC settings.

   **Met.** With the shipped trigger, peak RSS 35% lower and the index 87% smaller, for ~26%
   write throughput; fork-GC cycles also halve, because there is far less left to scan. The
   full-block-only variant was 16% / 22% for ~4.7%. See `benchmarks.md` round 3.

2. ~~`gc_blocks_denied` per fork-GC cycle drops measurably on the same workload.~~

   **Not met, and wrong as stated.** Denials are unchanged (408k vs 406k). A denial happens
   whenever a writer touched the last block since the fork, which inline repair does not
   affect — it changes how much garbage that block holds, not whether the delta for it is
   dropped. Reclaiming the denied block is a separate mechanism, measured in `benchmarks.md`
   round 3 and left to its own proposal.
3. Write-path throughput regression stays within an agreed budget (proposed: p99 of
   `FT.ADD`-equivalent indexing latency within 5% of baseline at the default threshold).

   **Met at the default, exceeded when enabled — needs a maintainer decision.** The default
   is `0`, where the feature is inert and the cost is nil, so the criterion as literally
   worded is satisfied. That reading is too convenient to rely on. When enabled, the shipped
   trigger costs ~26% write throughput (p50 0.121 → 0.171 ms); the full-block-only trigger
   cost ~5% and would have met the budget on any reading, but reclaimed a fraction as much —
   68 M residual records against 7.5 M.

   The budget was written before either number existed. Either it should be restated as a
   budget *when enabled*, with 26% accepted as the price of the reclaim, or the stride should
   become configurable so an operator picks their own point on that curve. Deciding this is a
   precondition for the default ever moving off `0`.

4. No change to query results, `FT.INFO` doc counts, or RDB round-trip behavior.

Criteria 1–3 are the gate: if the write-path cost cannot be held inside the budget while
showing a real reclaim win, the change should not land.

## Open questions for maintainers

1. Should the threshold be a percentage of block entries, an absolute dead-entry count, or
   both (whichever trips first)? A percentage alone under-triggers on small blocks.
2. Should inline repair also run on the *delete* path (where the doc-to-dead transition is
   known) rather than only on the next write that happens to touch the block?
3. Is `ENABLE_UNSTABLE_FEATURES` the right initial gate, or is a default-off config sufficient
   given there is no new command surface?

## See also

- [`design.md`](design.md) — mechanism, locking, and the failure modes that decide viability
- [`tasks.md`](tasks.md) — implementation checklist
- [`specs/garbage-collection/spec.md`](specs/garbage-collection/spec.md) — behavior delta
