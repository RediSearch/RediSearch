# Design: inline block repair on the write path

## Scope

Full-text term indexes and tag indexes first — everything reached through
`InvertedIndex_WriteEntryGeneric` / `InvertedIndex_WriteForwardIndexEntry`. Numeric range
trees, the missing-docs and existing-docs indexes, and vector indexes are explicitly out of
scope for the first iteration; see *Deferred* below.

## Mechanism

### The primitive

`IndexBlock::repair` already does the whole job for one block:

```rust
pub(crate) fn repair<'block, E: Encoder + DecodedBy<Decoder = D>, D: Decoder>(
    &'block self,
    block_idx: usize,
    doc_exist: impl Fn(DocId) -> bool,
    mut repair: Option<impl FnMut(&RSIndexResult<'block>, &RepairContext<'block>)>,
    _encoder: PhantomData<E>,
) -> std::io::Result<Option<RepairType>>
```

It decodes the block, re-encodes surviving records into a temporary index, and returns
`RepairType::Delete` or `RepairType::Replace { blocks, n_unique_docs_removed }`, or `None`
when nothing is dead. `InvertedIndex::scan_gc` is a loop over this across all blocks;
`InvertedIndex::apply_gc` splices the results back in and maintains `n_unique_docs`, the
block-count delta, and the byte accounting.

Inline repair needs neither the loop nor the fork: one block, repaired and spliced in the
same call, with no snapshot in between.

### New API

Add to `InvertedIndex<E>` in `src/redisearch_rs/inverted_index/src/gc.rs`:

```rust
/// Repair the last block in place if at least `threshold_pct` of its entries
/// belong to documents that no longer exist. Returns `None` when the block was
/// left untouched.
pub fn repair_last_block_if_dirty(
    &mut self,
    threshold_pct: u8,
    doc_exist: impl Fn(DocId) -> bool,
) -> Option<GcApplyInfo>
```

Contract:

- Operates only on `self.blocks.last()`. A writer has no business touching any other block,
  and restricting it here keeps the cost bound in the type rather than in a comment.
- Reuses `IndexBlock::repair` for the decode/re-encode, and the same accounting arithmetic
  `apply_gc` performs, so `n_unique_docs`, `bytes_freed`, `bytes_allocated`,
  `entries_removed`, and `block_count_delta` stay consistent between the two paths.
  Factor the accounting out of `apply_gc` rather than duplicating it — a second copy that
  drifts is how `n_unique_docs` goes wrong silently.
- **Must call `gc_marker_inc()` whenever it mutates.** This is the reader-invalidation
  contract; see *Concurrency* below.
- `ignored_last_block` is meaningless here (no snapshot) and must be reported as `false`.

### Deciding when to repair

A stride counter, but only viable because of the lazy re-encode landed in task 1a. The first
attempt at this section was measured and rejected: with eager re-encode, staying inside a 5%
write budget demanded a stride of 8–10× the block capacity, so a check always landed on a
block that had long since rotated away.

Lazy re-encode makes a *clean* tail-block check cost a decode pass and nothing else, which is
what changes the arithmetic:

| Encoding | clean check | `add_record` | writes/check @5% | @10% | block capacity |
|---|---|---|---|---|---|
| `Full` | 1.87 µs | 219 ns | 171 | 85 | 100 |
| `DocIdsOnly` | 12.90 µs | 92 ns | 2804 | 1402 | 1000 |
| `Numeric` | 2.18 µs | 124 ns | 352 | 176 | 100 |

At a 10% budget the affordable cadence is 0.85–1.4× block capacity — roughly one check per
block, which is the cadence the mechanism needs. **This is a deliberate trade of write
throughput for smaller, less bursty GC cycles**, made explicitly rather than as a free win.

The cost curve is also now the right shape. Under eager re-encode the *cheapest* possible
outcome — a clean block — was the most expensive to discover, so every wrong guess paid full
price. Now cost tracks benefit: a clean block is cheap to rule out, and the expensive
re-encode only happens when there is something to reclaim.

#### The trigger: block fill, plus a stride

The first implementation fired only when the tail block filled. That is still the most
valuable moment to check — `take_block` starts a new block on the first write arriving at a
full tail, so it is the last chance to repair that block inline, and the point at which it
holds the most reclaimable garbage it ever will while still reachable by a writer.

It is not sufficient on its own. **A posting list shorter than one block never fills, so it
was never repaired — and such a list is entirely tail, so the fork GC skips it too**, because
it discards any delta touching the last block. Every term below block capacity was therefore
reclaimed by neither path. In a natural-language index those terms are most of the
vocabulary; measured end-to-end, closing this gap was worth far more than the full-block case
alone (`benchmarks.md`, round 3).

So the trigger is: probe when the block has filled, when it holds fewer than
`PROBE_EVERY_WRITE_BELOW` entries, or every `PROBE_STRIDE` appends in between. The stride
bounds the added decode rate to one block decode per stride; the every-write bound below it
exists so there is no gap at the start where a list shorter than one stride is never probed.
Both constants are equal, which makes it one rule rather than two.

The cadence stays self-limiting: a repair that reclaims `k` entries moves the block `k`
entries back down, so its next probe is `k` writes further away, and check frequency scales
with how much garbage is actually being produced. No per-index state is needed — there is one
`InvertedIndex` per *term*, so a counter field would spend memory on every term to answer a
question the block length already answers.

`maybe_repair_tail_block` is the gate; `repair_tail_block` is the primitive underneath it,
kept separate so tests can drive a repair without staging a particular block length.

The cost is real and is the reason the feature stays off by default: at stride 8 the measured
write-throughput cost is ~26%, against ~5% for the full-block trigger alone. The stride is the
dial between the two, and the every-write-below-stride rule is the part most likely to be
worth revisiting first — it decodes on every append for the shortest lists.

A minimum-reclaim threshold is applied to the *result*: a `Replace` that removes fewer than
`min_reclaim_pct` of the block's entries is discarded rather than churning the block to drop
one entry. A `Replace` that yields more than one block is rejected outright — removing an
entry can widen a doc-ID delta past the encoder's range and force a split, which would grow
the index instead of shrinking it. Those entries are left to the fork GC, which can afford the
split because it is not on the write path.

#### What this does and does not reach

Writers only ever touch the tail block, and garbage accumulates in a block in proportion to
the time it spends *without* writes. The two facts pull against each other, and they bound
what inline repair can do:

- **Reaches:** hot terms under update-heavy traffic, where the tail keeps receiving writes
  while superseded versions of recently-written documents die in it. This is the traffic-spike
  case that motivated the change.
- **Does not reach:** cold terms, whose tail block accumulates garbage precisely because
  nothing writes to it, and non-tail blocks of any term.

Everything in the second row stays the fork GC's job. Inline repair narrows what a fork cycle
has to reclaim; it does not remove the need for one.

### Where it hooks in

`writeIndexEntry` in `src/indexer.c` is the single funnel for full-text term writes and
already consumes an `AddRecordOutcome { mem_growth, blocks_added }` to update
`spec->stats`. `src/tag_index.c` has the parallel call for tags.

The `doc_exist` predicate is the constraint on *where* the hook can live. The
`inverted_index` crate has no doc-table access; the FFI layer does, and already builds exactly
this closure for the fork GC:

```rust
// src/redisearch_rs/c_entrypoint/inverted_index_ffi/src/lib.rs
let doc_exists = |id| unsafe { DocTable_Exists(&doc_table, id) };
```

So the hook is a new FFI entry point alongside `InvertedIndex_WriteEntryGeneric` that takes the
doc table, not a change to `add_record` itself. Two options, to be decided in review:

- **(a) Extend the write call.** `InvertedIndex_WriteEntryGeneric` grows a doc-table argument
  and performs the repair itself, returning the reclaim in an extended `AddRecordOutcome`.
  One FFI crossing, but couples every writer to the doc table.
- **(b) Separate follow-up call.** Writers call `InvertedIndex_MaybeRepairTail(idx, &doc_table)`
  after the write. Two crossings, but writers that cannot supply a doc table (RDB load, the
  missing/existing-docs indexes) simply do not call it.

(b) is preferred: it keeps `add_record` unchanged, makes the feature opt-in per call site, and
makes "this path does not do inline repair" explicit at the call site rather than implicit in a
null argument.

Either way the extra reclaim must reach `spec->stats` with the correct sign.
`IndexStats_BlockCountAdd` already accepts a signed delta; `spec->stats.invertedSize` and
`numRecords` are plain counters and need to be decremented, which is new for the write path.

## Concurrency

This is where the proposal lives or dies, and it rested on one claim, now **confirmed**:

> **Open question A (resolved).** Does the full-text write path run under
> `RedisSearchCtx_LockSpecWrite`, excluding all concurrent readers of the same spec?

Yes. `AddDocumentCtx_Submit` has exactly one caller, and it is bracketed by the write lock
with no conditional in between:

```
src/spec.c:3491  RedisSearchCtx_LockSpecWrite(&sctx);
src/spec.c:3498  AddDocumentCtx_Submit(aCtx, &sctx, DOCUMENT_ADD_REPLACE);
src/spec.c:3504  RedisSearchCtx_UnlockSpec(&sctx);
```

and from there `Document_AddToIndexes` → `IndexDocument` → `Indexer_Process` → `indexText` →
`writeIndexEntry`, which is where the repair hook sits. `document.c` takes no lock of its own
on this path — the only lock ops in that file guard the partial-update path, and are a *read*
lock.

The safety argument follows: a repair mutating `self.blocks` is exactly what `apply_gc`
already does under the same lock, so inline repair introduces no new reader/writer
interleaving. The existing invalidation contract carries the rest — `RawIndexReaderCore`
caches `gc_marker` and `ii_unique_id` and compares them against the index to detect that
blocks moved, which is why `repair_tail_block` must bump the marker on any mutation (and, just
as deliberately, must *not* bump it on a no-op).

This holds for the full-text path that is wired. Any writer added later must be re-checked
against the same claim rather than assumed to inherit it — which is a further argument for the
opt-in call shape in option (b): a path that cannot supply the lock simply does not call.

Two further interactions to settle in review:

- **Suspended readers.** `RawIndexReaderCore` can be suspended across a lock release and
  re-promoted. A repair between suspend and resume is indistinguishable from a fork-GC apply in
  the same window, so the existing resume path should cover it — but this needs a test, not an
  assumption.
- **Concurrent fork GC.** A fork child scans the tail block, and the parent then discards that
  delta via `ignored_last_block` if `num_entries` changed. An inline repair changes
  `num_entries`, so it makes that discard *more* likely, not less — correct but wasteful.
  Once inline repair owns the tail, the fork GC's scan could skip the last block entirely and
  save the work. Worth doing, but as a follow-up, not in the first change.

## Failure modes considered

| Mode | Consequence | Mitigation |
|---|---|---|
| Repair on every write | Write throughput collapses | Stride counter; threshold; default off |
| Block churn — repair a block, immediately append, repair again | Wasted CPU, allocation thrash | Stride resets on repair, not on write |
| `n_unique_docs` drifts between inline and fork paths | Wrong `FT.INFO` doc counts, wrong IDF | Shared accounting helper, property test asserting the two paths agree |
| Repair splits one block into several (`Replace` with >1 block) | Tail is no longer the block the writer expects | Reject a `Replace` that does not reduce block count when applied to the tail |
| Doc table lookup per entry is slow | Repair cost exceeds the budget | Benchmark `DocTable_Exists` in the repair loop before committing to the design |

## Alternatives rejected

- **Reader-driven repair.** Iterators already skip dead docs and know exactly which blocks are
  dirty. Rejected: readers hold the read lock and cannot mutate, so this needs deferred queues
  or epoch reclamation — a much larger change for a signal the writer can approximate cheaply.
- **Repair all blocks of the touched index on write.** Unbounded per-write cost proportional to
  posting-list length; the hot terms with the longest lists would pay the most.
- **Replacing the fork GC entirely.** Cold terms are never written to, so their garbage would
  never be reclaimed. See the proposal's complement table.
- **Budgeted in-process incremental GC** (cursor over the terms trie, N terms per tick, no
  fork). A genuinely promising alternative that addresses the same latency problem from the
  other side, and it subsumes more of the fork GC's work. Deferred rather than rejected: it
  trades a fork stall for many short write-lock windows, and whether that nets out needs a
  prototype. Inline repair is the smaller, independently useful step and does not preclude it.

## Deferred

- Numeric range trees (`numeric_range_tree`), missing-docs, existing-docs indexes — same
  primitive applies, but each has its own accounting (numeric carries HLL registers that must
  be recomputed) and deserves separate benchmarking.
- Having the fork GC skip the tail block once inline repair owns it.
- Inline repair on the delete path rather than the next write.
