# Cost of the inverted-index copy-on-write (COW) snapshot

Team-facing summary of what the lock-free-read snapshot costs, where those costs
come from, when they bite, and the optimizations that bound them. Companion to
[`snapshot-cow-benchmark-plan.md`](./snapshot-cow-benchmark-plan.md).

> All numbers here are from a **synthetic Rust microbenchmark** (`inverted_index_bencher`,
> Numeric index, counting allocator; ~1 MB index at 100K docs) — treat percentages as
> directional. They measure **memory**, not latency. The MS MARCO macro run gives the
> real-world figure.

## What the snapshot is

To iterate a query **without holding the index lock the whole time**, a reader takes
an **owned, point-in-time snapshot** of the index's block storage: it pins the *entire*
as-of-capture block set (not just the part it reads), then walks it lock-free. Three
costs follow, at three different moments.

## The three cost components

| | **C1 — snapshot capture** | **C2 — GC copy of pinned blocks** | **C3 — retained memory** |
|---|---|---|---|
| **What** | Refcount-bump `sealed` (free), shallow-copy the `pending` pointer list, deep-copy the one partial tail block | GC deep-copies blocks a live snapshot still pins, instead of freeing/moving them (the copy-on-write) | Old block generation held alive alongside the live index while a reader's snapshot pins it |
| **When** | Every read that sees a changed index (**per `FT.CURSOR READ`**, not once per cursor) | Only when GC's apply phase runs while a snapshot is live | Only between a read and the next read, while a snapshot is held |
| **Scales with** | Block count (≈ index size); shrinks as a fraction as the index grows | Blocks pinned by live readers at apply time (**not** write volume) | One inter-read interval — self-limited by per-read revalidation; sustained only for an idle/unread open cursor |
| **Measured (10K → 100K)** | ~1.7 KB → ~8.9 KB (~1.7% → ~0.9% of index) | ~62 KB → ~628 KB per overlapping GC cycle (worst case) | ~112 KB → ~1.05 MB (≈ 2× index), **transient per interval** |
| **Risk** | Low — small, per read | Medium — bounded per GC overlap | Low in normal use (self-limited); the only sustained case is an idle open cursor |

**The number to quote: ~1–2%.** C1 is the always-on overhead per active read, shrinking
with index size, independent of write rate. C2/C3 are **zero** unless GC overlaps a live
reader.

## Where C2 comes from (and how it differs from fork-GC's COW)

There are two different "COW"s; C2 is the new one:

- **fork() page-COW (exists today, unchanged):** fork-GC forks a child that scans the
  index read-only; the OS copies memory *pages* that diverge while the parent keeps
  writing during the scan. Cost ∝ parent write volume, in the **child, at scan time**.
- **C2 block-COW (new, from this feature):** after the child dies, the **parent** runs
  `apply_gc` to rebuild the block storage. A surviving block a live reader snapshot pins
  (`Arc` refcount > 1) can't be moved/freed — it's `Arc::try_unwrap → clone`'d. Cost ∝
  blocks pinned by readers, in the **parent, at apply time**.

C2 is **not** the fork COW re-labeled — it's additive, and it's new versus master (where
blocks aren't `Arc`-shared and `apply_gc` mutates the owned `ThinVec` in place). The
measured **63%** is the worst case: 8 snapshots pinning *every* block while GC rebuilt
after a 30% delete → apply cloned all surviving blocks. With no reader pinning, that term
is **~0**.

## When the ~2× ceiling is actually reached

A momentary state requiring all of:

1. A reader/cursor holds a live snapshot (mid-query, or a cursor *between* reads).
2. Fork-GC's apply fires during that window — needs accumulated deletes past
   `FORK_GC_CLEAN_THRESHOLD` and the window to overlap a run (default `FORK_GC_RUN_INTERVAL`
   ~30s).
3. → apply clones the pinned survivors (C2), old + new coexist ≈ 1.7–2× (C3).

**Held only until the snapshot drops:** a healthy cursor drops it at the very next read
(see below); the only *sustained* ~2× is an idle/unread open cursor. **Above 2×** needs
multiple concurrent cursors each pinning a distinct pre-GC generation. **Essentially never
reached** by sub-second queries (finish inside the GC interval) or with no sustained deletes.
The benchmark forces it (`FORK_GC_RUN_INTERVAL=1`, `FORK_GC_CLEAN_THRESHOLD=1`) precisely
because production defaults make it rare.

## Why C3 is self-limiting (revalidation)

On every `FT.CURSOR READ`, `handleSpecLockAndRevalidate` calls the iterator's
`revalidate()`. When the index changed (gc_marker / block count / tail), it
`reset()`s — which **re-snapshots and drops the previous generation** — then
`skip_to(last_doc_id)` to continue. So a cursor holds **at most one generation at a
time**, and it's released at the next read. Retention doesn't accumulate over a cursor's
life; the ~2× is transient per inter-read interval. Consistency across reads is therefore
best-effort (newer data can appear between reads) — already true today, not a new change.

## Optimizations

### Done — `apply_gc` moves unpinned `sealed` (feature branch)

Previously `apply_gc` rebuilt `sealed` by cloning **every** block unconditionally,
deep-copying the whole compacted region on every GC cycle with work — even with no
readers (a reader-independent cost, additional to C2). Now the `sealed` `Arc` is
`try_unwrap`'d: unshared → blocks **moved** (no buffer copy); shared by a live snapshot →
cloned (and counted). This removes the always-on term and leaves only the reader-driven
COW. Guarded by a unit test (`apply_gc_moves_unpinned_sealed_and_cows_pinned_sealed`).

### Recommended for C3 — release the snapshot at cursor suspend

The idle-open-cursor residual is best handled by **not keeping the snapshot alive between
reads at all**: release it at cursor suspend, re-take + `skip_to(last_doc_id)` on resume.
This is correct because `revalidate()` already reconstructs position from a fresh snapshot
— keeping it alive between reads only preserved strict cross-read consistency (already
abandoned) and avoided the resume re-seek. Dropping it eliminates idle-cursor retention
outright, with no `MAXIDLE` dependency for memory.

This **supersedes** an earlier weak-ref proposal (hold `sealed` as `Weak` while suspended,
`upgrade()` on resume): weak-ref only additionally avoids the resume `skip_to` when the
index is unchanged. So the open question is **whether the per-resume `skip_to` re-seek is
cheap enough** (it's `O(log blocks + block scan)`, amortized over the read's chunk) — if
yes, plain drop-and-retake wins on simplicity; weak-ref is the fallback only if small-chunk
cursors show the re-seek as costly.

Both need: a C-side hook at cursor suspend, a releasable-snapshot Rust tweak, and the
borrowed-results safety (release only after the chunk's yielded results are consumed).

## Observability gap

`FT.INFO` / `GcApplyInfo` do **not** account for the COW copies (they read 0). Cumulative
`COW_CLONED_BLOCKS` / `COW_CLONED_BYTES` counters exist on the feature branch; still needed
is a current-retained-bytes gauge and an `FT.DEBUG` surface so retained COW memory is
visible operationally.

## Bottom line for the team

- Steady overhead is **C1 ≈ 1–2%** memory per active read, shrinking with index size.
- **C2/C3 are zero** unless GC overlaps a live reader; even then C3 is transient because
  per-read revalidation refreshes the snapshot and releases the old generation.
- The one sustained-retention case is an **idle open cursor**, which "release at suspend"
  eliminates.
- The sealed-rebuild always-on cost has been removed (`apply_gc` now moves unpinned blocks).
