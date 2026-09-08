# Benchmark results: task 1 gate

Bench: `src/redisearch_rs/inverted_index_bencher/benches/tail_block_repair.rs`

```bash
cargo bench --manifest-path src/redisearch_rs/Cargo.toml \
  -p inverted_index_bencher --bench tail_block_repair
```

Measures one full-block repair (via `scan_gc` on an index built to hold exactly one block)
against one `add_record` onto an almost-full tail block. `arith` and `hashset` are the two
`doc_exist` predicates bracketing the real `DocTable_Exists`; see the bench's module docs.

Criterion medians, Apple M-series laptop, single run. **Not a controlled environment** — treat
the ratios as order-of-magnitude, not as numbers to quote.

## Raw

| Encoding | entries/block | add_record | repair 0% dead | 5% | 25% | 50% |
|---|---|---|---|---|---|---|
| `Full` (text) | 100 | 160 ns | 7.10 µs | 6.39 µs | 6.05 µs | 4.61 µs |
| `DocIdsOnly` (tag) | 1000 | 72.9 ns | 28.18 µs | 24.93 µs | 25.76 µs | 19.96 µs |
| `Numeric` | 100 | 107 ns | 5.52 µs | 5.49 µs | 5.05 µs | 4.39 µs |

Repair column is the `hashset` predicate — the closer proxy to a doc-table lookup. The `arith`
predicate runs 10–40% faster on `DocIdsOnly` (17.7 µs at 0%), where 1000 liveness checks are a
large share of the work; on the 100-entry encodings the two are within run-to-run noise.

## Derived

Repairing each tail block exactly once before it rotates costs `repair / entries_per_block`
per write:

| Encoding | repair amortized per write | add_record | write-path overhead |
|---|---|---|---|
| `Full` | 71 ns | 160 ns | **+44%** |
| `DocIdsOnly` | 28 ns | 72.9 ns | **+39%** |
| `Numeric` | 55 ns | 107 ns | **+52%** |

Against the proposal's 5% write-path budget, the affordable repair rate is:

| Encoding | blocks between repairs at 5% budget |
|---|---|
| `Full` | ~9 |
| `DocIdsOnly` | ~8 |
| `Numeric` | ~10 |

## Verdict on the gate

**The stride mechanism as written in `design.md` does not survive.**

The design assumed a stride of writes could amortize repair to negligible. It cannot, because
the stride needed (~880 writes for `Full`) is 8–10× the block capacity (100). By the time the
stride fires, the tail block has rotated 8–10 times. The repair would land on an arbitrary
freshly-rotated block, which is the block least likely to hold dead entries — maximum cost,
minimum benefit.

Put plainly: at a 5% budget you can afford to repair roughly one tail block in nine, and you
cannot choose which one.

## Why, and what it implies

The cost is dominated by **re-encoding survivors, not by detecting them**. Evidence:

1. Repair gets *cheaper* as more entries die — `Full` runs 7.10 µs at 0% dead and 4.61 µs at
   50%. Fewer survivors, less re-encoding.
2. `IndexBlock::repair` calls `tmp_inverted_index.add_record(&result)` for every surviving
   record unconditionally, and when no entry turned out to be dead it returns `None`, throwing
   the entire re-encoded block away.

So **the most expensive case is the one with zero benefit**, and any triggering heuristic that
guesses wrong pays full price. That is the wrong cost curve for a write-path feature.

### This is also a live inefficiency in the fork GC

The waste is not specific to this proposal. Every fork-GC scan pays full re-encode on every
clean block it visits, and in a healthy index most blocks are clean. Making the re-encode lazy
— start the temporary block only once the first dead entry is seen, and copy the already-read
prefix at that point — costs a clean block a decode pass and nothing else.

That is worth doing on its own merits, independent of whether inline repair ever ships.

## Recommended next step

Do not proceed to task 2 as written. Instead:

1. Implement lazy re-encode in `IndexBlock::repair`. Ships as a fork-GC improvement with no
   inline-repair machinery attached, and is independently reviewable.
2. Re-run this bench. The 0%-dead column is the one to watch; if it drops far enough, the cost
   model changes shape — a cheap "is this block dirty" pass plus a re-encode paid only when it
   reclaims something — and the stride objection above may no longer apply.
3. Only then revisit the inline-repair mechanism in `design.md`, rewritten against the new
   numbers.

---

# Round 2: lazy re-encode (task 1a)

`IndexBlock::repair` now defers building the replacement block until it sees the first dead
entry, replaying the surviving prefix from the buffer at that point. A block with no dead
entries costs a decode pass and no allocation.

Measured as a paired run: current code, then the same bench with only `gc.rs` reverted,
back to back. `add_record` is the untouched control — the drift it shows is the floor on
how precisely the two runs can be compared. It moved 1.2% for `Full`, −8.7% for `DocIdsOnly`,
and −25.6% for `Numeric`, so **the `Full` column is the trustworthy one** and `Numeric`'s
should be read as direction-only.

The bench also gained deletion patterns, because the position of the first dead entry —
not the number of dead entries — decides how much prefix has to be replayed. The original
`prefix-N%` patterns kill the *first* entries, so they never exercise the replay at all;
`last-only` is its worst case and `every-10th` is the realistic middle.

## One block (`hashset` predicate, medians)

| Encoding | pattern | eager | lazy | change |
|---|---|---|---|---|
| `Full` | prefix-0% (clean) | 6.58 µs | **1.87 µs** | **−72%** |
| `Full` | prefix-5% | 8.26 µs | 8.97 µs | +9% |
| `Full` | prefix-25% | 6.74 µs | 7.91 µs | +17% |
| `Full` | prefix-50% | 6.43 µs | 6.39 µs | −1% |
| `Full` | every-10th | 8.26 µs | 8.50 µs | +3% |
| `Full` | last-only (worst) | 8.76 µs | 9.70 µs | +11% |
| `DocIdsOnly` | prefix-0% (clean) | 39.25 µs | **12.90 µs** | **−67%** |
| `DocIdsOnly` | last-only (worst) | 39.91 µs | 37.07 µs | −7% |
| `Numeric` | prefix-0% (clean) | 5.99 µs | **2.18 µs** | **−64%** |
| `Numeric` | last-only (worst) | 8.33 µs | 8.08 µs | −3% |

With the cheap `arith` predicate, where decode dominates and the liveness check is nearly
free, the clean-block win is larger still: −88% (`Full`), −85% (`DocIdsOnly`), −76%
(`Numeric`).

**Clean blocks get 3–7× cheaper. Dirty blocks land between −7% and +17%**, consistent with
the extra decode of the surviving prefix — which shrinks as more entries die, and vanishes
when the first entry is dead.

## Whole index (`garbage_collection` bench, `Scan`)

This is the number that matters for the fork GC, and it splits sharply by how deletions are
distributed across blocks.

| Pattern | 1 000 records | | 100 000 records | |
|---|---|---|---|---|
| | eager | lazy | eager | lazy |
| Random 30% — every block dirty | 51.8 µs | 66.7 µs **(+29%)** | 5.33 ms | 5.85 ms **(+10%)** |
| First 30% — 70% of blocks clean | 62.0 µs | **21.0 µs (−66%)** | 4.87 ms | **1.85 ms (−62%)** |
| Every 3rd block — 67% clean | 53.2 µs | **18.2 µs (−66%)** | 5.18 ms | **1.58 ms (−69%)** |

**The win is entirely a function of how many blocks are wholly clean.** Where deletions are
clustered or sparse, a scan gets ~3× cheaper. Where every single block holds a dead entry,
it gets 10–29% more expensive.

### Is the regression case realistic?

Partly, and it deserves care rather than dismissal. For a block of 100 entries and uniformly
random deletions, the chance a block is entirely clean is `(1-d)^100`: about 90% at `d`=0.1%,
36% at 1%, and effectively zero at 30%. So a high uniform deletion rate is genuinely the
losing case.

But the fork GC scans the *whole index* each cycle, and the dominant term is not blocks
within one posting list — it is posting lists with no deletions at all, whose blocks are all
clean and which the eager path currently re-encodes in full before discarding the work. The
`garbage_collection` bench cannot show that: it only ever measures a single index that does
have deletions in it. The measured −62% to −69% is therefore closer to the whole-scan reality
than the +10% to +29% is.

That reasoning is an argument, not a measurement, and it should be confirmed end-to-end
before this is treated as settled — see below.

## Verdict

Land it. Clean blocks 3–7× cheaper is a large, unambiguous win on the common case; the
worst measured regression is +29%, on a pattern (30% of documents deleted and uniformly
spread) that also implies the GC has far more real work to do than the scan overhead.

The regression is removable rather than inherent. The prefix is re-decoded only because the
survivors were decoded and dropped; since no entry before the first dead one was removed,
their encoded bytes are still valid verbatim in the replacement block. Copying that byte
range instead of replaying it would make the lazy path never worse than the eager one. It
needs care around block metadata and the expiration bitset, so it belongs in its own change.

## Follow-ups

- [ ] Copy the surviving prefix as raw bytes instead of re-decoding it, removing the
      dirty-block regression.
- [ ] Confirm the whole-scan argument above against a real index via the macro benchmarks,
      rather than inferring it from a single-posting-list bench.
- [ ] Re-open the inline-repair cost model in `design.md`. The clean-block case is now 3–7×
      cheaper, which is exactly the case a write-path trigger hits most often — the
      arithmetic in "Verdict on the gate" above was computed against the eager numbers and
      needs redoing.

## Caveats

- Single run, laptop, no pinning, no controlled CPU frequency. Some `Full` and `Numeric`
  measurements had 15–35% outliers.
- Both predicates are proxies. Real `DocTable_Exists` is a C call this crate cannot link — the
  bencher stubs it with `panic!`. It is plausibly *more* expensive than the `HashSet` probe,
  which would make these ratios optimistic.
- Deleted documents are a contiguous prefix, the cheapest pattern to re-encode. Scattered
  deletions cost more.
- Single-block, cache-hot, no lock contention, no allocator pressure. All of these push the
  real cost up, none down.

---

# Round 3: end-to-end macro benchmark

Answers the round-2 follow-up "confirm the whole-scan argument against a real index via the
macro benchmarks". Measures the shipped feature — `INLINE_GC_BLOCK_REPAIR_THRESHOLD` 0 vs 20
— on a churning index, plus a threshold sweep, plus two prototypes that are **not** part of
this change and are recorded here only as evidence for follow-up proposals.

Apple M-series laptop, 24 GB, release build, Redis 8.4.6 built from source (the module
refuses to load below 8.4.0). Same caveats as rounds 1 and 2: no pinning, no CPU frequency
control, `fork` on macOS.

## The stock GC benchmark cannot measure this feature

`tests/benchmarks/search-ftsb-1M-enwiki_abstract-hashes-gc.yml` describes its client phase as
"continuous HSET updates". It is not. The dataset's keys are `doc:<uuid>:<id>`, while its
memtier arguments generate `doc:<N>` — the two sets never intersect, so every write inserts a
new single-field document instead of updating an existing one.

Measured, at threshold 0: `num_docs` rose 1,000,000 → 1,034,374 while memtier reported 34,143
writes, i.e. every write created a document. Thirty-five forced GC cycles reclaimed 13,545
bytes in total. With no updates there are no logical deletes, so there is no garbage for
either GC path to collect, and `inline_gc_repairs` stays 0 whatever the threshold is set to.

That benchmark is also throttled to ~114 ops/s by its own `GC_FORCEINVOKE` calls: a forced
cycle on a 1.5 GB index takes 6–8 s and the client runs at `-c 1 -t 1`, so it spends most of
the run blocked. Fixing the key overlap is worth a separate change; it is not a defect in
this one.

## Harness used instead

- 200,000 real enwiki documents (`title`, `url`, `abstract`, all `TEXT SORTABLE`), loaded by
  `ftsb_redisearch` from the first 200k rows of the 1M enwiki dataset. A natural-language
  vocabulary matters: posting-list length distribution is what decides how much of the index
  a tail-block mechanism can reach.
- A churn client that discovers the loaded keys via `SCAN` and overwrites them with field
  values drawn from the same corpus, so updates are real updates and the term distribution is
  preserved as the index churns. memtier cannot do this — it can only write keys it invents.
- 180 s, 4 connections, `GC_FORCEINVOKE` from a *separate* connection every 45 s. Firing GC
  from a writer folds a minutes-long stall into the write latency being compared.
- ~5.1 M writes per run. Each arm reloads the dataset, so both start from identical state.

## Threshold 0 vs 20

Two reps per arm; both reps are given where they differ.

| Metric | thr 0 | thr 20 | Δ |
|---|---|---|---|
| write ops/s | 28,532 / 30,466 | 28,216 / 28,021 | −4.7% |
| write p50 | 0.124 / 0.118 ms | 0.128 / 0.128 ms | +5.8% |
| write p99 | 0.378 / 0.350 ms | 0.366 / 0.374 ms | +1.6% |
| `num_records` | 87.0 / 93.7 M | 72.2 / 71.5 M | −20% |
| `inverted_sz_mb` | 700 / 750 | 580 / 576 | −22% |
| peak `used_memory` | 1062 / 1070 MB | 899 / 895 MB | **−16%** |
| fork GC cycle time | 160 / 128 s | 124 / 122 s | −15% |
| `gc_blocks_denied` | 408,224 / 408,043 | 406,072 / 406,161 | −0.5% |
| `inline_gc_repairs` | 0 | 371,660 / 369,007 | — |
| share of reclaimed bytes | 0% | 44.3% / 44.0% | — |

**Success criterion 1 is met.** Peak RSS is 16% lower and the index 22% smaller, with the
arms non-overlapping across reps. The cost is ~4.7% write throughput, which is the trade the
config's help text advertises.

**Success criterion 2 is not met.** `gc_blocks_denied` is unchanged. Inline repair does not
stop the fork GC from denying tail blocks; it reduces how much garbage those blocks hold.
Criterion 2 should be restated or dropped — the mechanism does not act on the denial path.

The write-path overhead measured here (+5.8% p50) is far below round 1's +39–52% projection,
because repairs are rare relative to writes: a repair needs the tail block to be full *and*
20% dead, and most writes meet neither.

## Threshold sweep

One rep each, same harness, full-block trigger throughout.

| Threshold | write ops/s | peak RSS | `inverted_sz_mb` | `num_records` | inline repairs |
|---|---|---|---|---|---|
| 0 (off) | 29,118 | 1046 MB | 712 | 88.5 M | 0 |
| 5 | 27,984 | 872 | 553 | 68.6 M | 794,072 |
| 10 | 28,232 | 882 | 564 | 70.0 M | 569,806 |
| 20 | 26,864 | 868 | 550 | 68.0 M | 351,751 |
| 50 | 29,118 | 960 | 640 | 79.9 M | 170,331 |

5, 10 and 20 are indistinguishable on residency and within run-to-run noise on write cost.
50 gives up most of the benefit. The threshold is a weak lever within 5–20; the default of 20
is defensible and there is no evidence for moving it.

## Removing the full-block gate — measured here, and now shipped

**This is the trigger the change ships.** `repair_full_tail_block` returns early unless the tail
block is full, so a posting list shorter than one block is never repaired — and such a list
is *entirely* tail, so the fork GC skips it too. Those terms are reclaimed by nothing today,
and in a natural-language index they are most of the vocabulary. Probing the tail on a stride
(every 8 appends, and every append below 8 entries) instead:

| | thr 0 | thr 20, full-block | thr 20, stride probe |
|---|---|---|---|
| write ops/s | 29,118 | 26,864 | 21,488 (−26%) |
| write p50 | 0.121 ms | 0.130 ms | 0.171 ms |
| `num_records` | 88.5 M | 68.0 M | **7.5 M** |
| `inverted_sz_mb` | 712 | 550 | **95** |
| peak RSS | 1046 MB | 868 MB | **675 MB** |
| fork GC cycle time | 146 / 115 s | 129 / 113 s | 67 / 60 s |

The index stays essentially clean, and fork-GC cycles halve because there is far less to
scan. It costs 26% write throughput at stride 8, against ~5% for the full-block trigger — the
stride is the dial between them, and 8 is the value measured, not a tuned optimum.

The gain comes from short posting lists. A list that never fills a block is entirely tail, so
the fork GC skips it (it discards deltas touching the last block) and the full-block trigger
skipped it too; in a natural-language index those terms are most of the vocabulary. Both
paths ignoring the same terms is why the full-block-only variant left 68 M records behind
where this leaves 7.5 M.

## Prototype measured, not included in this change

An env-gated hack built only to size the opportunity. It is not in the diff.

**Reconciling the denied tail block in `apply_gc`.** When a writer appends to the last block
after the fork, the parent drops that block's delta and counts a denial. Repairing the live
tail block in the parent instead, with `min_reclaim_pct = 0`, measured with inline repair off
in both arms so the two mechanisms do not mask each other:

| Metric | off | on | Δ |
|---|---|---|---|
| `gc_blocks_denied` | 408,524 / 411,435 | 2,773 / 3,066 | **−99.3%** |
| `bytes_collected` per cycle | 205 / 192 MB | 299 / 310 MB | +55% |
| `num_records` | 85.9 / 88.5 M | 77.5 / 76.2 M | −12% |
| peak RSS | 1049 / 1062 MB | 941 / 976 MB | −9% |
| write ops/s | 28,376 | 28,798 | +1.5% |
| write p50 / p99 / p99.9 | 0.124 / 0.393 / 0.857 ms | 0.122 / 0.400 / 0.788 ms | ~0 |

It is the only thing measured here that moves `gc_blocks_denied`, and it recovers work the
child already paid for.

**Its write cost is unresolved.** The pair above shows none. A later factorial — inline ×
reconcile, interleaved, 2 reps — showed reconciliation pinned at ~12.3k ops/s in both reps
while the arms without it reached 25–28k when the machine was fast, with fork-GC cycle time
179 s → 242 s. Those two measurements contradict each other and I cannot explain the
difference; the GIL stall predicted for parent-side work may well be real. Treat
"reconciliation is free" as unsupported until it is re-measured on a fixed-write-count
harness.

That factorial did answer the interaction question, on garbage retained per million writes:
inline alone −23%, reconciliation alone −21%, both −31%. They overlap on the same tail-block
bytes, as expected, but each still reaches garbage the other misses — roughly 70% additive.

## Follow-ups

- [ ] Fix the key overlap in `search-ftsb-1M-enwiki_abstract-hashes-gc.yml`, or retire it —
      as written it cannot produce garbage.
- [ ] Restate or drop success criterion 2. Inline repair does not reduce `gc_blocks_denied`.
- [ ] Propose the tail-block reconciliation in `apply_gc` as its own change. On this evidence
      it is close to free and independent of inline repair.
- [x] Relax the full-block gate — done in this change; the numbers above are the shipped
      trigger.
- [x] Measure reconciliation and inline repair together — ~70% additive, see above.
- [x] Expose the probe stride as a config — `INLINE_GC_BLOCK_REPAIR_STRIDE`, with `0`
      reproducing the full-block-only trigger and its ~5% cost.
- [ ] Measure the stride between its endpoints. Only 0 and 8 have numbers; the curve between
      them is assumed.
- [ ] Re-measure reconciliation's write cost on a fixed-write-count harness, to settle the
      contradiction above. Round 4 supplies that harness.

## Caveats

- Two reps per arm for the 0-vs-20 comparison and the reconciliation pair; one rep each for
  the threshold sweep. Laptop, no pinning, no CPU frequency control.
- A forced fork-GC cycle takes 2+ minutes on this index, so only one or two cycles complete
  per 180 s run. Denial and reclaim counts come from that small number of cycles.
- Automatic GC scheduling is stopped (`GC_STOP_SCHEDULE`) and cycles are forced, so cycle
  cadence is set by the harness rather than by `FORK_GC_CLEAN_THRESHOLD`.
- Single index, single shard, one document shape.

---

# Round 4: automatic GC scheduling, fixed write count

Every earlier round forced GC on a timer with scheduling stopped, so cycle frequency was set
by the harness rather than by `FORK_GC_CLEAN_THRESHOLD` — the knob an operator actually has.
This round leaves the scheduler alone and bounds each arm by **write count instead of
duration**, so the arms produce the same amount of garbage. That matters: in the earlier
factorial, cells did between 2.17 M and 5.05 M writes, and a faster arm accumulating more
garbage reads as the slower arm looking cleaner.

200 000 enwiki documents, 3 000 003 writes per arm (exact, all eight arms), 4 connections, no
forced GC. Two reps per cell.

| Arm | ops/s | p50 | p99.9 | GC cycles | total GC time | `num_records` | peak RSS | `gc_blocks_denied` |
|---|---|---|---|---|---|---|---|---|
| inline off, threshold 100 | 23.7k | 0.186 ms | 0.85 ms | 1 | 144.7 s | 49.3 M | 849 MB | 375k |
| inline on, threshold 100 | 21.8k | 0.170 ms | 0.63 ms | 1.5 | **93.0 s** | 16.4 M | 618 MB | 204k |
| inline off, threshold 10k | 32.4k | 0.114 ms | 0.47 ms | 1 | 92.8 s | 46.0 M | 815 MB | 409k |
| inline on, threshold 10k | 21.7k | 0.170 ms | 0.68 ms | 1 | **75.1 s** | 21.0 M | 626 MB | 229k |

## Inline repair reduces total fork-GC time

144.7 s → 93.0 s at threshold 100 (−36%), 92.8 s → 75.1 s at threshold 10k (−19%). The
inline arms earn that while running *longer in wall-clock* — they write more slowly, so 3 M
writes take ~138 s against ~93 s — which gives the GC more opportunity, not less. Per second
of wall clock the GC duty cycle roughly halves.

Residency at equal work: 46–49 M records against 16–21 M, peak RSS 815–849 MB against
618–626 MB (−25%).

## Denials do drop here, unlike in round 3

375–409k against 204–229k, roughly halved. Round 3 measured them as unchanged and concluded
inline repair does not touch the denial path. Both are true of their own setup: a denial
fires per *index* whose last block moved since the fork, and under forced GC on a fully
garbage-laden index that is nearly every index either way. With automatic scheduling and a
much smaller index there are fewer indexes with a dirty tail to deny in the first place. The
round-3 phrasing — that inline repair changes how much garbage a denied block holds rather
than whether it is denied — remains the mechanism; the count moves as a second-order effect.

## The threshold lever does not appear at this scale

Raising `FORK_GC_CLEAN_THRESHOLD` from 100 to 10000 left the cycle count at 1 either way. A
cycle on this index takes 75–145 s while 3 M writes take 90–140 s, so the GC is
**duration-bound, not trigger-bound**: it runs one cycle essentially back to back for the
whole run and the trigger never becomes the binding constraint. A threshold cannot reduce a
frequency that is already one-per-run.

The hypothesis it was meant to test — that inline repair slows the growth of
`deletedOrUpdatedDocsFromLastRun` and so makes a higher threshold affordable — is therefore
neither confirmed nor refuted here. It needs a regime where cycles are short relative to
write volume; round 4b uses a 20 000-document index at the same write volume for that.

## Caveats

- `off-t100` is the noisy cell: reps of 14,664 and 32,702 ops/s, a 2.2× spread. Its mean and
  the −36% derived from it are soft. The other three cells agree within ~1% between reps.
- The inline-on arms sit at ~21.7k ops/s regardless of threshold, consistent with the
  write-path cost being a fixed tax. Against the fastest inline-off arm that is a 33%
  throughput cost, above the 26% measured under forced GC in round 3.
- One cycle per arm means the GC numbers rest on a single sample of a long, variable
  operation.
