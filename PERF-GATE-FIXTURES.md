# Perf-regression detection: known-answer fixture branches

Purpose: give the benchmark automation in `redisearch-benchmarks` a set of RediSearch refs
whose performance answer we already know, so we can measure the *detector* rather than the
code. Each fixture is a branch off one pinned base commit, carrying exactly one deliberate
perf change (or none), localised to one query type's hot path.

Nothing here is ever merged. Every commit subject starts with `DO NOT MERGE:`.

Everything in this document is decided. There is no calibration step and no measurement
gate: the branches get created as specified and the first campaign reports what they are
worth. Where a knob had a plausible range, the value below is the choice, not a suggestion.

## Campaign parameters (fixed for the whole set)

| | |
|---|---|
| Base commit | `1b21183483` (`origin/master`, 2026-08-20) |
| Architecture | `x86_64` |
| Setup | `oss-standalone` |
| `remote_setup` | `redisearch-m7` |
| `repetitions` | 5 per dispatch |
| `lto` | repo default — leave the input empty |
| `source_repo` | `RediSearch/RediSearch` |

`remote_setup` is fixed for the entire campaign, per the warning in
`repeat-benchmark-until-confident.yml`: RTS labels do not record instance class, so mixing
`redisearch-m7` and `redisearch-m7i-metal` samples silently blends two populations into one
series. `lto` is left at the repo default rather than pinned to 0 or 1, because the fixtures
should mirror the build configuration the gate will actually run against — a detector
validated on a non-production build shape has been validated on the wrong thing.

## What we are trying to learn

The framework (`Run Benchmark` + `Repeat Benchmark Until Confident` +
`scripts/assess_samples.py`) samples ops/sec per (test, sha, setup, arch) into
RedisTimeSeries and stops when the 95% CI half-width is within `precision` (default 5%,
`min_n=5`, `n_max=10`). Four questions:

1. **Sensitivity** — does an obvious regression get flagged?
2. **Specificity** — does an unchanged binary stay quiet? (false-positive rate)
3. **Attribution** — when one path regresses, do the *other* benchmarks stay flat, and do
   the benchmarks that *share* that path all move together?
4. **Resolution** — what is the smallest real shift it can call?

Question 4 is framed so it needs no pre-measurement. The `small` tier's true magnitude is an
**output** of the campaign, not an input: run those branches to a high sample count to
establish the real delta, then ask whether the detector's verdict at `n=5–10` agrees with
it. That is the question that matters for gating, and it does not require knowing in advance
whether the injection is worth 4% or 14%. If a `small` fixture turns out to land near
`obvious`, halve it later by applying the extra work on alternate records — a follow-up
branch, not a prerequisite.

Question 3 is why each regression is confined to code that only one case executes, and why
the benchmark set deliberately includes a **near-replicate pair** (B1/B2) and a
**shared-path sibling** (D shares the numeric reader with B).

## Benchmarks measured

Chosen for: mostly-disjoint hot paths, cheap datasets (these get run repeatedly, so no
6M-doc msmarco / 5.2M union-iterator sets), and coverage of **both** client tools, because
memtier and `ftsb_redisearch` feed different metrics into RTS and the framework's extraction
path differs per tool.

| # | Query type | Spec (`tests/benchmarks/`) | Client | Hot path exercised |
|---|---|---|---|---|
| **A** | Full-text single term | `search-ftsb-1M-enwiki_abstract-hashes-fulltext-simple-1word-query.yml` | ftsb, 64 workers | term posting-list decode (`codec/full.rs`, freqs+fields+offsets) → TFIDF scorer → top-10 heap |
| **B1** | Numeric range + SORTBY ASC, count only | `search-numeric-sortby.yml` (`… @trip_distance:[6,8] SORTBY trip_distance LIMIT 0 0`) | memtier, `-c 32` | numeric range tree → range-reader union → `FilterNumericReader` per-entry check. No scoring, no doc load. |
| **B2** | Same, SORTBY DESC | `search-numeric-sortby-desc.yml` | memtier, `-c 32` | **identical** to B1 |
| **C** | Aggregate GROUPBY + COLLECT | `search-groupby-collect-10K-entity-events-hash-cached-sortby-fields-tags-k50.yml` | ftsb, 16 workers | wildcard scan over 10K docs → `RLookup_LoadDocumentIndividual` (`LOAD 5`) → `group_by.c` per-row key + COLLECT → sorter |
| **D** | TAG ∩ NUMERIC + `LOAD *` + top-K | `search-filtering-tag-numeric.yml` (`FT.AGGREGATE idx:cardinality "@tag_field:{1} @numeric_field:[1 1000000]" LOAD * SORTBY 2 @numeric_field DESC MAX 1000`) | memtier, `-c 32` | tag inverted index → intersection → **`RLookup_LoadDocumentAll` per matching row** → `MAX 1000` top-K |

Four branch-bearing cases, five benchmarks. Two structural notes:

**B2 gets no branches of its own.** `search-numeric-sortby-desc` is byte-for-byte
`search-numeric-sortby` apart from the `DESC` keyword — same dataset, same index, same
filter — and with `LIMIT 0 0` nothing is materialised, so the comparator sign is the only
difference. Giving it its own `small` branch would mean making the identical source edit
twice. It is far more useful as a **replicate**: two benchmarks that must always move
together at the same magnitude, so if the detector calls one and not the other, that is a
direct read on its behaviour at that effect size, for zero extra branches. (If their
*baselines* differ materially, that is a finding about the workload, not the detector.)

**D shares the numeric reader with B**, so it is not a clean negative for the
`numeric-range-*` branches; the matrix predicts the diluted movement. That overlap is
deliberate — a regression that shows up strongly in one benchmark and weakly in another is
what real regressions look like, and it is the harder attribution case.

Deliberately *not* chosen: `vecsim-*` (cost lives in the VectorSimilarity submodule, so a
RediSearch-side injection is hard to size) and the union-iterator specs (5.2M docs, and
their queries mix TEXT + NUMERIC). `search-numeric.yml` (B without `SORTBY`) and
`search-filtering-tag-numeric-filter-pipeline.yml` (D with the numeric predicate moved into
a pipeline `FILTER`) are the natural later additions; they are left out to hold the campaign
at five benchmarks.

The five span three latency scales — A is a cheap query at high throughput (noisiest), B is
mid-cost, C and D are expensive per-query workloads — so the matrix also shows how the
detector's resolution varies with the noise floor.

### Verified query shapes for C and D

C's queries come from an S3 CSV, but they are generated by
`tests/benchmarks/scripts/generate_groupby_collect_dataset.py`, so the shape is known
without downloading anything. For the `collect-fields-tags-k50` variant it emits:

```
FT.AGGREGATE idx:entity_events * LOAD 5 @entityName @type @target @hasNotes @processed
  GROUPBY 1 @entityName REDUCE COLLECT <n> <tag fields> AS events
  SORTBY 2 @entityName ASC LIMIT 0 50
```

Two consequences, and both are load-bearing for the localisation claims below:

- The filter is `*` — a wildcard, **no TEXT term**. So A's injection into the term codec
  cannot leak into C.
- C uses `LOAD 5`, i.e. `RLookup_LoadDocumentIndividual`; D uses `LOAD *`, i.e.
  `RLookup_LoadDocumentAll`. Different functions in
  `rlookup/src/load_document/hash.rs`, so D's injection into `load_all` cannot leak into C —
  provided the D-noop refactor also stays inside `load_all` and does not touch the shared
  `hval_to_value` / `write_key` tail.

## Branch layout

Base every branch on `1b21183483` and record that SHA in each commit message. The branches
live in `RediSearch/RediSearch` (the workflow's default `source_repo`), so the naming is what
keeps them out of trouble: the shared `perf-fixture/` prefix makes the whole set greppable
and obviously not feature work, and every commit subject starts with `DO NOT MERGE:`. No PR
is ever opened for any of them — a fixture branch that acquires a PR has stopped being a
fixture. The prefix deliberately skips the usual `<handle>-` convention because these are
shared instruments rather than one person's WIP; there is no collision risk inside a
namespace nothing else uses.

Two consequences of living in the main repo, both worth handling up front:

- They are **long-lived and must not be garbage-collected**. Any stale-branch cleanup, and
  anyone auditing old branches, needs to know these are load-bearing — hence this file.
- They are pinned to a base SHA and **never rebased**. Refreshing the base means a new
  campaign, because RTS series are keyed by commit: rebasing silently starts a new series
  and orphans every sample collected so far.

```
perf-fixture/baseline                  1b21183483 + this document only — the reference series
perf-fixture/null-control              base + a comment appended to README.md
perf-fixture/smoke-sleep-everything    usleep(1000) per query — plumbing check, B1 only

perf-fixture/fulltext-term-obvious         case A
perf-fixture/fulltext-term-small
perf-fixture/fulltext-term-noop

perf-fixture/numeric-range-obvious         case B — scored on both B1 (ASC) and B2 (DESC)
perf-fixture/numeric-range-small
perf-fixture/numeric-range-noop

perf-fixture/aggregate-groupby-obvious     case C
perf-fixture/aggregate-groupby-small
perf-fixture/aggregate-groupby-noop

perf-fixture/tag-numeric-load-obvious      case D
perf-fixture/tag-numeric-load-small
perf-fixture/tag-numeric-load-noop
```

Fifteen branches: four cases x three tiers, plus `baseline`/`null-control` (the controls that
make a "no regression" verdict mean something) and `smoke-sleep-everything` (one cheap run
that proves the pipeline can report *anything* before we spend EC2 hours on subtlety).

## The regressions

### Injected cost must be non-eliminable

A trap worth stating before the tiers, because getting it wrong makes a fixture silently
worthless: most of the redundant work below is *dead* by construction — a decode whose
result is immediately overwritten, a pure predicate evaluated three times, a lookup whose
result is discarded. LLVM removes exactly that. A fixture that compiles down to the
baseline would report "flat", and we would conclude the detector missed nothing when in
fact there was nothing to detect.

So every injected pass is anchored: `std::hint::black_box` on the Rust side, a
`static volatile uint64_t` sink on the C side. That is unashamedly not code anyone would
merge, which is the right trade — these branches exist to carry a cost, not to look
plausible in review. The two injections that need no anchor are called out where they
appears: `aggregate-groupby-small` adds an `rm_malloc`/`rm_free` pair, an indirect call
through `RedisModule_Alloc`, which cannot be elided.

### Tier `obvious` — repeat the dominant per-document work 3x

Not a sleep. A per-query sleep has to be sized against a baseline latency we are not going
to measure (B's query is ~ms-scale, A's is ~100µs-scale, so one constant cannot serve both),
and `usleep` on the Redis main thread stalls the event loop and distorts the latency
distribution shape, which is exactly what the detector reads. A 3x multiplier on real work
needs no sizing and keeps the profile realistic. Each is written so the extra passes are
provably side-effect-free:

- **A** `inverted_index/src/reader/core.rs` — in `next_record`, decode the record 3x, keeping
  the last result; the extra passes save and restore `buf_pos`.
- **B** `inverted_index/src/reader/numeric.rs` — in `FilterNumericReader::next_record`, run
  the `value_in_range` filter path 3x per entry. Pure function on an already-decoded value.
- **C** `src/aggregate/group_by.c` — in `invokeGroupReducers`, do the `RLookupRow_Get` +
  `RSValue_Hash` key-building pass 3x per row; reducers still invoked once. Pure reads.
- **D** `src/result_processor.c` — in `rpLoader_loadDocument`, call
  `RLookup_LoadDocumentAll` three times on the row, keeping the last return code. Repeated
  loads are idempotent: `RLookupRow::write_key` replaces the slot and returns the previous
  `SharedValue`, which the caller drops, and it only increments `num_dyn_values` when the
  slot was empty. So the third load leaves exactly the row the first one produced.
  (An earlier draft used a throwaway scratch `RLookupRow`. That is not available from C —
  `struct RLookupRow` is only forward-declared in `rlookup_ffi.h`, so its size is unknown
  to the C side and it cannot be stack-allocated.)

`smoke-sleep-everything` is the crude version: `usleep(1000)` at the top of
`RSSearchCommand` and `RSAggregateCommand` in `src/module.c`. Those are the standalone
handlers, which is what `oss-standalone` runs; `DistSearchCommand` / `DistAggregateCommand`
are the coordinator entry points and answer `CLUSTERDOWN` when `NumShards == 0`, so a sleep
there would never execute. Expect a >10x collapse. It is a plumbing check, not a matrix
row — run it on B1 only, once, then park it.

### Tier `small` — one realistic pattern per case

Magnitudes are unknown by design (see question 4). Each is a real regression shape with a
per-unit cost that scales with rows or fields, so all four are plausibly in single-digit to
low-double-digit percent, and all four are provably behaviour-neutral.

- **A — term codec pays for its offsets twice.**
  In `inverted_index/src/codec/full.rs` (freqs+fields+offsets, which is what the enwiki TEXT
  fields use), decode the offsets blob and then re-decode it before returning, on every
  record. Realistic shape: a peek-then-read refactor. Localised to TEXT terms; C's filter is
  `*` and B/D have no TEXT term, so A is the only benchmark affected.

- **B — numeric codec pays for each record twice.**
  In `inverted_index/src/codec/numeric.rs`, the shared `decode` becomes a peek pass that
  decodes the record and rewinds, followed by the real decode. Deliberately the same shape
  as A-small one layer over, and it covers both numeric encodings, since `Numeric` and
  `NumericFloatCompression` both delegate to that one function. Localised to numeric
  records, so it hits B and — diluted — D's numeric leg.

  *An earlier draft used `_NUMERIC_RANGES_PARENTS` 0 → 1 (`.numericTreeMaxDepthRange` in
  `src/config.h`). That was wrong, and wrong in the direction that matters: it is not a
  read-path pessimisation at all.* `NumericRangeTree::find` substitutes a node's range for
  its subtree's leaves **only when the range is `contained_in` the filter bounds**, and a
  retained internal range is kept in sync on the way down (`add_without_cardinality` in
  `tree/insert.rs`), so it holds exactly the same entries as the leaves it replaces. The
  query therefore reads the same entries and unions *fewer* iterators — the setting is an
  optimisation, not a cost. What it does cost is the write path: every insert also writes
  into each retained ancestor, roughly doubling numeric index writes and memory. That lands
  in the dataset-load phase, which a `read-only` benchmark does not measure. As a fixture it
  would have reported flat, or slightly faster, and we would have blamed the detector.

- **C — per-row heap allocation for the group key.**
  In `src/aggregate/group_by.c` `invokeGroupReducers`, replace the VLA
  `const RSValue *groupvals[nkeys]` with `rm_malloc(nkeys * sizeof(*groupvals))` + `rm_free`.
  One malloc/free pair per input row, and this workload pushes all 10K docs through the
  grouper on every query. Extremely realistic — VLAs get replaced by heap allocations for
  portability or stack-safety reasons all the time.

- **D — `LOAD *` looks up each field name twice.**
  In `rlookup/src/load_document/hash.rs`, the `load_all` scan closure calls
  `rlookup.find_key_by_name(field_cstr)` once per field per row. Make it a check-then-get
  pair: look the name up, discard the cursor, look it up again, on every field. Realistic
  shape (a "does it exist? then fetch it" refactor) and the cost scales with fields x rows,
  which for this query is every field of tens of thousands of rows.

### Tier `noop` — expected flat

Not a docs-only change: each `noop` branch makes a **semantics-preserving edit to the same
hot function its sibling regresses**, so the binary's codegen and code layout shift while the
work does not. That is the harder and more useful null test — it asks whether the detector
survives layout noise, not just whether it survives a no-op build.

- A: `codec/full.rs` + `reader/core.rs` — rename locals, extract named `const`s for the
  literals, split a long function into two `#[inline(always)]` halves, add doc comments.
- B: `query_eval/src/nodes/numeric.rs` + `numeric_range_tree` — same treatment.
- C: `group_by.c` — extract the `groupvals` fill loop in `invokeGroupReducers` into a
  `static inline` helper, rename locals, add comments.
- D: `load_document/hash.rs` — extract the field-name-to-`CStr` conversion and the
  coerce-and-write tail into helpers **called only from `load_all`**, rename locals. Leave
  `hval_to_value` and `write_key` alone: they are shared with the individual-load path C
  uses, and touching them would put a codegen change into C's hot path too.

`perf-fixture/null-control` is the stricter companion: base SHA plus a comment appended to
`README.md`, which compiles to an identical binary. Any movement there is pure measurement
noise — instance variance, build variance, dataset load variance — and it calibrates what
"flat" means for the other fourteen.

## Expected result matrix

Δ ops/sec vs `perf-fixture/baseline` (negative = slower). "flat" = within the `null-control`
band. B1 and B2 must always agree. The `small` rows carry no percentage on purpose — their
magnitude is what the campaign is for; the prediction is the *direction* and *which columns
move*.

| Branch | A fulltext | B1 numeric ASC | B2 numeric DESC | C groupby | D tag+numeric `LOAD *` |
|---|---|---|---|---|---|
| `baseline` | — | — | — | — | — |
| `null-control` | flat | flat | flat | flat | flat |
| `smoke-sleep-everything` | not run | −90%+ | not run | not run | not run |
| `fulltext-term-obvious` | −50…−70% | flat | flat | flat | flat |
| `fulltext-term-small` | **regress** | flat | flat | flat | flat |
| `fulltext-term-noop` | flat | flat | flat | flat | flat |
| `numeric-range-obvious` | flat | −50…−70% | −50…−70% | flat | **regress, diluted** |
| `numeric-range-small` | flat | **regress** | **regress** | flat | **regress, diluted** |
| `numeric-range-noop` | flat | flat | flat | flat | flat |
| `aggregate-groupby-obvious` | flat | flat | flat | −50…−70% | flat |
| `aggregate-groupby-small` | flat | flat | flat | **regress** | flat |
| `aggregate-groupby-noop` | flat | flat | flat | flat | flat |
| `tag-numeric-load-obvious` | flat | flat | flat | flat | −50…−70% |
| `tag-numeric-load-small` | flat | flat | flat | flat | **regress** |
| `tag-numeric-load-noop` | flat | flat | flat | flat | flat |

Two cells are deliberately not "flat", and they are the most interesting ones:

- **`numeric-range-obvious` on D** — D's numeric child iterator does pay the 3x per-entry
  filter cost, but the numeric scan is a minority of D's query time (the `LOAD *` of tens of
  thousands of rows dominates), so the same source change should show up several times weaker
  here than in B. That is the realistic presentation of a shared-path regression, and it asks
  whether the detector resolves a diluted signal.
- **`numeric-range-small` on D** — the same shared-path dilution as the row above, at the
  smaller magnitude. D decodes numeric records too, but they are a thin slice of a query
  whose cost is dominated by `LOAD *`. This is the most likely cell in the whole matrix to
  sit below the detector's floor, which is exactly what makes it worth measuring.

The detector is scored on this matrix: positives it missed, flats it flagged, disagreement
between B1 and B2, and — for the `small` rows — whether `assess_samples.py` reached
`CONVERGED` at all or bailed out `NOT_GATEABLE` at `n_max`.

## Verification before any fixture is used

A fixture is only valid if it changes **speed and nothing else** — but 15 full Python suite
runs is not a good use of the time, so the checks are triaged by what can actually break:

1. **All 15 branches:** `./build.sh` clean, `make lint` clean.
2. **The four `small` branches only:** `./build.sh RUN_PYTEST`. These are the ones that could
   plausibly alter behaviour — a changed config default, a changed allocation, a changed
   lookup sequence — so they get the full suite. The `obvious` and `noop` tiers only repeat or
   re-shape work that already happens, and step 3 catches a mistake there.
3. **All 12 regression branches:** reply equivalence. Load a small dataset on `baseline` and
   on the branch, run that case's exact benchmark query, diff the full replies byte-for-byte.
   For B, diff replies only — `FT.PROFILE` counters are expected to differ there and only
   there.
4. Record the mechanism and the predicted matrix row in the commit message, so a later CI
   number can be read against an intent rather than reverse-engineered from a diff.

## Execution order

61 (branch, benchmark) pairs at 5 repetitions each, so ~305 runs, each a dataset load plus
120 s of measurement on an on-demand EC2 instance. Three phases:

1. **Plumbing** — `baseline` and `null-control` on all five benchmarks, plus
   `smoke-sleep-everything` on B1. 11 pairs. Answers "does a huge regression get reported,
   and how wide is the noise band?"
2. **Sensitivity + attribution** — the four `obvious` and four `noop` branches on all five
   benchmarks. 40 pairs. The main matrix, and the only phase needing the full cross product.
3. **Resolution** — the four `small` branches through `Repeat Benchmark Until Confident` with
   `precision=5, batch_size=5, n_max=10`, which also tells us how many samples a modest call
   actually needs. Pruned: each `small` branch gets its own benchmark, its replicate or
   shared-path sibling where one exists (B → B1+B2+D), and **one** unrelated benchmark as a
   negative. 10 pairs.

Dispatch shape. The `benchmark_filter` glob for case B picks up both the ASC and DESC specs
in one dispatch, and `test_regex` excludes the two `-optimized` variants the glob also
matches — those are `WITHOUTCOUNT` queries on a different 1M-key dataset that go through the
query optimizer instead of the plain numeric scan, and they are not part of this campaign:

```bash
gh workflow run "Run Benchmark" \
  --repo redis-performance/redisearch-benchmarks \
  -f source_repo=RediSearch/RediSearch \
  -f git_ref=perf-fixture/numeric-range-small \
  -f architecture=x86_64 \
  -f allowed_setups=oss-standalone \
  -f benchmark_filter='search-numeric-sortby*.yml' \
  -f test_regex='^search-numeric-sortby(-desc)?$' \
  -f repetitions=5
```
