# Intersection Iterator Rust Port — Detected Issues & Plan

This document summarises the issues found during analysis of E2E test failures introduced
by replacing the C `IntersectionIterator` with its Rust counterpart.

The goal is to keep every fix **scoped to the intersection iterator code** — i.e.
files under `src/redisearch_rs/c_entrypoint/iterators_ffi/` and
`src/redisearch_rs/rqe_iterators/` — and avoid touching unrelated code.

---

## Issue 1 — `iteratorFactor` child-sorting heuristic (confirmed)

### Failing test
`test_issues::test_mod5910`

### What the test checks
`FT.PROFILE` output for the query `(@n:[1 3] (@t:one | @t:two))`.
It asserts the *order* of children inside the intersection profile:

| Scenario | Expected first child | Expected second child |
|----------|---------------------|-----------------------|
| default config | UNION (est=2) | NUMERIC (est=3) |
| `_PRIORITIZE_INTERSECT_UNION_CHILDREN=true` | NUMERIC (est=3) | UNION (adj. est=4) |

### Root cause
The C constructor sorted children with a weighted comparator:

```c
static inline double iteratorFactor(const QueryIterator *it) {
  if (it->type == INTERSECT_ITERATOR)
    return 1.0 / ((IntersectionIterator *)it)->num_its;   // divide — deprioritise deep intersects
  if (it->type == UNION_ITERATOR && RSGlobalConfig.prioritizeIntersectUnionChildren)
    return (double)((UnionIterator *)it)->num;             // multiply — deprioritise wide unions
  return 1.0;
}
// sort key = NumEstimated(it) * iteratorFactor(it)
```

The Rust `Intersection::new` sorts only by raw `num_estimated()`, ignoring the factor.
With `prioritizeIntersectUnionChildren=true`, the UNION iterator (2 children × 2 = 4)
should rank *after* the NUMERIC iterator (3), but the current Rust code puts UNION first.

### Note on correctness
The factor is a **performance heuristic** — it controls which child acts as the pivot in
the merge loop. It does *not* affect which documents are returned, only how many `SkipTo`
calls are made. The first assertion (`test_mod5910`) already passes with the current code;
only the config-dependent second assertion fails.

### Proposed fix

**Changes confined to the intersection crate family only.**

1. **`rqe_iterators/src/intersection.rs`** — add `Intersection::new_presorted`:
   - identical to `new`, but skips the internal `sort_by_cached_key` call.

2. **`iterators_ffi/src/intersection.rs`** — apply the factor before creating `CRQEIterator`s:
   - Sort `kept_valid: Vec<NonNull<QueryIterator>>` by `NumEstimated * iteratorFactor`.
   - `iteratorFactor` for each raw pointer:
     - `INTERSECT_ITERATOR` → call `GetIntersectionIteratorNumChildren(ptr)` (our own exported function) → factor = `1.0 / num_children`
     - `UNION_ITERATOR` and `RSGlobalConfig.prioritizeIntersectUnionChildren` is true → access `UnionIterator.num` → factor = `num as f64`
     - everything else → `1.0`
   - Only sort when `!in_order` (mirrors C behaviour).
   - Call `Intersection::new_presorted` so no second sort is applied.

3. **`ffi/build.rs`** — add `union_iterator.h` to the bindgen header list so
   `UnionIterator` (and its `num` field) becomes available to Rust.
   The `heap_t *heap_min_id` field will be opaque but still pointer-sized — layout is correct.

---

## Issue 2 — `is_within_range` skips nested-aggregate offsets (suspected)

### Failing tests
Any test combining **nested intersections** with a **slop / in-order constraint** on the
*outer* intersection.  `testIssue_884` is **not** affected (outer intersection has slop=-1).

### Root cause
`RSIndexResult::is_within_range` in `inverted_index/src/index_result/core.rs` only collects
offset iterators from direct `Term` children:

```rust
if let Some(term) = child.as_term() {   // ← ignores Intersection/Union children
    iters.push(OffsetPositionIterator::new(term.offsets()));
}
```

The C `IndexResult_IsWithinRange` uses `RSIndexResult_HasOffsets` + `RSIndexResult_IterateOffsets`,
which **recurse** into nested Intersection/Union aggregates:

```c
case RSResultData_Intersection:
case RSResultData_Union:
    // has offsets if kind_mask ≠ Virtual and ≠ Numeric
    return mask != RSResultData_Virtual && mask != RS_RESULT_NUMERIC;
```

A query like `(A (B C))=>{$slop: 2}` creates an outer intersection whose children are a
Term `A` and a nested intersection result for `(B C)`.  The Rust code would silently ignore
`(B C)`'s offset data, effectively behaving as if there were only one term, and always
returning `true` from `within_range_*`.

### Proposed fix

**Change confined to `inverted_index/src/index_result/core.rs` only.**

Replace the `Term`-only loop in `is_within_range` with a recursive offset-collector that
mirrors `RSIndexResult_HasOffsets` + `RSIndexResult_IterateOffsets`:

- For `Term` children: push an `OffsetPositionIterator` as today.
- For `Intersection`/`Union` children (if their kind_mask is not purely Virtual/Numeric):
  recurse into their aggregate records and collect offsets from any `Term` leaves.

---

## Issue 3 — `testIssue_884` Python test fails (open / under investigation)

### Situation
- The Rust unit test `test_issue_884` passes.
- The Python E2E test `testIssue_884` still fails with "too few results (0 instead of 1)".

### What has been ruled out
- Memory layout mismatch — `types_rs.h` confirms identical layouts for `RSIndexResult`,
  `RSResultData`, `RSTermRecord`, `RSOffsetSlice`/`RSOffsetVector`.
- Algorithm bug — the Rust `find_consensus`, `agree_on_doc_id`, `within_range_in_order`,
  `within_range_unordered` are all confirmed equivalent to their C counterparts.
- `freq` / `field_mask` accumulation — `push_borrowed` correctly accumulates both.
- `iteratorFactor` — only affects *ordering*, not which documents are returned.

### Remaining hypotheses (to be verified by running the test with debug output)
1. The `RQEIteratorWrapper`'s `atEOF` field is not reset in the `read`/`skip_to` callbacks
   when returning a document (C callers may check `atEOF` after a successful call).
2. The C wrapper reads `header.current` but the Rust wrapper sets it to a pointer to
   `wrapper.inner.result`; if the result is moved when the wrapper is reallocated, the
   stored pointer becomes stale.  (Unlikely since the wrapper is boxed.)
3. A caller in C reads `lastDocId` before calling `Read` on the first iteration — if
   `lastDocId` starts at 0 and the first doc_id is also 0 (unlikely with real data), a
   skip-ahead could misfire.
4. The C post-processing code (scorer, highlighter) calls `AggregateResult_GetRecordsSlice`
   and iterates children — needs to verify this works with `Borrowed` variant at runtime.

### Next step
Build with `DEBUG=1` and run `./build.sh RUN_PYTEST TEST=testIssue_884` to get the
actual failure message.  Add `tracing::debug!` to `NewIntersectionIterator`, `read`, and
`skip_to` callbacks to trace what the Rust iterator returns during the failing query.

---

## Summary table

| Issue | Failing test(s) | Correctness impact | Fix scope | Status |
|-------|----------------|-------------------|-----------|--------|
| 1 — iteratorFactor sort | `test_mod5910` (2nd assertion) | None (perf only) | `iterators_ffi`, `rqe_iterators`, `ffi/build.rs` | Ready to implement |
| 2 — nested agg offsets | tests with nested slop | Yes — wrong slop results | `inverted_index/core.rs` | Ready to implement |
| 3 — testIssue_884 E2E | `testIssue_884` | Yes — 0 results | Unknown | Needs runtime data |
