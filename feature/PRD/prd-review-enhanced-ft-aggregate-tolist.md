# PRD Review: Enhanced FT.AGGREGATE with Whole Document Fetching, In-Group Sorting, Limiting, and Deduplication Control (Raymond James)

> Source: [Confluence](https://redislabs.atlassian.net/wiki/spaces/DX/pages/5888344192)

## PRD

[One Pager: Enhanced FT.AGGREGATE](https://redislabs.atlassian.net/wiki/spaces/DX/pages/5606178817)

## Validity

The proposed solution seems valid.

The capabilities to get the top-K, or to sort and get a window/slice of the documents and to optionally load additional fields, with or without de-duplication, are already supported for documents using the existing global aggregation pipeline. However it is not supported for rows after being grouped/reduced (collapsing one or more document rows into a single row)

The existing global pipeline `SORTBY/LIMIT` can sort/limit **groups**, but it cannot sort/limit the **documents inside a group** after reduction—those documents no longer exist as pipeline rows unless a reducer retains them (which we want to avoid retaining them and adding peak memory and cpu overhead)

By extending the `TOLIST` reducer to support:

- Loading/projecting specific fields or all fields,
- **in-group** `SORTBY` (rank documents inside each group),
- **in-group** `LIMIT offset count` (return a window / top-K per group),
- and `ALLOWDUPS` (dedup control),

we satisfy the PRD and also create a reusable internal result processor ("grouped collection top-K/Window") that can support additional future needs (e.g., "loading from docs behind `MIN/MAX/FIRST_VALUE` reducers" expressed as `TOLIST` with `LIMIT 0 1`, richer per-group document windows, etc.) without introducing a new public reducer/API.

We noted that "ties at boundary", multiple different docs having the same sort key, e.g., same min/max, has different semantic than dedup (`ALLOWDUPS` argument) which is concerned with the doc payload. Those docs are not duplicates and would be returned regardless of `ALLOWDUPS` argument, as long as they're within the OFFSET + LIMIT window. For example, if 15 docs have the same sort keys and the OFFSET+ COUNT is 10, we use a tie-breaker to pick the "best" 10 out of these 15.

And we also note that `MIN/MAX/FIRST_VALUE` will still return a scalar, while the new grouped collection top-K/Window reducer using `TOLIST` would return payload fields.

## Open Ends / Decisions to Take

- Specific `TOLIST` field names or star (`*`) are mandatory (already now a single field is required in `TOLIST`)
- **Tie breaker**
  - On shard - doc id
  - On Coordinator - arbitrary/first-seen, which is less stable although more performant
    - On AGGREGATE we would need to load the key on shard in case we would need to use it for tie-breaking, send it to the coord, process it in the coord and potentially discard it from the response if not needed - better to avoid this
    - Currently also `FIRST_VALUE` tie-break is arbitrary
- **Null values** - as now with `MIN/MAX/FIRST_VALUE` etc. (nulls are last)
- **`TOLIST` Window size** - unrelated to the `AGGREGATE` Window size (when exists) or the `FT.HYBRID` `COMBINE` Window size
  - `TOLIST` Window size (using `LIMIT`) is limiting the number of results **within** the group, not the global pipeline `LIMIT` which is limiting the **number** of groups
- **Partial results due to Timeout** (with `RETURN` policy)
  - Timeout during `GROUPBY` - today yields zero results (to avoid wrong response) - keep this behavior
    - If timeout after groups are calculated but during the `TOLIST` `SORTBY`
      - Can still return zero results - easier
      - Can return partial results (as current `SORTBY` works) - harder to test
- **SORTBY fields (ranking)**
  - Purpose: decide which documents are "best" within each group and therefore included in the requested window.
  - Inputs: only the fields listed under `SORTBY` (e.g., `SORTBY 4 @f3 DESC @f4 ASC`).
  - Comparison: lexicographic across sort keys:
    - compare key #1; if equal compare key #2; ...; if all equal apply tie-break (or accept arbitrary order).
  - Nulls: missing values are treated as _null_ and are ranked _last_ (nulls are worst).
  - Type handling: follow the same coercion/compare behavior as `RSValue_Cmp`.
- **De-duplication (distinct vs `ALLOWDUPS`)**
  - Purpose: decide whether two items in the returned list should be considered the "same" and collapsed (default) or both kept (`ALLOWDUPS`).
  - Inputs: the **payload** returned by `TOLIST`:
    - `TOLIST <N> @field`
      - Payload is the single `field` value.
      - Can be enhanced later on if necessary to support more than a single field
    - `TOLIST <N> *`
      - Payload is the entire document (loaded fields)
  - Key point: dedup is **not** based on the `SORTBY` keys. Two different documents can share the same sort keys but still have different payloads.
  - Composite payload: for `TOLIST *`, payload equality must support deep comparison (maps/arrays), because relying on string serialization isn't stable (map key order isn't guaranteed).

## Tasks

| ID | Part | Task | Depends on | Points | Short design description |
| --- | --- | --- | --- | --- | --- |
| T1 | Standalone/Shard | Extend `TOLIST` args parsing | - | 2 | Parse mandatory `<field\|*>` and optional `ALLOWDUPS` / in-group `SORTBY` / in-group `LIMIT`. Strict validation; preserve legacy `TOLIST 1 @field`. |
| T2 | Standalone/Shard | Enforce explicit loading requirements | T1 | 2 | Require explicit `LOAD *` for `TOLIST *` and explicit availability of in-group sort fields. Fail fast with clear error (no implicit full load). |
| T3 | Standalone/Shard | In-group comparator (multi-field + nulls-last + full-array compare) | T1,T2 | 1 | Implement comparator used by in-group sorting: lexicographic on fields; nulls-last; for arrays compare full array lexicographically (feature-local), plus hooks for tie-breaker. |
| T4 | Standalone/Shard | In-group top-K window result processor | T3 | 3 | Per-group bounded collector with `K=offset+count`, uses T3 comparator, slices requested window on finalize. |
| T5 | Standalone/Shard | Dedup + `ALLOWDUPS` behavior | T4 | 2 | Default distinct (dedup); `ALLOWDUPS` disables dedup. Define equality for dedup and ensure standalone behavior matches PRD. |
| T6 | Standalone/Shard | Tie-breaker (shard: doc id) | T4 | 2 | When sort keys tie: deterministic compare by doc id (avoid first-seen). Ensure doc id is available per T2 (no implicit full load). |
| T7 | Tests (Standalone) | RLTest standalone coverage | T1-T6 | 3 | Tests for: mandatory arg, explicit `LOAD *`, in-group `SORTBY/LIMIT`, nulls-last, dedup vs `ALLOWDUPS`, shard doc-id tie-break. |
| T8 | Cluster-Shard | Shard-side bounded emission (<=K) | T4,T5,T6,T7 | 2 | Ensure each shard emits no more than `offset+count` candidates per group (ranked with shard tie-break). |
| T9 | Cluster-Coordinator | Coordinator merge + rerank + window + dedup | T8 | 5 | Merge per-group lists; deserialize, dedup across shards if not `ALLOWDUPS`; sort globally; apply final `LIMIT offset count`. `SearchResult` remains per Group, but contains a composite structure that needs to be sorted by specific fields |
| T10 | Cluster-Coordinator | Tie-breaker (coord: arbitrary) | T9 | 1 | When sort keys tie after merge: first-seen. |
| T11 | Tests (Cluster) | RLTest cluster coverage | T8-T10 | 3 | Multi-shard tests for: merge correctness, coord dedup, bounded K, coord key-name tie-break. |
| T12 | Perf/Memory | Bounded memory + regression checks | T4,T9 | 2 | Validate memory bounded by groups×K; large offsets; CPU overhead acceptable; no leaks. |
| T13 | Docs | Docs/examples update | | 2 | |

## Estimation

- **Total Points:** 30 points
- **Sprints:** 30/8 ~= 4 sprints (1 developer)
  - Some work can be parallelized
- **Detailed design and breakdown:** 5 points
- **Total:** 5 sprints
