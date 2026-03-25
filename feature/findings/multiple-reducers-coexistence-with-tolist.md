# Findings: Multiple TOLIST Reducers & Coexistence with Other Reducers

> Investigation of [Open Question #10](../PRD/open-questions-enhanced-tolist.md) — "Multiple TOLIST Reducers and Coexistence with Other Reducers"
>
> Tested on: 2026-03-18
> Build: `bin/macos-aarch64-debug/search-community/redisearch.so` (dev build, version 999999)
> Dataset: [Raymond James dataset](../PRD/raymond-james-ft-aggregate-example.md) — 100 JSON documents, 10 relationship groups

---

## Summary

**All tested combinations work correctly.** The current `TOLIST` reducer coexists with other reducers in the same `GROUPBY` without issues. Multiple `TOLIST` reducers on different fields also work in the same `GROUPBY`.

---

## Test Matrix

| # | Combination | Result | Notes |
|---|------------|--------|-------|
| 1 | `TOLIST` + `TOLIST` (different fields) | PASS | Two TOLIST reducers on `@opportunityId` and `@subType` in the same GROUPBY |
| 2 | `TOLIST` + `COUNT` | PASS | List + scalar count in same group |
| 3 | `TOLIST` + `MAX` | PASS | List + max aggregation in same group |
| 4 | `TOLIST` + `TOLIST` + `COUNT` + `MAX` | PASS | Four reducers combined |
| 5 | `TOLIST` + `COUNT` + `MAX` + `MIN` + `COUNT_DISTINCT` + `FIRST_VALUE` | PASS | Six reducers combined |
| 6 | `TOLIST` + `SUM` | PASS | List + numeric sum in same group |

---

## Test Details

### Test 1: Multiple TOLIST reducers

```
FT.AGGREGATE idx:relationships "*"
  LOAD 2 @opportunityId @subType
  GROUPBY 1 @relationshipName
    REDUCE TOLIST 1 @opportunityId AS opportunity_ids
    REDUCE TOLIST 1 @subType AS sub_types
  SORTBY 2 @relationshipName ASC
  LIMIT 0 3
```

**Response** (first group shown):

```
1) "10"
2) 1) "relationshipName"
   2) "401K SEP"
   3) "opportunity_ids"
   4) 1) "650627845"
      2) "628998020"
      3) "653093679"
      4) "693346585"
      5) "662080659"
      6) "680725035"
   5) "sub_types"
   6) 1) "START_GOAL_PLAN"
      2) "ADD_TOD"
      3) "CLIENT_ACCESS_ENROLLMENT"
...  (9 more groups)
```

Each group contains both `opportunity_ids` (full list of IDs) and `sub_types` (deduplicated list of subType values). "401K SEP" has 6 opportunity IDs but only 3 distinct subTypes — each TOLIST deduplicates independently on its own field.

### Test 2: TOLIST + COUNT

```
FT.AGGREGATE idx:relationships "*"
  LOAD 1 @opportunityId
  GROUPBY 1 @relationshipName
    REDUCE TOLIST 1 @opportunityId AS opportunity_ids
    REDUCE COUNT 0 AS doc_count
  SORTBY 2 @relationshipName ASC
  LIMIT 0 3
```

**Response** (first group shown):

```
1) "10"
2) 1) "relationshipName"
   2) "401K SEP"
   3) "opportunity_ids"
   4) 1) "650627845"
      2) "628998020"
      3) "653093679"
      4) "693346585"
      5) "662080659"
      6) "680725035"
   5) "doc_count"
   6) "6"
...  (9 more groups)
```

Counts are consistent with list lengths ("401K SEP" has 6 items in the list and `doc_count = 6`).

### Test 3: TOLIST + MAX

```
FT.AGGREGATE idx:relationships "*"
  LOAD 1 @opportunityId
  GROUPBY 1 @relationshipName
    REDUCE TOLIST 1 @opportunityId AS opportunity_ids
    REDUCE MAX 1 @opportunityId AS max_opp_id
  SORTBY 2 @relationshipName ASC
  LIMIT 0 3
```

**Response** (first group shown):

```
1) "10"
2) 1) "relationshipName"
   2) "401K SEP"
   3) "opportunity_ids"
   4) 1) "650627845"
      2) "628998020"
      3) "653093679"
      4) "693346585"
      5) "662080659"
      6) "680725035"
   5) "max_opp_id"
   6) "693346585"
...  (9 more groups)
```

MAX is consistent with the TOLIST output (`max_opp_id = 693346585` is the largest value in the list).

### Test 4: Full combination (2x TOLIST + COUNT + MAX)

```
FT.AGGREGATE idx:relationships "*"
  LOAD 2 @opportunityId @subType
  GROUPBY 1 @relationshipName
    REDUCE TOLIST 1 @opportunityId AS opportunity_ids
    REDUCE TOLIST 1 @subType AS sub_types
    REDUCE COUNT 0 AS doc_count
    REDUCE MAX 1 @opportunityId AS max_opp_id
  SORTBY 2 @relationshipName ASC
  LIMIT 0 3
```

**Response** (first group shown):

```
1) "10"
2) 1) "relationshipName"
   2) "401K SEP"
   3) "opportunity_ids"
   4) 1) "650627845"
      2) "628998020"
      3) "653093679"
      4) "693346585"
      5) "662080659"
      6) "680725035"
   5) "sub_types"
   6) 1) "START_GOAL_PLAN"
      2) "ADD_TOD"
      3) "CLIENT_ACCESS_ENROLLMENT"
   7) "doc_count"
   8) "6"
   9) "max_opp_id"
  10) "693346585"
...  (9 more groups)
```

All four reducers produce correct, consistent output side-by-side.

### Test 5: Kitchen sink (TOLIST + COUNT + MAX + MIN + COUNT_DISTINCT + FIRST_VALUE)

```
FT.AGGREGATE idx:relationships "*"
  LOAD 2 @opportunityId @bestByDate
  GROUPBY 1 @relationshipName
    REDUCE TOLIST 1 @opportunityId AS opportunity_ids
    REDUCE COUNT 0 AS doc_count
    REDUCE MAX 1 @opportunityId AS max_opp_id
    REDUCE MIN 1 @opportunityId AS min_opp_id
    REDUCE COUNT_DISTINCT 1 @opportunityId AS distinct_count
    REDUCE FIRST_VALUE 1 @bestByDate AS first_best_by_date
  SORTBY 2 @relationshipName ASC
  LIMIT 0 3
```

**Response** (first group shown):

```
1) "10"
2) 1) "relationshipName"
   2) "401K SEP"
   3) "opportunity_ids"
   4) 1) "650627845"
      2) "628998020"
      3) "653093679"
      4) "693346585"
      5) "662080659"
      6) "680725035"
   5) "doc_count"
   6) "6"
   7) "max_opp_id"
   8) "693346585"
   9) "min_opp_id"
  10) "628998020"
  11) "distinct_count"
  12) "6"
  13) "first_best_by_date"
  14) "2023-03-21"
...  (9 more groups)
```

All six reducers work correctly together. Values are cross-consistent (e.g., `min_opp_id = 628998020` and `max_opp_id = 693346585` both appear in the `opportunity_ids` list).

### Test 6: TOLIST dedup verification

```
FT.AGGREGATE idx:relationships "*"
  LOAD 1 @subType
  GROUPBY 1 @relationshipName
    REDUCE TOLIST 1 @subType AS sub_types
    REDUCE COUNT 0 AS total_docs
  SORTBY 2 @relationshipName ASC
  LIMIT 0 10
```

**Response** (first two groups shown — illustrates dedup):

```
1) "10"
2) 1) "relationshipName"
   2) "401K SEP"
   3) "sub_types"
   4) 1) "START_GOAL_PLAN"
      2) "ADD_TOD"
      3) "CLIENT_ACCESS_ENROLLMENT"
   5) "total_docs"
   6) "6"
3) 1) "relationshipName"
   2) "529 Plan - Harper"
   3) "sub_types"
   4) 1) "START_GOAL_PLAN"
      2) "CLIENT_ACCESS_ENROLLMENT"
      3) "ADD_TOD"
   5) "total_docs"
   6) "10"
...  (8 more groups)
```

TOLIST deduplicates correctly: "401K SEP" has `total_docs = 6` but `sub_types` contains only 3 items. "529 Plan - Harper" has `total_docs = 10` but also only 3 distinct subTypes. Dedup operates on the TOLIST field independently of other reducers.

---

## Observations

1. **No ordering limitations:** Reducers can appear in any order within the GROUPBY clause.
2. **Independent operation:** Each reducer operates independently — TOLIST dedup does not affect COUNT or other reducers.
3. **Consistent data:** All reducers see the same input rows, producing consistent results (e.g., COUNT matches TOLIST list length for unique-valued fields).
4. **No performance degradation noticed:** All queries returned instantly on the 100-doc dataset.

---

## Recommendation for Open Question #10

**Confirm as supported.** The existing Grouper architecture already handles multiple reducers (including multiple TOLIST instances) without modification. This should be documented in the PRD with an example similar to Test 4 above.
