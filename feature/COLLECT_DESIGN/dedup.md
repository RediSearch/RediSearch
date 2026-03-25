# COLLECT Deduplication

## Assumption

For documents with the same payload, the sort keys will be the same.

## Proposed Flow

### 1. Early-out: Check against heap minimum

Before any dedup comparison, check if the entry's sort keys are worse than the heap's current worst (via `mmh_peek_min()`). If the heap is full and the new entry wouldn't be inserted anyway, skip dedup entirely — no cost.

### 2. Dedup against heap entries

If the entry qualifies for insertion (heap not full, or better than worst), scan existing heap entries and compare against each. Use sort keys as a fast-path filter: if sort keys differ, entries are not duplicates — skip the deep comparison. Only perform the deep comparison when sort keys match.

### 3. Insert if unique

If no duplicate found, insert into the heap (or exchange with worst).

## What Exists Today

### `RSValue_Cmp` / `RSValue_Equal`

- **Scalars** (Number, String, RedisString): full comparison support.
- **Array**: `RSValue_CmpNC` compares **only the first element** (`compare_arrays_first`). A full `compare_arrays` exists but is unused (marked TODO for SORTABLE).
- **Map** (`RSValueType_Map`): **no comparison** — `RSValue_CmpNC` returns 0 for maps, making all maps "equal".

### `RSValue_Hash`

- Scalars: hashed by value.
- Array: recursively hashes all elements.
- Map: recursively hashes all key-value pairs.

Hash works for maps, but equality comparison does not.

### Heap (`mm_heap_t`)

- `mmh_peek_min()` / `mmh_peek_max()`: access worst/best entry — usable for early-out.
- Internal `data[]` array (1-indexed, `count` entries): entries can be iterated via `h->data[1..h->count]` for linear scan. No public iteration API, but the struct is exposed in the header.

### TOLIST Dedup (reference pattern)

`to_list.c` uses a `dict` with `RSValue_Hash` + `RSValue_Equal` for single-field dedup. Works for scalar values. Would not produce correct results for maps (since `RSValue_Equal` delegates to `RSValue_CmpNC` which returns 0 for all maps).

## What's Missing

### Deep comparison for HASH documents

HASH documents in COLLECT entries are represented as flat key-value arrays (`RSValueType_Array` of alternating name/value pairs). `RSValue_CmpNC` for arrays only compares the first element — this is insufficient for deep equality.

**Needed:** A full array comparison (element-wise). The code already has `compare_arrays` (unused):

```c
// TODO: Use when SORTABLE is not looking only at the first array element
static inline int compare_arrays(const RSValue *arr1, const RSValue *arr2, QueryError *qerr) {
  uint32_t len1 = arr1->_arrval.len;
  uint32_t len2 = arr2->_arrval.len;
  uint32_t len = MIN(len1, len2);
  for (uint32_t i = 0; i < len; i++) {
    int rc = RSValue_Cmp(arr1->_arrval.vals[i], arr2->_arrval.vals[i], qerr);
    if (rc != 0) return rc;
  }
  return len1 - len2;
}
```

This could be used directly, or a dedicated `RSValue_DeepEqual` could be introduced that uses it.

### Deep comparison for JSON documents

JSON documents are serialized as `RSValueType_Map`. Currently `RSValue_CmpNC` returns 0 for all map comparisons — making every map "equal" to every other map.

**Needed:** A map comparison function. Since maps are ordered (entries stored in insertion order), a straightforward approach is:

1. Compare lengths.
2. For each entry `i`, compare `key[i]` and `value[i]` recursively.

`RSValue_Hash` already handles maps recursively, so the hash side works. Only the equality/comparison side is missing.

## Summary

| Component | HASH (`*` = array) | JSON (`*` = map) |
|-----------|--------------------|--------------------|
| Hash (`RSValue_Hash`) | Works (recursive) | Works (recursive) |
| Sort key comparison (`RSValue_Cmp`) | Works (scalars) | Works (scalars) |
| Deep equality | Needs full array compare (code exists, unused) | Needs map compare (not implemented) |
| Heap early-out (`mmh_peek_min`) | Available | Available |
| Heap iteration for dedup scan | Via `h->data[1..count]` (struct exposed) | Same |
