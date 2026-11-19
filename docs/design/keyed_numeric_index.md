# Keyed numeric index (`MAP NUMERIC`)

Status: proof of concept, RAM only, behind `ENABLE_UNSTABLE_FEATURES`. It follows
the shape of the sparse vector field (RediSearch/RediSearch#8346): an unstable
field type with its own index structure, rejected where unsupported.

## Problem

Documents often carry an object of `key -> number` where the keys are data, for
example per-period metrics: `{"rev:fy0": 48200000, "headcount:m2026_06": 212}`.
Queries ask for a range on one named key ("`rev:fy0` above 50M") and combine
several keys in one query.

Existing options do not fit:

- **One field per key.** The set of keys grows with time (a new period adds
  keys) and hits the attribute cap per index. Every new key is a schema change,
  and `FT.ALTER` on a large index is a reindex-scale operation.
- **A multi-value numeric field plus a TAG field** over the same object. The two
  indexes are independent, so "key `a` with value above 5" also matches a
  document where `a` is small and some other key is large. Key and value cannot
  be paired.

## Surface

```
FT.CREATE idx ON JSON PREFIX 1 company: SCHEMA
    $.hq      AS hq      TAG
    $.metrics AS metrics MAP NUMERIC

JSON.SET company:1 $ '{"hq":"US","metrics":{"rev:fy0":48200000,"headcount:m2026_06":212}}'

FT.AGGREGATE idx "@hq:{US} @metrics[rev:fy0]:[(50000000 +inf]"
    LOAD 3 $.metrics["headcount:m2026_06"] AS hc SORTBY 2 @hc DESC LIMIT 0 50 DIALECT 2
```

Query rules:

- `@metrics[key]:[lo hi]` matches documents whose `metrics` object has `key` with
  a value in the range. Range syntax is the existing numeric one, including
  exclusive bounds and `-inf`/`+inf`.
- `@metrics[$k]:[lo hi]` takes the key from `PARAMS`. An unquoted key may contain
  `[A-Za-z0-9_:.-]`; any other key must be passed as a parameter.
- `[-inf +inf]` means "has the key". A key that was never indexed matches
  nothing (not an error).
- The field composes with other predicates (AND, OR, NOT) like any numeric
  predicate and works in `FT.SEARCH` and `FT.AGGREGATE`.

Indexing rules:

- The attribute must resolve to a JSON object. Every member value must be a
  number; a non-numeric or nested-object member fails indexing of that document
  (counted as an indexing failure, as for an invalid sparse vector).
- An empty object is valid and indexes no keys.
- `LOAD` and `SORTBY` use the existing JSON path loading, for example
  `$.metrics["rev:fy0"]`. The field is not sortable and has no new load path.
- JSON only. `FT.CREATE` rejects the field on HASH indexes and on disk/Flex
  indexes. Without `ENABLE_UNSTABLE_FEATURES` it is rejected as an unknown type.
- `FT.HYBRID` does not support the field.

## Data model

```rust
pub struct KeyedNumericIndex {
    /// Boxed so a tree keeps its address when the map grows: a query iterator
    /// reads it with the spec lock released.
    keys: FxHashMap<Box<[u8]>, Box<NumericRangeTree>>,
    memory_usage: usize,
    /// Tells a replaced index apart from the one a GC scan saw.
    unique_id: u32,
}
```

One index per `MAP NUMERIC` field. It maps each key to a regular
`NumericRangeTree`, reused unchanged: same splitting, same range iterator, same
revalidation. A tree is created on the first value for its key. A key whose tree
is empty after GC is removed from the map.

## Why one tree per key

A query names the key, so the lookup is a hash probe followed by an ordinary
range seek in that key's tree. Only entries of that key are visited, and every
entry visited has the right key; the work is proportional to the matches, not to
the number of documents with any key.

The cost is fixed overhead per key (a tree root plus map entry), which makes the
design suited to a moderate number of distinct keys with many documents each,
and poor for a key space that is nearly unique per document.

## Alternatives considered

- **One field per key.** Hits the attribute cap, needs a schema change per new
  key, and cannot be created lazily from data.
- **Multi-value numeric with coded values.** Encode the key into the number,
  such as `key_id * 2^k + value`, so one tree serves all keys and a key range is
  a numeric range. Doubles have 53 bits of mantissa, so key space and value
  precision compete; negative and fractional values need an order-preserving
  transform; the key-to-id dictionary needs its own persistence and GC; and
  values returned from the index are not the user's values. A bounded-precision
  trick should not define the semantics of a user-visible field.
- **Element-scoped `NESTED` groups.** Index `(key, value)` pairs as elements of
  an array so a predicate must match within one element. This is the most
  general answer and also the largest: it needs element identity through the
  iterators, a join between sub-field indexes, and new syntax. It can be added
  later without replacing this index, which serves the common "named key plus
  range" case directly.

## Persistence

The field definition is stored in the spec RDB with a new field encoding
version, as other field types are. The trees are not persisted: on load, the
documents are re-indexed from the keyspace and the trees rebuilt, as numeric
fields are. Like #8346, the field is stored regardless of the unstable gate
state at load time; the gate applies to `FT.CREATE`.

## Garbage collection

A tree is identified by the pair (field, key). The numeric GC scanner, which
today addresses a tree by field index, addresses it by the pair for these
fields, in both the fork and the thread policies. Applying a delta checks
`unique_id` as today, so a field replaced between scan and apply is skipped.
After applying, a key with an empty tree is removed from the map and the memory
accounting is adjusted. `FT.INFO` reports the number of keys and the memory used
by the field.

## Limits and out of scope

- Flex/disk, HASH, `FT.HYBRID`, cluster-specific behavior.
- `MAP TAG` and other value types; `NESTED` syntax.
- Sortable values; sorting uses JSON path loading of the value.
- Unquoted key characters beyond `[A-Za-z0-9_:.-]`; use a parameter.
- No cap on distinct keys per field in the POC, so memory grows with the key
  space; per-key overhead is the main thing to measure before stabilizing.
- Benchmarks are a follow-up, not part of the first change.
