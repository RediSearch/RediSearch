# Keyed numeric index (`MAP NUMERIC`)

Status: proof of concept, RAM only, behind `ENABLE_UNSTABLE_FEATURES`. It follows
the shape of the sparse vector field (RediSearch/RediSearch#8346): an unstable
field type with its own index structure, RAM and JSON only.

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
- The field takes no other type or option (`SORTABLE`, `NOINDEX`, ...). Keyed
  predicates are not used by the query optimizer.
- JSON only. `FT.CREATE` and `FT.ALTER` reject the field on HASH indexes, and on
  Flex indexes through `SearchDisk_MarkUnsupportedFieldIfDiskEnabled`, as for
  GEOSHAPE. With `ENABLE_UNSTABLE_FEATURES` off, `FT.CREATE`, `FT.ALTER` and
  parsing of `@field[key]:` fail with an error naming the flag. RDB load and
  indexing are not gated.

## Data model

```rust
pub struct KeyedNumericIndex {
    /// Boxed so growing the map moves a pointer instead of a whole tree.
    keys: FxHashMap<Box<[u8]>, Box<NumericRangeTree>>,
    memory_usage: usize,
    /// Tells a replaced index apart from the one a GC scan saw.
    unique_id: u32,
}
```

One index per `MAP NUMERIC` field. It maps each key to a regular
`NumericRangeTree`, reused unchanged: same splitting and range iterator. A tree
is created on the first value for its key. A key whose tree is empty after GC is
removed from the map.

Because GC can remove a key while a query is parked, a keyed iterator looks its
key up again through the field's index on revalidation (as #8346's dimension
iterators do). It aborts if the index was replaced (`unique_id`), the key is
gone, or the key now holds a different tree.

## Query parsing and evaluation

Only the DIALECT 2+ parser knows the syntax. The lexer matches `@name[key]:` and
`@name[$param]:` as one token run (`MODIFIER KEY COLON`), and the grammar adds
`expr ::= modifier KEY COLON numeric_range`; the range then lexes as it always did.
Because the pattern needs the closing `]` and the `:` right after it, `@f[1 2]`, `@f [..]` and
every other existing use of `[` lex as before. With `ENABLE_UNSTABLE_FEATURES` off the lexer
fails the query with the error that names the flag.

Key escaping: an unquoted key holds only `[A-Za-z0-9_:.-]` and is taken verbatim, with no
backslash escapes and no case folding (keys are case sensitive). Any other key, including the
empty key, goes through `$param`, whose value is binary safe.

The key is stored on the numeric query node next to its filter. A keyed node is not seen by
the query optimizer's SORTBY range pagination. The field must be `MAP NUMERIC`: a keyed
clause on another field type, and a plain `@metrics:[lo hi]` on a `MAP NUMERIC` field, are
errors. `FT.HYBRID` evaluates its search subquery through the same path, so keyed clauses
work there too, but it is not covered by tests.

## Why one tree per key

A query names the key, so the lookup is a hash probe followed by an ordinary
range seek in that key's tree. Only entries of that key are visited, and every
entry visited has the right key; the work is proportional to the matches, not to
the number of documents with any key.

The cost is fixed overhead per key (a tree root plus map entry), which makes the
design suited to a moderate number of distinct keys with many documents each,
and poor for a key space that is nearly unique per document.

## Alternatives considered

- **One field per key.** See Problem.
- **Multi-value numeric with coded values.** Encode the key into the number,
  such as `key_id * 2^k + value`, so one tree serves all keys and a key range is
  a numeric range. Doubles have 53 bits of mantissa, so key space and value
  precision compete; negative and fractional values need an order-preserving
  transform; the key-to-id dictionary needs its own persistence and GC; and
  values returned from the index are not the user's values.
- **Element-scoped `NESTED` groups.** Index `(key, value)` pairs as elements of
  an array so a predicate must match within one element. This is the most
  general answer and also the largest: it needs element identity through the
  iterators, a join between sub-field indexes, and new syntax. It can be added
  later without replacing this index, which serves the common "named key plus
  range" case directly.

## Persistence

The spec RDB encoding version does not change, so an index without a `MAP NUMERIC`
field is written and read exactly as before. The field is stored as its name, path,
type bit and options like any other field, and has no payload of its own; a reader
recognises it by the type bit, not by the encoding version. The trees are not
persisted: on load the documents are re-indexed from the keyspace and the trees
rebuilt, as numeric fields are.

As in #8346, RDB load and indexing are not gated, so a field created with the
flag on keeps loading and indexing after the flag is turned off. The gate cannot
cover the format itself, which requirement 3 of
[CONTRIBUTING-unstable-features.md](../CONTRIBUTING-unstable-features.md#3-do-not-touch-persisted-or-replicated-state)
excludes. An RDB without a `MAP NUMERIC` field loads on an older build unchanged. An older
build that meets the unknown type bit does not reject it: the field loads with no
recognised type, and the server later crashes or fails on it:

- `FT.INFO` reaches `RS_ABORT_ALWAYS` in `FieldSpec_GetTypeNames`, and so does the
  crash-report `INFO` section (`IndexSpec_AddToInfo`).
- A query that resolves the field through the spec cache (`RETURN`, `LOAD`, `SORTBY`,
  or `@m` in `APPLY`/`FILTER`) panics in the `rlookup` crate, where
  `FieldSpecTypes::from_bits(..).unwrap()` rejects the unknown bit.
- Documents with a value at the field's path fail indexing.

Downgrading therefore requires dropping the field first.

Bumping the encoding version would not help. The loader checks only a lower bound
(`INDEX_MIN_COMPAT_VERSION`) and has no upper-bound check, so an RDB with a higher version
loads into the same unknown-type field and fails the same way. A bump would only change
the RDB stamp of every user's index.

## Garbage collection

A sibling of the numeric GC scanner (like #8346's sparse scanner) covers these
fields in both the fork and the thread policies, reusing the per-tree scan and
apply code. A tree is identified by (field index, index `unique_id`, key, tree
`unique_id`). Applying a delta skips the entry if the index was replaced, the key
is gone, or it holds a different tree. Emptied keys are removed under the write
lock and the memory accounting is adjusted. `FT.INFO` reports the number of keys
and the memory used by the field.

## Limits and out of scope

- Flex/disk, HASH, cluster-specific behavior.
- `FT.HYBRID`: unsupported.
- `MAP TAG` and other value types; `NESTED` syntax.
- Sortable values; sorting uses JSON path loading of the value.
- Unquoted key characters beyond `[A-Za-z0-9_:.-]`; use a parameter.
- No cap on distinct keys per field in the POC, so memory grows with the key
  space; per-key overhead is the main thing to measure before stabilizing.
- Benchmarks are a follow-up, not part of the first change.
