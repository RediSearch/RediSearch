# Multi-level compound index on NESTED JSON arrays (TAG + NUMERIC)

Status: in progress, landing as a stack of PRs (§7). The first PR adds this document and the
routing layer, the `compound_index` crate.

This design builds on two earlier proposals:

- **Element-scoped matching for JSON arrays**: the `NESTED` schema syntax (R1) and the
  `@group:( … )` query semantics, where every condition in the group must hold on the same array
  element (R2).
- **Keyed attribute index**: a compound `(key, value)` index that makes those conditions fast when
  the group fixes a key. This document generalises it to several attributes, its "compound index
  over several attributes" step.

Scope: TAG and NUMERIC attributes only, JSON documents only, RAM mode. §4 sketches how Flex
could be supported later.

---

## 1. What it is

A NESTED field holds a JSON array of objects. A **compound index** on the group puts the
attributes in an order, and each attribute's type decides what the level below it looks like:

```
INDEX 3 color size price

color   TAG      → TrieMap: value → child
size    TAG      → TrieMap: value → child
price   NUMERIC  → NumericRangeTree → leaves → postings (docId, value)
```

Every **element** of the array follows exactly one path from the root to a terminal, and its doc
id is posted at that terminal. A doc id in a terminal therefore means "this doc has an element
with exactly these values on every level above". That makes the pairing correct by construction,
whichever levels the query fixes.

### Running example

```json
items:C1  {"items":[{"color":"red","size":"M","price":20},{"color":"blue","size":"L","price":90}]}
items:C2  {"items":[{"color":"red","size":"L","price":45}]}
items:C3  {"items":[{"color":"red","size":"M","price":35},{"color":"red","size":"M","price":15}]}
items:C4  {"items":[{"color":"blue","size":"M","price":25}]}
```

Doc ids 1–4. The index contents:

```
color
 ├─ "red"
 │   └─ size
 │       ├─ "M" → price tree: (d1,20) (d3,35) (d3,15)      ← d3 twice: two red/M elements
 │       └─ "L" → price tree: (d2,45)
 └─ "blue"
     └─ size
         ├─ "L" → price tree: (d1,90)
         └─ "M" → price tree: (d4,25)
```

`@items:( @color:{red} @size:{L} @price:[(40 +inf] )` → one terminal (red → L), range read →
{d2}. With today's flat indexes (`color` TAG, `size` TAG, `price` NUMERIC over `[*]`) the same
conditions give red {1,2,3} ∩ L {1,2} ∩ price>40 {1,2} = **{d1, d2}**. d1 matches through its
*blue* L item priced 90, because flat postings only share the doc id.

---

## 2. Creating the index

### Syntax

The NESTED group syntax comes from the element-scoped matching proposal. The `INDEX` clause comes
from the keyed-attribute proposal, written in argument-count style so the `ArgsCursor` parser
handles it the way it handles `NESTED <n>`:

```
FT.CREATE idx:orders ON JSON PREFIX 1 items: SCHEMA
  $.order_id AS order_id TAG
  $.items[*] AS items NESTED 3
      $.color AS color TAG
      $.size  AS size  TAG CASESENSITIVE
      $.price AS price NUMERIC
    INDEX 3 color size price
```

The keyed-attribute `(k, v)` case is one routing level and a terminal:

```
  $.metrics[*] AS metrics NESTED 2  $.k AS k TAG  $.v AS v NUMERIC  INDEX 2 k v
```

### Rules checked at `FT.CREATE`

| Rule | Why |
|---|---|
| The NESTED path must end in `[*]` (or another multi-result path) | The group indexes elements |
| Sub-paths are relative to the element (`$.color`) and must be single-valued per element in v1 | A multi-valued attribute would post the element under every combination (see §3, "cross product") |
| `INDEX` lists each sub-field at most once: at most `MAX_LEVELS` routing levels (3, a constant of the `compound_index` crate) plus the terminal | Bounds recursion and planner fan-out |
| Every level but the last must be TAG, or NUMERIC with `BUCKETS` (see "Optional limits" below) | A TAG level is an equality map with stable children. A NUMERIC level with dynamic ranges would need range nodes that own subtrees and re-bucket them on split. Fixed buckets never split, so they behave like an equality level |
| The last level is NUMERIC (range tree) or TAG (posting list per value) | These are the two terminal kinds |
| Every sub-field must appear in `INDEX` in v1 | The planner can then serve any group from the index alone (§5). Sub-fields outside the index need general element matching, out of scope |
| Supported sub-field options: TAG `CASESENSITIVE`, plus the limits below (`MAXCARD` on TAG; `BUCKETS`, `RANGE` on NUMERIC). `SEPARATOR`, `SORTABLE`, `NOINDEX`, `INDEXMISSING`, `INDEXEMPTY`, `WITHSUFFIXTRIE` are rejected in v1 | A separator splits one string into several tags, which gives an element several values on one level: the cross product of §3, "Limits and costs". The other options keep ingest and GC simple |
| A NESTED field counts as **one** field toward `SPEC_MAX_FIELDS` (1,024) | Many attributes per entity is the reason the feature exists |
| Not supported `ON HASH` | Hashes have no arrays |

### Optional limits: cardinality and numeric ranges

The tree's size and the planner's worst-case fan-out are the product of the distinct values on
each level. Declaring bounds up front makes both predictable and catches bad data at ingest.
Each option is per sub-field and optional.

```
$.items[*] AS items NESTED 4
    $.color AS color TAG     MAXCARD 64
    $.size  AS size  TAG     MAXCARD 8
    $.year  AS year  NUMERIC BUCKETS 4 2020 2022 2024 2026
    $.price AS price NUMERIC RANGE 0 100000
  INDEX 4 color size year price
```

| Option | Applies to | Meaning | At ingest | Effect on the index and the planner |
|---|---|---|---|---|
| `MAXCARD <n>` | TAG level | At most `n` distinct values on this level, counted per parent node | A value that would be the `n+1`-th child is an index error for the doc (the doc isn't indexed and the error shows in `FT.INFO`, like a type mismatch) | Caps children per node, so tree size ≤ ∏ `MAXCARD` × terminal size. An `Any` walk on this level opens at most `n` children, so the product of the `MAXCARD`s bounds the terminals any query can reach |
| `BUCKETS <n> b₁ … bₙ` | NUMERIC level, any position | Fixed boundaries giving the intervals `[b₁,b₂) … [bₙ₋₁,bₙ)`, at most `MAX_BUCKETS` (1,024) of them, checked at `FT.CREATE` in either position. No dynamic splits | A value outside `[b₁, bₙ)` is an index error | **As a middle level**: an equality level whose children are the buckets (cardinality n−1). **As the terminal**: a range tree pre-split at the boundaries that never splits again, so leaf count and memory per terminal are fixed |
| `RANGE <lo> <hi>` | NUMERIC terminal | Valid values are `[lo, hi]` | A value outside is an index error | Validation only. The range tree still splits dynamically, but within known bounds, which the first splits can use |

#### Queries on a bucketed middle level

A range on a bucketed middle level is an ordinary numeric range: each end is inclusive
(`[lo`), exclusive (`(lo`) or unbounded (`-inf`, `+inf`), as in `@year:[(2022 +inf]`. Each bucket
is half-open, `[bᵢ, bᵢ₊₁)`, and relates to the range in one of three ways:

| Bucket vs range | Example, buckets `[2022, 2024)` and `[2024, 2026)` | Result |
|---|---|---|
| Wholly inside | `[2022 (2024]` contains every value of `[2022, 2024)` | Selected |
| Wholly outside | `[2022 (2024]` contains no value of `[2024, 2026)` | Skipped |
| Partly inside | `[2022 2024]` contains 2024, the first value of `[2024, 2026)`; `(2022 (2024]` misses 2022, the first value of `[2022, 2024)` | The query fails |

A partly covered bucket fails the query because its entries include elements outside the range,
and the terminal stores only the last level's value, so they can't be filtered out. That makes
`@year:[2022 2024]` fail where `@year:[2022 (2024]` succeeds: the error has to name the bucket
boundaries so the user can see the half-open form. A range with a NaN end fails too, and an
empty range (such as `(2023 2023]`) selects no bucket. The routing layer implements this rule as
`BucketRange`, with `std::ops::Bound` ends.

A later version can lift the restriction: terminal entries would store the middle level's raw
value as an extra payload, and partly covered buckets would be filtered on it. That costs one
extra value per entry.

A bucketed **terminal** has no such problem. Its leaves hold `(docId, value)`, so edge leaves are
filtered by value as in any numeric range query today.

#### Defaults

All three options are absent by default, which means no cardinality cap and dynamic ranges. A
NESTED field then has no ingest limit beyond what a TAG or multi-value NUMERIC field has today:
any number of elements per document and any number of distinct paths, made visible through
`FT.INFO` (§3, "Limits and costs"). These options are the only user-facing limits of the
feature. Everything else is an internal constant (`MAX_LEVELS`, `MAX_BUCKETS`) or a query-time
bound (§5, step 3), none of them configurable.

#### Limitation: only the terminal numeric adapts to the data

The full `NumericRangeTree`, with ranges that split as data arrives and exact filtering of
partial leaves, exists only at the **terminal**. Its leaves are posting lists of
`(docId, value)`, and each entry carries the value the tree splits and filters on.

A NUMERIC attribute on a **middle** level can only be a `BUCKETS` level. Its leaves own subtrees,
and nothing below them stores the middle level's value, so the tree could neither redistribute
elements on a split nor filter a range that cuts a leaf. Hence the fixed buckets, and the rule
that a range must cover whole buckets.

In practice:

- **One numeric attribute per group**, such as the keyed `(k, v)` case or `color, size, price`:
  declare it last. It gets the adaptive tree and needs no `BUCKETS`.
- **Two numeric attributes**, such as `year` and `price`: only the last one adapts. The other is a
  bucketed middle level, with user-chosen boundaries.

Each node of a bucket level holds a slot for every declared bucket, used or not (8 bytes each),
and a walk over the level reads every slot in its range. `MAX_BUCKETS` bounds both per node.

Ways to lift the limitation later, all after the terminals PR:

1. **An adaptive numeric middle level**: a range tree whose leaves own subtrees. Every terminal
   entry also stores the middle level's value (8 more bytes per element per numeric middle level).
   A split rebuilds the two halves of the subtree from those values, and a range that cuts a leaf
   is filtered on them instead of rejected. This needs a reader that filters on two values.
2. **A filter attribute**: store the value in terminal entries without routing on it,
   `INDEX 3 color size price FILTER year`. Ranges on it are exact and need no splits, but a query
   narrowing only by `year` still reads every terminal.
3. **Sparse bucket storage**: keep only the buckets in use per node, sorted, so memory and walk
   cost follow the data instead of the declared bucket count. This would let `MAX_BUCKETS` be
   raised a lot.

### Code

- `src/field_spec.h`: add `INDEXFLD_T_NESTED = 0x80`, extend `INDEXTYPE_TO_POS` and
  `INDEXFLD_NUM_TYPES`, and add a `nestedOpts` member. `FieldSpec.types` is an 8-bit bitfield, so
  0x80 is its last free bit: the next field type after this one has to widen it.
  ```c
  struct {
    arrayof(NestedSubField) subFields;   // name, relative path (compiled), type, tag flags, limits
    uint8_t levels[MAX_LEVELS + 1];      // sub-field index per level, in INDEX order, terminal last
    uint8_t numLevels;
    struct CompoundIndex *index;         // Rust-owned, opaque to C
  } nestedOpts;
  ```
  Sub-fields are **not** `FieldSpec`s. They get no field bit, no field index and no top-level name.
  They are only reachable inside `@items:( … )`.
- `src/spec.c`: `parseFieldSpec` gets an `AC_AdvanceIfMatch(ac, SPEC_NESTED_STR)` branch that
  reads `<n>` sub-field definitions using the existing TAG/NUMERIC option parsers on a sub-cursor,
  then `INDEX <n> …`. Paths are compiled once with `japi->pathParse`.
- `FT.ALTER … ADD` can add a new NESTED field, which triggers the usual rescan. Changing an existing
  group's levels is not supported (you'd drop and recreate the index).
- `FT.INFO` gets one attribute entry with a `nested` sub-array listing the sub-fields and levels,
  plus index stats (§3, "Memory").

---

## 3. Indexing the data

### Data structure

The index has two layers:

- **The routing layer**: the `compound_index` crate
  ([lib.rs](../../src/redisearch_rs/compound_index/src/lib.rs)), added by the first PR.
  `CompoundIndex<T>` holds the routing levels and owns one terminal of type `T` per distinct
  path. It knows nothing about what a terminal stores. It provides:
  - the routing levels: `Level::Tag` (a TrieMap per node, optional per-node `max_cardinality`) and
    `Level::Buckets` (fixed half-open buckets, one slot per bucket on every node, at most
    `MAX_BUCKETS` of them);
  - `get_or_insert_with(path, init)`, which validates the whole path (key types, bucket
    bounds, cardinality) before creating anything, so a rejected insert leaves the index
    unchanged, and returns `&mut T`. This is the first half of **route, then post**: the index
    routes an element to its terminal and the caller posts the doc id there, so the routing
    layer never handles doc ids;
  - `check_paths(paths)`, which runs the same checks on all of one document's paths together,
    so ingest can index a document whole or not at all (§3, "Ingest path");
  - `collect_terminals(constraints, max_terminals, timeout)`, the planner's walk (§5), which
    fails instead of truncating past `max_terminals`, and stops when the query times out.

  Removing terminals comes with GC (fifth PR), which removes each emptied terminal by its path.
- **The terminals** (second PR), reusing existing structures unchanged:
  ```rust
  enum Terminal {
      /// Last level NUMERIC: an ordinary range tree.
      Numeric(NumericRangeTree),
      /// Last level TAG: one doc-id posting list per value.
      Tag(TrieMap<InvertedIndex /* DocIdsOnly */>),
  }
  ```
  `Terminal` owns the post half of route, then post: one method adds a doc id with the element's
  terminal value to whichever structure it wraps, and ingest (§3) calls it on the `&mut Terminal`
  that `get_or_insert_with` returns.
  Each terminal is read by the iterators that exist today, which keeps the query side cheap.
  The per-terminal stats the planner needs (`unique_docs`, `entries`) come from these structures.

Keeping the routing layer generic over `T` is also what makes the Flex design in §4 possible: only
the terminal type changes.

### Main types of the routing layer

The crate is built around one rule: **check everything first, then touch the tree**. Writes check
a whole path, or a whole document's paths, before inserting anything. Queries check every
constraint before visiting any node. A bad path or query is rejected up front, and the only
errors left during a walk are its terminal bound and the query timeout.

Public types, used by the later PRs:

- **`CompoundIndex<T>`**: the tree. It holds the routing levels, one root node, and a terminal of
  type `T` at the end of each distinct path. It is generic over `T` so the routing logic does not
  depend on how postings are stored: a range tree or tag postings in RAM (second PR), postings on
  disk for Flex (§4).
- **`Level`**: one routing level, `Tag(TagLevel)` or `Buckets(BucketLevel)`, fixed at creation in
  `INDEX` order. Each node stores its children according to its level's kind.
  - **`TagLevel`**: children keyed by exact value in a `TrieMap`, the structure the TAG index
    uses. Its optional `max_cardinality` (`MAXCARD`) caps the children of each node, which bounds
    both memory and the fan-out of a walk.
  - **`BucketLevel`**: fixed half-open buckets `[bᵢ, bᵢ₊₁)` with one slot per bucket on every
    node, so a numeric value routes by array index. At most `MAX_BUCKETS` buckets, because every
    node pays for every slot (§2, "Limitation").
- **`Key`**: one element's value on one level, `Tag(bytes)` or `Numeric(f64)`. A path is one `Key`
  per level. Ingest builds it from an element's attributes, already normalised (case folding).
- **`Constraint`**: what a query asks of one level: `Any`, `TagIn`, `TagNotIn`, `TagPrefix`, or
  `BucketRange`. The planner (fourth PR) turns `@items:( … )` into one `Constraint` per level
  (§5, step 1). The terminal attribute's own condition, such as a price range, is not a
  `Constraint`: the terminal applies it.
- **`BucketRange`**: a numeric range on a bucket level with `Bound` ends (inclusive, exclusive or
  unbounded), like a query's `[lo hi]`. It selects the buckets wholly inside it and is rejected if
  one is only partly inside, since nothing below a bucket level can filter the elements outside
  the range.
- **The walk's bounds**: `max_terminals` bounds how many terminals it returns, so the number of
  iterators the UNION opens. A `TimeoutChecker` (the `timeout` crate's trait, which query
  iterators already use) is checked on every child the walk considers, entered or not, so the
  query timeout bounds the work of a query that leaves a level open and matches little. Past either one, the walk fails
  rather than returning a truncated, wrong result.
- **`num_terminals`**: a running count of terminals, so `FT.INFO` reads it without a walk. It
  changes in exactly two places: +1 when an insert creates a path, −1 when GC (fifth PR) removes
  one.
- **Errors**: `SchemaError` (levels), `BucketsError` (boundaries), `PathError` (a path that does not
  route), `InsertError` (adds the cardinality limit), `CollectError` (adds the terminal bound and
  the timeout). Each names the level at fault, so the C side can build a user-facing message.

Internal types, private to the crate, that carry the "check first" rule:

- **`Step`**: a `Key` after it is checked against its level. `route()` produces one per level, a
  tag value or a bucket index, or a `PathError`. Every later walk over the path (lookup, insert,
  cardinality check) uses `Step`s and never re-checks them.
- **`LevelPlan`**: a `Constraint` after it is checked and prepared for its level. Sets are sorted
  and deduplicated, and a `BucketRange` becomes a range of slot indexes. The walk reads
  `LevelPlan`s, so this preparation runs once per query instead of once per node.

### Ingest path

1. **Load** (`document_basic.c`): for a NESTED field, `JSON_GetWithCompiledPath(root, path)`
   returns the elements. For each element, evaluate every compiled sub-path with
   `japi->getWithPath(element, subPath)`. That produces a row of `(value₁ … valueₙ)` per
   element. Store the rows in the `DocumentField` (a new `FLD_VAR_T_NESTED_ROWS` variant).
2. **Preprocess** (`document.c`, `preprocessorMap[IXFLDPOS_NESTED]`): validate the types and
   normalise TAG values (case folding unless `CASESENSITIVE`; no separator split, see §2). Drop elements
   that are missing any level's attribute. They aren't indexed, so they never match a group,
   which gives SQL's NULL behaviour. There is no cap on elements per document, as for
   multi-value fields today.
3. **Bulk add** (`IndexerBulkAdd`, new `case IXFLDPOS_NESTED`): under the spec write lock, first
   call `check_paths` on all the document's rows together, and fail the document if it errors.
   Checking each row alone is not enough: two new values under one parent can each fit a
   `MAXCARD` limit alone and break it together, which would leave the document half indexed.
   Then, for each row, call `get_or_insert_with` on the routing values, which cannot fail
   after a successful check, and insert into the terminal:
   - NUMERIC terminal: `NumericRangeTree::add(doc_id, value, is_multivalued = true, …)`.
   - TAG terminal: append `doc_id` to that value's posting list.
   - Rows are inserted in element order. A doc's rows can hit the same terminal several times
     (C3 above). That's the multi-value case the numeric tree already handles
     ([insert.rs](../../src/redisearch_rs/numeric_range_tree/src/tree/insert.rs)): the leaf gets
     `Index_HasMultiValue`, and the reader skips repeats (`read_skip_multi`). For TAG terminals,
     insert each `(terminal, value, doc_id)` only once per doc.
4. **Apply** (`IndexerBulkApply`): update stats along each touched path.

Doc ids are monotonic per terminal because every add or update of a document gets a new doc id.
This is the invariant every posting list in the codebase already relies on.

### Updates, deletes, GC

- An update is a delete followed by an add, as today. Old entries become garbage under the old doc
  id.
- GC: add a scanner `index_gc/src/scanners/compound.rs`. It walks the tree depth-first and, at each
  terminal, reuses the numeric and inverted-index GC routines. The fork-GC delta message carries
  the **path** (`value₁ … valueₙ₋₁`) plus the existing per-terminal delta, so the parent finds the
  same terminal. After the deltas are applied, each emptied terminal is removed by its path, with
  the TrieMap entries left without children, so a value that no element uses any more doesn't
  leak a subtree.

### Limits and costs

- **Cross product**: if v1 ever allows a multi-valued attribute (`"color": ["red","white"]`, or
  `"red,white"` with a `SEPARATOR`), one element would post under every combination. v1 rejects it
  at `FT.CREATE` (single-valued sub-paths, no `SEPARATOR`) and at ingest (a sub-path yielding more
  than one value is an index error).
- **Memory**: a terminal costs about one range tree root, or one TrieMap entry plus one inverted
  index. Many sparse prefixes mean many small terminals, a fixed cost per distinct path at every
  level. Report `nested_terminals`, `nested_entries` and `nested_bytes` in `FT.INFO`. There is no
  cap on distinct paths, as there is none on distinct TAG values: `MAXCARD` is the opt-in bound,
  and `FT.INFO` gives visibility without a new way for ingest to fail.

---

## 4. Persistence

### RDB

In RAM mode the RDB stores only the **spec** (`IndexSpec_RdbSave` → `FieldSpec_RdbSave`,
[spec.c](../../src/spec.c)). Inverted indexes and numeric trees are rebuilt after load by a full
rescan (`IndexSpec_ScanAndReindex`, [indexes.c](../../src/indexes.c)). The compound index follows
the same rule: only its definition is saved, and the tree is rebuilt by the rescan.

Bump `INDEX_CURRENT_VERSION` to a new `INDEX_NESTED_VERSION` (32, the next free version), and only read the block below
when `encver >= INDEX_NESTED_VERSION`. The field's `types` already round-trips as an unsigned, so
loaders before 32 never see `INDEXFLD_T_NESTED`.

```
FieldSpec (existing header: name, path, types, options, sortIdx)
if types & NESTED:
  u64  nested_format          = 1
  u64  num_sub_fields
  repeat: string name, string rel_path, u64 type, u64 tag_flags,
          u64 limit_flags                     (bit 0 MAXCARD, bit 1 BUCKETS, bit 2 RANGE)
          if MAXCARD: u64 max_card
          if BUCKETS: u64 n; repeat n: f64 boundary
          if RANGE:   f64 lo, f64 hi
  u64  num_levels
  repeat: u64 sub_field_index
```

On load, re-run the §2 validation on the decoded definition (level types, the terminal kind,
sub-field references) and fail `IndexSpec_RdbLoad` with `REDISMODULE_ERR` on any mismatch.

### Flex (later)

A possible Flex design: keep the **compound structure in memory** (the routing levels, the range
trees' inner nodes and leaf bounds, and the stats), and store only the **posting lists** on
disk. Each terminal, or each numeric leaf, gets a stable posting id, and its doc ids live in the
disk engine under that id, the way `SearchDisk_IndexTags` and `SearchDisk_IndexNumeric` store
postings today. The routing layer and the planner's walk (§5) are unchanged. Only the terminal
type and its iterators switch to disk. This is to be designed with the Flex owners once the RAM
version is in.

---

## 5. Query planning: from `@items:( … )` to iterators

### Syntax and parsing

`@items:( @color:{red} @size:{M} @price:[-inf (30] )`.

- Grammar ([parser.y](../../src/query_parser/v2/parser.y)): today `modifier COLON text_expr`, with
  `text_expr ::= LP text_expr RP`, owns `@field:( … )`. A NESTED group needs a full `expr` (tags
  and ranges) inside the parentheses, which is an LALR conflict. Proposal: when the lexer emits a
  `MODIFIER` whose field resolves to NESTED, it emits `NESTED_MODIFIER` instead, and a new rule
  `expr ::= NESTED_MODIFIER COLON LP expr RP` takes it. While the inner `expr` is parsed, field
  lookups go to the group's sub-fields (a scope pushed on `QueryParseCtx`), so `@color` means
  `items.color`. Without a spec (coordinator-side parse), it falls back to text semantics, as
  today.
- AST: a new `QN_NESTED { const FieldSpec *fs; QueryNode *inner; }`. Inner leaves are the existing
  `QN_TAG` and `QN_NUMERIC` nodes, each with a sub-field reference instead of a `FieldSpec`.
- Eval: a new `query_eval/src/nodes/nested.rs`, next to `tag.rs` and `numeric.rs`.

### Step 1: normalise the inner expression into per-level constraints

Flatten ANDs and push NOTs down. Each conjunction becomes one `compound_index::Constraint` per
routing level, plus a constraint on the terminal:

| Inner clause | Constraint |
|---|---|
| `@color:{red}`, `@color:{red\|blue}` | `TagIn([red, blue])` |
| `-@color:{red}` | `TagNotIn([red])` |
| `@color:{re*}` | `TagPrefix("re")` (TrieMap prefix walk, bounded as in step 3; unlike `MAXPREFIXEXPANSIONS` it fails instead of truncating) |
| no clause on the level | `Any` |
| `@year:[lo hi]`, `[(lo hi]`, … on a `BUCKETS` middle level | `BucketRange { start, end }` with each end `Included`, `Excluded` or `Unbounded`. It selects the buckets wholly inside the range and fails if one is only partly inside (§2, "Optional limits") |
| `@price:[lo hi]` (terminal NUMERIC), several ranges ANDed | terminal range `[lo, hi]` (intersected) |
| `@price` absent | terminal range `[-inf, +inf]` |

An `|` across sub-fields inside the group (`@color:{red} | @size:{M}`) becomes **disjunctive
normal form**: each branch is its own conjunction, planned separately, and the branches are UNIONed.
Cap the number of branches with an internal constant (16, not configurable) and fail the query
with a clear error past it. A clause on something that isn't a sub-field of the group is a parse
error.

### Step 2: walk the tree to collect terminals

`CompoundIndex::collect_terminals` walks the routing levels with one constraint each:

```
TagIn(set)       → for v in set: if let Some(c) = node.children.find(v) { walk(c) }
TagPrefix(p)     → for (_, c) in node.children.prefixed_iter(p): walk(c)
TagNotIn(set)    → for (v, c) in node.children.iter(): if v ∉ set { walk(c) }
BucketRange(r)   → for c in the slots of the buckets inside r: walk(c)
Any              → for every child c: walk(c)
at the terminal depth: return the terminal
```

For each terminal returned, build an existing iterator:

- **Numeric terminal**: `build_numeric_filter_iterator` over **that** terminal's tree, which is
  already a union of the overlapping leaves
  ([numeric.rs](../../src/redisearch_rs/rqe_iterators/src/inverted_index/numeric.rs)).
- **Tag terminal**: one inverted-index iterator per allowed value, or a UNION of them.

Then **UNION all terminal iterators** (`union_opaque::build_union`, heap mode when many). The
UNION and `read_skip_multi` remove duplicate doc ids, so each doc comes out once. The group's
iterator is the result, in doc-id order, so it composes with outer INTERSECT, UNION, NOT and
OPTIONAL with no change to them.

### Why every shape is correct, not only the full prefix

A doc id in a terminal stands for an element whose values on every level are exactly that path.
If the walk only enters children that satisfy each level's constraint, and the terminal's
iterator applies the last constraint, every doc returned has an element satisfying **all** the
group's conditions. Skipping a level (`Any`) makes the walk wider, never less correct. So unlike
a prefix-only compound index, **no fallback to a different matcher is needed**, as long as every
condition is on an indexed level. That is guaranteed by the "every sub-field is a level" rule in
§2. The crate's property test checks this walk against a brute-force filter over the inserted
elements.

### The shapes a query can take

Using the running example's `INDEX 3 color size price`:

| Query group | Walk | Terminals read |
|---|---|---|
| `color=red, size=M, price<30` | 2 lookups, 1 range | 1 |
| `color={red\|blue}, size=M` | 2 × 1 lookup, full range | 2 |
| `color=red, price<30` (size skipped) | 1 lookup, then all sizes under red | sizes(red) |
| `size=M, price<30` (color skipped) | all colors, then 1 lookup each | colors |
| `color=re*` | prefix walk | children under the matching colors |
| `color≠red` | all colors but red | colors − 1 |
| `price<30` only | every terminal | all |
| `@color:{red} \| @size:{M}` | DNF: 2 walks, UNION | union of both |
| `-@items:( color=red )` | the group as above, then the outer NOT | — (doc level: no red element) |

### Step 3: estimates and guardrails

- **Estimate**: the group's `num_estimated` is the sum of the visited terminals' `unique_docs`.
  The outer INTERSECT uses this to order its children, so a selective group drives the
  intersection.
- **Expansion bounds**: none of these is a config. The walk fails the query instead of
  truncating past a bound. Silent truncation, as prefix expansion does today, would return wrong
  results.
  - The query timeout bounds the walk's work. A level the query leaves open is entered through
    every child even when nothing matches below it: `@items:( @size:{XL} )` with no `MAXCARD` on
    `color` enters every color to find no XL. The walk runs while the iterator tree is built,
    before any result processor checks the timeout, so it checks the timeout itself on every child
    it considers, entered or not, as the prefix and wildcard expansions check it during their trie walks.
  - Internal constants bound the terminals returned and the iterators opened on them (10K and
    100K). That bounds the memory of the UNION, as `MAXPREFIXEXPANSIONS` does for a prefix.
- **Profile**: `FT.PROFILE` shows a `NESTED` iterator with `terminals_visited`, `levels_fixed` and
  the per-terminal children. That makes a badly ordered `INDEX` visible.

### Later: making wide walks cheaper

Two options, both deferred until measurements show a need:

1. **Level postings**: an internal node optionally keeps a deduplicated doc-id posting of
   everything below it. `@items:( color=red )` with every later level `Any` then reads one list
   instead of unioning every size's terminal. It costs memory roughly equal to one TAG index per
   materialised level. Possible syntax: `INDEX 3 color size price MATERIALIZE 1 color`.
2. **Several indexes per group** with a cost-based choice: `INDEX …` repeated, the planner picks
   the index with the longest constrained prefix, then the smallest estimated terminals. This
   is MongoDB's model. v1 has one index per group, so the planner only ever chooses between
   walks, never between indexes.

---

## 6. What this design does not cover

- Sub-fields outside the index, TEXT, GEO, GEOSHAPE and VECTOR sub-fields, and arrays inside
  arrays (general element matching, with element positions in postings or hidden
  sub-documents).
- NUMERIC on a non-terminal level without `BUCKETS`, and ranges that cover part of a bucket on a
  bucketed middle level.
- Using the matched element's value in the pipeline. A natural follow-up: a NUMERIC terminal's
  reader already decodes the value, so the group iterator could expose it as `@items.price` in
  the result for SORTBY/APPLY. With several matching elements the rule (min, max or first) has to
  be specified.
- Returning only the matched elements.
- Coordinator changes: none are expected, since matching is per document, so per shard. The
  coordinator only has to forward the new syntax untouched.

---

## 7. PR stack

Each PR builds on the one before it. PRs 1 and 2 are Rust only. PR 3 is the write side and PR 4
the read side, each testable end to end on its own. PR 5 reclaims memory: queries already skip
deleted doc ids, so it is not needed for correctness, but it must land before the feature ships.

1. **Design and routing layer**: this document and the `compound_index` crate: tag and bucket
   levels with their limits, path insert that checks before mutating, `check_paths` for a whole
   document, and the constraint walk with its terminal bound and timeout check. Pure Rust,
   tested under `miri`, with property tests against brute-force filters.
2. **Terminals**: the `Terminal` type over `NumericRangeTree` and tag posting lists, inserting an
   element's terminal value, the iterator builders (one numeric filter iterator or tag iterator
   per terminal, UNIONed), and the per-terminal stats the planner uses. Rust tests linked against
   the C bundle.
3. **Write side: schema and ingest**:
   - `INDEXFLD_T_NESTED`, `nestedOpts`, and the FFI crate `c_entrypoint/compound_index_ffi`.
   - `FT.CREATE` parsing and validation (`NESTED`, `INDEX`, `MAXCARD`, `BUCKETS`, `RANGE`), and
     `FT.INFO`.
   - The spec RDB block with `INDEX_NESTED_VERSION`.
   - Ingest: element rows in `document_basic.c`, the preprocessor, `check_paths` on the whole
     document, then bulk add and apply. Error cases: missing attribute, multi-valued sub-path,
     limits.
   - Python tests: create and reject cases, indexing documents and checking `FT.INFO`, and a
     `DEBUG RELOAD` round-trip.
4. **Read side: parser, planner and eval**:
   - The `NESTED_MODIFIER` token, sub-field lookup scoped to the group, and `QN_NESTED`. A group on
     a non-NESTED field must keep today's text meaning.
   - `query_eval/src/nodes/nested.rs`: normalisation into `Constraint`s, DNF, the walk's bounds and the request timeout, the
     UNION of terminal iterators, estimates and profile output.
   - Python correctness tests against a brute-force evaluator on random documents, for every
     shape in the §5 table.
   - Benchmarks in the PR description: shard-sized data (about 55K docs × 280 elements), a full
     prefix, a skipped level and range-only, against one NUMERIC field per combination.
5. **GC**: the compound scanner, fork-GC deltas keyed by path, and removing each emptied
   terminal by its path. Tests for update and delete churn followed by GC, checking that terminal
   and memory counts drop.

### Follow-ups tracked from review

Raised while reviewing the first PR and deliberately left to the PR that owns the code:

| Follow-up | Owner |
|---|---|
| An insert under a tag level does three trie lookups (find, insert, find). Add an entry-style API to `TrieMap` | Terminals (2) |
| A coordinator-side parse without a spec falls back to text semantics. Check that the coordinator forwards `@items:( … )` untouched, so cluster and standalone agree | Read side (4) |
| The error for a range that cuts a bucket must name the bucket boundaries, so users see the half-open form (`[2022 (2024]`) | Read side (4) |
| Pass the request timeout (`rqe_iterators`' `TimeoutContextRequest`) to `collect_terminals`, map `CollectError::TimedOut` to the query's timeout error, and drop `max_nodes` from `NestedLimits`. Keep the terminal, reader and branch bounds as internal constants, not `FT.CONFIG` | Read side (4) |
| `FT.CREATE` rejects more than `MAX_BUCKETS` buckets for `BUCKETS` in either position. On the terminal, the pre-split range tree has one leaf per bucket, so it carries the same per-terminal cost | Write side (3) |
| Whether to lift "only the terminal adapts" (§2, "Limitation"): an adaptive numeric middle level, a filter attribute, or sparse buckets | After Terminals (2), driven by workloads with two numeric attributes |

## 8. Open questions

- Should the parser conflict be solved in the lexer (`NESTED_MODIFIER`), or with new syntax that
  doesn't overlap `@f:(text)`?
- Error or warning when the expansion cap is hit? This design says error. Product may prefer a
  warning with partial results, as prefix queries do today.
- Should v1 already allow sub-fields that aren't index levels, served by a slower
  "filter on load" path, or keep the strict rule?
- When a value breaks `MAXCARD`, `BUCKETS` or `RANGE`: reject the whole document (proposed), or
  index the document without the offending element?
- Should `MAXCARD` count distinct values per parent node (as the routing layer does, which bounds
  fan-out exactly) or across the whole level (easier to explain, but a looser bound)?
