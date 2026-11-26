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
| Every level but the last must be TAG. A NUMERIC sub-field must be last | A TAG level is an equality map with stable children. A NUMERIC level would need range nodes that own subtrees and redistribute them on a split, and nothing below it stores its value to split or filter on (see "Numeric attributes" below) |
| The last level is NUMERIC (range tree) or TAG (posting list per value) | These are the two terminal kinds |
| Every sub-field must appear in `INDEX` in v1 | The planner can then serve any group from the index alone (§5). Sub-fields outside the index need general element matching, out of scope |
| Supported sub-field options: TAG `CASESENSITIVE`, plus the limits below (`MAXCARD` on TAG, `RANGE` on NUMERIC). `SEPARATOR`, `SORTABLE`, `NOINDEX`, `INDEXMISSING`, `INDEXEMPTY`, `WITHSUFFIXTRIE` are rejected in v1 | A separator splits one string into several tags, which gives an element several values on one level: the cross product of §3, "Limits and costs". The other options keep ingest and GC simple |
| A NESTED field counts as **one** field toward `SPEC_MAX_FIELDS` (1,024) | Many attributes per entity is the reason the feature exists |
| Not supported `ON HASH` | Hashes have no arrays |

### Optional limits: cardinality and numeric ranges

The tree's size and the planner's worst-case fan-out are the product of the distinct values on
each level. Declaring bounds up front makes both predictable and catches bad data at ingest.
Each option is per sub-field and optional.

```
$.items[*] AS items NESTED 3
    $.color AS color TAG     MAXCARD 64
    $.size  AS size  TAG     MAXCARD 8
    $.price AS price NUMERIC RANGE 0 100000
  INDEX 3 color size price
```

| Option | Applies to | Meaning | At ingest | Effect on the index and the planner |
|---|---|---|---|---|
| `MAXCARD <n>` | TAG level | At most `n` distinct values on this level, counted per parent node | A value that would be the `n+1`-th child is an index error for the doc (the doc isn't indexed and the error shows in `FT.INFO`, like a type mismatch) | Caps children per node, so tree size ≤ ∏ `MAXCARD` × terminal size. An `Any` walk on this level opens at most `n` children, so the product of the `MAXCARD`s bounds the terminals any query can reach |
| `RANGE <lo> <hi>` | NUMERIC terminal | Valid values are `[lo, hi]` | A value outside is an index error | Validation only. The range tree still splits dynamically, but within known bounds, which the first splits can use |

#### Defaults

Both options are absent by default, which means no cardinality cap and no value bounds. A NESTED
field then has no ingest limit beyond what a TAG or multi-value NUMERIC field has today: any
number of elements per document and any number of distinct paths, made visible through
`FT.INFO` (§3, "Limits and costs"). These options are the only user-facing limits of the
feature. Everything else is an internal constant (`MAX_LEVELS`) or a query-time bound (§5,
step 3), none of them configurable.

#### Numeric attributes: only the terminal

The full `NumericRangeTree`, with ranges that split as data arrives and exact filtering of
partial leaves, exists only at the **terminal**. Its leaves are posting lists of
`(docId, value)`, and each entry carries the value the tree splits and filters on. A routing
level has no such value below it: its children own whole subtrees, and nothing under them stores
the routing level's value, so a numeric routing level could neither redistribute elements on a
split nor filter a range that cuts one of its children.

So a group routes on tags and puts its one numeric attribute last, as in `color, size, price`
or the keyed `(k, v)` case. To also narrow by a second number, such as a year, store a label
for the range it falls in (`"year_band": "2022-2023"`) and route on it as a TAG:
`@year_band:{2022-2023 | 2024-2025}`. The application picks the bands, and a query names whole
bands, which is all a fixed-boundary numeric level could offer.

Ways to support a second numeric attribute later:

1. **An adaptive numeric routing level**: a range tree whose leaves own subtrees. Every terminal
   entry also stores the routing level's value (8 more bytes per element per numeric routing
   level). A split rebuilds the two halves of the subtree from those values, and a range that
   cuts a leaf is filtered on them. This needs a reader that filters on two values.
2. **A filter attribute**: store the value in terminal entries without routing on it,
   `INDEX 3 color size price FILTER year`. Ranges on it are exact and need no splits, but a query
   narrowing only by `year` still reads every terminal.

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
  - the routing levels, each a `TagLevel`: a TrieMap per node, with an optional per-node
    `max_cardinality`;
  - `get_or_insert_with(path, init)`, which checks the cardinality limits before creating
    anything, so a rejected insert leaves the index unchanged, and returns `&mut T`. This is the first half of **route, then post**: the index
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
      /// Last level TAG: one doc-id posting list per value, as a TAG field's index.
      Tag(TagIndex<InMemoryMode>),
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
the cardinality limits for a whole path, or a whole document's paths, before inserting anything.
Every tag value is valid on every level, so a query has nothing to reject up front, and the only
errors of a walk are its terminal bound and the query timeout.

Public types, used by the later PRs:

- **`CompoundIndex<T>`**: the tree. It holds the routing levels, one root node, and a terminal of
  type `T` at the end of each distinct path. It is generic over `T` so the routing logic does not
  depend on how postings are stored: a range tree or tag postings in RAM (second PR), postings on
  disk for Flex (§4).
- **`TagLevel`**: one routing level, fixed at creation in `INDEX` order. Each node keeps its
  children keyed by exact value in a `TrieMap`, the structure the TAG index uses. The optional
  `max_cardinality` (`MAXCARD`) caps the children of each node, which bounds both memory and the
  fan-out of a walk.
- **A path** is one tag value (`&[u8]`) per level. Ingest builds it from an element's attributes,
  already normalised (case folding). A path of another length is a caller's bug and panics: C
  builds one value per level, and the FFI checks the rows it receives.
- **`Constraint`**: what a query asks of one level: `Any`, `TagIn`, `TagNotIn` or `TagPrefix`. The
  planner (fourth PR) turns `@items:( … )` into one `Constraint` per level (§5, step 1). The
  terminal attribute's own condition, such as a price range, is not a `Constraint`: the terminal
  applies it.
- **`TagSet`**: the values of `TagIn` and `TagNotIn`, sorted and without repeats by
  construction, so the walk looks each value up once and binary-searches a negation.
- **The walk's bounds**: `max_terminals` bounds how many terminals it returns, so the number of
  iterators the UNION opens. A `TimeoutChecker` (the `timeout` crate's trait, which query
  iterators already use) is checked on every child the walk considers, entered or not, so the
  query timeout bounds the work of a query that leaves a level open and matches little. Past
  either one, the walk fails rather than returning a truncated, wrong result.
- **`num_terminals`**: a running count of terminals, so `FT.INFO` reads it without a walk. It
  changes in exactly two places: +1 when an insert creates a path, −1 when GC (fifth PR) removes
  one.
- **Errors**: `SchemaError` (levels), `CardinalityExceeded` (from an insert or `check_paths`,
  naming the level and its limit, so the C side can build a user-facing message) and
  `CollectError` (the terminal bound and the timeout).

### Ingest path

1. **Load** (`document_basic.c`): for a NESTED field, `JSON_GetWithCompiledPath(root, path)`
   returns the elements. For each element, evaluate every compiled sub-path with
   `japi->getWithPath(element, subPath)`. That produces a row of `(value₁ … valueₙ)` per
   element. Store the rows in the `DocumentField` (a new `FLD_VAR_T_NESTED_ROWS` variant).
2. **Preprocess** (`document.c`, `preprocessorMap[IXFLDPOS_NESTED]`): validate the types and
   normalise TAG values (case folding unless `CASESENSITIVE`; no separator split, see §2). Drop elements
   that are missing any level's attribute. They aren't indexed, so they never match a group,
   which gives SQL's NULL behaviour. There is no cap on elements per document, as for
   multi-value fields today. Then check every row of the
   document (`NestedIndex::check_document`: value types, NaN, routing, and `check_paths` on all
   rows together), and fail the document if it errors. A preprocessor failure drops the whole
   document; an indexer failure would come after its other fields are written. Checking each row
   alone is not enough: two new values under one parent can each fit a `MAXCARD` limit alone and
   break it together, which would leave the document half indexed.
3. **Bulk add** (`IndexerBulkAdd`, new `case IXFLDPOS_NESTED`): under the spec write lock, run
   the same check again (it cannot fail while no level has `MAXCARD`, see §7 (3)).
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

The block shares encoding version 31 (`INDEX_NESTED_VERSION`) with SPARSEVECTOR: no release has
shipped 31 yet, so NESTED needs no version of its own. Only a NESTED field writes the block, so
indexes without one are unaffected. The block starts with its own format number, so its layout can
change later without another index-wide bump. Load applies the same validation as `FT.CREATE`,
rejects a NESTED type bit combined with any other type or with `Dynamic` (they share the union),
and rejects strings with an embedded NUL.

```
FieldSpec (existing header: name, path, types, options, sortIdx)
if types & NESTED:
  u64  nested_format          = 1
  u64  num_sub_fields
  repeat: string name, string rel_path, u64 type, u64 tag_flags,
          u64 limit_flags                     (bit 0 MAXCARD, bit 1 RANGE)
          if MAXCARD: u64 max_card
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
| `-@color:{red}` | `TagNotIn([red])`: an element holding a color other than red. An element with no `color` was never indexed (§3, "Ingest path"), so it matches neither `@color:{red}` nor its negation, as `NOT` does on a SQL `NULL` |
| `@color:{re*}` | `TagPrefix("re")`. A prefix shorter than `MINPREFIX` matches nothing, as on a TAG field. On a routing level it is a TrieMap prefix walk, bounded by the walk limits of step 3, which fail instead of truncating. On a tag terminal each matching value opens a reader; at most `MAXPREFIXEXPANSIONS` of them per prefix and branch, across the terminals reached, with the same warning as a TAG field |
| no clause on the level | `Any` |
| `@price:[lo hi]` (terminal NUMERIC), several ranges ANDed | terminal range `[lo, hi]` (intersected) |
| `@price` absent | terminal range `[-inf, +inf]` |

An `|` across sub-fields inside the group (`@color:{red} | @size:{M}`) becomes **disjunctive
normal form**: each branch is its own conjunction, planned separately, and the branches are UNIONed.
Cap the number of branches with an internal constant (16, not configurable) and fail the query
with a limit error past it. A clause with a single branch extends every conjunction in place, so planning
a long AND is linear in its clauses; only a clause with several branches copies the conjunctions,
and the cap allows few of those. A clause on something that isn't a sub-field of the group is a parse error.

### Step 2: walk the tree to collect terminals

`CompoundIndex::collect_terminals` walks the routing levels with one constraint each:

```
TagIn(set)       → for v in set: if let Some(c) = node.children.find(v) { walk(c) }
TagPrefix(p)     → for (_, c) in node.children.prefixed_iter(p): walk(c)
TagNotIn(set)    → for (v, c) in node.children.iter(): if v ∉ set { walk(c) }
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
- NUMERIC on a routing level, and so a second numeric attribute in a group (§2, "Numeric
  attributes: only the terminal").
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

1. **Design and routing layer**: this document and the `compound_index` crate: tag levels
   with their limits, path insert that checks before mutating, `check_paths` for a whole
   document, and the constraint walk with its terminal bound and timeout check. Pure Rust,
   tested under `miri`, with property tests against brute-force filters.
2. **Terminals**: the `nested_index` crate. `Terminal` is a `NumericRangeTree`, the tree
   top-level NUMERIC fields use, or the Rust `TagIndex<InMemoryMode>`. Top-level TAG fields still
   use the C tag index, so reading tag terminals in the read side (4) is the first production use
   of the Rust `TagIndex` reader and iterator. `NestedIndex`
   pairs the routing layer with its terminal kind, and `index_document` checks every element of a
   document (value type, NaN, all paths together with `check_paths`) before writing any of them.
   Rust tests linked against the C bundle. Opening iterators on terminals needs the search
   context and a revalidation design, so it moves to the read side (4).
3. **Write side: schema and ingest**:
   - `INDEXFLD_T_NESTED` (`0x80`, the last bit of `FieldSpec.types`), `nestedOpts` with the
     sub-field definitions kept in C, and the FFI crate `c_entrypoint/nested_index_ffi`.
   - Behind `ENABLE_UNSTABLE_FEATURES`, as SPARSEVECTOR is: creating a field is gated; loading
     and indexing an existing one are not.
   - `FT.CREATE` / `FT.ALTER` parsing and validation (`NESTED`, `INDEX`, `CASESENSITIVE`,
     `MAXCARD`, `RANGE`), `FT.INFO` attributes, and the spec fingerprint.
   - The spec RDB block, sharing encoding version 31 with SPARSEVECTOR.
   - The definition lives behind a pointer (`NestedDef`), so `FieldSpec` does not grow for other
     field types. With the flag off, `NESTED` (and `SPARSEVECTOR`) do not end the previous
     field's options, so a field named `nested` keeps its meaning.
   - JSON values follow a top-level JSON field: a `null` element is skipped, a boolean TAG value
     is the tag `"true"` or `"false"`, and a NUL byte in any TAG value rejects the document.
   - Ingest: `json.c` reads each element's sub-fields into `NestedRows` before the spec lock,
     folding TAG case as a JSON TAG field does. The NESTED preprocessor checks the whole
     document with `NestedIndex_CheckDocument`, so a rejected document has none of its fields
     indexed: by the time the indexer runs, the fields before it are already written. Without
     `MAXCARD` the check reads only the field's levels, never the tree, so it needs no lock and
     nothing indexed in between changes its outcome. `NestedIndex_IndexDocument` repeats it and
     writes under the lock; its deltas feed `invertedSize`, `numRecords` and the block count.
   - `MAXCARD` is rejected ("not supported yet") until GC (5): without GC a limit would keep
     counting the values of deleted and replaced documents and reject later updates. Bringing it
     back also brings back a stateful check, which the preprocessor can no longer run without the
     lock (see the follow-ups below).
   - Python tests: create and reject cases, indexing documents and checking `FT.INFO`, a reload,
     `FT.ALTER`, and query safety.
   - Deferred: NESTED fields are not counted in `INFO MODULES` field statistics yet.
4. **Read side: parser, planner and eval**:
   - Parsing (DIALECT 2 and up, with `ENABLE_UNSTABLE_FEATURES` on). The lexer emits
     `NESTED_MODIFIER` for `@name` only when the text goes on with `:(` and `name` is a NESTED
     field, so `@items:{red}` keeps the type error of any non-TAG field and `@title:(…)` on a
     TEXT field is unchanged. The coordinator parses queries that mention KNN without a spec;
     there a group is recognised because it names a field inside its parentheses, which a text
     group never does. `nested_open ::= NESTED_MODIFIER
     COLON LP` opens the sub-field scope on `QueryParseCtx` when it reduces, before the first inner
     field name does; `expr ::= nested_open expr RP` closes it and builds `QN_NESTED`. Inside the
     group a field name resolves only against the group's sub-fields ("Unknown sub-field"), and the
     TAG and NUMERIC rules check the sub-field's type. A group inside a group is rejected, and
     so is any clause a group does not support (text, lexicographic ranges, `ISMISSING`, ...),
     naming the group rather than a field type.
   - AST. `QN_NESTED { fieldIndex, filters }` has the inner expression as its only child. Its
     `QN_TAG` and `QN_NUMERIC` leaves carry the NESTED field's index, so every C pass that indexes
     `spec->fields` reads a valid field, plus a `subField` read only under `QN_NESTED`.
     `FT.EXPLAIN` prints `@items.color`. Query expansion and the SORTBY optimizer do not enter a
     group.
   - Planning (`nested_index::query`). The inner expression becomes an `Expr` over per-level
     `Clause`s; NOTs are pushed down (a negated range becomes the two ranges outside it) and ANDs
     distributed over ORs, capped at `max_branches` (16). Clauses on one level are merged, value
     lists first, then prefixes, then negations, so the plan or the error does not depend on
     how the query orders them, with value sets kept sorted so a merge is linear in their sizes:
     tag sets intersect, negations add up, ranges intersect, prefixes narrow; a branch that can
     match nothing is dropped. A negated prefix, or a prefix and a negation on one level, has no
     constraint and is rejected. A property test checks that an element matches the expression
     exactly when it matches one of the planned branches.
   - Eval (`query_eval/src/nodes/nested.rs`). Each branch walks the routing levels with
     `collect_terminals`, sharing `max_terminals` (10K) and the request timeout across branches.
     The timeout is read every 100 nodes entered. Past it, the walk runs before any document is
     read, so the query answers as for any timeout before the first read: an error under
     `FAIL`, otherwise no results with a timeout warning. The whole iterator tree is replaced
     by an empty one, since a tree built in part could answer a NOT with documents that do not
     match. The limits are `query_eval::NestedLimits` in the evaluator's `Config`, internal
     constants rather than `FT.CONFIG` options. A
     numeric terminal opens `NumericIteratorVariant::from_tree` with a filter the `QN_NESTED` node
     owns, so it lives as long as the AST; a tag terminal opens `TagIndex::open_reader` per value,
     listing the terminal's values for `Any`, `TagNotIn` and prefixes. At most `max_readers` (100K)
     iterators per group. The group is their UNION, which `FT.PROFILE` shows as a `UNION` of
     query type `NESTED`; its estimate is the union's.
   - Terminals own their tree or tag index through the pointer `Box::into_raw` returned, and hand
     it out (`Terminal::numeric_ptr`, `Terminal::tag_ptr`), as the numeric `RangeSource` and the
     tag `TrieLookup` require. Iterators revalidate as on top-level fields: a tag reader
     re-finds its value, and a numeric reader aborts when its tree changes shape, ending a
     cursor's results early exactly as on a top-level NUMERIC field. Terminals are only freed
     with the field until GC (5).
   - Tests: flow tests, standalone and in cluster, for every shape in the §5 table, tag
     terminals, case folding, PARAMS, composition with top-level fields, KNN, a cursor read
     across writes, updates and deletes, `FT.EXPLAIN`, `FT.PROFILE` and errors, plus 150 random
     groups against a brute-force evaluator on 60 random documents; Rust tests of the planner
     (with a property test) and of the evaluator.
   - Deferred: the benchmarks; exposing the limits as module configuration; `terminals_visited`
     and `levels_fixed` in `FT.PROFILE`.
5. **GC**: the compound scanner, fork-GC deltas keyed by path, and removing each emptied
   terminal by its path. Tests for update and delete churn followed by GC, checking that terminal
   and memory counts drop.

### Follow-ups tracked from review

Raised while reviewing the first PR and deliberately left to the PR that owns the code:

| Follow-up | Owner |
|---|---|
| An insert under a tag level does three trie lookups (find, insert, find). Add an entry-style API to `TrieMap` | Write side (3), where ingest cost is measured |
| Float compression is fixed when a `NestedIndex` is created, from `TerminalKind`. A top-level NUMERIC field reads the global setting when its tree is first created. Decide whether NESTED follows the field rule (read the setting per new terminal) or the index rule (fix it at `FT.CREATE`) | Write side (3) |
| Reading tag terminals is the first production use of the Rust `TagIndex` reader and iterator, `open_reader` and `TrieLookup`. Covered by the read side's tag-terminal flow tests; unit tests of a suspended tag terminal reader still to add | GC (5), with revalidation |
| A suspended query holds iterators on terminals, and GC can free a whole terminal while it is suspended, a use-after-free. Until GC (5) nothing frees a terminal before its field. GC must make revalidation detect it, for example with a revision counter on the compound index that GC bumps when it drops a terminal, checked before an iterator touches its terminal | GC (5) |
| Whether to support a second numeric attribute (§2, "Numeric attributes: only the terminal"): an adaptive numeric routing level or a filter attribute | After Terminals (2), driven by workloads with two numeric attributes |
| Only postings are reported (in `inverted_sz_mb`); the routing layer's tries and the terminals' own overhead are not in `FT.INFO` or the index memory total yet | Read side (4), with `FT.PROFILE` and memory reporting |
| `NestedIndex::num_records` must be decremented when GC removes postings | GC (5) |
| Re-enable `MAXCARD` once GC frees stale values. Its check counts existing children, so it depends on the tree: either run it in the preprocessor under the spec read lock and accept that a concurrent write can still fail the indexer, or keep it in the indexer and roll back the document's other fields | GC (5) |

## 8. Open questions

- Should the parser conflict be solved in the lexer (`NESTED_MODIFIER`), or with new syntax that
  doesn't overlap `@f:(text)`?
- Error or warning when the expansion cap is hit? This design says error. Product may prefer a
  warning with partial results, as prefix queries do today.
- Should v1 already allow sub-fields that aren't index levels, served by a slower
  "filter on load" path, or keep the strict rule?
- When a value breaks `MAXCARD` or `RANGE`: reject the whole document (proposed), or
  index the document without the offending element?
- Should `MAXCARD` count distinct values per parent node (as the routing layer does, which bounds
  fan-out exactly) or across the whole level (easier to explain, but a looser bound)?
