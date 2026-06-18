# Design: per-block field-expiration bitset

## Overview

Give every inverted-index entry a bit — "a field this entry belongs to has a
field-level expiration (HFE) for this document" — stored in a **side bitset on
the block** (not inside the encoded entry), and have expiration-aware iterators
consult it to skip the TTL-table lookup for entries whose fields never expire.
When the bit is clear, none of the entry's fields can be expired, and the matched
field(s) of any query are a subset of the entry's fields, so every `Verify*`
path would report the document as valid (not expired) — the iterator returns
"not expired" without touching the TTL table at all.

The bit is **field-granular**: for tag/numeric indexes (one index per field) it
reflects that single field; for term indexes it reflects whether *any* field in
the posting's field mask has an expiration. This is tighter than a doc-level
"does this document have any field TTL" bit, which would force a lookup on every
posting of a document that has a TTL on *any* field, even when the queried field
never expires.

The document-id codec is left **completely unchanged** — the bit rides alongside
the block, not in its byte stream. (An earlier design packed the bit into the
delta's least-significant bit; see *Alternatives*. It was rejected because it
turned the codec into a format with a fragile monotonicity invariant and forced
churn across every encoder/decoder/seek and their tests. The side-bitset keeps
the codec a pure doc-id codec.)

## Data model: `IndexBlock.expiration_bits`

`IndexBlock` (`src/redisearch_rs/inverted_index/src/index/core.rs`) gains:

```rust
pub(crate) expiration_bits: Option<Box<[u8]>>,
```

- A bitset indexed by **entry ordinal** within the block: bit `i` is set iff a
  field the `i`-th entry belongs to has a field-level expiration for its document.
- `Option<Box<[u8]>>` (one pointer, 8 bytes) rather than `Vec<u8>` (24 bytes) so
  a block whose documents have no field expirations — the common case — costs
  only the idle pointer (`None`) and no heap allocation. `ThinVec<u8>` would also
  be 8 bytes but has no `serde` impl, and `IndexBlock` is serialized across the
  fork-GC boundary via `rmp_serde`, so the bitset must serialize with it.
- Grown lazily and only up to the highest set ordinal (`set_expiration_bit`);
  ordinals beyond the grown length read as `false` (`expiration_bit`).

## Where the signal comes from: `RSIndexResult.has_field_expiration`

`RSIndexResult` (`#[repr(C)]`, exported to C) gains a `pub has_field_expiration:
bool`. It round-trips:

- **Write (indexing).** The C producers compute it per entry from the *field(s)*
  the entry belongs to (see *Producers*). `doc->fieldExpirations` is already moved
  into the TTL table by `doAssignIds` before postings are written, so the
  per-field presence is read back from the doc table. `add_record` writes the bit
  into the block's bitset at the new entry's ordinal.
- **Read (query).** The reader sets it on the decoded result from the block's
  bitset; the iterator reads it before deciding whether to call
  `TimeToLiveTable_Verify*`.

## Reader

`IndexReaderCore` tracks `entry_in_block` (ordinal of the next entry to decode),
reset to 0 on every block change:

- `next_record`: after decode, `result.has_field_expiration =
  block.expiration_bit(entry_in_block)`, then increments.
- `seek_record`: `Decoder::seek` now returns `Option<u16>` — `None` for "not
  found", `Some(advanced)` = entries traversed from the cursor's start up to and
  including the landed entry. The reader maps that to the landed ordinal
  (`entry_in_block + advanced - 1`) and reads the bit there. `RawDocIdsOnly`'s
  binary-search `seek` (fixed 4-byte stride) computes `advanced` from the landed
  absolute ordinal; the qint/varint seeks count loop iterations. This is the only
  `Decoder` API change, and it is internal to the crate — the public
  `IndexReader::seek_record` still returns `bool`.

## Producers (set the bit at index time)

Each producer computes the bit from the field(s) the entry belongs to, reading
per-field presence from the doc table (`DocTable_GetFieldExpirations` /
`DocTable_FieldHasExpiration` in `src/doc_table.h`):

- **Term**: `indexText` (`src/indexer.c`) builds, once per document, the mask of
  the document's **text** fields that have an expiration (`FIELD_BIT` space, text
  fields only — non-text fields have no `ftId`), then sets the bit per entry as
  `(entry->fieldMask & expiringTextFields) != 0` → `writeIndexEntry` →
  `InvertedIndex_WriteForwardIndexEntry`.
- **Tag**: `tagIndexer` (`src/document.c`) sets the bit to
  `DocTable_FieldHasExpiration(docId, fs->index)` for the tag field →
  `TagIndex_Index` → `TagIndex_WritePostings` → `tagIndex_Put`.
- **Numeric / geo**: `numericIndexer` (`src/document.c`) sets the bit to
  `DocTable_FieldHasExpiration(docId, fs->index)` → `NumericRangeTree_Add` macro →
  `_NumericRangeTree_Add` (FFI) → `NumericRangeTree::add` → `node_add` (recursive)
  → `NumericRange::add` / `add_without_cardinality`, which stamp the record. A
  **range split** re-decodes entries (the reader sets the bit) and re-adds them,
  so the split path passes the decoded `result.has_field_expiration` to preserve
  it.

## Iterator gate

`FieldExpirationChecker::is_expired`
(`src/redisearch_rs/rqe_iterators/src/expiration_checker.rs`) short-circuits:
after the index-wide `ttl != NULL` gate, for the **`Default` predicate** only, if
`!result.has_field_expiration` it returns `false` (not expired) without touching
the TTL table. This is sound because a query's matched fields are always a subset
of the entry's fields, so a clear bit (no field of the entry expires) guarantees
no matched field expires. When the bit is set the real per-field lookup runs, so
it is correct even if the bit was set by a field other than the one queried.

The short-circuit deliberately excludes the **`Missing` predicate**
(`ismissing`, served by the `Missing` iterator over a per-field missing-docs
index). Those postings do not carry the expiration bit, and `Verify*` under the
`Missing` predicate returns a different answer for documents that *do* have a TTL
entry (a clear bit would not imply "valid"), so that path always runs the full
`TimeToLiveTable_Verify*` check — preserving exact `ismissing` semantics.

The vector/hybrid path is unaffected — VecSim indexes have no `IndexBlock`, so
they keep using the C `DocTable_CheckFieldExpirationPredicate` path (correct,
just unoptimized).

## Behavior change: HEXPIRE/HPERSIST reindex when a field gains its first TTL

`Indexes_UpdateMatchingHashFieldExpiration` (`src/indexes.c`) currently refreshes
only the TTL table on `HEXPIRE`/`HPERSIST`. Because the bit is baked into the
postings at index time, a **field that gains its first field-level expiration**
(that field's `0→1` transition) now needs a reindex (`IndexSpec_UpdateDoc`): that
field's postings still hold bit=0, so iterators would skip the check and wrongly
return the now-expiring field.

The trigger is computed by `fieldExpirationGained(before, after)`, which compares
the document's field-index set in the TTL table (`before`) with the set this
update would apply (`after`, both sorted by field index) and returns true iff
`after` contains a field not in `before`. A field *losing* its TTL (in `before`,
not in `after`) and value-only changes leave a stale *set* bit, which is harmless
(an extra lookup that resolves to "not expired"), so they stay on the cheap
metadata-only refresh. This matters for the field-granular design: a doc that
already has a TTL on one field but gains one on a *second* field must still
reindex (the second field's postings are stale at 0) — a doc-level "has any TTL"
trigger would miss it.

## Garbage collection

Fork-GC repair (`src/redisearch_rs/inverted_index/src/gc.rs`) rebuilds a block by
decoding survivors and re-`add_record`-ing them into a fresh block. The repair
loop tracks the old block's entry ordinal and sets `result.has_field_expiration`
from the old bitset before re-adding, so `add_record` re-propagates it into the
new block's bitset — no TTL-table lookups during GC. The same mechanism carries
the bit across numeric range splits.

## No persistence impact

Inverted indexes are not serialized to RDB (the only RDB inverted-index code is
`InvertedIndex_RdbLoad_Consume`, which consumes the legacy format); on load they
are rebuilt from the keyspace via `IndexSpec_ScanAndReindex`. The bit is
recomputed during that scan from each document's field expirations. The
`serde`-via-`rmp_serde` serialization of `IndexBlock` is for **fork-GC IPC only**
(parent/child), not persistence — which is why the bitset field must keep its
`serde` derives.

## Edge cases

- **Wide field masks (`Index_WideSchema`):** handled — the term producer builds
  the expiring-text mask and ANDs it with the entry's mask in `t_fieldMask`
  (128-bit) space, so wide masks work without a separate path.
- **Multi-value entries (same doc id repeated in a block):** each posting gets its
  own field-mask-derived bit at its own ordinal.
- **`RawDocIdsOnly` seek:** must report `advanced` consistently with the fixed
  4-byte stride; covered by a dedicated test.
- **Numeric range split:** must preserve the decoded bit on re-add (covered).
- **Doc whose only expiration is on its vector field:** the field-granular term/
  tag/numeric producers don't see that field as one of their own, so those
  postings keep bit=0 and the gate skips the lookup → correct *and* with no wasted
  probe. The vector field's expiration is checked on the (unchanged) C vector
  path.

## Alternatives considered

- **Bit packed into the delta's LSB (inline in the encoded entry).** Zero extra
  bytes and rides the bytes already decoded, but it makes the codec a format with
  a "packed delta stays monotonic" invariant (load-bearing for the `RawDocIdsOnly`
  binary search), forces pack/unpack edits across every encoder, decoder, and
  seek, changes the on-wire bytes (breaking all direct-codec byte/round-trip
  tests), and complicates block-split overflow accounting. Rejected in favor of
  the side bitset, which leaves the codec untouched. (Query-time cost is
  equivalent: the side bitset is a sequential, cache-friendly read of ≤125 bytes
  per block, negligible next to the TTL hash lookup it removes.)
- **TTL-table Bloom filter (keyed by doc id, global).** `O(1)` to maintain on
  HEXPIRE and small, but it only fronts the same `buckets[doc_id]` direct-slot
  array, which is already a near-free sequential read under ascending iteration;
  a hash-scattered Bloom is neutral-to-worse there. Rejected as the primary
  mechanism; kept as a possible fallback if the reindex-on-HEXPIRE cost proves
  unacceptable.
- **Per-inverted-index Bloom filter (per posting list).** Keyed by doc id but
  scoped per list, so HEXPIRE (which doesn't know which lists a doc is in) can't
  update it without re-tokenizing — same reindex cost as the inline bit — and
  lazy updates would introduce false negatives (a correctness bug). Also the most
  memory-hungry (per-posting fan-out). Rejected.

## Testing strategy

- **Rust `inverted_index`** (`tests/expiration_bit.rs`): bitset get/set;
  per-encoder sequential round-trip (doc-ids-only, fields, numeric, raw);
  `RawDocIdsOnly` binary-search seek with the bit set; block-split onto a new
  block's first entry; GC re-encode carry.
- **Rust `numeric_range_tree`**: existing suite exercises the new `add` signature
  through inserts and splits; debug-dump snapshots updated for the +8-byte block.
- **Python e2e** (`tests/pytests/test_expire.py`): the existing field-expiration
  suite (term/tag/numeric/JSON/wide-schema/hybrid) passes unchanged, plus two new
  tests — `testInlineFieldExpirationBitReindexOnHexpire` (index without a TTL,
  verify matches, `HPEXPIRE`, verify term/tag/numeric all filter the doc) and
  `testInlineFieldExpirationBitReindexWhenSiblingFieldAlreadyExpiring` (a field
  gains its first TTL while a sibling field already has one — exercises the
  field-granular reindex trigger that a doc-level trigger would miss).
- **Build/lint**: full `./build.sh` (C+Rust) green; `make fmt` + `make lint`
  clean.
