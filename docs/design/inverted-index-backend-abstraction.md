# Inverted-index backend abstraction

**Status:** foundation landed on a master-based branch (`IndexBackend` trait + rename +
storage-surface expansion). Snapshot backend re-layering is future work.

## Goal

Let master's proven in-place inverted index and the copy-on-write / snapshot index
(MOD-16139) **coexist in one build tree, selectable at compile time**, so the snapshot
feature can ship opt-in (`--features snapshot-reads`) and be validated against master before
becoming the default — with master as a zero-overhead fallback until then.

## Why not two forked implementations

The naive approach — keep two full implementations behind `#[cfg]` — is a maintenance sink:
the two diverge across ~2,000 lines (struct, `add_record`, `memory_usage`, the entire GC, the
entire reader), and every future change to indexing/read/GC would have to be done twice.

## The elegant solution: two traits the codebase already has

The two implementations already share an **identical public contract** — the snapshot feature
was built to preserve it (`add_record`, `scan_gc`, `apply_gc`, and the `GcScanDelta` /
`GcApplyInfo` types are byte-identical on both). So the abstraction is a straightforward
trait extraction, not a reconciliation:

| Concern | Trait | Notes |
|---------|-------|-------|
| storage: write / reader construction / GC / introspection | **`IndexBackend`** | new; both backends implement it |
| iteration **+ revalidation** | **`IndexReader`** | **already exists**; both backends already implement it |

### The key insight: revalidation belongs to the *reader*, and already does

The one place the backends genuinely differ is **staleness detection after concurrent GC**:

- **in-place:** the reader compares a captured `gc_marker` (counter) to the index's current one.
- **snapshot:** the reader compares `Arc::ptr_eq` on the `sealed` region (pointer identity).

But this difference is **already hidden** behind a single reader-trait method that both
backends implement:

```rust
IndexReader::needs_revalidation(&self) -> bool
```

The FFI already exposes this uniformly (`IndexReader_Revalidate`); the C side never sees the
mechanism. There is **nothing new to abstract** — the codebase already put revalidation on the
reader, where it belongs, and each backend keeps its own (optimal) mechanism. The snapshot
backend's pointer-identity approach is strictly better than a counter (it ignores appends and
only revalidates on actual compaction), so we must *not* unify these into a shared token — the
`bool` question is the right level of abstraction.

### The index-level `gc_marker` FFI is vestigial

`InvertedIndex_GcMarker` / `GcMarkerInc` have **no callers** (C or tests). The marker is bumped
in Rust (`apply_gc`) and captured/compared inside the reader; the exported functions are dead.
They can be dropped on master exactly as #10348 dropped them on the feature — after which the
FFI has **no backend-specific surface** at all.

## Resulting shape

- `IndexBackend` + `IndexReader` are the whole contract. The FFI and query engine depend only
  on them plus the (orthogonal) encoding enum.
- Backend selection is a compile-time type alias:
  ```rust
  #[cfg(feature = "snapshot-reads")] pub type InvertedIndex<E> = SnapshotInvertedIndex<E>;
  #[cfg(not(feature = "snapshot-reads"))] pub type InvertedIndex<E> = InPlaceInvertedIndex<E>;
  ```
  Monomorphized — **zero runtime dispatch** on the read hot path.
- `gc_marker` stays an in-place-internal detail (its reader uses it); it is not part of any
  trait, so the snapshot backend simply doesn't have it.

## Steps

1. **Done:** `IndexBackend` trait + impl for the in-place index; rename `InvertedIndex` struct
   → `InPlaceInvertedIndex` with an `InvertedIndex` alias (FFI/C ABI unchanged); expand the
   trait to the full storage surface (`blocks_summary`, `last_doc_id`).
2. **Small, master-only:** drop the vestigial `gc_marker` FFI exports.
3. **On re-layering:** the snapshot index implements `IndexBackend` (its reader already
   implements `IndexReader`); add the `snapshot-reads` feature + the selecting alias; route the
   FFI's introspection/GC calls through the trait so both backends flow through unchanged.
