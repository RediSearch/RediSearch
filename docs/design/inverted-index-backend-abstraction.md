# Inverted-index backend abstraction

**Status:** foundation branch. `InvertedIndex` remains the concrete storage type; this
change extracts the shared storage surface into traits without selecting or renaming
the implementation.

## Goal

Make code that only needs inverted-index storage capabilities independent of the
particular storage wrapper it receives. Today the module has three storage shapes:

| Storage type | Extra behavior |
|--------------|----------------|
| `InvertedIndex` | Owns blocks, encoding, readers, GC, and introspection |
| `EntriesTrackingIndex` | Tracks total entries in addition to unique docs |
| `FieldMaskTrackingIndex` | Tracks the union of field masks |

They expose the same core operations: writes, reader construction, GC scan/apply,
and introspection. `IndexBackend` names that contract so generic code and tests can
exercise those paths once instead of depending on a specific wrapper.

## Shape

| Concern | Trait | Notes |
|---------|-------|-------|
| storage: write / reader construction / GC / introspection | `IndexBackend` | implemented by `InvertedIndex` and the storage wrappers |
| prepared numeric writes | `NumericIndexBackend` | extension trait for numeric range indexes that already prepare values before writing |
| iteration and revalidation | `IndexReader` | already exists; remains reader-specific |

Field-mask and numeric filtering remain reader adapters because they are query-time
concerns, not storage behavior.

## Reader Revalidation

Revalidation belongs to readers. `IndexReader::needs_revalidation` already gives the
FFI and query engine the right question to ask without exposing the underlying marker.
The index-level `InvertedIndex_GcMarker` and `InvertedIndex_GcMarkerInc` FFI exports
had no callers, so removing them keeps the public Rust-to-C surface focused on the
reader-level operation that is actually used.

`HasInnerIndex` remains a narrow identity hook for the current term readers to compare
themselves with an opaque wrapper. It is intentionally separate from `IndexBackend`
because most storage operations do not need that identity coupling.

## Non-goals

- Do not rename `InvertedIndex`.
- Do not add a compile-time storage selector.
- Do not introduce a snapshot-read feature in this PR.
- Do not add runtime dispatch on read paths; the traits use static dispatch.

## Result

The PR lands a small contract that is useful on master as-is:

1. `IndexBackend` covers the storage operations shared by `InvertedIndex`,
   `EntriesTrackingIndex`, and `FieldMaskTrackingIndex`.
2. `NumericIndexBackend` covers prepared numeric writes used by numeric range indexes.
3. Generic tests exercise write, read, GC, and wrapper bookkeeping through the trait.
4. Dead `gc_marker` FFI exports are removed; revalidation remains on `IndexReader`.
