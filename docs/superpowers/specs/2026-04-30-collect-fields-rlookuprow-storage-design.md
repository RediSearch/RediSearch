# Spec: Migrate `RemoteCollectCtx` storage to `Vec<RLookupRow<'a>>`

**Date**: 2026-04-30
**Status**: Design — pending implementation plan
**Scope**: Single PR, Rust-side only, behavior-preserving refactor.

## Project context

This is the first of three planned PRs that together add `FIELDS *` (wildcard) support to the COLLECT reducer:

| PR | Scope |
|---|---|
| **This PR (storage refactor)** | Switch `RemoteCollectCtx` storage to a row-shaped container. No behavior change. |
| Follow-up PR (wildcard on shard) | Honor `has_wildcard` in `RemoteCollectReducer`. Plumb `RLookup *` through FFI. Lift the factory-side rejection of `*` for the remote case. Single-shard tests. |
| Follow-up PR (wildcard end-to-end) | `LocalCollectReducer` wildcard finalize. Coordinator parser. `buildCollectArgs` propagation. Cluster tests. |

This document covers **only the first PR**. Each follow-up will get its own design document.

## Problem statement

The current `RemoteCollectCtx` (`src/redisearch_rs/reducers/src/collect/remote.rs`) accumulates collected rows as two parallel vectors:

```rust
pub struct RemoteCollectCtx {
    field_values: Vec<Vec<SharedValue>>,
    sort_values: Vec<Vec<SharedValue>>,
}
```

This shape is fine for today's `FIELDS @x ...` parsing but blocks the upcoming `FIELDS *` (wildcard) work because:

- The wildcard's emission set is the source `RLookup`'s keys (filtered), not a fixed projection list known at parse time.
- A row-shaped storage that keys values by source `RLookupKey::dstidx` lets both the explicit-FIELDS and the wildcard finalize paths read from the same container.

The goal of this PR is **only** to switch the storage shape so the future wildcard work has a natural place to land. **No behavior changes, no FFI changes, no parser changes.**

## Goals

1. Replace `field_values: Vec<Vec<SharedValue>>` and `sort_values: Vec<Vec<SharedValue>>` with one `rows: Vec<RLookupRow<'a>>` field.
2. Tie the storage's lifetime to `RemoteCollectReducer<'a>`'s lifetime, matching the existing borrow structure (`field_keys: Box<[&'a RLookupKey<'a>]>`).
3. Preserve every observable property: same FFI surface, same `add()` / `finalize()` output for every existing test, same drop semantics under bumpalo.

## Non-goals

- No `FIELDS *` parsing or finalize logic — that lands in the wildcard-on-shard follow-up.
- No `LocalCollectReducer` changes — its input shape (shard payloads) does not need this storage layout, and any wildcard-on-coordinator work is independently scoped.
- No new FFI parameters. `&'a RLookup<'a>` is **not** plumbed in this PR; it is added by the wildcard-on-shard follow-up when there's an actual consumer.
- No SORTBY / LIMIT / cap restoration — that lives in `2026-04-29-collect-sortby-limit-merge-restore-plan.md` and is independent. Whichever lands second resolves the merge mechanically.
- No parser tightening (`FIELDS *` vs `FIELDS <n> ...` mutual exclusivity) — that is a separate in-flight PR.
- No `include_sort_keys` → `is_internal` rename — that is a separate in-flight PR.

## Background

`RLookupRow<'a>` (`src/redisearch_rs/rlookup/src/row.rs`) is a row container with:

- `sorting_vector: RSSortingVectorRef<'a>` — borrowed view; defaults to an empty placeholder (`RSSortingVectorRef::empty()`) compatible with any lifetime.
- `dyn_values: ThinVec<Option<SharedValue>>` — owned, indexed by `RLookupKey::dstidx`.
- `num_dyn_values: u32`.

When the reducer never calls `set_sorting_vector` on a stored row, the `'a` parameter is purely a marker for *logical* dependence on the source `RLookup` (whose `dstidx` numbering gives the row indices their meaning), not a real borrow into row data. Stored values are owned `SharedValue` clones (Arc bumps).

The reducer is constructed once per query (per group) with `field_keys: Box<[&'a RLookupKey<'a>]>` and `sort_keys: Box<[&'a RLookupKey<'a>]>`, both borrowing from the source `RLookup`. The new storage's `'a` matches.

## Design

### Type changes

```rust
// Before
pub struct RemoteCollectCtx {
    field_values: Vec<Vec<SharedValue>>,
    sort_values: Vec<Vec<SharedValue>>,
}

// After
pub struct RemoteCollectCtx<'a> {
    rows: Vec<RLookupRow<'a>>,
}
```

`RemoteCollectReducer<'a>::alloc_instance` returns `&mut RemoteCollectCtx<'a>` (one new lifetime annotation; the bump arena is owned by the reducer, so `'a` flows through naturally).

The C-visible FFI casts inside `c_entrypoint/reducers_ffi/src/collect/remote.rs` continue to use elided lifetimes:

```rust
let collect = unsafe { ctx.cast::<RemoteCollectCtx>().as_mut().unwrap() };
```

The same pattern is already used today for `RemoteCollectReducer` (also `<'a>`); the cast itself is unsafe and lifetime-erasing.

### `add()` semantics

```rust
pub fn add(&mut self, r: &RemoteCollectReducer<'a>, src_row: &RLookupRow<'_>) {
    let mut dst = RLookupRow::new();
    for key in r.field_keys.iter() {
        if let Some(v) = src_row.get(key) {
            dst.write_key(key, v.clone());
        }
    }
    if r.include_sort_keys {
        for key in r.sort_keys.iter() {
            if let Some(v) = src_row.get(key) {
                dst.write_key(key, v.clone());
            }
        }
    }
    self.rows.push(dst);
}
```

Two notable storage changes vs. today:

- **Sort values are not stored when `include_sort_keys` is false.** Today's code stores them unconditionally and emits only when the flag is true. The new code skips both storage and emission. **Output is identical**; this is a small memory saving.
- **Missing values become `None` slots in `dyn_values` instead of explicit `SharedValue::null_static()` entries in the projected vec.** The `null_static` translation happens at emission time. Output to the C side is identical.

### `finalize()` semantics

```rust
pub fn finalize(&mut self, r: &RemoteCollectReducer<'a>) -> SharedValue {
    let rows = mem::take(&mut self.rows);
    SharedValue::new_array(rows.into_iter().map(|row| {
        let mut entries = Vec::with_capacity(
            r.field_keys.len()
                + if r.include_sort_keys { r.sort_keys.len() } else { 0 },
        );
        for key in r.field_keys.iter() {
            let val = row.get(key).cloned().unwrap_or_else(SharedValue::null_static);
            entries.push((SharedValue::new_string(key.name().to_bytes().to_vec()), val));
        }
        if r.include_sort_keys {
            for key in r.sort_keys.iter() {
                let val = row.get(key).cloned().unwrap_or_else(SharedValue::null_static);
                entries.push((SharedValue::new_string(key.name().to_bytes().to_vec()), val));
            }
        }
        SharedValue::new_map(entries)
    }))
}
```

Each emitted map has `field_keys` first, then `sort_keys` if `include_sort_keys` — same order as today.

### Drop semantics

`RemoteCollectCtx` is bump-arena-allocated (`Bump` does not run destructors). `collectRemoteFreeInstance` calls `ptr::drop_in_place(ctx.cast::<RemoteCollectCtx>())`. With the new field, that walks:

1. `RemoteCollectCtx::rows: Vec<RLookupRow<'a>>` — drops the Vec, releasing each row.
2. Each `RLookupRow::dyn_values: ThinVec<Option<SharedValue>>` — drops, releasing each `Option<SharedValue>`.
3. Each `Some(SharedValue)` — Arc decrement.

This is the same chain depth as today's `Vec<Vec<SharedValue>>` and the same number of Arc decrements.

### Memory characteristics

Per-row storage today vs. after this PR. Let `K = field_keys.len()`, `S = sort_keys.len()`, `N = max(dstidx among written keys) + 1`.

**Today** — two `Vec<SharedValue>` per row:

- `field_values[i]: Vec<SharedValue>` — 24 byte header (cap, len, ptr) + `8 * K` bytes on heap.
- `sort_values[i]: Vec<SharedValue>` — 24 byte header + `8 * S` bytes on heap (`S = 0` and the inner vec is empty when there's no SORTBY, but the empty vec's 24-byte header is still pushed per row).

Total per row ≈ `48 + 8 * (K + S)` bytes.

**After** — one `RLookupRow<'a>` per row:

- `RLookupRow` struct itself ≈ 32 bytes (16 byte sorting-vector view + 8 byte `ThinVec` header + 4 byte counter + padding).
- `dyn_values` heap allocation: 8 bytes per `Option<SharedValue>` slot × `N` slots (`Option<Arc<T>>` is niche-optimized to pointer-sized).
- Plus, when `include_sort_keys=true`, sort-key writes share the same `dyn_values` (deduped by `dstidx`); when false, they are not stored at all.

Total per row ≈ `32 + 8 * N` bytes.

The new shape is **smaller** when `K + S ≥ N - 2` (roughly: when the projected keys are dstidx-dense). It is larger only when projected keys sit at sparse high dstidx values in a wide source lookup — i.e. `N >> K`. The worst-case overhead is bounded by the source RLookup's width, the same upper bound that any other `RLookupRow` storage in the codebase pays. Acceptable for the layout uniformity it buys downstream.

## Testing

Existing tests must pass without modification:

- `src/redisearch_rs/reducers/tests/collect.rs` — `remote_external_collect_emits_only_requested_fields`, `remote_internal_collect_includes_sort_fields_for_coordinator_merge`, `local_collect_projects_remote_maps_and_fills_missing_fields_with_null`, `local_collect_accepts_resp2_flat_array_payloads`.
- All `tests/pytests/test_groupby_collect.py` cases.

One new Rust unit test:

- `remote_storage_dedupes_same_key_in_fields_and_sortby` — register one key, set both `field_keys` and `sort_keys` to that single key, set `include_sort_keys=true`, add one row. Storage holds one `dyn_values` slot for that key (deduped by `dstidx`). Finalize emits the key **twice** in the output map (once via the `field_keys` loop, once via the `sort_keys` loop) — bit-identical to today's parallel-vec behavior. The test pins this so the storage dedup doesn't accidentally suppress the emission duplication.

## Risks and follow-ups

### What this enables — wildcard-on-shard follow-up (out of scope here)

The point of this refactor is to make `FIELDS *` natural to add later. The next PR will:

- Add `src_lookup: &'a RLookup<'a>` to `RemoteCollectReducer`.
- Add `*const ffi::RLookup` to `CollectReducer_CreateRemote`.
- Lift the factory-side rejection of `*` (`if (data.has_wildcard) { error … }` in `RDCRCollect_New`) for the remote branch.
- Implement wildcard `add()` and `finalize()` paths that iterate the captured lookup's cursor and filter by `is_tombstone()` / `RLookupKeyFlag::Hidden` (option-C semantics; schema-rule special-key filtering deferred as future work).
- Address one wildcard-specific emission concern: when `has_wildcard=true` and `include_sort_keys=true`, the wildcard cursor sweep already emits sort keys (they're members of the lookup). The wildcard PR will gate or dedupe to avoid double-emission. This is purely a finalize-time concern; **this PR's storage is forward-compatible with no changes** because writes overlapping `dstidx` already dedupe inside `dyn_values`.

The wildcard-end-to-end follow-up will then propagate `*` on cluster: coordinator parser, `buildCollectArgs` serialization, `LocalCollectReducer` finalize.

### Risks confirmed not blocking

| Concern | Resolution |
|---|---|
| FFI cast with new `<'a>` parameter | Identical pattern to today's `RemoteCollectReducer<'a>` casts; lifetime is erased by the unsafe boundary. |
| `Drop` chain through `Vec<RLookupRow<'a>>` | Same depth and same Arc-decrement count as today. |
| `SharedValue::null_static` sentinel for missing fields | Translated at emission time via `.unwrap_or_else(SharedValue::null_static)`; identical output. |
| Empty `RemoteCollectCtx` (no `add()` calls) | `mem::take` of empty Vec → empty array. Identical. |
| Memory regression in narrow `FIELDS @x` queries | Bounded by source-RLookup `dstidx` width. Acceptable. |
| `field_keys` overlapping `sort_keys` on same `dstidx` | Both writes pull from same source value; benign overwrite. Same emission as today. |
| Coexistence with the SORTBY / LIMIT plan | Independent change; conflict-resolves mechanically. |
| Coexistence with the `include_sort_keys` → `is_internal` rename PR | Trivial textual conflict. |
| Coexistence with the parser-tightening PR | This PR doesn't touch parser. |

## Implementation outline

The implementation plan will land separately (via the writing-plans skill) but the change set is small enough to scope here:

1. Edit `src/redisearch_rs/reducers/src/collect/remote.rs`: change `RemoteCollectCtx` per the type changes above; rewrite `add()` and `finalize()`.
2. Edit `src/redisearch_rs/c_entrypoint/reducers_ffi/src/collect/remote.rs`: update casts to `RemoteCollectCtx` (lifetime elision sufficient).
3. Add the one new Rust unit test.
4. Verify per `.claude/skills/verify/SKILL.md`: full Rust suite + pytest suite + lint + fmt.

No FFI header regeneration. No C-side changes. No pytest changes.

## Definition of done

- `cargo nextest run -p reducers` green.
- `cd src/redisearch_rs && cargo nextest run` green.
- `./build.sh && ./build.sh RUN_PYTEST TEST=test_groupby_collect` green.
- `./build.sh RUN_PYTEST` green.
- `make lint` clean.
- `make fmt CHECK=1` clean.
- One new Rust unit test for the FIELDS-overlaps-SORTBY storage-dedupe vs emission-duplication invariant.
- No FFI signature changes; no parser changes; no `LocalCollectReducer` changes.
