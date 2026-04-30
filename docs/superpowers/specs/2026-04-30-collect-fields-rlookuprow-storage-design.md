# Spec: Migrate `RemoteCollectCtx` storage to `Vec<RLookupRow<'a>>`

**Date**: 2026-04-30
**Status**: Design — pending implementation plan
**Scope**: Single PR, Rust-side only, primarily a storage-shape refactor; bundles two small finalize-time improvements (key-dedup, name-allocation hoisting) that fall out for free.

## Project context

This is the first of three planned PRs that together add `FIELDS *` (wildcard) support to the COLLECT reducer:

| PR | Scope |
|---|---|
| **This PR (storage refactor)** | Switch `RemoteCollectCtx` storage to a row-shaped container. Bundles two free finalize-time micro-improvements (dedup + name-allocation hoisting). No FFI / parser changes. |
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

The primary goal of this PR is to switch the storage shape so the future wildcard work has a natural place to land. **No FFI changes, no parser changes.** Two small finalize-time improvements are bundled because they fall out naturally from the storage refactor and would feel artificial to split into a follow-up:

1. **Dedup overlapping `field_keys` / `sort_keys` in emission** — today's code emits a key twice when it appears in both lists (with `include_sort_keys=true`). The new code emits it once. No existing test pins the duplicate behavior (`test_collect_internal_duplicate_field_and_sort` uses `assertIn`, not exact-count assertion).
2. **Hoist name `SharedValue` allocations out of the per-row loop** — today's code rebuilds each name's `Vec<u8>` and `Arc` on every emission; the new code builds them once per `finalize` and clones (cheap Arc bump) per row.

## Goals

1. Replace `field_values: Vec<Vec<SharedValue>>` and `sort_values: Vec<Vec<SharedValue>>` with one `rows: Vec<RLookupRow<'a>>` field.
2. Tie the storage's lifetime to `RemoteCollectReducer<'a>`'s lifetime, matching the existing borrow structure (`field_keys: Box<[&'a RLookupKey<'a>]>`).
3. Preserve every observable property *that today's tests pin*: same FFI surface, same drop semantics under bumpalo, same output for every existing Rust unit and pytest case (the dedup tightening only affects an emission shape no existing test asserts on exact-count terms).

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
    // Always copy sort-key values into `dst` (independent of `include_sort_keys`):
    // a planned follow-up caches sort values for SORTBY/heap re-use.
    for key in r.sort_keys.iter() {
        if let Some(v) = src_row.get(key) {
            dst.write_key(key, v.clone());
        }
    }
    self.rows.push(dst);
}
```

One notable storage change vs. today: **missing values become `None` slots in `dyn_values` instead of explicit `SharedValue::null_static()` entries in the projected vec.** The `null_static` translation happens at emission time. Output to the C side is identical.

Sort-key storage is unconditional, mirroring today's `sort_values.push(sv)` (which today always runs regardless of `include_sort_keys`). The `include_sort_keys` flag remains an *emission* gate only — it controls whether sort keys appear in `finalize`'s output, not whether they're stored.

### `finalize()` semantics

```rust
pub fn finalize(&mut self, r: &RemoteCollectReducer<'a>) -> SharedValue {
    let rows = mem::take(&mut self.rows);

    // Build the deduped emission template once per finalize. Order:
    // field_keys first, then sort_keys not already seen (when internal).
    // dstidx is the canonical key identity (u16, well under 32 in practice).
    // `HashSet::insert` returns `true` only on first occurrence, so it
    // doubles as the dedup predicate inside `.filter`.
    let sort_extras: &[&RLookupKey<'a>] =
        if r.include_sort_keys { &r.sort_keys } else { &[] };
    let mut seen: HashSet<u16> =
        HashSet::with_capacity(r.field_keys.len() + sort_extras.len());
    let template: Vec<(&RLookupKey<'a>, SharedValue)> = r.field_keys
        .iter()
        .chain(sort_extras)
        .filter(|key| seen.insert(key.dstidx))
        .map(|key| (*key, SharedValue::new_string(key.name().to_bytes().to_vec())))
        .collect();

    SharedValue::new_array(rows.into_iter().map(|row| {
        let entries: Vec<_> = template
            .iter()
            .map(|(key, name)| {
                let val = row.get(key).cloned().unwrap_or_else(SharedValue::null_static);
                (name.clone(), val)
            })
            .collect();
        SharedValue::new_map(entries)
    }))
}
```

Two improvements over today's emission, both intentional and bundled with this storage refactor because they fall out for free:

- **Dedup**: when a key appears in both `field_keys` and `sort_keys` (with `include_sort_keys=true`), today's code emits it twice in the output map. The new code emits it **once** — a tightening of the internal wire format. The existing test that touches this scenario (`test_collect_internal_duplicate_field_and_sort` in `tests/pytests/test_groupby_collect.py`) uses `assertIn`, not an exact-count assertion, so it passes under both behaviors. No pytest regression.
- **Hoisted name allocations**: today's code calls `SharedValue::new_string(key.name().to_bytes().to_vec())` once per `(row, key)` pair — a fresh `Vec<u8>` + Arc per emission. The new code builds these once per `finalize` and clones (cheap Arc bump) per row. For groups with many rows this is a meaningful allocation reduction.

Map order remains: deduped `field_keys` first, then any new `sort_keys` (when internal).

### Drop semantics

`RemoteCollectCtx` is bump-arena-allocated (`Bump` does not run destructors). `collectRemoteFreeInstance` calls `ptr::drop_in_place(ctx.cast::<RemoteCollectCtx>())`. With the new field, that walks:

1. `RemoteCollectCtx::rows: Vec<RLookupRow<'a>>` — drops the Vec, releasing each row.
2. Each `RLookupRow::dyn_values: ThinVec<Option<SharedValue>>` — drops, releasing each `Option<SharedValue>`.
3. Each `Some(SharedValue)` — Arc decrement.

This is the same chain depth as today's `Vec<Vec<SharedValue>>` and the same number of Arc decrements.

### Memory characteristics

Per-row storage today vs. after this PR. Let `K = field_keys.len()`, `S = sort_keys.len()`, `N = max(dstidx among written keys) + 1`. Sort-key values are always stored (matches today's behavior), so `N` covers both `field_keys` and `sort_keys`.

**Today** — two `Vec<SharedValue>` per row, both stored inline in the outer `Vec`'s heap:

- 24 byte `Vec<SharedValue>` slot for `field_values[i]` + `8 * K` byte data heap.
- 24 byte `Vec<SharedValue>` slot for `sort_values[i]` + `8 * S` byte data heap (when `S = 0` the data heap is the static `dangling()` sentinel — zero alloc — but the 24-byte slot is still occupied per row).

Total per row ≈ `48 + 8 * (K + S)` bytes (struct slots + data heap).

**After** — one `RLookupRow<'a>` per row, stored inline in the outer `Vec`'s heap:

- 24 byte `RLookupRow` slot (8 byte `RSSortingVectorRef` = single `ThinVec` ptr to the empty-header singleton, 8 byte `dyn_values` `ThinVec` ptr, 4 byte `num_dyn_values` + 4 byte tail padding).
- 16 byte `dyn_values` heap header (u64 cap + u64 len) + `8 * N` byte data area (`Option<SharedValue>` is niche-optimized to pointer-sized via `Arc`'s null niche).

Total per row ≈ `40 + 8 * N` bytes. Sort-key writes always share the same `dyn_values`, deduped by `dstidx` — independent of `include_sort_keys`. This matches today's unconditional `sort_values.push`. The flag gates emission, not storage.

The new shape is **smaller** when `K + S ≥ N - 1` (roughly: whenever the projected keys are dstidx-dense in the source lookup). It is larger only when projected keys sit at sparse high dstidx values in a wide source lookup — i.e. `N >> K + S`. The worst-case overhead is bounded by the source RLookup's width, the same upper bound that any other `RLookupRow` storage in the codebase pays. Acceptable for the layout uniformity it buys downstream.

## Testing

Existing tests must pass without modification:

- `src/redisearch_rs/reducers/tests/collect.rs` — `remote_external_collect_emits_only_requested_fields`, `remote_internal_collect_includes_sort_fields_for_coordinator_merge`, `local_collect_projects_remote_maps_and_fills_missing_fields_with_null`, `local_collect_accepts_resp2_flat_array_payloads`.
- All `tests/pytests/test_groupby_collect.py` cases.

Two new Rust unit tests:

- `remote_finalize_dedupes_overlapping_field_and_sort_key` — register one key, set both `field_keys` and `sort_keys` to that single key, set `include_sort_keys=true`, add one row. Storage holds one `dyn_values` slot for that key (deduped by `dstidx`). Finalize emits the key **once** in the output map. This pins the new dedup behavior described in `finalize()` semantics above. Today's behavior would have been "emits twice"; we are intentionally tightening that.
- `remote_finalize_hoists_name_allocations` — accumulate two rows under the same reducer; assert the emitted maps' name `SharedValue`s are the *same Arc* (use `Arc::ptr_eq` on the underlying value, exposed via the relevant `SharedValue` API or test helper). This locks in the per-`finalize` allocation hoisting against accidental regression.

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
| `field_keys` overlapping `sort_keys` on same `dstidx` | Both writes pull from same source value; benign overwrite. Emission is intentionally tightened from "twice" to "once" — see `finalize()` semantics. No existing test breaks. |
| Coexistence with the SORTBY / LIMIT plan | Independent change; conflict-resolves mechanically. |
| Coexistence with the `include_sort_keys` → `is_internal` rename PR | Trivial textual conflict. |
| Coexistence with the parser-tightening PR | This PR doesn't touch parser. |

## Implementation outline

The implementation plan will land separately (via the writing-plans skill) but the change set is small enough to scope here:

1. Edit `src/redisearch_rs/reducers/src/collect/remote.rs`: change `RemoteCollectCtx` per the type changes above; rewrite `add()` and `finalize()`.
2. Edit `src/redisearch_rs/c_entrypoint/reducers_ffi/src/collect/remote.rs`: update casts to `RemoteCollectCtx` (lifetime elision sufficient).
3. Add the two new Rust unit tests.
4. Verify per `.claude/skills/verify/SKILL.md`: full Rust suite + pytest suite + lint + fmt.

No FFI header regeneration. No C-side changes. No pytest changes.

## Definition of done

- `cargo nextest run -p reducers` green.
- `cd src/redisearch_rs && cargo nextest run` green.
- `./build.sh && ./build.sh RUN_PYTEST TEST=test_groupby_collect` green.
- `./build.sh RUN_PYTEST` green.
- `make lint` clean.
- `make fmt CHECK=1` clean.
- Two new Rust unit tests: `remote_finalize_dedupes_overlapping_field_and_sort_key` and `remote_finalize_hoists_name_allocations`.
- No FFI signature changes; no parser changes; no `LocalCollectReducer` changes.
