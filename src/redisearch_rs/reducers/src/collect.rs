/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::cmp::Ordering;

use bumpalo::Bump;
use min_max_heap::MinMaxHeap;
use rlookup::{RLookupKey, RLookupRow};

use crate::Reducer;
use value::comparison::cmp_fields;
use value::{Array, Map, SharedValue, Value};

/// The COLLECT reducer aggregates rows within each group, with optional field
/// projection, sorting, and limiting.
///
/// Configuration (field keys, sort keys, limits) is parsed in C and passed to
/// Rust via [`CollectReducer::new`]. The [`RLookupKey`][ffi::RLookupKey]
/// pointers are borrowed from the [`RLookup`][ffi::RLookup] infrastructure and
/// outlive this reducer.
///
/// This struct must be `#[repr(C)]` and its first field must be a [`Reducer`]
/// because it is downcast in C to `ffi::Reducer`, which reads vtable pointers
/// directly.
#[repr(C)]
pub struct CollectReducer<'a> {
    reducer: Reducer,
    /// Arena allocator for [`CollectCtx`] instances, matching the `BlkAlloc`
    /// pattern used by C reducers. All instances are freed at once when the
    /// reducer is dropped.
    arena: Bump,
    /// Projected field keys. Empty when only a wildcard is used.
    field_keys: Box<[&'a RLookupKey<'a>]>,
    /// Whether the wildcard `*` was specified in the FIELDS clause.
    has_wildcard: bool,
    /// Sort keys for in-group ordering. Empty when SORTBY is omitted.
    sort_keys: Box<[&'a RLookupKey<'a>]>,
    /// Bitmask where bit `i` is 0 for DESC and 1 for ASC (matching
    /// `SORTASCMAP_INIT`). Only meaningful for the first
    /// `sort_keys.len()` bits.
    sort_asc_map: u64,
    /// Optional LIMIT clause: `(offset, count)`.
    limit: Option<(u64, u64)>,
}

/// Per-group instance of the [`CollectReducer`].
///
/// Entries are kept in either an insertion-order [`Storage::Array`]
/// (no `SORTBY`) or a bounded top-K [`Storage::Heap`] (with `SORTBY`);
/// the variant is fixed at construction by [`CollectCtx::new`] and never
/// changes.
///
/// [`CollectCtx::add`] captures the sort key values eagerly and defers
/// field projection through a closure passed to
/// [`CollectCtx::insert_entry`], so the per-row allocation and
/// [`SharedValue::clone`] cost is only paid for entries that survive the
/// per-variant cap.
/// [`CollectCtx::finalize`] drains the active variant, applies the `LIMIT`
/// window, and serialises each surviving row as a `{field_name: value}` map.
///
/// Because `CollectCtx` is arena-allocated ([`Bump`] does not run destructors),
/// `ptr::drop_in_place` must be called to run destructors for the inner
/// heap-allocated collections and decrement `SharedValue` refcounts.
pub struct CollectCtx {
    /// Per-variant entry storage; the variant is pinned at construction
    /// time by [`CollectCtx::new`] based on whether `SORTBY` is present.
    storage: Storage,
}

impl<'a> CollectReducer<'a> {
    /// Create a new `CollectReducer` with the given pre-parsed configuration.
    ///
    /// The raw pointers in `field_keys` and `sort_keys` are stored but not
    /// dereferenced here; they are only dereferenced (unsafely) in
    /// [`CollectCtx::add`] and [`CollectCtx::finalize`].
    pub fn new(
        field_keys: Box<[&'a RLookupKey<'a>]>,
        has_wildcard: bool,
        sort_keys: Box<[&'a RLookupKey<'a>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
    ) -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            field_keys,
            has_wildcard,
            sort_keys,
            sort_asc_map,
            limit,
        }
    }

    /// Get a mutable reference to the base reducer.
    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.reducer
    }

    /// Allocate a new [`CollectCtx`] instance from the arena.
    pub fn alloc_instance(&self) -> &mut CollectCtx {
        self.arena.alloc(CollectCtx::new(self))
    }

    /// Effective top-K heap capacity (`offset + count`) for the heap path.
    ///
    /// Resolves a missing `LIMIT` to `(0, `[`DEFAULT_LIMIT`]`)` per design
    /// doc §6.5, matching the C reducer's default when `SORTBY` is present
    /// without `LIMIT`. `saturating_add` guards against overflow on
    /// pathological `(offset, count)` pairs; the result is clamped to
    /// `u64::MAX` and narrowed to `usize` for [`MinMaxHeap::with_capacity`].
    ///
    /// Only meaningful on the heap path (`!sort_keys.is_empty()`); callers
    /// on the array path use `ffi::RSGlobalConfig.maxAggregateResults` as
    /// the cap instead (consulted inside the shared `insert_entry` helper).
    const fn heap_cap(&self) -> usize {
        let (offset, count) = match self.limit {
            Some(pair) => pair,
            None => (0, DEFAULT_LIMIT),
        };
        offset.saturating_add(count) as usize
    }

    // The accessors below exist only for the C++ parser tests
    // (`test_cpp_collect.cpp`) via `reducers_ffi`. Remove them once those
    // tests are migrated to Python flow tests.

    /// Number of explicitly listed field keys (excludes the wildcard).
    pub const fn field_keys_len(&self) -> usize {
        self.field_keys.len()
    }

    /// Whether the wildcard `*` was specified in the FIELDS clause.
    pub const fn has_wildcard(&self) -> bool {
        self.has_wildcard
    }

    /// Number of sort keys.
    pub const fn sort_keys_len(&self) -> usize {
        self.sort_keys.len()
    }

    /// The ASC/DESC bitmask for sort keys.
    pub const fn sort_asc_map(&self) -> u64 {
        self.sort_asc_map
    }

    /// Whether a LIMIT clause was specified.
    pub const fn has_limit(&self) -> bool {
        self.limit.is_some()
    }

    /// The LIMIT offset value (0 if no limit).
    pub const fn limit_offset(&self) -> u64 {
        match self.limit {
            Some((offset, _)) => offset,
            None => 0,
        }
    }

    /// The LIMIT count value (0 if no limit).
    pub const fn limit_count(&self) -> u64 {
        match self.limit {
            Some((_, count)) => count,
            None => 0,
        }
    }
}

impl CollectCtx {
    /// Create a new per-group collect reducer instance.
    ///
    /// The storage variant is chosen here and never changes for the lifetime
    /// of the instance: [`Storage::Array`] when `SORTBY` is absent (entries
    /// are collected in insertion order) and [`Storage::Heap`] otherwise
    /// (bounded top-K min-max heap, sized to `offset + count` with
    /// [`DEFAULT_LIMIT`] filling in for a missing `LIMIT`).
    pub fn new(r: &CollectReducer) -> Self {
        let storage = if r.sort_keys.is_empty() {
            Storage::Array(Vec::new())
        } else {
            Storage::Heap(MinMaxHeap::with_capacity(r.heap_cap()))
        };
        Self { storage }
    }

    /// Project field and sort values from `row` and forward them to
    /// [`Self::insert_entry`] for cap-aware storage.
    ///
    /// `sort_vals` are materialised eagerly because the comparator reads
    /// them on every heap decision. `projected` is produced by a closure
    /// that runs at most once, and only for entries that survive the
    /// per-variant cap — see [`Self::insert_entry`]. Missing values are
    /// materialised as [`SharedValue::null_static`].
    pub fn add(&mut self, r: &CollectReducer, row: &RLookupRow) {
        let sort_vals: Vec<SharedValue> = r
            .sort_keys
            .iter()
            .map(|key| {
                row.get(key)
                    .cloned()
                    .unwrap_or_else(SharedValue::null_static)
            })
            .collect();
        self.insert_entry(r, &sort_vals, || {
            r.field_keys
                .iter()
                .map(|key| {
                    row.get(key)
                        .cloned()
                        .unwrap_or_else(SharedValue::null_static)
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
        });
    }

    /// Store a candidate entry, enforcing the per-variant cap.
    ///
    /// This is the single integration point shared by the row-oriented
    /// [`Self::add`] (GROUPBY reducer path) and the future coordinator
    /// path, which will pass a closure producing `projected` from an
    /// already-materialised shard response rather than from an
    /// [`RLookupRow`].
    ///
    /// `sort_vals` is consumed eagerly — the comparator reads it on every
    /// heap decision. `project` is invoked at most once, **only for
    /// entries that survive the cap**, so rows that would be dropped
    /// never pay the projection cost (boxed-slice allocation + one
    /// [`SharedValue::clone`] per field key).
    ///
    /// ## Array path (no `SORTBY`)
    ///
    /// Effective cap = explicit `LIMIT offset + count` when present, else
    /// `ffi::RSGlobalConfig.maxAggregateResults` (read per-call so runtime
    /// `CONFIG SET` takes effect). Entries are pushed in insertion order
    /// while `len < cap`; further entries are dropped silently before
    /// `project` is invoked.
    ///
    /// ## Heap path (`SORTBY` present)
    ///
    /// Cap comes from [`CollectReducer::heap_cap`]. Entries are pushed
    /// while `heap.len() < cap`; once full, the new entry replaces the
    /// current max only if it compares strictly better. Survival is
    /// decided by building only the [`EntryKey`] for the candidate and
    /// comparing it against [`MinMaxHeap::peek_max`]'s key — the
    /// [`HeapEntry::projected`] payload is built (via `project`) only
    /// for survivors. Ties between equal [`EntryKey`]s resolve in an
    /// unspecified order.
    pub fn insert_entry<F>(&mut self, r: &CollectReducer, sort_vals: &[SharedValue], project: F)
    where
        F: FnOnce() -> Box<[SharedValue]>,
    {
        match &mut self.storage {
            Storage::Array(v) => {
                let cap = match r.limit {
                    Some((offset, count)) => offset.saturating_add(count) as usize,
                    // SAFETY: `ffi::RSGlobalConfig` is the module-global
                    // config instance initialised once during module load;
                    // we only read a single `usize` field here.
                    None => unsafe { ffi::RSGlobalConfig.maxAggregateResults },
                };
                if v.len() < cap {
                    v.push(project());
                }
            }
            Storage::Heap(h) => {
                let cap = r.heap_cap();
                if cap == 0 {
                    return;
                }
                // Build only the comparator-visible half of the
                // candidate; `EntryKey` cannot (by construction) reach
                // `projected`, so the survival check runs without ever
                // building it.
                let candidate_key = EntryKey {
                    sort_vals: sort_vals.to_vec().into_boxed_slice(),
                    sort_asc_map: r.sort_asc_map,
                };
                if h.len() < cap {
                    h.push(HeapEntry {
                        key: candidate_key,
                        projected: project(),
                    });
                } else if candidate_key
                    < h.peek_max().expect("heap is full, so peek_max is Some").key
                {
                    h.push_pop_max(HeapEntry {
                        key: candidate_key,
                        projected: project(),
                    });
                }
                // else: candidate loses or ties — drop without ever
                // calling `project`.
            }
        }
    }

    /// Serialize all collected entries as an array of maps, consuming the
    /// stored entries.
    ///
    /// Each map contains `{field_name: value}` entries keyed by the
    /// [`RLookupKey`] name, in the order declared in the `FIELDS` clause.
    ///
    /// ## Drain order
    ///
    /// - **Heap path** (`SORTBY` present): entries are drained via
    ///   [`MinMaxHeap::pop_min`], yielding best→worst order under the
    ///   "best = min" convention fixed by [`EntryKey`]'s [`Ord`] impl.
    /// - **Array path** (`SORTBY` absent): entries are drained in
    ///   insertion order.
    ///
    /// ## LIMIT trim
    ///
    /// The `(offset, count)` pair in [`CollectReducer::limit`] is applied
    /// as `skip(offset).take(count)` over the drained sequence, emitting
    /// at most `count` entries starting at rank `offset`. When `LIMIT` is
    /// absent, all drained entries are emitted (the heap path is already
    /// bounded by [`DEFAULT_LIMIT`] via [`CollectReducer::heap_cap`]; the
    /// array path is bounded by `ffi::RSGlobalConfig.maxAggregateResults`
    /// via [`Self::insert_entry`]).
    ///
    /// `offset > len` yields an empty array; `count > len - offset` emits
    /// the remainder without padding.
    pub fn finalize(&mut self, r: &CollectReducer) -> SharedValue {
        let projected_rows: Vec<Box<[SharedValue]>> = match &mut self.storage {
            Storage::Array(v) => std::mem::take(v),
            Storage::Heap(h) => {
                let mut out = Vec::with_capacity(h.len());
                while let Some(he) = h.pop_min() {
                    out.push(he.projected);
                }
                out
            }
        };

        let (offset, take) = match r.limit {
            Some((o, c)) => (o as usize, c as usize),
            None => (0, usize::MAX),
        };

        let row_maps: Vec<SharedValue> = projected_rows
            .into_iter()
            .skip(offset)
            .take(take)
            .map(|projected| {
                let pairs: Box<[_]> = projected
                    .into_vec()
                    .into_iter()
                    .zip(r.field_keys.iter())
                    .map(|(val, key)| {
                        let name_val = SharedValue::new_string(key.name().to_bytes().to_vec());
                        (name_val, val)
                    })
                    .collect();
                SharedValue::new(Value::Map(Map::new(pairs)))
            })
            .collect();
        SharedValue::new(Value::Array(Array::new(row_maps.into_boxed_slice())))
    }
}

/// Default cap on heap-path results when `SORTBY` is specified without
/// `LIMIT`, matching the C implementation's `DEFAULT_LIMIT`.
const DEFAULT_LIMIT: u64 = 10;

/// Per-variant entry storage for [`CollectCtx`].
///
/// The variant is chosen in [`CollectCtx::new`] based on whether `SORTBY`
/// is present and never changes for the lifetime of the instance.
///
/// - [`Storage::Array`] is used when `SORTBY` is absent. Projected row
///   payloads are kept in insertion order and emitted in the same order
///   on finalisation. The array-path cap (explicit `LIMIT` or
///   `RSGlobalConfig.maxAggregateResults`) is enforced by the shared
///   `insert_entry` helper.
/// - [`Storage::Heap`] is used when `SORTBY` is present. A bounded top-K
///   min-max heap of [`HeapEntry`]s sized to `offset + count`; the shared
///   `insert_entry` helper evicts the current max when a better entry
///   arrives. On finalisation the heap is drained via `pop_min`, yielding
///   best→worst order under the "best = min" convention fixed by
///   [`EntryKey`]'s [`Ord`] impl.
enum Storage {
    Array(Vec<Box<[SharedValue]>>),
    Heap(MinMaxHeap<HeapEntry>),
}

/// Heap element: comparator-visible [`EntryKey`] alongside the projected
/// payload that [`CollectCtx::finalize`] emits.
///
/// `Ord` is delegated to `key` so `projected` is structurally unreachable
/// from the comparator. This lets [`CollectCtx::insert_entry`] decide
/// heap survival by building only an `EntryKey` candidate, and defer
/// building `projected` to survivors — the "deferred projection"
/// optimisation.
struct HeapEntry {
    key: EntryKey,
    projected: Box<[SharedValue]>,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

/// Comparator-visible half of a heap entry: the `SORTBY` key values and
/// the ASC/DESC direction map.
///
/// `Ord::cmp` delegates to [`value::comparison::cmp_fields`] so the per-key
/// policy (direction from `sort_asc_map` with bit `i` set ⇒ ASC matching
/// `SORTASCMAP_GETASC`, missing-worst regardless of direction, num-to-str
/// fallback, type-incompatibility handling) is defined once and shared
/// with the top-level `SearchResult_CmpByFields`. Ties across all sort
/// keys compare `Equal`; the heap path therefore resolves ties in an
/// unspecified order.
///
/// Heap convention: **best = min**. A value the user asked to sort first
/// compares `Less`; the top-K heap path keeps the `offset + count`
/// smallest-by-`cmp` entries, evicting the current max when a better
/// entry arrives. This is the opposite of the "best = max" convention
/// [`cmp_fields`] is written for (C's `RPSorter` drains via `mmh_pop_max`),
/// hence the trailing `Ordering::reverse` in [`EntryKey::cmp`].
///
/// ## Missing-value semantics
///
/// A missing sort key (materialised as [`Value::Null`] by [`CollectCtx::add`])
/// is mapped to [`None`] before delegation, so in both ASC and DESC an entry
/// with a missing key ranks **worst**: it sorts after an entry with any
/// present value. Missing-on-both sides is `Equal` and falls through to the
/// next pair.
struct EntryKey {
    sort_vals: Box<[SharedValue]>,
    sort_asc_map: u64,
}

impl PartialEq for EntryKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for EntryKey {}

impl PartialOrd for EntryKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EntryKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // Map [`Value::Null`] (the missing-key marker set by `CollectCtx::add`)
        // to `None` so `cmp_fields`'s missing-worst branch applies; any other
        // value passes through as `Some` and reaches `compare_with_query_error`.
        fn as_opt(v: &SharedValue) -> Option<&Value> {
            match &**v {
                Value::Null => None,
                other => Some(other),
            }
        }
        let pairs = self
            .sort_vals
            .iter()
            .zip(other.sort_vals.iter())
            .map(|(a, b)| (as_opt(a), as_opt(b)));
        // `cmp_fields` follows the "best = max" convention used by C's
        // `SearchResult_CmpByFields`; reverse the result for the "best = min"
        // convention this heap path uses (see type-level doc).
        cmp_fields(pairs, self.sort_asc_map, None).reverse()
    }
}

#[cfg(test)]
mod tests {
    //! Pure comparator tests on `EntryKey::cmp`. End-to-end tests that
    //! drive the public `CollectReducer` / `CollectCtx` surface live in
    //! `reducers/tests/integration/collect.rs`.

    use super::*;

    fn str_val(s: &str) -> Value {
        Value::String(value::String::from_vec(s.as_bytes().to_vec()))
    }

    /// Build an [`EntryKey`] from the given `sort_vals` / `sort_asc_map`.
    fn key(sort_vals: Vec<Value>, sort_asc_map: u64) -> EntryKey {
        let sort_vals: Box<[SharedValue]> = sort_vals
            .into_iter()
            .map(SharedValue::new)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        EntryKey {
            sort_vals,
            sort_asc_map,
        }
    }

    #[test]
    fn single_key_asc_orders_natural() {
        let a = key(vec![Value::Number(1.0)], 0b1);
        let b = key(vec![Value::Number(2.0)], 0b1);
        assert_eq!(a.cmp(&b), Ordering::Less);
        assert_eq!(b.cmp(&a), Ordering::Greater);
    }

    #[test]
    fn single_key_desc_inverts() {
        let a = key(vec![Value::Number(1.0)], 0b0);
        let b = key(vec![Value::Number(2.0)], 0b0);
        // DESC: the larger value sorts first (Less).
        assert_eq!(a.cmp(&b), Ordering::Greater);
        assert_eq!(b.cmp(&a), Ordering::Less);
    }

    #[test]
    fn missing_ranks_worst_under_asc() {
        let present = key(vec![Value::Number(42.0)], 0b1);
        let missing = key(vec![Value::Null], 0b1);
        assert_eq!(present.cmp(&missing), Ordering::Less);
        assert_eq!(missing.cmp(&present), Ordering::Greater);
    }

    #[test]
    fn missing_ranks_worst_under_desc_too() {
        // The missing-worst policy must NOT flip with DESC.
        let present = key(vec![Value::Number(42.0)], 0b0);
        let missing = key(vec![Value::Null], 0b0);
        assert_eq!(present.cmp(&missing), Ordering::Less);
        assert_eq!(missing.cmp(&present), Ordering::Greater);
    }

    #[test]
    fn both_missing_compares_equal() {
        let a = key(vec![Value::Null], 0b1);
        let b = key(vec![Value::Null], 0b1);
        assert_eq!(a.cmp(&b), Ordering::Equal);
    }

    #[test]
    fn multi_key_first_decides() {
        // asc_map = 0b11 ⇒ both ASC. First key strictly decides.
        let a = key(vec![Value::Number(1.0), Value::Number(999.0)], 0b11);
        let b = key(vec![Value::Number(2.0), Value::Number(0.0)], 0b11);
        assert_eq!(a.cmp(&b), Ordering::Less);
    }

    #[test]
    fn multi_key_equal_primary_second_decides() {
        let a = key(vec![Value::Number(1.0), str_val("apple")], 0b11);
        let b = key(vec![Value::Number(1.0), str_val("banana")], 0b11);
        assert_eq!(a.cmp(&b), Ordering::Less);
    }

    #[test]
    fn multi_key_mixed_directions() {
        // asc_map = 0b01 ⇒ key 0 ASC, key 1 DESC. Primary ties, secondary
        // decides with reversed direction: 1.0 is "greater" (worse) than 2.0
        // under DESC.
        let a = key(vec![Value::Number(1.0), Value::Number(1.0)], 0b01);
        let b = key(vec![Value::Number(1.0), Value::Number(2.0)], 0b01);
        assert_eq!(a.cmp(&b), Ordering::Greater);
    }

    #[test]
    fn all_sort_keys_equal_compares_equal() {
        let a = key(vec![Value::Number(1.0)], 0b1);
        let b = key(vec![Value::Number(1.0)], 0b1);
        assert_eq!(a.cmp(&b), Ordering::Equal);
    }

    #[test]
    fn empty_sort_vals_compares_equal() {
        let a = key(vec![], 0);
        let b = key(vec![], 0);
        assert_eq!(a.cmp(&b), Ordering::Equal);
    }
}
