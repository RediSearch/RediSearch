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
use value::comparison::compare_with_query_error_to_int;
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
/// Each call to [`CollectCtx::add`] projects the configured field and sort
/// keys from the source row and stores the cloned values in either the
/// insertion-order [`Storage::Array`] (no `SORTBY`) or the bounded top-K
/// [`Storage::Heap`] (with `SORTBY`). [`CollectCtx::finalize`] drains the
/// active variant and serialises the rows as an array of maps.
///
/// Because `CollectCtx` is arena-allocated ([`Bump`] does not run destructors),
/// `ptr::drop_in_place` must be called to run destructors for the inner
/// heap-allocated collections and decrement `SharedValue` refcounts.
pub struct CollectCtx {
    /// Per-variant entry storage; the variant is pinned at construction
    /// time by [`CollectCtx::new`] based on whether `SORTBY` is present.
    storage: Storage,
    /// Monotonic per-group insertion counter. Assigned to each
    /// [`CollectEntry::seq`] and used as the final tie-break in
    /// [`EntryOrd::cmp`] so that heap-path output is stable across ties.
    seq_counter: u64,
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
        Self {
            storage,
            seq_counter: 0,
        }
    }

    /// Project field and sort values from `row` and forward them to
    /// [`Self::insert_entry`] for cap-aware storage.
    ///
    /// For each configured field and sort key the value is looked up in the
    /// row and cloned (incrementing its refcount). Missing values are
    /// materialised as [`SharedValue::null_static`].
    pub fn add(&mut self, r: &CollectReducer, row: &RLookupRow) {
        let projected: Vec<SharedValue> = r
            .field_keys
            .iter()
            .map(|key| {
                row.get(key)
                    .cloned()
                    .unwrap_or_else(SharedValue::null_static)
            })
            .collect();
        let sort_vals: Vec<SharedValue> = r
            .sort_keys
            .iter()
            .map(|key| {
                row.get(key)
                    .cloned()
                    .unwrap_or_else(SharedValue::null_static)
            })
            .collect();
        self.insert_entry(r, &projected, &sort_vals);
    }

    /// Store a pre-projected entry, enforcing the per-variant cap.
    ///
    /// This is the single integration point shared by the row-oriented
    /// [`Self::add`] (GROUPBY reducer path) and the future coordinator
    /// path, which will fill `projected` / `sort_vals` from an already
    /// materialised shard response rather than an [`RLookupRow`].
    ///
    /// The per-group `seq_counter` is incremented on every call — even for
    /// entries dropped by cap enforcement — so that sequence numbers
    /// observed by downstream consumers match insertion order regardless
    /// of eviction.
    ///
    /// ## Array path (no `SORTBY`)
    ///
    /// Effective cap = explicit `LIMIT offset + count` when present, else
    /// `ffi::RSGlobalConfig.maxAggregateResults` (read per-call so runtime
    /// `CONFIG SET` takes effect). Entries are pushed in insertion order
    /// while `len < cap`; further entries are dropped silently.
    ///
    /// ## Heap path (`SORTBY` present)
    ///
    /// Cap comes from [`CollectReducer::heap_cap`]. Entries are pushed
    /// while `heap.len() < cap`; once full, the new entry replaces the
    /// current max only if it compares strictly better, via
    /// [`MinMaxHeap::push_pop_max`] which is a no-op when the new entry is
    /// `>=` the current max. Monotonic `seq` guarantees strict ordering
    /// even when all sort values tie, so ties resolve deterministically in
    /// favour of earlier inserts.
    pub fn insert_entry(
        &mut self,
        r: &CollectReducer,
        projected: &[SharedValue],
        sort_vals: &[SharedValue],
    ) {
        let seq = self.seq_counter;
        self.seq_counter += 1;
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
                    v.push(CollectEntry {
                        projected: projected.to_vec().into_boxed_slice(),
                        sort_vals: sort_vals.to_vec().into_boxed_slice(),
                        seq,
                    });
                }
            }
            Storage::Heap(h) => {
                let cap = r.heap_cap();
                if cap == 0 {
                    return;
                }
                let eo = EntryOrd {
                    entry: CollectEntry {
                        projected: projected.to_vec().into_boxed_slice(),
                        sort_vals: sort_vals.to_vec().into_boxed_slice(),
                        seq,
                    },
                    sort_asc_map: r.sort_asc_map,
                };
                if h.len() < cap {
                    h.push(eo);
                } else {
                    // `push_pop_max` is a no-op when `eo > current max`,
                    // so worse-than-max entries are dropped; strictly
                    // better entries evict the current max.
                    h.push_pop_max(eo);
                }
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
    ///   "best = min" convention fixed by [`EntryOrd`]'s [`Ord`] impl.
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
        let entries: Vec<CollectEntry> = match &mut self.storage {
            Storage::Array(v) => std::mem::take(v),
            Storage::Heap(h) => {
                let mut out = Vec::with_capacity(h.len());
                while let Some(eo) = h.pop_min() {
                    out.push(eo.entry);
                }
                out
            }
        };

        let (offset, take) = match r.limit {
            Some((o, c)) => (o as usize, c as usize),
            None => (0, usize::MAX),
        };

        let row_maps: Vec<SharedValue> = entries
            .into_iter()
            .skip(offset)
            .take(take)
            .map(|entry| {
                let pairs: Box<[_]> = entry
                    .projected
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
/// - [`Storage::Array`] is used when `SORTBY` is absent. Entries are kept
///   in insertion order; on finalisation they are emitted in the same order.
///   The array-path cap (explicit `LIMIT` or `RSGlobalConfig.maxAggregateResults`)
///   is enforced by the shared `insert_entry` helper.
/// - [`Storage::Heap`] is used when `SORTBY` is present. A bounded top-K
///   min-max heap sized to `offset + count`; the shared `insert_entry`
///   helper evicts the current max when a better entry arrives.
///   On finalisation the heap is drained via `pop_min`, yielding
///   best→worst order under the "best = min" convention fixed by
///   [`EntryOrd`]'s [`Ord`] impl.
enum Storage {
    Array(Vec<CollectEntry>),
    Heap(MinMaxHeap<EntryOrd>),
}

/// A collected row held in either the insertion-order array (no `SORTBY`) or
/// the bounded top-K heap (with `SORTBY`).
///
/// `projected` holds the output values, one per configured `FIELDS` key, in
/// declaration order; this is what gets serialised by [`CollectCtx::finalize`].
/// `sort_vals` holds the `SORTBY` key values in declaration order and is
/// empty on the array path. `seq` is a monotonic per-group insertion index
/// used as the final tie-break in [`EntryOrd::cmp`].
struct CollectEntry {
    projected: Box<[SharedValue]>,
    sort_vals: Box<[SharedValue]>,
    seq: u64,
}

/// Heap-ordering wrapper around a [`CollectEntry`].
///
/// `Ord::cmp` iterates `sort_vals` pairwise and applies the ASC/DESC direction
/// encoded in `sort_asc_map` (bit `i` set ⇒ ASC, bit `i` clear ⇒ DESC, matching
/// `SORTASCMAP_GETASC` in C). Ties across all sort keys fall through to `seq`
/// ascending — earlier inserts win — which keeps the emitted order stable.
///
/// Heap convention: **best = min**. A value the user asked to sort first
/// compares `Less`; the top-K heap path keeps the `offset + count`
/// smallest-by-`cmp` entries, evicting the current max when a better entry
/// arrives.
///
/// ## Missing-value semantics
///
/// A missing sort key (materialised as [`Value::Null`] by [`CollectCtx::add`])
/// ranks **worst regardless of direction**: in both ASC and DESC an entry with
/// a missing key sorts after an entry with any present value. Missing-on-both
/// sides is `Equal` and falls through to the next pair. This matches the
/// `SearchResult_CmpByFields` policy so reducer-side and top-level `SORTBY`
/// agree on NULL ordering.
// TODO(MOD-14803, PR #9194): once `value::comparison::cmp_fields` lands,
// replace the inline pairwise-with-direction-and-missing-worst loop below
// with a single `cmp_fields(pairs, self.sort_asc_map, None)` call so this
// policy is defined exactly once, shared with `SearchResult_CmpByFields`.
struct EntryOrd {
    entry: CollectEntry,
    sort_asc_map: u64,
}

impl PartialEq for EntryOrd {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for EntryOrd {}

impl PartialOrd for EntryOrd {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EntryOrd {
    fn cmp(&self, other: &Self) -> Ordering {
        for (i, (a, b)) in self
            .entry
            .sort_vals
            .iter()
            .zip(other.entry.sort_vals.iter())
            .enumerate()
        {
            let asc = (self.sort_asc_map >> i) & 1 == 1;
            let pair = match (&**a, &**b) {
                (Value::Null, Value::Null) => Ordering::Equal,
                (Value::Null, _) => Ordering::Greater,
                (_, Value::Null) => Ordering::Less,
                (va, vb) => {
                    let base = compare_with_query_error_to_int(va, vb, None).cmp(&0);
                    if asc { base } else { base.reverse() }
                }
            };
            if pair != Ordering::Equal {
                return pair;
            }
        }
        self.entry.seq.cmp(&other.entry.seq)
    }
}

#[cfg(test)]
mod tests {
    //! Pure comparator tests on `EntryOrd::cmp`. End-to-end tests that
    //! drive the public `CollectReducer` / `CollectCtx` surface live in
    //! `reducers/tests/integration/collect.rs`.

    use super::*;

    fn str_val(s: &str) -> Value {
        Value::String(value::String::from_vec(s.as_bytes().to_vec()))
    }

    /// Build an [`EntryOrd`] with no projected columns and the given
    /// `sort_vals` / `seq` / `sort_asc_map`.
    fn ord_entry(sort_vals: Vec<Value>, seq: u64, sort_asc_map: u64) -> EntryOrd {
        let sort_vals: Box<[SharedValue]> = sort_vals
            .into_iter()
            .map(SharedValue::new)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        EntryOrd {
            entry: CollectEntry {
                projected: Box::new([]),
                sort_vals,
                seq,
            },
            sort_asc_map,
        }
    }

    #[test]
    fn single_key_asc_orders_natural() {
        let a = ord_entry(vec![Value::Number(1.0)], 0, 0b1);
        let b = ord_entry(vec![Value::Number(2.0)], 1, 0b1);
        assert_eq!(a.cmp(&b), Ordering::Less);
        assert_eq!(b.cmp(&a), Ordering::Greater);
    }

    #[test]
    fn single_key_desc_inverts() {
        let a = ord_entry(vec![Value::Number(1.0)], 0, 0b0);
        let b = ord_entry(vec![Value::Number(2.0)], 1, 0b0);
        // DESC: the larger value sorts first (Less).
        assert_eq!(a.cmp(&b), Ordering::Greater);
        assert_eq!(b.cmp(&a), Ordering::Less);
    }

    #[test]
    fn missing_ranks_worst_under_asc() {
        let present = ord_entry(vec![Value::Number(42.0)], 0, 0b1);
        let missing = ord_entry(vec![Value::Null], 1, 0b1);
        assert_eq!(present.cmp(&missing), Ordering::Less);
        assert_eq!(missing.cmp(&present), Ordering::Greater);
    }

    #[test]
    fn missing_ranks_worst_under_desc_too() {
        // PR 9194 alignment: the missing-worst policy must NOT flip with DESC.
        let present = ord_entry(vec![Value::Number(42.0)], 0, 0b0);
        let missing = ord_entry(vec![Value::Null], 1, 0b0);
        assert_eq!(present.cmp(&missing), Ordering::Less);
        assert_eq!(missing.cmp(&present), Ordering::Greater);
    }

    #[test]
    fn both_missing_falls_through_to_seq() {
        let a = ord_entry(vec![Value::Null], 0, 0b1);
        let b = ord_entry(vec![Value::Null], 1, 0b1);
        assert_eq!(a.cmp(&b), Ordering::Less);
    }

    #[test]
    fn multi_key_first_decides() {
        // asc_map = 0b11 ⇒ both ASC. First key strictly decides.
        let a = ord_entry(vec![Value::Number(1.0), Value::Number(999.0)], 0, 0b11);
        let b = ord_entry(vec![Value::Number(2.0), Value::Number(0.0)], 1, 0b11);
        assert_eq!(a.cmp(&b), Ordering::Less);
    }

    #[test]
    fn multi_key_equal_primary_second_decides() {
        let a = ord_entry(vec![Value::Number(1.0), str_val("apple")], 0, 0b11);
        let b = ord_entry(vec![Value::Number(1.0), str_val("banana")], 1, 0b11);
        assert_eq!(a.cmp(&b), Ordering::Less);
    }

    #[test]
    fn multi_key_mixed_directions() {
        // asc_map = 0b01 ⇒ key 0 ASC, key 1 DESC. Primary ties, secondary
        // decides with reversed direction: 1.0 is "greater" (worse) than 2.0
        // under DESC.
        let a = ord_entry(vec![Value::Number(1.0), Value::Number(1.0)], 0, 0b01);
        let b = ord_entry(vec![Value::Number(1.0), Value::Number(2.0)], 1, 0b01);
        assert_eq!(a.cmp(&b), Ordering::Greater);
    }

    #[test]
    fn all_sort_keys_equal_tiebreaks_by_seq() {
        let a = ord_entry(vec![Value::Number(1.0)], 0, 0b1);
        let b = ord_entry(vec![Value::Number(1.0)], 1, 0b1);
        assert_eq!(a.cmp(&b), Ordering::Less);
    }

    #[test]
    fn empty_sort_vals_tiebreaks_by_seq_only() {
        let a = ord_entry(vec![], 0, 0);
        let b = ord_entry(vec![], 1, 0);
        assert_eq!(a.cmp(&b), Ordering::Less);
    }
}
