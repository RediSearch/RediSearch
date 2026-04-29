/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Cap-aware storage shared by the COLLECT reducer variants. The accumulation
//! strategy is picked once by [`Storage::new`] (array if `SORTBY` is absent,
//! heap otherwise); the cap is resolved once by [`resolve_cap`].

use min_max_heap::MinMaxHeap;
use std::cmp::Ordering;
use value::comparison::cmp_fields;
use value::{SharedValue, Value};

/// Default cap on heap-path results when `SORTBY` is specified without
/// `LIMIT`, matching the C implementation's `DEFAULT_LIMIT`.
pub(crate) const DEFAULT_LIMIT: u64 = 10;

pub(crate) enum Storage {
    Array(Vec<Box<[SharedValue]>>),
    Heap(MinMaxHeap<HeapEntry>),
}

/// One drained row produced by [`Storage::drain_with_limit`]: projected field
/// columns, plus the heap-path sort-key columns when present (`None` on the
/// array path).
pub(crate) type DrainedRow = (Box<[SharedValue]>, Option<Box<[SharedValue]>>);

/// Heap element split into the comparator-visible [`EntryKey`] and the
/// projected row payload, so [`Storage::insert_entry`] can decide survival
/// without building `projected` for losing candidates.
pub(crate) struct HeapEntry {
    pub(crate) key: EntryKey,
    pub(crate) projected: Box<[SharedValue]>,
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

/// Comparator-visible half of a [`HeapEntry`].
///
/// Heap convention: **best = min**. The top-K heap path keeps the `offset +
/// count` smallest-by-`cmp` entries and evicts the current max when a better
/// entry arrives. This inverts the "best = max" convention that
/// [`value::comparison::cmp_fields`] is written for, hence the trailing
/// [`Ordering::reverse`] in [`EntryKey::cmp`].
///
/// A sort value materialised as [`Value::Null`] is mapped to [`None`] before
/// delegation so [`cmp_fields`]'s missing-worst rule applies in both ASC and
/// DESC. Ties across every key compare `Equal`; the heap path therefore
/// resolves ties in an unspecified order.
pub(crate) struct EntryKey {
    pub(crate) sort_vals: Box<[SharedValue]>,
    pub(crate) sort_asc_map: u64,
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
        cmp_fields(pairs, self.sort_asc_map, None).reverse()
    }
}

impl Storage {
    /// The heap variant is preallocated to `cap` so cap-bound inserts never
    /// reallocate; the array variant enforces the cap at insertion time.
    pub(crate) fn new(is_array: bool, cap: usize) -> Self {
        if is_array {
            Self::Array(Vec::new())
        } else {
            Self::Heap(MinMaxHeap::with_capacity(cap))
        }
    }

    /// Cap-aware insert with **deferred projection**: `project` runs at most
    /// once, only for entries that survive the cap.
    ///
    /// `sort_vals` is consumed eagerly because the comparator reads it on
    /// every heap decision. `sort_asc_map` matches [`cmp_fields`]: bit `i` set
    /// ⇒ ASC for sort key `i`.
    ///
    /// Heap path: the explicit `cap == 0` short-circuit is required; without
    /// it the strictly-better branch's [`MinMaxHeap::peek_max`]`.unwrap()`
    /// would panic on an empty heap. When the heap is full, survival is
    /// decided from only the candidate's [`EntryKey`] — losers never pay the
    /// [`HeapEntry::projected`] allocation cost. Equal-key ties lose to the
    /// already-resident entry, so survivors are unspecified among ties.
    pub(crate) fn insert_entry<F>(
        &mut self,
        cap: usize,
        sort_asc_map: u64,
        sort_vals: &[SharedValue],
        project: F,
    ) where
        F: FnOnce() -> Box<[SharedValue]>,
    {
        match self {
            Self::Array(v) => {
                if v.len() < cap {
                    v.push(project());
                }
            }
            Self::Heap(h) => {
                if cap == 0 {
                    return;
                }
                let candidate_key = EntryKey {
                    sort_vals: sort_vals.to_vec().into_boxed_slice(),
                    sort_asc_map,
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
            }
        }
    }

    /// Drain in best→worst order (heap) or insertion order (array) and apply
    /// `skip(offset).take(count)` from `limit`.
    ///
    /// `limit == None` is equivalent to `(0, usize::MAX)` — drain everything
    /// currently in storage in natural order.
    pub(crate) fn drain_with_limit(&mut self, limit: Option<(u64, u64)>) -> Vec<DrainedRow> {
        let drained: Vec<DrainedRow> = match self {
            Self::Array(v) => std::mem::take(v).into_iter().map(|p| (p, None)).collect(),
            Self::Heap(h) => {
                let mut out = Vec::with_capacity(h.len());
                while let Some(he) = h.pop_min() {
                    out.push((he.projected, Some(he.key.sort_vals)));
                }
                out
            }
        };

        let (offset, take) = match limit {
            Some((o, c)) => (o as usize, c as usize),
            None => (0, usize::MAX),
        };
        drained.into_iter().skip(offset).take(take).collect()
    }
}

/// Resolve the effective per-variant cap once at construction.
///
/// - **Array path** (`is_array`): explicit `LIMIT offset + count` when
///   present, otherwise `ffi::RSGlobalConfig.maxAggregateResults` is
///   sampled here. Sampling at construction (rather than per row) makes
///   the cap of an in-flight aggregation immune to a concurrent
///   `CONFIG SET MAXAGGREGATERESULTS`.
/// - **Heap path** (`!is_array`): `offset + count`, with [`DEFAULT_LIMIT`]
///   filling in for a missing `LIMIT`.
///
/// `saturating_add` clamps pathological `(offset, count)` pairs to
/// `u64::MAX` before the `usize` narrowing.
pub(crate) fn resolve_cap(is_array: bool, limit: Option<(u64, u64)>) -> usize {
    if is_array {
        match limit {
            Some((offset, count)) => offset.saturating_add(count) as usize,
            // SAFETY: `ffi::RSGlobalConfig` is the module-global config
            // instance initialised once during module load; we only read
            // a single `usize` field here.
            None => unsafe { ffi::RSGlobalConfig.maxAggregateResults },
        }
    } else {
        let (offset, count) = match limit {
            Some(pair) => pair,
            None => (0, DEFAULT_LIMIT),
        };
        offset.saturating_add(count) as usize
    }
}

#[cfg(test)]
mod tests {
    //! [`EntryKey::cmp`] tests live here because they touch private module
    //! internals. End-to-end tests through the public reducer surface live
    //! in `reducers/tests/collect.rs`.

    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    fn str_val(s: &str) -> Value {
        Value::String(value::String::from_vec(s.as_bytes().to_vec()))
    }

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

    /// `project` closure that increments `counter` on each call.
    fn counting_project(
        counter: &AtomicUsize,
        tag: f64,
    ) -> impl FnOnce() -> Box<[SharedValue]> + '_ {
        move || {
            counter.fetch_add(1, AtomicOrdering::Relaxed);
            vec![SharedValue::new_num(tag)].into_boxed_slice()
        }
    }

    #[test]
    fn insert_entry_array_path_caps_at_len() {
        let mut s = Storage::new(/* is_array */ true, /* cap */ 3);
        for i in 0..5 {
            s.insert_entry(3, 0, &[], || {
                vec![SharedValue::new_num(i as f64)].into_boxed_slice()
            });
        }
        let drained = s.drain_with_limit(None);
        assert_eq!(drained.len(), 3, "array path must cap at `cap`");
        let nums: Vec<f64> = drained
            .iter()
            .map(|(p, _)| p[0].as_num().expect("expected num"))
            .collect();
        assert_eq!(nums, vec![0.0, 1.0, 2.0]);
    }

    #[test]
    fn insert_entry_array_path_drops_after_cap_without_calling_project() {
        let counter = AtomicUsize::new(0);
        let mut s = Storage::new(true, 2);
        for v in [1.0_f64, 2.0, 3.0, 4.0] {
            s.insert_entry(2, 0, &[], counting_project(&counter, v));
        }
        assert_eq!(
            counter.load(AtomicOrdering::Relaxed),
            2,
            "array path must not call `project` for entries beyond the cap"
        );
    }

    #[test]
    fn insert_entry_heap_path_evicts_worst_when_better_arrives() {
        let mut s = Storage::new(/* is_array */ false, /* cap */ 3);
        for v in [4.0, 1.0, 5.0, 2.0, 3.0] {
            let sort_vals = [SharedValue::new_num(v)];
            s.insert_entry(3, 0b1, &sort_vals, || {
                vec![SharedValue::new_num(v)].into_boxed_slice()
            });
        }
        let drained = s.drain_with_limit(None);
        assert_eq!(drained.len(), 3);
        let nums: Vec<f64> = drained
            .iter()
            .map(|(p, _)| p[0].as_num().expect("expected num"))
            .collect();
        assert_eq!(nums, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn insert_entry_heap_path_does_not_call_project_for_losers() {
        let counter = AtomicUsize::new(0);
        let mut s = Storage::new(false, 2);
        for v in [1.0_f64, 2.0] {
            let sort_vals = [SharedValue::new_num(v)];
            s.insert_entry(2, 0b1, &sort_vals, counting_project(&counter, v));
        }
        for v in [100.0_f64, 200.0] {
            let sort_vals = [SharedValue::new_num(v)];
            s.insert_entry(2, 0b1, &sort_vals, counting_project(&counter, v));
        }
        assert_eq!(
            counter.load(AtomicOrdering::Relaxed),
            2,
            "heap path must not call `project` for entries that lose the cap race"
        );
    }
}
