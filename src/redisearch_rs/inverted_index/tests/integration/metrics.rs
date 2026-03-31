/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`MetricEntry`] and [`MetricsVec`].
//!
//! These types track yieldable metrics (vector distance, score, etc.) attached
//! to query results. The metrics machinery never dereferences the borrowed
//! [`RLookupKey`]: it only stores and compares the pointer. The tests below
//! therefore use stack-allocated POD `RLookupKey` values whose contents are
//! irrelevant — only their addresses matter.

use ffi::RLookupKey;
use inverted_index::{MetricEntry, MetricsVec};
use std::ptr;

/// Builds a zero-initialized `RLookupKey` suitable for pointer-identity tests.
///
/// The metrics code only stores and compares the borrow's address, so the
/// field values are intentionally meaningless.
fn make_key() -> RLookupKey {
    RLookupKey {
        dstidx: 0,
        svidx: 0,
        flags: 0,
        path: ptr::null(),
        name: ptr::null(),
        name_len: 0,
        next: ptr::null_mut(),
    }
}

// ── MetricEntry ──────────────────────────────────────────────────────────

#[test]
fn entry_with_key_stores_key_and_value() {
    let key = make_key();
    let entry = MetricEntry::with_key(&key, 1.5);

    assert_eq!(entry.value(), 1.5);
    let stored = entry.key().expect("key should be present");
    assert!(ptr::eq(stored, &key));
}

#[test]
fn entry_without_key_has_no_key() {
    let entry = MetricEntry::without_key(2.5);

    assert!(entry.key().is_none());
    assert_eq!(entry.value(), 2.5);
}

#[test]
fn entry_set_value_replaces_value_only() {
    let key = make_key();
    let mut entry = MetricEntry::with_key(&key, 1.0);

    entry.set_value(42.0);

    assert_eq!(entry.value(), 42.0);
    assert!(ptr::eq(entry.key().unwrap(), &key));
}

#[test]
fn entry_equality_uses_pointer_identity_for_keys() {
    let key_a = make_key();
    let key_b = make_key();

    let e1 = MetricEntry::with_key(&key_a, 1.0);
    let e2 = MetricEntry::with_key(&key_a, 1.0);
    let e3 = MetricEntry::with_key(&key_b, 1.0);
    let e4 = MetricEntry::with_key(&key_a, 2.0);

    assert_eq!(e1, e2, "same key pointer + same value → equal");
    assert_ne!(e1, e3, "different key pointer → not equal");
    assert_ne!(e1, e4, "same key, different value → not equal");
}

#[test]
fn entry_equality_without_keys() {
    let n1 = MetricEntry::without_key(3.0);
    let n2 = MetricEntry::without_key(3.0);
    let n3 = MetricEntry::without_key(4.0);

    assert_eq!(n1, n2, "no key + same value → equal");
    assert_ne!(n1, n3, "no key + different value → not equal");
}

#[test]
fn entry_with_key_not_equal_to_keyless_entry() {
    let key = make_key();
    let with = MetricEntry::with_key(&key, 5.0);
    let without = MetricEntry::without_key(5.0);

    assert_ne!(with, without);
    assert_ne!(without, with);
}

// ── MetricsVec — basic state ─────────────────────────────────────────────

#[test]
fn new_vec_is_empty() {
    let v: MetricsVec<'_> = MetricsVec::new();
    assert!(v.is_empty());
    assert_eq!(v.len(), 0);
    assert!(v.get(0).is_none());
    assert_eq!(v.iter().count(), 0);
}

#[test]
fn default_matches_new() {
    let v: MetricsVec<'_> = MetricsVec::default();
    assert!(v.is_empty());
    assert_eq!(v.len(), 0);
    assert_eq!(v, MetricsVec::new());
}

#[test]
fn push_with_and_without_key_grows_len() {
    let key = make_key();
    let mut v = MetricsVec::new();

    v.push_without_key(1.0);
    assert_eq!(v.len(), 1);
    assert!(!v.is_empty());

    v.push_with_key(&key, 2.0);
    assert_eq!(v.len(), 2);

    assert_eq!(v.get(0), Some(&MetricEntry::without_key(1.0)));
    assert_eq!(v.get(1), Some(&MetricEntry::with_key(&key, 2.0)));
    assert!(v.get(2).is_none());
}

#[test]
fn reset_clears_all_entries() {
    let key = make_key();
    let mut v = MetricsVec::new();
    v.push_with_key(&key, 1.0);
    v.push_without_key(2.0);
    assert_eq!(v.len(), 2);

    v.reset();

    assert!(v.is_empty());
    assert_eq!(v.len(), 0);
    assert!(v.get(0).is_none());

    // The vec is still usable after reset.
    v.push_without_key(9.0);
    assert_eq!(v.len(), 1);
    assert_eq!(v.get(0), Some(&MetricEntry::without_key(9.0)));
}

// ── MetricsVec — get / get_mut / iter ────────────────────────────────────

#[test]
fn get_mut_allows_in_place_mutation() {
    let key = make_key();
    let mut v = MetricsVec::new();
    v.push_with_key(&key, 1.0);
    v.push_without_key(2.0);

    v.get_mut(0).unwrap().set_value(10.0);
    v.get_mut(1).unwrap().set_value(20.0);

    assert_eq!(v.get(0).unwrap().value(), 10.0);
    assert_eq!(v.get(1).unwrap().value(), 20.0);
    assert!(v.get_mut(2).is_none());
}

#[test]
fn iter_yields_entries_in_insertion_order() {
    let key_a = make_key();
    let key_b = make_key();
    let mut v = MetricsVec::new();
    v.push_with_key(&key_a, 1.0);
    v.push_without_key(2.0);
    v.push_with_key(&key_b, 3.0);

    let collected: Vec<f64> = v.iter().map(|e| e.value()).collect();
    assert_eq!(collected, vec![1.0, 2.0, 3.0]);

    let key_count = v.iter().filter(|e| e.key().is_some()).count();
    assert_eq!(key_count, 2);
}

// ── MetricsVec — find_by_key_mut ─────────────────────────────────────────

#[test]
fn find_by_key_mut_returns_first_pointer_match() {
    let key_a = make_key();
    let key_b = make_key();
    let mut v = MetricsVec::new();
    v.push_with_key(&key_a, 1.0);
    v.push_with_key(&key_b, 2.0);
    v.push_with_key(&key_a, 3.0); // duplicate key — must not be returned first

    let entry = v.find_by_key_mut(&key_a).expect("key_a should be found");
    assert_eq!(entry.value(), 1.0, "first match wins");
    entry.set_value(11.0);

    // Mutation persisted; the duplicate is untouched.
    assert_eq!(v.get(0).unwrap().value(), 11.0);
    assert_eq!(v.get(2).unwrap().value(), 3.0);
}

#[test]
fn find_by_key_mut_returns_none_when_absent() {
    let key_a = make_key();
    let key_other = make_key();
    let mut v = MetricsVec::new();
    v.push_with_key(&key_a, 1.0);

    assert!(v.find_by_key_mut(&key_other).is_none());
}

#[test]
fn find_by_key_mut_skips_keyless_entries() {
    // Even if a keyless entry matches via "no key", `find_by_key_mut` looks
    // up by pointer identity and must skip entries without a key.
    let key = make_key();
    let mut v = MetricsVec::new();
    v.push_without_key(1.0);
    v.push_without_key(2.0);

    assert!(v.find_by_key_mut(&key).is_none());
}

// ── MetricsVec — concat ──────────────────────────────────────────────────

#[test]
fn concat_moves_entries_and_empties_source() {
    let key_a = make_key();
    let key_b = make_key();

    let mut dst = MetricsVec::new();
    dst.push_with_key(&key_a, 1.0);

    let mut src = MetricsVec::new();
    src.push_with_key(&key_b, 2.0);
    src.push_without_key(3.0);

    dst.concat(&mut src);

    assert!(src.is_empty(), "source must be drained");
    assert_eq!(dst.len(), 3);
    assert_eq!(dst.get(0).unwrap().value(), 1.0);
    assert_eq!(dst.get(1).unwrap().value(), 2.0);
    assert!(ptr::eq(dst.get(1).unwrap().key().unwrap(), &key_b));
    assert!(dst.get(2).unwrap().key().is_none());
}

#[test]
fn concat_with_empty_source_is_a_noop() {
    let key = make_key();
    let mut dst = MetricsVec::new();
    dst.push_with_key(&key, 1.0);
    let snapshot = dst.clone();

    let mut src = MetricsVec::new();
    dst.concat(&mut src);

    assert_eq!(dst, snapshot);
    assert!(src.is_empty());
}

#[test]
fn concat_into_empty_destination() {
    let key = make_key();
    let mut dst = MetricsVec::new();

    let mut src = MetricsVec::new();
    src.push_with_key(&key, 1.0);
    src.push_without_key(2.0);

    dst.concat(&mut src);

    assert!(src.is_empty());
    assert_eq!(dst.len(), 2);
    assert_eq!(dst.get(0).unwrap().value(), 1.0);
    assert_eq!(dst.get(1).unwrap().value(), 2.0);
}

// ── MetricsVec — as_metrics_slice ────────────────────────────────────────

#[test]
fn as_metrics_slice_matches_entries() {
    let key = make_key();
    let mut v = MetricsVec::new();
    v.push_with_key(&key, 1.0);
    v.push_without_key(2.0);

    let slice = v.as_metrics_slice();
    assert_eq!(slice.len, 2);
    assert!(!slice.data.is_null());

    // SAFETY: `slice.data` points to the first of `slice.len` valid
    // `MetricEntry` values owned by `v`, which has not been mutated.
    let view = unsafe { std::slice::from_raw_parts(slice.data, slice.len) };
    assert_eq!(view[0], MetricEntry::with_key(&key, 1.0));
    assert_eq!(view[1], MetricEntry::without_key(2.0));
}

#[test]
fn as_metrics_slice_on_empty_vec_has_zero_len() {
    let v: MetricsVec<'_> = MetricsVec::new();
    let slice = v.as_metrics_slice();
    // `data` may be a dangling-but-non-null sentinel for an empty ThinVec —
    // we don't constrain its value, only that `len == 0` so consumers cannot
    // dereference it.
    assert_eq!(slice.len, 0);
}

// ── MetricsVec — clone / equality ────────────────────────────────────────

#[test]
fn clone_produces_equal_independent_vec() {
    let key = make_key();
    let mut original = MetricsVec::new();
    original.push_with_key(&key, 1.0);
    original.push_without_key(2.0);

    let mut cloned = original.clone();
    assert_eq!(original, cloned);

    // Mutating the clone must not affect the original.
    cloned.get_mut(0).unwrap().set_value(99.0);
    assert_eq!(original.get(0).unwrap().value(), 1.0);
    assert_eq!(cloned.get(0).unwrap().value(), 99.0);
    assert_ne!(original, cloned);
}

#[test]
fn vecs_with_same_entries_are_equal() {
    let key = make_key();
    let mut a = MetricsVec::new();
    let mut b = MetricsVec::new();
    a.push_with_key(&key, 1.0);
    b.push_with_key(&key, 1.0);
    assert_eq!(a, b);

    a.push_without_key(2.0);
    assert_ne!(a, b, "different lengths → not equal");

    b.push_without_key(2.0);
    assert_eq!(a, b);
}
