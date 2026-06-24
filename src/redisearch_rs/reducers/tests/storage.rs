/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Unit tests for the bounded COLLECT storage families ([`ArrayStorage`] and
//! [`HeapStorage`]) selected by [`Storage`].

extern crate redisearch_rs;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use reducers::collect::heap::HeapEntry;
use reducers::collect::storage::{ArrayStorage, DEFAULT_LIMIT, HeapStorage, ProjectedRow, Storage};
use rlookup::{RLookupKey, RLookupKeyFlags, RLookupRow};
use value::SharedValue;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

mod common;

use common::{SORT_ASC, SORT_DESC};

fn make_key() -> RLookupKey<'static> {
    RLookupKey::new(c"v", RLookupKeyFlags::empty())
}

fn sort_vals(v: f64) -> Box<[Option<SharedValue>]> {
    vec![Some(SharedValue::new_num(v))].into_boxed_slice()
}

/// A one-field [`ProjectedRow`] tagged with `v`, as a reducer's `project`
/// closure would produce.
fn projected(key: &RLookupKey<'_>, v: f64) -> ProjectedRow {
    let mut r = RLookupRow::new();
    r.write_key(key, SharedValue::new_num(v));
    ProjectedRow::new(r)
}

/// `project` closure that increments `counter` on each call.
fn counting_project<'a>(
    counter: &'a mut usize,
    key: &'a RLookupKey<'_>,
    tag: f64,
) -> impl FnOnce() -> ProjectedRow + 'a {
    move || {
        *counter += 1;
        projected(key, tag)
    }
}

fn drained_nums(key: &RLookupKey<'_>, drained: &[ProjectedRow]) -> Vec<f64> {
    drained
        .iter()
        .map(|pr| {
            pr.row()
                .get(key)
                .expect("row missing key")
                .as_num()
                .expect("num")
        })
        .collect()
}

/// Unwrap the array family from a [`Storage`] built with `sortby = false`.
fn as_array(storage: &mut Storage<()>) -> &mut ArrayStorage {
    match storage {
        Storage::Array(a) => a,
        Storage::Heap(_) => panic!("expected ArrayStorage"),
    }
}

/// Unwrap the heap family from a [`Storage`] built with `sortby = true`.
fn as_heap(storage: &mut Storage<()>) -> &mut HeapStorage<()> {
    match storage {
        Storage::Heap(h) => h,
        Storage::Array(_) => panic!("expected HeapStorage"),
    }
}

#[test]
fn array_push_caps_at_cap_in_insertion_order() {
    let key = make_key();
    let mut s = Storage::new(false, Some((0, 3)), 0);
    let a = as_array(&mut s);
    for i in 0..5 {
        a.push(|| projected(&key, i as f64));
    }
    let drained: Vec<_> = a.drain().collect();
    assert_eq!(drained.len(), 3);
    assert_eq!(drained_nums(&key, &drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn array_push_drops_excess_without_calling_project() {
    let key = make_key();
    let mut counter = 0;
    let mut s = Storage::new(false, Some((0, 2)), 0);
    let a = as_array(&mut s);
    for v in [1.0_f64, 2.0, 3.0, 4.0] {
        a.push(counting_project(&mut counter, &key, v));
    }
    assert_eq!(
        counter, 2,
        "`project` must not run for entries beyond the cap"
    );
}

#[test]
fn heap_consider_keeps_top_k_under_asc() {
    let key = make_key();
    let mut s = Storage::new(true, Some((0, 3)), SORT_ASC);
    let h = as_heap(&mut s);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        h.consider(sort_vals(i), (), || projected(&key, i));
    }
    let drained: Vec<_> = h.drain().map(HeapEntry::into_projected).collect();
    // ASC: smallest is best; heap drains best→worst.
    assert_eq!(drained_nums(&key, &drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn heap_consider_keeps_top_k_under_desc() {
    let key = make_key();
    let mut s = Storage::new(true, Some((0, 3)), SORT_DESC);
    let h = as_heap(&mut s);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        h.consider(sort_vals(i), (), || projected(&key, i));
    }
    let drained: Vec<_> = h.drain().map(HeapEntry::into_projected).collect();
    // DESC: largest is best; heap drains best→worst.
    assert_eq!(drained_nums(&key, &drained), vec![5.0, 4.0, 3.0]);
}

#[test]
fn heap_consider_skips_project_for_doomed_candidates() {
    let key = make_key();
    let mut counter = 0;
    let mut s = Storage::new(true, Some((0, 2)), SORT_ASC);
    let h = as_heap(&mut s);
    // Fill with the two best candidates first.
    for v in [0.0_f64, 1.0] {
        h.consider(sort_vals(v), (), counting_project(&mut counter, &key, v));
    }
    // Each subsequent candidate is worse than the worst survivor (1.0)
    // under ASC, so `project` must not run.
    for v in [2.0_f64, 3.0, 4.0] {
        h.consider(sort_vals(v), (), counting_project(&mut counter, &key, v));
    }
    assert_eq!(
        counter, 2,
        "`project` must not run for candidates worse than the heap's worst"
    );
}

#[test]
fn heap_consider_invokes_project_on_eviction() {
    let key = make_key();
    let mut counter = 0;
    let mut s = Storage::new(true, Some((0, 2)), SORT_ASC);
    let h = as_heap(&mut s);
    // Each insert is strictly better than the current worst, so every
    // candidate must be projected.
    for v in [5.0_f64, 3.0, 1.0] {
        h.consider(sort_vals(v), (), counting_project(&mut counter, &key, v));
    }
    assert_eq!(counter, 3);
}

#[test]
fn drain_array_applies_skip_take() {
    let key = make_key();
    let mut s = Storage::new(false, Some((1, 2)), 0);
    let a = as_array(&mut s);
    for i in 0..5 {
        a.push(|| projected(&key, i as f64));
    }
    let drained: Vec<_> = a.drain().collect();
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0]);
}

#[test]
fn drain_heap_applies_skip_take_after_best_first_order() {
    let key = make_key();
    let mut s = Storage::new(true, Some((1, 2)), SORT_ASC);
    let h = as_heap(&mut s);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        h.consider(sort_vals(i), (), || projected(&key, i));
    }
    // Top-3 under ASC = [0, 1, 2] best→worst; offset 1, count 2 → [1, 2].
    let drained: Vec<_> = h.drain().map(HeapEntry::into_projected).collect();
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0]);
}

#[test]
fn heap_uses_default_limit_when_no_explicit_limit() {
    let key = make_key();
    let mut s = Storage::new(true, None, SORT_ASC);
    let h = as_heap(&mut s);
    for i in 0..(DEFAULT_LIMIT as usize + 5) {
        h.consider(sort_vals(i as f64), (), || projected(&key, i as f64));
    }
    let drained: Vec<_> = h.drain().map(HeapEntry::into_projected).collect();
    assert_eq!(drained.len(), DEFAULT_LIMIT as usize);
}

#[test]
fn storage_drain_heap_yields_rows_best_first() {
    // The `Storage::drain` convenience discards each entry's ranking key and
    // yields values best→worst, which is all the client-facing local reducer
    // needs.
    let key = make_key();
    let mut s = Storage::new(true, Some((0, 3)), SORT_ASC);
    {
        let h = as_heap(&mut s);
        for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
            h.consider(sort_vals(i), (), || projected(&key, i));
        }
    }
    let drained: Vec<_> = s.drain().collect();
    assert_eq!(drained_nums(&key, &drained), vec![0.0, 1.0, 2.0]);
}

// ===== ProjectedRow `Hash` / `Eq` =====

/// Build a [`ProjectedRow`] with an exact `dyn_values` layout: slot `i` is left
/// `None` when `slots[i]` is `None`, otherwise the value is written at
/// `dstidx == i`. `set_dyn_capacity` pre-fills every slot with `None`, so
/// trailing `None`s in `slots` are materialized (not truncated) — letting us
/// exercise the trailing-strip rule explicitly.
fn pr_vals(slots: Vec<Option<SharedValue>>) -> ProjectedRow {
    let mut row = RLookupRow::new();
    row.set_dyn_capacity(slots.len());
    for (i, slot) in slots.into_iter().enumerate() {
        if let Some(v) = slot {
            let mut key = RLookupKey::new(c"v", RLookupKeyFlags::empty());
            key.dstidx = i as u16;
            row.write_key(&key, v);
        }
    }
    ProjectedRow::new(row)
}

/// Numeric shorthand for [`pr_vals`].
fn pr(slots: &[Option<f64>]) -> ProjectedRow {
    pr_vals(slots.iter().map(|s| s.map(SharedValue::new_num)).collect())
}

fn hash_of(pr: &ProjectedRow) -> u64 {
    let mut h = DefaultHasher::new();
    pr.hash(&mut h);
    h.finish()
}

/// `Some` number, for the layout literals below.
const fn n(x: f64) -> Option<f64> {
    Some(x)
}

/// `Some` string value, for the layout literals below.
fn s(bytes: &str) -> Option<SharedValue> {
    Some(SharedValue::new_string(bytes.as_bytes().to_vec()))
}

#[test]
fn projected_row_all_none_equal_regardless_of_length() {
    let a = pr(&[None]);
    let b = pr(&[None, None, None]);
    assert!(a == b);
    assert_eq!(hash_of(&a), hash_of(&b));
    // An empty layout is also all-`None` once trimmed.
    assert!(a == pr(&[]));
    assert_eq!(hash_of(&a), hash_of(&pr(&[])));
}

#[test]
fn projected_row_trailing_none_is_ignored() {
    // Interior `None` kept, trailing `None`(s) dropped.
    let a = pr(&[None, n(3.0), None]);
    let b = pr(&[None, n(3.0), None, None]);
    assert!(a == b);
    assert_eq!(hash_of(&a), hash_of(&b));

    // Trailing `None` after a value collapses to the bare value.
    let c = pr(&[n(3.0), None]);
    let d = pr(&[n(3.0)]);
    assert!(c == d);
    assert_eq!(hash_of(&c), hash_of(&d));
}

#[test]
fn projected_row_leading_none_is_positional() {
    // A leading empty slot is content: `[None, 3]` is a different row than `[3]`.
    let a = pr(&[None, n(3.0)]);
    let b = pr(&[n(3.0)]);
    assert!(a != b);
    assert_ne!(hash_of(&a), hash_of(&b));
}

#[test]
fn projected_row_compares_values_position_by_position() {
    assert!(pr(&[n(1.0), n(2.0)]) == pr(&[n(1.0), n(2.0)]));
    assert_eq!(
        hash_of(&pr(&[n(1.0), n(2.0)])),
        hash_of(&pr(&[n(1.0), n(2.0)]))
    );

    // Order matters.
    assert!(pr(&[n(1.0), n(2.0)]) != pr(&[n(2.0), n(1.0)]));
    assert_ne!(
        hash_of(&pr(&[n(1.0), n(2.0)])),
        hash_of(&pr(&[n(2.0), n(1.0)]))
    );

    // Differing value at the same position.
    assert!(pr(&[n(1.0), n(2.0)]) != pr(&[n(1.0), n(9.0)]));
}

#[test]
fn projected_row_string_values() {
    // Equal string rows hash and compare equal.
    let a = pr_vals(vec![s("foo"), s("bar")]);
    let b = pr_vals(vec![s("foo"), s("bar")]);
    assert!(a == b);
    assert_eq!(hash_of(&a), hash_of(&b));

    // A differing string at one position breaks equality.
    let c = pr_vals(vec![s("foo"), s("baz")]);
    assert!(a != c);
    assert_ne!(hash_of(&a), hash_of(&c));

    // The trailing-strip and leading-positional rules hold for strings too.
    assert!(pr_vals(vec![s("foo"), None]) == pr_vals(vec![s("foo")]));
    assert_eq!(
        hash_of(&pr_vals(vec![s("foo"), None])),
        hash_of(&pr_vals(vec![s("foo")]))
    );
    assert!(pr_vals(vec![None, s("foo")]) != pr_vals(vec![s("foo")]));
    assert_ne!(
        hash_of(&pr_vals(vec![None, s("foo")])),
        hash_of(&pr_vals(vec![s("foo")]))
    );
}

#[test]
fn projected_row_mixed_value_types() {
    // Rows mixing numbers and strings compare and hash by position.
    let a = pr_vals(vec![n(1.0).map(SharedValue::new_num), s("x")]);
    let b = pr_vals(vec![n(1.0).map(SharedValue::new_num), s("x")]);
    assert!(a == b);
    assert_eq!(hash_of(&a), hash_of(&b));

    let c = pr_vals(vec![n(1.0).map(SharedValue::new_num), s("y")]);
    assert!(a != c);
    assert_ne!(hash_of(&a), hash_of(&c));
}

#[test]
fn heap_drain_entries_expose_sort_vals_in_best_first_order() {
    // The heap-family drain keeps each value's ranking key so the remote reducer
    // can re-attach the deferred SORTBY columns (see `HeapEntry::into_parts`).
    let key = make_key();
    let mut s = Storage::new(true, Some((0, 3)), SORT_ASC);
    let h = as_heap(&mut s);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        h.consider(sort_vals(i), (), || projected(&key, i));
    }
    let snapshots: Vec<f64> = h
        .drain()
        .map(|entry| {
            let (ranking_key, _value) = entry.into_parts();
            ranking_key.sort_vals()[0]
                .as_ref()
                .and_then(|v| v.as_num())
                .expect("sort snapshot present")
        })
        .collect();
    // Best→worst under ASC = smallest sort values first.
    assert_eq!(snapshots, vec![0.0, 1.0, 2.0]);
}
