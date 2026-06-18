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
fn heap_dedups_by_identity_keeping_best() {
    // DISTINCT spike: identical projected content collapses to one entry, and
    // `push_increase` keeps the best-ranked representative per identity.
    let key = make_key();
    let mut s = Storage::new(true, Some((0, 10)), SORT_ASC);
    let h = as_heap(&mut s);
    // Identity v=1 arrives twice (sort 5 then 1); ASC → best is sort 1.
    // Identity v=2 arrives once (sort 3).
    h.consider(sort_vals(5.0), (), || projected(&key, 1.0));
    h.consider(sort_vals(1.0), (), || projected(&key, 1.0));
    h.consider(sort_vals(3.0), (), || projected(&key, 2.0));
    let drained: Vec<_> = h.drain().map(HeapEntry::into_projected).collect();
    // Deduped to one row per identity. The order proves keep-best: v=1 is kept
    // at its better sort (1), so under ASC it precedes v=2 (sort 3). Had the
    // worse v=1 (sort 5) survived, v=2 would sort ahead of it.
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0]);
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
