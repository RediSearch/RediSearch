/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Unit tests for the bounded COLLECT storage families ([`UnrankedStorage`] and
//! [`RankedStorage`], each with a `Plain` / `Distinct` variant) selected by
//! [`Storage`].

extern crate redisearch_rs;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use reducers::collect::storage::{
    DEFAULT_LIMIT, ProjectedRow, RankedStorage, Storage, UnrankedStorage,
};
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

/// Adapt an owned snapshot into the borrowed, cloneable iterator `consider`
/// takes; production passes borrows straight from the row / shard payload.
fn refs(vals: &[Option<SharedValue>]) -> impl Iterator<Item = Option<&SharedValue>> + Clone {
    vals.iter().map(Option::as_ref)
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

/// Unwrap the unranked family from a [`Storage`] built with `sortby = false`.
/// The `Plain` / `Distinct` variant is chosen by the `distinct` flag in
/// [`Storage::new`]; the methods dispatch on it internally.
fn as_unranked(storage: &mut Storage<()>) -> &mut UnrankedStorage {
    match storage {
        Storage::Unranked(u) => u,
        _ => panic!("expected UnrankedStorage"),
    }
}

/// Unwrap the ranked family from a [`Storage`] built with `sortby = true`.
/// The `Plain` / `Distinct` variant is chosen by the `distinct` flag in
/// [`Storage::new`]; the methods dispatch on it internally.
fn as_ranked(storage: &mut Storage<()>) -> &mut RankedStorage<()> {
    match storage {
        Storage::Ranked(r) => r,
        _ => panic!("expected RankedStorage"),
    }
}

#[test]
fn array_push_caps_at_cap_in_insertion_order() {
    let key = make_key();
    let mut s = Storage::new(false, false, Some((0, 3)), 0);
    let a = as_unranked(&mut s);
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
    let mut s = Storage::new(false, false, Some((0, 2)), 0);
    let a = as_unranked(&mut s);
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
    let mut s = Storage::new(true, false, Some((0, 3)), SORT_ASC);
    let h = as_ranked(&mut s);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        h.consider(refs(&sort_vals(i)), (), || projected(&key, i));
    }
    let drained: Vec<_> = h.drain().collect();
    // ASC: smallest is best; heap drains best→worst.
    assert_eq!(drained_nums(&key, &drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn heap_consider_keeps_top_k_under_desc() {
    let key = make_key();
    let mut s = Storage::new(true, false, Some((0, 3)), SORT_DESC);
    let h = as_ranked(&mut s);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        h.consider(refs(&sort_vals(i)), (), || projected(&key, i));
    }
    let drained: Vec<_> = h.drain().collect();
    // DESC: largest is best; heap drains best→worst.
    assert_eq!(drained_nums(&key, &drained), vec![5.0, 4.0, 3.0]);
}

#[test]
fn heap_consider_skips_project_for_doomed_candidates() {
    let key = make_key();
    let mut counter = 0;
    let mut s = Storage::new(true, false, Some((0, 2)), SORT_ASC);
    let h = as_ranked(&mut s);
    // Fill with the two best candidates first.
    for v in [0.0_f64, 1.0] {
        h.consider(
            refs(&sort_vals(v)),
            (),
            counting_project(&mut counter, &key, v),
        );
    }
    // Each subsequent candidate is worse than the worst survivor (1.0) under
    // ASC. `project` and the sort-value snapshot are co-gated on winning the
    // borrow comparison, so neither must run for a doomed candidate.
    for v in [2.0_f64, 3.0, 4.0] {
        h.consider(
            refs(&sort_vals(v)),
            (),
            counting_project(&mut counter, &key, v),
        );
    }
    assert_eq!(
        counter, 2,
        "`project` (and the snapshot clone) must not run for candidates worse than the worst"
    );
}

#[test]
fn heap_consider_invokes_project_on_eviction() {
    let key = make_key();
    let mut counter = 0;
    let mut s = Storage::new(true, false, Some((0, 2)), SORT_ASC);
    let h = as_ranked(&mut s);
    // Each insert is strictly better than the current worst, so every
    // candidate must be projected.
    for v in [5.0_f64, 3.0, 1.0] {
        h.consider(
            refs(&sort_vals(v)),
            (),
            counting_project(&mut counter, &key, v),
        );
    }
    assert_eq!(counter, 3);
}

#[test]
fn drain_array_applies_skip_take() {
    let key = make_key();
    let mut s = Storage::new(false, false, Some((1, 2)), 0);
    let a = as_unranked(&mut s);
    for i in 0..5 {
        a.push(|| projected(&key, i as f64));
    }
    let drained: Vec<_> = a.drain().collect();
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0]);
}

#[test]
fn drain_heap_applies_skip_take_after_best_first_order() {
    let key = make_key();
    let mut s = Storage::new(true, false, Some((1, 2)), SORT_ASC);
    let h = as_ranked(&mut s);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        h.consider(refs(&sort_vals(i)), (), || projected(&key, i));
    }
    // Top-3 under ASC = [0, 1, 2] best→worst; offset 1, count 2 → [1, 2].
    let drained: Vec<_> = h.drain().collect();
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0]);
}

#[test]
fn heap_uses_default_limit_when_no_explicit_limit() {
    let key = make_key();
    let mut s = Storage::new(true, false, None, SORT_ASC);
    let h = as_ranked(&mut s);
    for i in 0..(DEFAULT_LIMIT as usize + 5) {
        h.consider(refs(&sort_vals(i as f64)), (), || projected(&key, i as f64));
    }
    let drained: Vec<_> = h.drain().collect();
    assert_eq!(drained.len(), DEFAULT_LIMIT as usize);
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
fn heap_drain_with_sort_vals_exposes_snapshot_in_best_first_order() {
    // `drain_with_sort_vals` pairs each row with its sort-key snapshot so the
    // remote reducer can re-attach the deferred SORTBY columns.
    let key = make_key();
    let mut s = Storage::new(true, false, Some((0, 3)), SORT_ASC);
    let h = as_ranked(&mut s);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        h.consider(refs(&sort_vals(i)), (), || projected(&key, i));
    }
    let snapshots: Vec<f64> = h
        .drain_with_sort_vals()
        .map(|(_row, sort_vals)| {
            sort_vals[0]
                .as_ref()
                .and_then(|v| v.as_num())
                .expect("sort snapshot present")
        })
        .collect();
    // Best→worst under ASC = smallest sort values first.
    assert_eq!(snapshots, vec![0.0, 1.0, 2.0]);
}

// ===== DISTINCT storage families =====
//
// These exercise the dedup *mechanism*. Identity is the field-aware
// `ProjectedRow` `Hash`/`Eq` (its projected values), so rows with equal values
// collapse and rows with differing values stay distinct.

#[test]
fn distinct_array_keeps_first_arrival_per_identity() {
    let key = make_key();
    let mut s = Storage::new(false, true, Some((0, 10)), 0);
    let d = as_unranked(&mut s);
    // Values 1 and 2 each arrive twice; only the first arrival of each survives,
    // in arrival order.
    for v in [1.0_f64, 2.0, 1.0, 3.0, 2.0] {
        d.push(|| projected(&key, v));
    }
    let drained: Vec<_> = d.drain().collect();
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0, 3.0]);
}

#[test]
fn distinct_array_caps_distinct_count() {
    let key = make_key();
    let mut s = Storage::new(false, true, Some((0, 2)), 0);
    let d = as_unranked(&mut s);
    for v in [1.0_f64, 2.0, 3.0, 4.0] {
        d.push(|| projected(&key, v));
    }
    // Cap of 2 distinct rows: the first two distinct arrivals survive.
    let drained: Vec<_> = d.drain().collect();
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0]);
}

#[test]
fn distinct_array_applies_skip_take() {
    let key = make_key();
    let mut s = Storage::new(false, true, Some((1, 2)), 0);
    let d = as_unranked(&mut s);
    for v in [1.0_f64, 2.0, 3.0, 1.0] {
        d.push(|| projected(&key, v));
    }
    // Distinct arrivals = [1, 2, 3]; offset 1, count 2 → [2, 3].
    let drained: Vec<_> = d.drain().collect();
    assert_eq!(drained_nums(&key, &drained), vec![2.0, 3.0]);
}

#[test]
fn distinct_heap_dedups_keeping_best_ranked() {
    let key = make_key();
    // Sort by a separate ranking value while deduping on the projected `v`.
    let mut s = Storage::new(true, true, Some((0, 10)), SORT_DESC);
    let d = as_ranked(&mut s);
    // Two rows with v=1 (ranks 1 and 5) and two with v=2 (ranks 2 and 4). Under
    // DESC the best (largest) rank per identity wins: v=1 keeps rank 5, v=2 keeps
    // rank 4.
    let inserts = [(1.0_f64, 1.0_f64), (2.0, 2.0), (1.0, 5.0), (2.0, 4.0)];
    for (v, rank) in inserts {
        d.consider(refs(&sort_vals(rank)), (), || projected(&key, v));
    }
    let drained: Vec<_> = d.drain().collect();
    // Two distinct identities survive; drained best→worst by the kept rank
    // (v=1 @5 then v=2 @4).
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0]);
}

#[test]
fn distinct_heap_stays_bounded_on_insert() {
    let key = make_key();
    let cap = 3;
    let mut s = Storage::new(true, true, Some((0, cap as u64)), SORT_DESC);
    let d = as_ranked(&mut s);
    // Feed many distinct identities; the queue must keep only the top `cap` by
    // rank (DESC ⇒ largest), bounded on insert rather than at drain.
    for v in 0..20_u32 {
        let v = v as f64;
        d.consider(refs(&sort_vals(v)), (), || projected(&key, v));
    }
    let drained: Vec<_> = d.drain().collect();
    assert_eq!(drained.len(), cap);
    // Top-3 under DESC = [19, 18, 17] best→worst.
    assert_eq!(drained_nums(&key, &drained), vec![19.0, 18.0, 17.0]);
}

#[test]
fn distinct_heap_duplicates_do_not_consume_capacity() {
    let key = make_key();
    let mut s = Storage::new(true, true, Some((0, 2)), SORT_DESC);
    let d = as_ranked(&mut s);
    // Only two distinct identities (v=1, v=2), each offered several times. The
    // cap of 2 must not evict either identity despite the repeated inserts.
    for (v, rank) in [
        (1.0_f64, 1.0_f64),
        (2.0, 2.0),
        (1.0, 3.0),
        (2.0, 4.0),
        (1.0, 9.0),
    ] {
        d.consider(refs(&sort_vals(rank)), (), || projected(&key, v));
    }
    let drained: Vec<_> = d.drain().collect();
    // Both identities survive; v=1 kept rank 9, v=2 kept rank 4 → DESC order.
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0]);
}
