/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Unit tests for the bounded [`Storage`] shared by the COLLECT reducer
//! variants.

extern crate redisearch_rs;

use reducers::collect::storage::{DEFAULT_LIMIT, Storage};
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

fn row(key: &RLookupKey<'_>, v: f64) -> RLookupRow<'static> {
    let mut r = RLookupRow::new();
    r.write_key(key, SharedValue::new_num(v));
    r
}

/// `project` closure that increments `counter` on each call.
fn counting_project<'a>(
    counter: &'a mut usize,
    key: &'a RLookupKey<'_>,
    tag: f64,
) -> impl FnOnce() -> RLookupRow<'static> + 'a {
    move || {
        *counter += 1;
        row(key, tag)
    }
}

fn drained_nums(key: &RLookupKey<'_>, drained: &[RLookupRow<'_>]) -> Vec<f64> {
    drained
        .iter()
        .map(|r| r.get(key).expect("row missing key").as_num().expect("num"))
        .collect()
}

#[test]
fn insert_entry_array_caps_at_cap_in_insertion_order() {
    let key = make_key();
    let mut s = Storage::new(false, Some((0, 3)), 0);
    for i in 0..5 {
        s.insert_entry(|| sort_vals(i as f64), (), || row(&key, i as f64));
    }
    let drained: Vec<_> = s.drain().collect();
    assert_eq!(drained.len(), 3);
    assert_eq!(drained_nums(&key, &drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn insert_entry_array_drops_excess_without_calling_project() {
    let key = make_key();
    let mut counter = 0;
    let mut s = Storage::new(false, Some((0, 2)), 0);
    for v in [1.0_f64, 2.0, 3.0, 4.0] {
        s.insert_entry(|| sort_vals(v), (), counting_project(&mut counter, &key, v));
    }
    assert_eq!(
        counter, 2,
        "`project` must not run for entries beyond the cap"
    );
}

#[test]
fn insert_entry_heap_keeps_top_k_under_asc() {
    let key = make_key();
    let mut s = Storage::new(true, Some((0, 3)), SORT_ASC);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        s.insert_entry(|| sort_vals(i), (), || row(&key, i));
    }
    let drained: Vec<_> = s.drain().collect();
    // ASC: smallest is best; heap drains best→worst.
    assert_eq!(drained_nums(&key, &drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn insert_entry_heap_keeps_top_k_under_desc() {
    let key = make_key();
    let mut s = Storage::new(true, Some((0, 3)), SORT_DESC);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        s.insert_entry(|| sort_vals(i), (), || row(&key, i));
    }
    let drained: Vec<_> = s.drain().collect();
    // DESC: largest is best; heap drains best→worst.
    assert_eq!(drained_nums(&key, &drained), vec![5.0, 4.0, 3.0]);
}

#[test]
fn insert_entry_heap_skips_project_for_doomed_candidates() {
    let key = make_key();
    let mut counter = 0;
    let mut s = Storage::new(true, Some((0, 2)), SORT_ASC);
    // Fill with the two best candidates first.
    for v in [0.0_f64, 1.0] {
        s.insert_entry(|| sort_vals(v), (), counting_project(&mut counter, &key, v));
    }
    // Each subsequent candidate is worse than the worst survivor (1.0)
    // under ASC, so `project` must not run.
    for v in [2.0_f64, 3.0, 4.0] {
        s.insert_entry(|| sort_vals(v), (), counting_project(&mut counter, &key, v));
    }
    assert_eq!(
        counter, 2,
        "`project` must not run for candidates worse than the heap's worst"
    );
}

#[test]
fn insert_entry_heap_invokes_project_on_eviction() {
    let key = make_key();
    let mut counter = 0;
    let mut s = Storage::new(true, Some((0, 2)), SORT_ASC);
    // Each insert is strictly better than the current worst, so every
    // candidate must be projected.
    for v in [5.0_f64, 3.0, 1.0] {
        s.insert_entry(|| sort_vals(v), (), counting_project(&mut counter, &key, v));
    }
    assert_eq!(counter, 3);
}

#[test]
fn drain_array_applies_skip_take() {
    let key = make_key();
    let mut s = Storage::new(false, Some((1, 2)), 0);
    for i in 0..5 {
        s.insert_entry(|| sort_vals(i as f64), (), || row(&key, i as f64));
    }
    let drained: Vec<_> = s.drain().collect();
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0]);
}

#[test]
fn drain_heap_applies_skip_take_after_best_first_order() {
    let key = make_key();
    let mut s = Storage::new(true, Some((1, 2)), SORT_ASC);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        s.insert_entry(|| sort_vals(i), (), || row(&key, i));
    }
    // Top-3 under ASC = [0, 1, 2] best→worst; offset 1, count 2 → [1, 2].
    let drained: Vec<_> = s.drain().collect();
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0]);
}

#[test]
fn insert_entry_heap_uses_default_limit_when_no_explicit_limit() {
    let key = make_key();
    let mut s = Storage::new(true, None, SORT_ASC);
    for i in 0..(DEFAULT_LIMIT as usize + 5) {
        s.insert_entry(|| sort_vals(i as f64), (), || row(&key, i as f64));
    }
    let drained: Vec<_> = s.drain().collect();
    assert_eq!(drained.len(), DEFAULT_LIMIT as usize);
}
