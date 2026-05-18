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
        let sv = SharedValue::new_num(i as f64);
        s.insert_entry(|| [Some(&sv)], || row(&key, i as f64));
    }
    let drained: Vec<_> = s.drain(true).collect();
    assert_eq!(drained.len(), 3);
    assert_eq!(drained_nums(&key, &drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn insert_entry_array_drops_excess_without_calling_project() {
    let key = make_key();
    let mut counter = 0;
    let mut s = Storage::new(false, Some((0, 2)), 0);
    for v in [1.0_f64, 2.0, 3.0, 4.0] {
        let sv = SharedValue::new_num(v);
        s.insert_entry(|| [Some(&sv)], counting_project(&mut counter, &key, v));
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
        let sv = SharedValue::new_num(i);
        s.insert_entry(|| [Some(&sv)], || row(&key, i));
    }
    let drained: Vec<_> = s.drain(true).collect();
    // ASC: smallest is best; heap drains best→worst.
    assert_eq!(drained_nums(&key, &drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn insert_entry_heap_keeps_top_k_under_desc() {
    let key = make_key();
    let mut s = Storage::new(true, Some((0, 3)), SORT_DESC);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        let sv = SharedValue::new_num(i);
        s.insert_entry(|| [Some(&sv)], || row(&key, i));
    }
    let drained: Vec<_> = s.drain(true).collect();
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
        let sv = SharedValue::new_num(v);
        s.insert_entry(|| [Some(&sv)], counting_project(&mut counter, &key, v));
    }
    // Each subsequent candidate is worse than the worst survivor (1.0)
    // under ASC, so `project` must not run.
    for v in [2.0_f64, 3.0, 4.0] {
        let sv = SharedValue::new_num(v);
        s.insert_entry(|| [Some(&sv)], counting_project(&mut counter, &key, v));
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
        let sv = SharedValue::new_num(v);
        s.insert_entry(|| [Some(&sv)], counting_project(&mut counter, &key, v));
    }
    assert_eq!(counter, 3);
}

#[test]
fn drain_array_applies_skip_take() {
    let key = make_key();
    let mut s = Storage::new(false, Some((1, 2)), 0);
    for i in 0..5 {
        let sv = SharedValue::new_num(i as f64);
        s.insert_entry(|| [Some(&sv)], || row(&key, i as f64));
    }
    let drained: Vec<_> = s.drain(true).collect();
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0]);
}

#[test]
fn drain_without_limit_ignores_stored_limit() {
    let key = make_key();
    let mut s = Storage::new(false, Some((1, 2)), 0);
    for i in 0..3 {
        let sv = SharedValue::new_num(i as f64);
        s.insert_entry(|| [Some(&sv)], || row(&key, i as f64));
    }
    let drained: Vec<_> = s.drain(false).collect();
    assert_eq!(drained_nums(&key, &drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn drain_heap_applies_skip_take_after_best_first_order() {
    let key = make_key();
    let mut s = Storage::new(true, Some((1, 2)), SORT_ASC);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        let sv = SharedValue::new_num(i);
        s.insert_entry(|| [Some(&sv)], || row(&key, i));
    }
    // Top-3 under ASC = [0, 1, 2] best→worst; offset 1, count 2 → [1, 2].
    let drained: Vec<_> = s.drain(true).collect();
    assert_eq!(drained_nums(&key, &drained), vec![1.0, 2.0]);
}

#[test]
fn insert_entry_heap_uses_default_limit_when_no_explicit_limit() {
    let key = make_key();
    let mut s = Storage::new(true, None, SORT_ASC);
    for i in 0..(DEFAULT_LIMIT as usize + 5) {
        let sv = SharedValue::new_num(i as f64);
        s.insert_entry(|| [Some(&sv)], || row(&key, i as f64));
    }
    let drained: Vec<_> = s.drain(true).collect();
    assert_eq!(drained.len(), DEFAULT_LIMIT as usize);
}

#[test]
fn insert_entry_heap_does_not_materialize_sort_view_for_doomed_candidates() {
    use std::cell::Cell;
    let key = make_key();
    let mut s = Storage::new(true, Some((0, 2)), SORT_ASC);

    // Fill heap with the two best candidates first.
    for v in [0.0_f64, 1.0] {
        let sv = SharedValue::new_num(v);
        s.insert_entry(|| [Some(&sv)], || row(&key, v));
    }

    // Each subsequent candidate is worse than the worst survivor (1.0)
    // under ASC. `Storage` must call `sort_view` exactly once (for the
    // borrow-compare) — a second call would mean it wastefully
    // materialized the owned snapshot for a doomed candidate.
    for v in [2.0_f64, 3.0, 4.0] {
        let sv = SharedValue::new_num(v);
        let calls = Cell::new(0);
        s.insert_entry(
            || {
                calls.set(calls.get() + 1);
                [Some(&sv)]
            },
            || row(&key, v),
        );
        assert_eq!(
            calls.get(),
            1,
            "sort_view must be invoked exactly once for a doomed candidate \
             (compare-only); a second call indicates wasted materialization",
        );
    }
}

#[test]
fn insert_entry_heap_materializes_sort_view_for_winners() {
    use std::cell::Cell;
    let key = make_key();
    let mut s = Storage::new(true, Some((0, 2)), SORT_ASC);

    // Fill with two retainable candidates.
    for v in [5.0_f64, 3.0] {
        let sv = SharedValue::new_num(v);
        s.insert_entry(|| [Some(&sv)], || row(&key, v));
    }

    // Winner under ASC: 1.0 beats the heap's worst (5.0). `Storage` must
    // call `sort_view` twice — once for the borrow-compare, once to
    // materialize the owned snapshot now that survival is confirmed.
    let sv = SharedValue::new_num(1.0);
    let calls = Cell::new(0);
    s.insert_entry(
        || {
            calls.set(calls.get() + 1);
            [Some(&sv)]
        },
        || row(&key, 1.0),
    );
    assert_eq!(
        calls.get(),
        2,
        "sort_view must be invoked twice for a winner: compare, then materialize",
    );
}
