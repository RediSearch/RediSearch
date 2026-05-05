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

use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use reducers::collect::storage::{DEFAULT_LIMIT, DrainedItem, Storage};
use value::SharedValue;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// `sort_asc_map` with bit 0 set: single ASC sort key.
const ASC: u64 = 0b1;
/// `sort_asc_map` with bit 0 clear: single DESC sort key.
const DESC: u64 = 0b0;

fn val(v: f64) -> Box<[SharedValue]> {
    vec![SharedValue::new_num(v)].into_boxed_slice()
}

/// `project` closure that increments `counter` on each call.
fn counting_project(counter: &AtomicUsize, tag: f64) -> impl FnOnce() -> Box<[SharedValue]> + '_ {
    move || {
        counter.fetch_add(1, AtomicOrdering::Relaxed);
        val(tag)
    }
}

fn drained_nums(drained: &[DrainedItem<Box<[SharedValue]>>]) -> Vec<f64> {
    drained
        .iter()
        .map(|item| item.projected[0].as_num().expect("expected num"))
        .collect()
}

#[test]
fn insert_entry_array_caps_at_cap_in_insertion_order() {
    let mut s = Storage::<Box<[SharedValue]>>::new(/* sortby */ false, Some((0, 3)), 0);
    for i in 0..5 {
        s.insert_entry(|| val(i as f64), || val(i as f64));
    }
    let drained: Vec<_> = s.drain(true).collect();
    assert_eq!(drained.len(), 3);
    assert_eq!(drained_nums(&drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn insert_entry_array_drops_excess_without_calling_project() {
    let counter = AtomicUsize::new(0);
    let mut s = Storage::<Box<[SharedValue]>>::new(/* sortby */ false, Some((0, 2)), 0);
    for v in [1.0_f64, 2.0, 3.0, 4.0] {
        s.insert_entry(|| val(v), counting_project(&counter, v));
    }
    assert_eq!(
        counter.load(AtomicOrdering::Relaxed),
        2,
        "`project` must not run for entries beyond the cap"
    );
}

#[test]
fn insert_entry_heap_keeps_top_k_under_asc() {
    let mut s = Storage::<Box<[SharedValue]>>::new(/* sortby */ true, Some((0, 3)), ASC);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        s.insert_entry(|| val(i), || val(i));
    }
    let drained: Vec<_> = s.drain(true).collect();
    // ASC: smallest is best; heap drains best→worst.
    assert_eq!(drained_nums(&drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn insert_entry_heap_keeps_top_k_under_desc() {
    let mut s = Storage::<Box<[SharedValue]>>::new(/* sortby */ true, Some((0, 3)), DESC);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        s.insert_entry(|| val(i), || val(i));
    }
    let drained: Vec<_> = s.drain(true).collect();
    // DESC: largest is best; heap drains best→worst.
    assert_eq!(drained_nums(&drained), vec![5.0, 4.0, 3.0]);
}

#[test]
fn insert_entry_heap_skips_project_for_doomed_candidates() {
    let counter = AtomicUsize::new(0);
    let mut s = Storage::<Box<[SharedValue]>>::new(/* sortby */ true, Some((0, 2)), ASC);
    // Fill with the two best candidates first.
    for v in [0.0_f64, 1.0] {
        s.insert_entry(|| val(v), counting_project(&counter, v));
    }
    // Each subsequent candidate is worse than the worst survivor (1.0)
    // under ASC, so `project` must not run.
    for v in [2.0_f64, 3.0, 4.0] {
        s.insert_entry(|| val(v), counting_project(&counter, v));
    }
    assert_eq!(
        counter.load(AtomicOrdering::Relaxed),
        2,
        "`project` must not run for candidates worse than the heap's worst"
    );
}

#[test]
fn insert_entry_heap_invokes_project_on_eviction() {
    let counter = AtomicUsize::new(0);
    let mut s = Storage::<Box<[SharedValue]>>::new(/* sortby */ true, Some((0, 2)), ASC);
    // Each insert is strictly better than the current worst, so every
    // candidate must be projected.
    for v in [5.0_f64, 3.0, 1.0] {
        s.insert_entry(|| val(v), counting_project(&counter, v));
    }
    assert_eq!(counter.load(AtomicOrdering::Relaxed), 3);
}

#[test]
fn drain_array_applies_skip_take() {
    let mut s = Storage::<Box<[SharedValue]>>::new(false, Some((1, 2)), 0);
    for i in 0..5 {
        s.insert_entry(|| val(i as f64), || val(i as f64));
    }
    let drained: Vec<_> = s.drain(true).collect();
    assert_eq!(drained_nums(&drained), vec![1.0, 2.0]);
}

#[test]
fn drain_without_limit_ignores_stored_limit() {
    let mut s = Storage::<Box<[SharedValue]>>::new(false, Some((1, 2)), 0);
    for i in 0..3 {
        s.insert_entry(|| val(i as f64), || val(i as f64));
    }
    let drained: Vec<_> = s.drain(false).collect();
    assert_eq!(drained_nums(&drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn drain_heap_applies_skip_take_after_best_first_order() {
    let mut s = Storage::<Box<[SharedValue]>>::new(/* sortby */ true, Some((1, 2)), ASC);
    for i in [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0] {
        s.insert_entry(|| val(i), || val(i));
    }
    // Top-3 under ASC = [0, 1, 2] best→worst; offset 1, count 2 → [1, 2].
    let drained: Vec<_> = s.drain(true).collect();
    assert_eq!(drained_nums(&drained), vec![1.0, 2.0]);
}

#[test]
fn drain_heap_carries_sort_vals_snapshot() {
    let mut s = Storage::<Box<[SharedValue]>>::new(/* sortby */ true, Some((0, 2)), ASC);
    for v in [3.0_f64, 1.0, 2.0] {
        s.insert_entry(|| val(v), || val(v));
    }
    let drained: Vec<_> = s.drain(true).collect();
    let snapshot_nums: Vec<f64> = drained
        .iter()
        .map(|item| {
            item.sort_vals
                .as_ref()
                .expect("heap drain must carry sort_vals")[0]
                .as_num()
                .expect("expected num")
        })
        .collect();
    assert_eq!(snapshot_nums, vec![1.0, 2.0]);
}

#[test]
fn drain_array_does_not_carry_sort_vals_snapshot() {
    let mut s = Storage::<Box<[SharedValue]>>::new(/* sortby */ false, Some((0, 2)), 0);
    s.insert_entry(|| val(0.0), || val(0.0));
    let drained: Vec<_> = s.drain(true).collect();
    assert!(drained.iter().all(|item| item.sort_vals.is_none()));
}

#[test]
fn insert_entry_heap_uses_default_limit_when_no_explicit_limit() {
    let mut s = Storage::<Box<[SharedValue]>>::new(/* sortby */ true, None, ASC);
    for i in 0..(DEFAULT_LIMIT as usize + 5) {
        s.insert_entry(|| val(i as f64), || val(i as f64));
    }
    let drained: Vec<_> = s.drain(true).collect();
    assert_eq!(drained.len(), DEFAULT_LIMIT as usize);
}
