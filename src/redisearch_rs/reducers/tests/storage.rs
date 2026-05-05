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
use value::SharedValue;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn drained_nums(drained: &[Box<[SharedValue]>]) -> Vec<f64> {
    drained
        .iter()
        .map(|p| p[0].as_num().expect("expected num"))
        .collect()
}

#[test]
fn insert_entry_array_caps_at_len_in_insertion_order() {
    let mut s = Storage::new(false, Some((0, 3)));
    for i in 0..5 {
        s.insert_entry(|| vec![SharedValue::new_num(i as f64)].into_boxed_slice());
    }
    let drained: Vec<_> = s.drain(true).collect();
    assert_eq!(drained.len(), 3, "array variant must cap at `cap`");
    assert_eq!(drained_nums(&drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn insert_entry_heap_caps_at_len_in_insertion_order() {
    let mut s = Storage::new(true, Some((0, 3)));
    for i in 0..5 {
        s.insert_entry(|| vec![SharedValue::new_num(i as f64)].into_boxed_slice());
    }
    let drained: Vec<_> = s.drain(true).collect();
    assert_eq!(drained.len(), 3, "heap variant must cap at `cap`");
    assert_eq!(drained_nums(&drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn insert_entry_drops_after_cap_without_calling_project() {
    let mut counter = 0;
    let mut s = Storage::new(false, Some((0, 2)));
    for v in [1.0_f64, 2.0, 3.0, 4.0] {
        s.insert_entry(|| {
            counter += 1;
            vec![SharedValue::new_num(v)].into_boxed_slice()
        });
    }
    assert_eq!(
        counter, 2,
        "`project` must not run for entries beyond the cap"
    );
}

#[test]
fn drain_applies_skip_take() {
    let mut s = Storage::new(false, Some((1, 2)));
    for i in 0..5 {
        s.insert_entry(|| vec![SharedValue::new_num(i as f64)].into_boxed_slice());
    }
    let drained: Vec<_> = s.drain(true).collect();
    assert_eq!(drained_nums(&drained), vec![1.0, 2.0]);
}

#[test]
fn drain_without_limit_ignores_stored_limit() {
    let mut s = Storage::new(false, Some((1, 2)));
    for i in 0..3 {
        s.insert_entry(|| vec![SharedValue::new_num(i as f64)].into_boxed_slice());
    }
    let drained: Vec<_> = s.drain(false).collect();
    assert_eq!(drained_nums(&drained), vec![0.0, 1.0, 2.0]);
}

#[test]
fn insert_entry_heap_uses_default_limit_when_no_explicit_limit() {
    let mut s = Storage::new(true, None);
    for i in 0..(DEFAULT_LIMIT as usize + 5) {
        s.insert_entry(|| vec![SharedValue::new_num(i as f64)].into_boxed_slice());
    }
    let drained: Vec<_> = s.drain(true).collect();
    assert_eq!(drained.len(), DEFAULT_LIMIT as usize);
}

#[test]
fn iter_yields_buffered_rows_in_insertion_order_under_cap() {
    let mut s = Storage::new(false, Some((0, 3)));
    for i in 0..5 {
        s.insert_entry(|| vec![SharedValue::new_num(i as f64)].into_boxed_slice());
    }
    let nums: Vec<f64> = s
        .iter()
        .map(|row| row[0].as_num().expect("expected num"))
        .collect();
    assert_eq!(nums, vec![0.0, 1.0, 2.0]);
}
