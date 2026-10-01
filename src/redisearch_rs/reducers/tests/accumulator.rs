/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` and `STDDEV`, driven through
//! [`AccumulatorReducer`] the way the grouper drives them.

extern crate redisearch_rs;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

use reducers::accumulator::{Accumulator, AccumulatorReducer};
use reducers::count::Count;
use reducers::min_max::{Extreme, MinMax};
use reducers::std_dev::StdDev;
use reducers::sum::Sum;
use rlookup::{RLookupKey, RLookupKeyFlags, RLookupRow};
use value::{SharedValue, Value};

fn key() -> RLookupKey<'static> {
    RLookupKey::new(c"price", RLookupKeyFlags::empty())
}

/// Reduces one group made of one row per entry of `values`; `None` leaves the
/// row without the property.
fn reduce<A: Accumulator>(accumulator: A, key: &RLookupKey, values: &[Option<SharedValue>]) -> f64 {
    let reducer = AccumulatorReducer::new(accumulator);
    let state = reducer.new_state();
    for value in values {
        let mut row = RLookupRow::new();
        if let Some(value) = value {
            row.write_key(key, value.clone());
        }
        reducer.accumulator().add(state, &row);
    }
    match *reducer.accumulator().finalize(state) {
        Value::Number(result) => result,
        ref other => panic!("expected a number, got {other:?}"),
    }
}

fn num(n: f64) -> Option<SharedValue> {
    Some(SharedValue::new_num(n))
}

fn string(s: &str) -> Option<SharedValue> {
    Some(SharedValue::new_string(s.as_bytes().to_vec()))
}

#[test]
fn count_counts_every_row() {
    let key = key();
    assert_eq!(reduce(Count, &key, &[]), 0.0);
    assert_eq!(reduce(Count, &key, &[num(1.0), None, string("x")]), 3.0);
}

/// Rows whose property is missing or not numeric are skipped; numeric strings count.
#[test]
fn sum_and_avg_skip_non_numeric_rows() {
    let key = key();
    let rows = [num(1.5), None, string("2.5"), string("abc"), num(-1.0)];

    assert_eq!(reduce(Sum::new(&key, false), &key, &rows), 3.0);
    assert_eq!(reduce(Sum::new(&key, true), &key, &rows), 1.0);
}

#[test]
fn sum_and_avg_of_no_numbers_are_nan() {
    let key = key();
    for average in [false, true] {
        assert!(reduce(Sum::new(&key, average), &key, &[]).is_nan());
        assert!(reduce(Sum::new(&key, average), &key, &[None, string("abc")]).is_nan());
    }
}

#[test]
fn min_and_max() {
    let key = key();
    let rows = [num(3.0), string("-2"), None, num(7.5), string("abc")];

    assert_eq!(reduce(MinMax::new(&key, Extreme::Min), &key, &rows), -2.0);
    assert_eq!(reduce(MinMax::new(&key, Extreme::Max), &key, &rows), 7.5);
}

#[test]
fn min_and_max_of_no_numbers_are_infinite() {
    let key = key();
    assert_eq!(
        reduce(MinMax::new(&key, Extreme::Min), &key, &[None]),
        f64::INFINITY
    );
    assert_eq!(
        reduce(MinMax::new(&key, Extreme::Max), &key, &[None]),
        f64::NEG_INFINITY
    );
}

/// A NaN replaces the current extreme and is itself replaced by the next number.
#[test]
fn min_and_max_let_nan_through() {
    let key = key();
    for extreme in [Extreme::Min, Extreme::Max] {
        let nan_last = reduce(MinMax::new(&key, extreme), &key, &[num(1.0), num(f64::NAN)]);
        assert!(nan_last.is_nan(), "{extreme:?}");

        let number_after_nan = [num(1.0), num(f64::NAN), num(2.0)];
        let result = reduce(MinMax::new(&key, extreme), &key, &number_after_nan);
        assert_eq!(result, 2.0, "{extreme:?}");
    }
}

#[test]
fn std_dev_is_the_sample_standard_deviation() {
    let key = key();
    let rows = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0].map(num);
    let expected = (32.0_f64 / 7.0).sqrt();
    assert!((reduce(StdDev::new(&key), &key, &rows) - expected).abs() < 1e-12);
}

/// An array contributes each of its numeric elements; other values are skipped.
#[test]
fn std_dev_flattens_arrays_and_skips_non_numeric_values() {
    let key = key();
    let array = SharedValue::new_array([
        SharedValue::new_num(2.0),
        SharedValue::new_string(b"abc".to_vec()),
        SharedValue::new_string(b"4".to_vec()),
    ]);
    let rows = [Some(array), None, string("abc"), num(6.0)];
    assert_eq!(reduce(StdDev::new(&key), &key, &rows), 2.0);
}

/// Only a direct array is flattened: one behind a reference has no number.
#[test]
fn std_dev_does_not_flatten_an_array_behind_a_reference() {
    let key = key();
    let array = SharedValue::new_array([SharedValue::new_num(100.0)]);
    let rows = [
        Some(SharedValue::new(Value::Ref(array))),
        num(1.0),
        num(3.0),
    ];
    assert_eq!(reduce(StdDev::new(&key), &key, &rows), 2.0_f64.sqrt());
}

#[test]
fn std_dev_of_fewer_than_two_numbers_is_zero() {
    let key = key();
    assert_eq!(reduce(StdDev::new(&key), &key, &[]), 0.0);
    assert_eq!(reduce(StdDev::new(&key), &key, &[num(5.0), None]), 0.0);
}

/// Runs `reducer` over two groups the way the grouper drives the C vtable: a
/// state per group, rows interleaved between them, then finalize and free.
/// Returns each group's result.
///
/// # Safety
///
/// `reducer` must be a reducer returned by one of the `*Reducer_Create`
/// constructors, reading `key` if it reads a property. It is freed.
unsafe fn reduce_interleaved(
    reducer: *mut ffi::Reducer,
    key: &RLookupKey,
    rows: [(usize, f64); 4],
) -> [f64; 2] {
    // SAFETY: `reducer` is live (see above); the reference ends with this statement.
    let vtable = unsafe { &*reducer };
    assert!(vtable.FreeInstance.is_none());
    let new_instance = vtable.NewInstance.unwrap();
    let add = vtable.Add.unwrap();
    let finalize = vtable.Finalize.unwrap();
    let free = vtable.Free.unwrap();

    // SAFETY: `reducer` is live.
    let groups = [unsafe { new_instance(reducer) }, unsafe {
        new_instance(reducer)
    }];
    for (group, num) in rows {
        let mut row = RLookupRow::new();
        row.write_key(key, SharedValue::new_num(num));
        // SAFETY: `groups[group]` is a state of `reducer`, and `row` is a live row.
        unsafe { add(reducer, groups[group], std::ptr::from_ref(&row).cast()) };
    }
    let results = groups.map(|group| {
        // SAFETY: `group` is a state of `reducer`; the returned value is owned.
        let value = unsafe { SharedValue::from_raw(finalize(reducer, group).cast()) };
        match *value {
            Value::Number(result) => result,
            ref other => panic!("expected a number, got {other:?}"),
        }
    });
    // SAFETY: `reducer` is live and nothing uses it or its states afterwards.
    unsafe { free(reducer) };
    results
}

#[test]
fn vtable_keeps_interleaved_groups_apart() {
    use redisearch_rs::reducers::accumulator::{
        CountReducer_Create, MinMaxReducer_Create, StdDevReducer_Create, SumReducer_Create,
    };

    let key = key();
    let key_ptr = std::ptr::from_ref(&key).cast::<ffi::RLookupKey>();
    let rows = [(0, 1.0), (1, 10.0), (0, 3.0), (1, 20.0)];

    // SAFETY: each reducer reads `key`, which outlives it and is not mutated.
    unsafe {
        assert_eq!(
            reduce_interleaved(CountReducer_Create(), &key, rows),
            [2.0, 2.0]
        );
    }
    // SAFETY: as above.
    unsafe {
        let sums = reduce_interleaved(SumReducer_Create(key_ptr, false), &key, rows);
        assert_eq!(sums, [4.0, 30.0]);
    }
    // SAFETY: as above.
    unsafe {
        let averages = reduce_interleaved(SumReducer_Create(key_ptr, true), &key, rows);
        assert_eq!(averages, [2.0, 15.0]);
    }
    // SAFETY: as above.
    unsafe {
        let maxima = reduce_interleaved(MinMaxReducer_Create(key_ptr, true), &key, rows);
        assert_eq!(maxima, [3.0, 20.0]);
    }
    // SAFETY: as above.
    unsafe {
        let deviations = reduce_interleaved(StdDevReducer_Create(key_ptr), &key, rows);
        assert_eq!(deviations, [2.0_f64.sqrt(), 50.0_f64.sqrt()]);
    }
}
