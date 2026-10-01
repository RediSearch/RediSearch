/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `FIRST_VALUE` and the exact
//! `COUNT_DISTINCT`, driven through [`AccumulatorReducer`] the way the grouper
//! drives them.

extern crate redisearch_rs;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

use reducers::accumulator::{Accumulator, AccumulatorReducer};
use reducers::count::Count;
use reducers::count_distinct::CountDistinct;
use reducers::first_value::{FirstValue, SortBy};
use reducers::min_max::{Extreme, MinMax};
use reducers::std_dev::StdDev;
use reducers::sum::Sum;
use rlookup::{RLookupKey, RLookupKeyFlags, RLookupRow};
use value::{SharedValue, Value};

fn key() -> RLookupKey<'static> {
    RLookupKey::new(c"price", RLookupKeyFlags::empty())
}

/// Reduces one group made of one row per entry of `rows`, each row holding a
/// value per key; `None` leaves the row without that key.
fn reduce_rows<A: Accumulator, const N: usize>(
    accumulator: A,
    keys: [&RLookupKey; N],
    rows: &[[Option<SharedValue>; N]],
) -> SharedValue {
    let reducer = AccumulatorReducer::new(accumulator);
    let state = reducer.new_state();
    for values in rows {
        let mut row = RLookupRow::new();
        for (key, value) in keys.iter().zip(values) {
            if let Some(value) = value {
                row.write_key(key, value.clone());
            }
        }
        reducer.accumulator().add(state, &row);
    }
    let result = reducer.accumulator().finalize(state);
    // SAFETY: `state` came from `new_state` and is not used afterwards.
    unsafe { reducer.drop_state(state) };
    result
}

/// Reduces one group made of one row per entry of `values`; `None` leaves the
/// row without the property.
fn reduce<A: Accumulator>(accumulator: A, key: &RLookupKey, values: &[Option<SharedValue>]) -> f64 {
    let rows: Vec<_> = values.iter().map(|value| [value.clone()]).collect();
    number(&reduce_rows(accumulator, [key], &rows))
}

fn number(value: &SharedValue) -> f64 {
    match **value {
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

/// Without a sort key, the first row's value is kept, even a missing one (as null).
#[test]
fn first_value_keeps_the_first_row() {
    let key = key();
    let first = |rows: &[Option<SharedValue>]| {
        let rows: Vec<_> = rows.iter().map(|value| [value.clone()]).collect();
        reduce_rows(FirstValue::new(&key, None), [&key], &rows)
    };
    assert_eq!(number(&first(&[num(3.0), num(1.0)])), 3.0);
    assert!(matches!(*first(&[None, num(1.0)]), Value::Null));
    assert!(matches!(*first(&[]), Value::Null));
}

/// Runs `FIRST_VALUE` of the first element of each row, sorted by the second.
fn first_value_by(ascending: bool, rows: &[[Option<SharedValue>; 2]]) -> SharedValue {
    let key = key();
    let mut sort_key = RLookupKey::new(c"rank", RLookupKeyFlags::empty());
    // Its own row slot, apart from `key`'s.
    sort_key.dstidx = 1;
    let sort_by = SortBy {
        key: &sort_key,
        ascending,
    };
    reduce_rows(
        FirstValue::new(&key, Some(sort_by)),
        [&key, &sort_key],
        rows,
    )
}

#[test]
fn first_value_by_keeps_the_row_whose_sort_key_comes_first() {
    let rows = [
        [string("a"), num(3.0)],
        [string("b"), num(1.0)],
        [string("c"), num(2.0)],
    ];
    assert_eq!(first_value_by(true, &rows).as_str_bytes(), Some(&b"b"[..]));
    assert_eq!(first_value_by(false, &rows).as_str_bytes(), Some(&b"a"[..]));
}

/// A missing sort key, or one referring to null, is null.
#[test]
fn first_value_by_never_prefers_a_null_sort_key() {
    let null_ref = Some(SharedValue::new(Value::Ref(SharedValue::new(Value::Null))));
    for null in [None, null_ref] {
        let rows = [[string("a"), num(1.0)], [string("b"), null.clone()]];
        for ascending in [true, false] {
            let result = first_value_by(ascending, &rows);
            assert_eq!(
                result.as_str_bytes(),
                Some(&b"a"[..]),
                "{ascending} {null:?}"
            );
        }
    }
}

/// When the first row has no sort key, its value is kept until a later row beats
/// the first non-null sort key, which does not itself replace the value.
#[test]
fn first_value_by_after_a_null_sort_key_needs_a_row_to_beat_the_first_non_null_one() {
    let not_beaten = [
        [string("a"), None],
        [string("b"), num(5.0)],
        [string("c"), num(7.0)],
    ];
    let result = first_value_by(true, &not_beaten);
    assert_eq!(result.as_str_bytes(), Some(&b"a"[..]));

    let beaten = [
        [string("a"), None],
        [string("b"), num(5.0)],
        [string("c"), num(3.0)],
    ];
    let result = first_value_by(true, &beaten);
    assert_eq!(result.as_str_bytes(), Some(&b"c"[..]));
}

/// Missing properties and the static null are not counted.
#[test]
fn count_distinct_counts_distinct_values() {
    let key = key();
    let rows = [
        num(1.0),
        num(1.0),
        num(2.0),
        string("a"),
        string("a"),
        None,
        Some(SharedValue::null_static()),
    ];
    assert_eq!(reduce(CountDistinct::new(&key), &key, &rows), 3.0);
    assert_eq!(reduce(CountDistinct::new(&key), &key, &[]), 0.0);
}

/// Only the static null is skipped; any other null value is counted.
#[test]
fn count_distinct_counts_a_non_static_null() {
    let key = key();
    let rows = [Some(SharedValue::new(Value::Null))];
    assert_eq!(reduce(CountDistinct::new(&key), &key, &rows), 1.0);
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
    let free_instance = vtable.FreeInstance;
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
        if let Some(free_instance) = free_instance {
            // SAFETY: `group` is a state of `reducer`, and is not used afterwards.
            unsafe { free_instance(reducer, group) };
        }
        number(&value)
    });
    // SAFETY: `reducer` is live and nothing uses it or its states afterwards.
    unsafe { free(reducer) };
    results
}

#[test]
fn vtable_keeps_interleaved_groups_apart() {
    use redisearch_rs::reducers::accumulator::{
        CountDistinctReducer_Create, CountReducer_Create, FirstValueReducer_Create,
        MinMaxReducer_Create, StdDevReducer_Create, SumReducer_Create,
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
    // SAFETY: as above.
    unsafe {
        let first = reduce_interleaved(
            FirstValueReducer_Create(key_ptr, key_ptr, false),
            &key,
            rows,
        );
        assert_eq!(first, [3.0, 20.0]);
    }
    // SAFETY: as above.
    unsafe {
        let distinct = reduce_interleaved(CountDistinctReducer_Create(key_ptr), &key, rows);
        assert_eq!(distinct, [2.0, 2.0]);
    }
}

/// Only the reducers whose group states own something free them per group.
#[test]
fn vtable_frees_group_states_only_when_they_own_something() {
    use redisearch_rs::reducers::accumulator::{
        CountDistinctReducer_Create, FirstValueReducer_Create, StdDevReducer_Create,
        SumReducer_Create,
    };

    let key = key();
    let key_ptr = std::ptr::from_ref(&key).cast::<ffi::RLookupKey>();
    // SAFETY: each reducer reads `key`, which outlives it and is not mutated.
    let reducers = unsafe {
        [
            (SumReducer_Create(key_ptr, false), false),
            (StdDevReducer_Create(key_ptr), false),
            (
                FirstValueReducer_Create(key_ptr, std::ptr::null(), true),
                true,
            ),
            (CountDistinctReducer_Create(key_ptr), true),
        ]
    };
    for (reducer, frees_states) in reducers {
        // SAFETY: `reducer` is live; the reference ends with this statement.
        let (free_instance, free) = unsafe { ((*reducer).FreeInstance, (*reducer).Free.unwrap()) };
        assert_eq!(free_instance.is_some(), frees_states);
        // SAFETY: `reducer` is live and not used afterwards.
        unsafe { free(reducer) };
    }
}
