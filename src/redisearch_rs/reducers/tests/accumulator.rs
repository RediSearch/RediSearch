/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV` and `FIRST_VALUE`, driven
//! through [`AccumulatorReducer`] the way the grouper drives them.

extern crate redisearch_rs;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

use reducers::accumulator::{Accumulator, AccumulatorReducer};
use reducers::count::Count;
use reducers::first_value::{FirstValue, SortBy};
use reducers::min_max::{Extreme, MinMax};
use reducers::std_dev::StdDev;
use reducers::sum::{Sum, SumMode};
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
        reducer.add(state, &row);
    }
    let result = reducer.finalize(state);
    reducer.drop_state(state);
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

    assert_eq!(reduce(Sum::new(&key, SumMode::Sum), &key, &rows), 3.0);
    assert_eq!(reduce(Sum::new(&key, SumMode::Average), &key, &rows), 1.0);
}

#[test]
fn sum_and_avg_of_no_numbers_are_nan() {
    let key = key();
    for mode in [SumMode::Sum, SumMode::Average] {
        assert!(reduce(Sum::new(&key, mode), &key, &[]).is_nan());
        assert!(reduce(Sum::new(&key, mode), &key, &[None, string("abc")]).is_nan());
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

fn std_dev(values: &[f64]) -> f64 {
    let key = key();
    let rows = values.iter().copied().map(num).collect::<Vec<_>>();
    reduce(StdDev::new(&key), &key, &rows)
}

#[test]
fn std_dev_of_constant_groups_is_zero() {
    assert_eq!(std_dev(&[0.0, 0.0, 0.0]), 0.0);
    assert_eq!(std_dev(&[7.5, 7.5, 7.5, 7.5]), 0.0);
}

#[test]
fn std_dev_handles_negative_and_mixed_signs() {
    assert_eq!(std_dev(&[-3.0, -1.0, 1.0]), 2.0);
    assert_eq!(std_dev(&[-1.0, -3.0, -5.0]), 2.0);
}

#[test]
fn std_dev_propagates_nan_once_there_are_two_values() {
    assert_eq!(std_dev(&[f64::NAN]), 0.0);
    assert!(std_dev(&[f64::NAN, 1.0]).is_nan());
    assert!(std_dev(&[1.0, f64::NAN]).is_nan());
    assert!(std_dev(&[1.0, f64::NAN, 3.0]).is_nan());
    assert!(std_dev(&[f64::NAN, f64::NAN]).is_nan());
}

/// An infinity makes the running mean or the squared deviation non-finite, so any
/// group of two or more values containing one reduces to NaN.
#[test]
fn std_dev_with_infinities() {
    assert_eq!(std_dev(&[f64::INFINITY]), 0.0);
    assert_eq!(std_dev(&[f64::NEG_INFINITY]), 0.0);
    assert!(std_dev(&[f64::INFINITY, f64::INFINITY]).is_nan());
    assert!(std_dev(&[f64::NEG_INFINITY, f64::NEG_INFINITY]).is_nan());
    assert!(std_dev(&[f64::INFINITY, 1.0]).is_nan());
    assert!(std_dev(&[1.0, f64::INFINITY]).is_nan());
    assert!(std_dev(&[f64::INFINITY, f64::NEG_INFINITY]).is_nan());
}

/// A numeric string beyond the `f64` range converts to an infinity, which takes
/// part in the calculation rather than being skipped.
#[test]
fn std_dev_counts_numeric_strings_that_overflow_to_infinity() {
    let key = key();
    for overflowing in ["1e309", "-1e309"] {
        let rows = [string(overflowing), num(1.0)];
        assert!(reduce(StdDev::new(&key), &key, &rows).is_nan());
    }
}

#[test]
fn std_dev_with_intermediate_overflow() {
    // The sum of squares overflows to +inf.
    assert_eq!(std_dev(&[0.0, 1e308]), f64::INFINITY);
    // The difference overflows to +inf, so the mean becomes +inf and the sum of
    // squares inf * -inf.
    assert!(std_dev(&[-1e308, 1e308]).is_nan());
}

/// Welford's update stays accurate when the mean dwarfs the spread.
#[test]
fn std_dev_of_large_closely_spaced_values() {
    let base = 1e9;
    let result = std_dev(&[base + 4.0, base + 7.0, base + 13.0, base + 16.0]);
    assert!((result - 30.0_f64.sqrt()).abs() < 1e-6);
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

/// Runs `accumulator` over two groups the way the grouper does: a state per
/// group, rows interleaved between them, then finalize and drop. Returns each
/// group's result.
fn reduce_interleaved<A: Accumulator>(
    accumulator: A,
    key: &RLookupKey,
    rows: [(usize, f64); 4],
) -> [f64; 2] {
    let reducer = AccumulatorReducer::new(accumulator);
    let groups = [reducer.new_state(), reducer.new_state()];
    for (group, num) in rows {
        let mut row = RLookupRow::new();
        row.write_key(key, SharedValue::new_num(num));
        reducer.add(groups[group], &row);
    }
    groups.map(|group| {
        let value = reducer.finalize(group);
        reducer.drop_state(group);
        number(&value)
    })
}

#[test]
fn interleaved_groups_are_kept_apart() {
    let key = key();
    let rows = [(0, 1.0), (1, 10.0), (0, 3.0), (1, 20.0)];

    assert_eq!(reduce_interleaved(Count, &key, rows), [2.0, 2.0]);
    let sum = Sum::new(&key, SumMode::Sum);
    assert_eq!(reduce_interleaved(sum, &key, rows), [4.0, 30.0]);
    let avg = Sum::new(&key, SumMode::Average);
    assert_eq!(reduce_interleaved(avg, &key, rows), [2.0, 15.0]);
    let max = MinMax::new(&key, Extreme::Max);
    assert_eq!(reduce_interleaved(max, &key, rows), [3.0, 20.0]);
    let std_dev = StdDev::new(&key);
    let expected = [2.0_f64.sqrt(), 50.0_f64.sqrt()];
    assert_eq!(reduce_interleaved(std_dev, &key, rows), expected);
    let sort_by = SortBy {
        key: &key,
        ascending: false,
    };
    let first = FirstValue::new(&key, Some(sort_by));
    assert_eq!(reduce_interleaved(first, &key, rows), [3.0, 20.0]);
}

/// Only the reducers whose group states own something free them per group.
#[test]
fn vtable_frees_group_states_only_when_they_own_something() {
    use redisearch_rs::reducers::accumulator::{
        FirstValueReducer_Create, StdDevReducer_Create, SumReducer_Create,
    };

    /// Whether `reducer` registers `FreeInstance`; frees it.
    fn frees_states(reducer: *mut ffi::Reducer) -> bool {
        // SAFETY: `reducer` is live; the reference ends with this statement.
        let vtable = unsafe { &*reducer };
        let (free_instance, free) = (vtable.FreeInstance, vtable.Free.unwrap());
        // SAFETY: `reducer` is live and not used afterwards.
        unsafe { free(reducer) };
        free_instance.is_some()
    }

    let key = key();
    let key_ptr = std::ptr::from_ref(&key).cast::<ffi::RLookupKey>();
    // SAFETY: each reducer reads `key`, which outlives it and is not mutated.
    let sum = unsafe { SumReducer_Create(key_ptr, false) };
    assert!(!frees_states(sum));
    // SAFETY: as above.
    let std_dev = unsafe { StdDevReducer_Create(key_ptr) };
    assert!(!frees_states(std_dev));
    // SAFETY: as above.
    let first = unsafe { FirstValueReducer_Create(key_ptr, std::ptr::null(), true) };
    assert!(frees_states(first));
}
