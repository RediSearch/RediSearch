/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the simple single-key reducers (COUNT, MIN/MAX, STDDEV,
//! TOLIST, FIRST_VALUE, RANDOM_SAMPLE, COUNT_DISTINCT), driving each
//! through `add` → `finalize`.

extern crate redisearch_rs;

use rlookup::{RLookupKey, RLookupKeyFlags, RLookupRow};
use value::{SharedValue, Value};

redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn make_key(name: &'static std::ffi::CStr, dstidx: u16) -> RLookupKey<'static> {
    let mut key = RLookupKey::new(name, RLookupKeyFlags::empty());
    key.dstidx = dstidx;
    key
}

fn num_row(key: &RLookupKey<'_>, v: f64) -> RLookupRow<'static> {
    let mut row = RLookupRow::new();
    row.write_key(key, SharedValue::new_num(v));
    row
}

fn value_row(key: &RLookupKey<'_>, v: SharedValue) -> RLookupRow<'static> {
    let mut row = RLookupRow::new();
    row.write_key(key, v);
    row
}

fn string_value(s: &str) -> SharedValue {
    SharedValue::new_string(s.as_bytes().to_vec())
}

fn array_entries(value: &SharedValue) -> &[SharedValue] {
    let Value::Array(array) = &**value else {
        panic!("expected array, got {value:?}");
    };
    array
}

mod count {
    use reducers::count::CountReducer;

    #[test]
    fn counts_rows() {
        let r = CountReducer::new();
        let ctx = r.alloc_instance();
        assert_eq!(ctx.finalize().as_num(), Some(0.0));
        for _ in 0..3 {
            ctx.add();
        }
        assert_eq!(ctx.finalize().as_num(), Some(3.0));
    }
}

mod minmax {
    use super::*;
    use reducers::minmax::MinMaxReducer;

    #[test]
    fn empty_group_is_infinity() {
        let key = make_key(c"v", 0);
        let min = MinMaxReducer::new(&key, false);
        assert_eq!(
            min.alloc_instance().finalize().as_num(),
            Some(f64::INFINITY)
        );
        let max = MinMaxReducer::new(&key, true);
        assert_eq!(
            max.alloc_instance().finalize().as_num(),
            Some(f64::NEG_INFINITY)
        );
    }

    #[test]
    fn folds_min_and_max() {
        let key = make_key(c"v", 0);
        for (is_max, expected) in [(false, -2.0), (true, 7.5)] {
            let r = MinMaxReducer::new(&key, is_max);
            let ctx = r.alloc_instance();
            for v in [3.0, -2.0, 7.5] {
                ctx.add(&r, &num_row(&key, v));
            }
            assert_eq!(ctx.finalize().as_num(), Some(expected));
        }
    }

    #[test]
    fn non_numeric_values_are_skipped() {
        let key = make_key(c"v", 0);
        let r = MinMaxReducer::new(&key, false);
        let ctx = r.alloc_instance();
        ctx.add(&r, &value_row(&key, string_value("not a number")));
        ctx.add(&r, &num_row(&key, 4.0));
        assert_eq!(ctx.finalize().as_num(), Some(4.0));
    }
}

mod stddev {
    use super::*;
    use reducers::stddev::StdDevReducer;

    #[test]
    fn sample_stddev_of_known_values() {
        let key = make_key(c"v", 0);
        let r = StdDevReducer::new(&key);
        let ctx = r.alloc_instance();
        for v in [1.0, 2.0, 3.0] {
            ctx.add(&r, &num_row(&key, v));
        }
        // mean = 2, sample variance = 1
        assert!((ctx.finalize().as_num().unwrap() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn fewer_than_two_values_yield_zero() {
        let key = make_key(c"v", 0);
        let r = StdDevReducer::new(&key);
        let empty = r.alloc_instance();
        assert_eq!(empty.finalize().as_num(), Some(0.0));
        let single = r.alloc_instance();
        single.add(&r, &num_row(&key, 42.0));
        assert_eq!(single.finalize().as_num(), Some(0.0));
    }

    #[test]
    fn array_values_are_expanded_elementwise() {
        let key = make_key(c"v", 0);
        let r = StdDevReducer::new(&key);
        let as_rows = r.alloc_instance();
        for v in [1.0, 2.0, 3.0] {
            as_rows.add(&r, &num_row(&key, v));
        }
        let as_array = r.alloc_instance();
        let arr = SharedValue::new_array([1.0, 2.0, 3.0].map(SharedValue::new_num));
        as_array.add(&r, &value_row(&key, arr));
        assert_eq!(as_rows.finalize().as_num(), as_array.finalize().as_num());
    }
}

mod to_list {
    use super::*;
    use reducers::to_list::ToListReducer;

    #[test]
    fn collects_distinct_values_in_insertion_order() {
        let key = make_key(c"v", 0);
        let r = ToListReducer::new(&key);
        let ctx = r.alloc_instance();
        for s in ["a", "b", "a", "c", "b"] {
            ctx.add(&r, &value_row(&key, string_value(s)));
        }
        ctx.add(&r, &RLookupRow::new()); // missing key: skipped

        let out = ctx.finalize();
        let items = array_entries(&out);
        let strs: Vec<_> = items.iter().map(|v| v.as_str_bytes().unwrap()).collect();
        assert_eq!(strs, [b"a", b"b", b"c"]);
    }

    #[test]
    fn array_values_contribute_distinct_elements() {
        let key = make_key(c"v", 0);
        let r = ToListReducer::new(&key);
        let ctx = r.alloc_instance();
        let arr = SharedValue::new_array(["x", "y", "x"].map(string_value));
        ctx.add(&r, &value_row(&key, arr));

        let out = ctx.finalize();
        assert_eq!(array_entries(&out).len(), 2);
    }

    #[test]
    fn numbers_and_equal_numeric_values_dedup() {
        let key = make_key(c"v", 0);
        let r = ToListReducer::new(&key);
        let ctx = r.alloc_instance();
        ctx.add(&r, &num_row(&key, 1.0));
        ctx.add(&r, &num_row(&key, 1.0));
        ctx.add(&r, &num_row(&key, 2.0));
        let out = ctx.finalize();
        assert_eq!(array_entries(&out).len(), 2);
    }
}

mod first_value {
    use super::*;
    use reducers::first_value::FirstValueReducer;

    #[test]
    fn no_by_first_row_wins_even_as_null() {
        let key = make_key(c"v", 0);
        let r = FirstValueReducer::new(&key, None, true);
        let ctx = r.alloc_instance();
        ctx.add(&r, &RLookupRow::new()); // missing key locks in null
        ctx.add(&r, &value_row(&key, string_value("late")));
        assert!(matches!(&*ctx.finalize(), Value::Null));
    }

    #[test]
    fn by_ascending_and_descending() {
        let ret = make_key(c"name", 0);
        let sort = make_key(c"rank", 1);
        for (ascending, expected) in [(true, "low"), (false, "high")] {
            let r = FirstValueReducer::new(&ret, Some(&sort), ascending);
            let ctx = r.alloc_instance();
            for (name, rank) in [("mid", 5.0), ("high", 9.0), ("low", 1.0)] {
                let mut row = RLookupRow::new();
                row.write_key(&ret, string_value(name));
                row.write_key(&sort, SharedValue::new_num(rank));
                ctx.add(&r, &row);
            }
            assert_eq!(
                ctx.finalize().as_str_bytes(),
                Some(expected.as_bytes()),
                "ascending={ascending}"
            );
        }
    }

    #[test]
    fn null_best_sortval_is_replaced_without_replacing_value() {
        // C parity quirk: when the stored sort value is null, a later
        // non-null sort value replaces only the sort value, keeping the
        // first row's returned value.
        let ret = make_key(c"name", 0);
        let sort = make_key(c"rank", 1);
        let r = FirstValueReducer::new(&ret, Some(&sort), true);
        let ctx = r.alloc_instance();

        let mut first = RLookupRow::new();
        first.write_key(&ret, string_value("first"));
        ctx.add(&r, &first); // sort value missing → null

        let mut second = RLookupRow::new();
        second.write_key(&ret, string_value("second"));
        second.write_key(&sort, SharedValue::new_num(1.0));
        ctx.add(&r, &second);

        assert_eq!(ctx.finalize().as_str_bytes(), Some(b"first".as_slice()));
    }

    #[test]
    fn empty_group_is_null() {
        let key = make_key(c"v", 0);
        let r = FirstValueReducer::new(&key, None, true);
        assert!(matches!(&*r.alloc_instance().finalize(), Value::Null));
    }
}

mod random_sample {
    use super::*;
    use reducers::random_sample::RandomSampleReducer;

    #[test]
    fn keeps_everything_under_capacity() {
        let key = make_key(c"v", 0);
        let r = RandomSampleReducer::new(&key, 10);
        let ctx = r.alloc_instance();
        for v in [1.0, 2.0, 3.0] {
            ctx.add(&r, &num_row(&key, v));
        }
        let out = ctx.finalize();
        let nums: Vec<_> = array_entries(&out).iter().map(|v| v.as_num()).collect();
        assert_eq!(nums, [Some(1.0), Some(2.0), Some(3.0)]);
    }

    #[test]
    fn reservoir_is_bounded_and_drawn_from_input() {
        let key = make_key(c"v", 0);
        let r = RandomSampleReducer::new(&key, 5);
        let ctx = r.alloc_instance();
        for v in 0..100 {
            ctx.add(&r, &num_row(&key, f64::from(v)));
        }
        let out = ctx.finalize();
        let items = array_entries(&out);
        assert_eq!(items.len(), 5);
        for v in items {
            let n = v.as_num().unwrap();
            assert!((0.0..100.0).contains(&n));
        }
    }

    #[test]
    fn zero_capacity_yields_empty_array() {
        let key = make_key(c"v", 0);
        let r = RandomSampleReducer::new(&key, 0);
        let ctx = r.alloc_instance();
        ctx.add(&r, &num_row(&key, 1.0));
        assert!(array_entries(&ctx.finalize()).is_empty());
    }
}

mod count_distinct {
    use super::*;
    use reducers::count_distinct::CountDistinctReducer;

    #[test]
    fn counts_distinct_values() {
        let key = make_key(c"v", 0);
        let r = CountDistinctReducer::new(&key);
        let ctx = r.alloc_instance();
        for s in ["a", "b", "a", "c", "b", "a"] {
            ctx.add(&r, &value_row(&key, string_value(s)));
        }
        assert_eq!(ctx.finalize().as_num(), Some(3.0));
    }

    #[test]
    fn nulls_and_missing_are_skipped() {
        let key = make_key(c"v", 0);
        let r = CountDistinctReducer::new(&key);
        let ctx = r.alloc_instance();
        ctx.add(&r, &value_row(&key, SharedValue::null_static()));
        ctx.add(&r, &RLookupRow::new());
        assert_eq!(ctx.finalize().as_num(), Some(0.0));
        ctx.add(&r, &num_row(&key, 1.0));
        assert_eq!(ctx.finalize().as_num(), Some(1.0));
    }
}
