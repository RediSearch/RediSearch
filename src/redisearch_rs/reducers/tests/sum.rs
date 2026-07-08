/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the SUM / AVG reducer that drive [`SumReducer`] through
//! `add` → `finalize`.

extern crate redisearch_rs;

use reducers::sum::SumReducer;
use rlookup::{RLookupKey, RLookupKeyFlags, RLookupRow};
use value::SharedValue;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn make_key() -> RLookupKey<'static> {
    RLookupKey::new(c"v", RLookupKeyFlags::empty())
}

fn num_row(key: &RLookupKey<'_>, v: f64) -> RLookupRow<'static> {
    let mut row = RLookupRow::new();
    row.write_key(key, SharedValue::new_num(v));
    row
}

fn finalized_num(r: &SumReducer<'_>, ctx: &reducers::sum::SumCtx) -> f64 {
    ctx.finalize(r)
        .as_num()
        .expect("SUM/AVG must finalize to a number")
}

#[test]
fn empty_group_is_nan() {
    let key = make_key();
    for is_avg in [false, true] {
        let r = SumReducer::new(&key, is_avg);
        let ctx = r.alloc_instance();
        assert!(finalized_num(&r, ctx).is_nan());
    }
}

#[test]
fn sum_totals_numbers() {
    let key = make_key();
    let r = SumReducer::new(&key, false);
    let ctx = r.alloc_instance();
    for v in [1.0, 2.0, 3.5] {
        ctx.add(&r, &num_row(&key, v));
    }
    assert_eq!(finalized_num(&r, ctx), 6.5);
}

#[test]
fn avg_divides_by_count() {
    let key = make_key();
    let r = SumReducer::new(&key, true);
    let ctx = r.alloc_instance();
    for v in [1.0, 2.0, 6.0] {
        ctx.add(&r, &num_row(&key, v));
    }
    assert_eq!(finalized_num(&r, ctx), 3.0);
}

#[test]
fn numeric_strings_are_coerced() {
    let key = make_key();
    let r = SumReducer::new(&key, false);
    let ctx = r.alloc_instance();
    let mut row = RLookupRow::new();
    row.write_key(&key, SharedValue::new_string(b"2.5".to_vec()));
    ctx.add(&r, &row);
    assert_eq!(finalized_num(&r, ctx), 2.5);
}

#[test]
fn non_numeric_and_missing_values_are_skipped() {
    let key = make_key();
    let r = SumReducer::new(&key, true);
    let ctx = r.alloc_instance();

    ctx.add(&r, &num_row(&key, 4.0));

    // Non-numeric string: skipped, must not count towards the AVG divisor.
    let mut row = RLookupRow::new();
    row.write_key(&key, SharedValue::new_string(b"not a number".to_vec()));
    ctx.add(&r, &row);

    // Row without the key: skipped as well.
    ctx.add(&r, &RLookupRow::new());

    assert_eq!(finalized_num(&r, ctx), 4.0);
}
