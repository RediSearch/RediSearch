/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Collecting a JSON object into a [`KeyedNumericBatch`], over the mock RedisJSON API.

use keyed_numeric_index::{BatchFromJsonError, KeyedNumericBatch, KeyedNumericIndex};
use redis_json_api::mock::with_json_api;
use redis_module::{RedisString, key::KeyFlags};
use serde_json::{Value, json};

use crate::helpers::{MAX_DEPTH_RANGE, all};

/// Collect `doc` (the whole document is the object) into a batch.
fn collect(doc: Value) -> Result<KeyedNumericBatch, BatchFromJsonError> {
    redis_mock::init_redis_module_mock();
    with_json_api(Some(doc), |api, ctx| {
        // SAFETY: `c"doc"` outlives the call, and the mock ignores the context.
        let name = unsafe { RedisString::from_raw_parts(None, c"doc".as_ptr(), 3) };
        // SAFETY: `ctx` is the mock context handed out by `with_json_api`.
        let obj = unsafe { api.open_key_with_flags(ctx, &name, KeyFlags::empty()) }
            .expect("the mock document is present");
        // SAFETY: `ctx` is the mock context handed out by `with_json_api`.
        unsafe { KeyedNumericBatch::from_json(obj, ctx) }
    })
}

/// Add the batch for `doc_id` 1 to a fresh index.
fn index_of(batch: &KeyedNumericBatch) -> KeyedNumericIndex {
    let mut index = KeyedNumericIndex::new(false);
    index.add_batch(1, batch, false, MAX_DEPTH_RANGE);
    index
}

#[test]
#[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
fn numbers_become_members() {
    let batch = collect(json!({"a": 1, "b": 2.5, "c": -3})).unwrap();

    assert_eq!(batch.len(), 3);
    let index = index_of(&batch);
    assert_eq!(all(&index, b"a"), vec![(1, 1.0)]);
    assert_eq!(all(&index, b"b"), vec![(1, 2.5)]);
    assert_eq!(all(&index, b"c"), vec![(1, -3.0)]);
}

#[test]
#[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
fn an_empty_object_is_an_empty_batch() {
    assert!(collect(json!({})).unwrap().is_empty());
}

#[test]
#[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
fn extreme_numbers_keep_their_f64_value() {
    let batch = collect(json!({
        "i64_max": i64::MAX,
        "i64_min": i64::MIN,
        "above_2_53": 9_007_199_254_740_993_i64,
        "neg_zero": -0.0,
        "u64_max": u64::MAX,
    }))
    .unwrap();

    let index = index_of(&batch);
    assert_eq!(all(&index, b"i64_max"), vec![(1, i64::MAX as f64)]);
    assert_eq!(all(&index, b"i64_min"), vec![(1, i64::MIN as f64)]);
    assert_eq!(
        all(&index, b"above_2_53"),
        vec![(1, 9_007_199_254_740_992.0)]
    );
    assert_eq!(all(&index, b"u64_max"), vec![(1, u64::MAX as f64)]);
    let neg_zero = all(&index, b"neg_zero");
    assert_eq!(neg_zero.len(), 1);
    assert_eq!(neg_zero[0].1, 0.0);
}

#[test]
#[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
fn an_int_that_get_int_rejects_is_read_as_a_double() {
    // The mock reports an integer beyond `i64` with the `Int` type, as providers may,
    // although `get_int` returns nothing for it.
    let batch = collect(json!({"above_i64": 9_223_372_036_854_775_808_u64})).unwrap();

    assert_eq!(batch.len(), 1);
    let index = index_of(&batch);
    assert_eq!(
        all(&index, b"above_i64"),
        vec![(1, 9_223_372_036_854_775_808.0)]
    );
}

#[test]
#[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
fn keys_may_be_empty_or_non_ascii() {
    let batch = collect(json!({"": 1, "ключ": 2})).unwrap();

    let index = index_of(&batch);
    assert_eq!(all(&index, b""), vec![(1, 1.0)]);
    assert_eq!(all(&index, "ключ".as_bytes()), vec![(1, 2.0)]);
}

#[test]
#[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
fn a_member_that_is_not_a_number_fails_the_object() {
    for bad in [
        json!(null),
        json!(true),
        json!("1"),
        json!([1]),
        json!({"nested": 1}),
    ] {
        let err = collect(json!({"good": 1, "bad": bad})).unwrap_err();
        assert_eq!(err, BatchFromJsonError::MemberNotNumber, "member {bad}");
    }
}

#[test]
#[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
fn a_value_that_is_not_an_object_is_rejected() {
    for value in [json!(1), json!([1]), json!("s"), json!(null)] {
        let err = collect(value.clone()).unwrap_err();
        assert_eq!(err, BatchFromJsonError::NotAnObject, "value {value}");
    }
}
