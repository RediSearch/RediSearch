/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![expect(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

use std::cmp::Ordering;
use value::comparison::{CompareError, compare};
use value::{Array, Map, RsString, RsValue, RsValueTrio, SharedRsValue};

use redis_mock::mock_or_stub_missing_redis_c_symbols;

mock_or_stub_missing_redis_c_symbols!();

#[unsafe(no_mangle)]
pub static mut RSDummyContext: *mut redis_mock::ffi::RedisModuleCtx =
    redis_mock::globals::redis_module_ctx();

fn array(values: impl IntoIterator<Item = RsValue>) -> RsValue {
    RsValue::Array(Array::new(
        values.into_iter().map(SharedRsValue::new).collect(),
    ))
}

fn trio(left: RsValue, middle: RsValue, right: RsValue) -> RsValue {
    RsValue::Trio(RsValueTrio::new(
        SharedRsValue::new(left),
        SharedRsValue::new(middle),
        SharedRsValue::new(right),
    ))
}

#[test]
fn null_equals_null() {
    let result = compare(&RsValue::Null, &RsValue::Null, false).unwrap();
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn null_is_less_than_number() {
    let result = compare(&RsValue::Null, &RsValue::Number(1.0), false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn number_is_greater_than_null() {
    let result = compare(&RsValue::Number(1.0), &RsValue::Null, false).unwrap();
    assert_eq!(result, Ordering::Greater);
}

#[test]
fn number_less_than() {
    let result = compare(&RsValue::Number(1.0), &RsValue::Number(2.0), false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn number_equal() {
    let result = compare(&RsValue::Number(42.0), &RsValue::Number(42.0), false).unwrap();
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn number_greater_than() {
    let result = compare(&RsValue::Number(3.0), &RsValue::Number(2.0), false).unwrap();
    assert_eq!(result, Ordering::Greater);
}

#[test]
fn number_nan_returns_error() {
    let result = compare(&RsValue::Number(f64::NAN), &RsValue::Number(1.0), false);
    assert!(matches!(result, Err(CompareError::NaNFloat)));
}

#[test]
fn string_equal() {
    let s1 = RsValue::String(RsString::from_vec(b"abc".to_vec()));
    let s2 = RsValue::String(RsString::from_vec(b"abc".to_vec()));
    let result = compare(&s1, &s2, false).unwrap();
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn string_less_than() {
    let s1 = RsValue::String(RsString::from_vec(b"abc".to_vec()));
    let s2 = RsValue::String(RsString::from_vec(b"abe".to_vec()));
    let result = compare(&s1, &s2, false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn number_vs_parseable_string() {
    let n = RsValue::Number(10.0);
    let s = RsValue::String(RsString::from_vec(b"20".to_vec()));
    let result = compare(&n, &s, false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn string_vs_number_reversed() {
    // string("20") vs num(10.0) should be Greater (reversed from number perspective).
    let s = RsValue::String(RsString::from_vec(b"20".to_vec()));
    let n = RsValue::Number(10.0);
    let result = compare(&s, &n, false).unwrap();
    assert_eq!(result, Ordering::Greater);
}

#[test]
#[cfg_attr(miri, ignore = "miri does not support FFI functions")]
fn number_vs_unparseable_string_with_fallback() {
    let n = RsValue::Number(5.0);
    let s = RsValue::String(RsString::from_vec(b"hello".to_vec()));
    // num_to_str(5.0) = "5", byte-wise "5" < "hello"
    let result = compare(&n, &s, true).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn number_vs_unparseable_string_without_fallback() {
    let n = RsValue::Number(5.0);
    let s = RsValue::String(RsString::from_vec(b"hello".to_vec()));
    let result = compare(&n, &s, false);
    assert!(matches!(
        result,
        Err(CompareError::NoNumberToStringFallback)
    ));
}

#[test]
fn ref_ref_delegates_to_inner() {
    let v1 = RsValue::Ref(SharedRsValue::new(RsValue::Number(1.0)));
    let v2 = RsValue::Ref(SharedRsValue::new(RsValue::Number(2.0)));
    let result = compare(&v1, &v2, false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn ref_left_delegates_to_inner() {
    let v1 = RsValue::Ref(SharedRsValue::new(RsValue::Number(5.0)));
    let result = compare(&v1, &RsValue::Number(5.0), false).unwrap();
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn ref_right_delegates_to_inner() {
    let v2 = RsValue::Ref(SharedRsValue::new(RsValue::Number(5.0)));
    let result = compare(&RsValue::Number(5.0), &v2, false).unwrap();
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn trio_compares_by_left_element() {
    let t1 = trio(
        RsValue::Number(1.0),
        RsValue::Number(99.0),
        RsValue::Number(99.0),
    );
    let t2 = trio(
        RsValue::Number(2.0),
        RsValue::Number(0.0),
        RsValue::Number(0.0),
    );
    let result = compare(&t1, &t2, false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn array_equal() {
    let a1 = array([RsValue::Number(1.0), RsValue::Number(2.0)]);
    let a2 = array([RsValue::Number(1.0), RsValue::Number(2.0)]);
    let result = compare(&a1, &a2, false).unwrap();
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn array_differing_element() {
    let a1 = array([RsValue::Number(1.0), RsValue::Number(2.0)]);
    let a2 = array([RsValue::Number(1.0), RsValue::Number(1.0)]);
    let result = compare(&a1, &a2, false).unwrap();
    assert_eq!(result, Ordering::Greater);
}

#[test]
fn array_shorter_is_less_when_prefix_matches() {
    let a1 = array([RsValue::Number(1.0)]);
    let a2 = array([RsValue::Number(1.0), RsValue::Number(2.0)]);
    let result = compare(&a1, &a2, false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn array_longer_is_greater_when_prefix_matches() {
    let a1 = array([RsValue::Number(1.0), RsValue::Number(2.0)]);
    let a2 = array([RsValue::Number(1.0)]);
    let result = compare(&a1, &a2, false).unwrap();
    assert_eq!(result, Ordering::Greater);
}

#[test]
fn map_comparison_returns_error() {
    let m1 = RsValue::Map(Map::new(Box::new([])));
    let m2 = RsValue::Map(Map::new(Box::new([])));
    let result = compare(&m1, &m2, false);
    assert!(matches!(result, Err(CompareError::MapComparison)));
}

#[test]
fn incompatible_types_returns_error() {
    let s = RsValue::String(RsString::from_vec(b"hello".to_vec()));
    let a = array([RsValue::Number(1.0)]);
    let result = compare(&s, &a, false);
    assert!(matches!(result, Err(CompareError::IncompatibleTypes)));
}
