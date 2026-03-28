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
use value::{Array, Map, SharedValue, String, Trio, Value};

fn array(values: impl IntoIterator<Item = Value>) -> Value {
    Value::Array(Array::new(
        values.into_iter().map(SharedValue::new).collect(),
    ))
}

fn trio(left: Value, middle: Value, right: Value) -> Value {
    Value::Trio(Trio::new(
        SharedValue::new(left),
        SharedValue::new(middle),
        SharedValue::new(right),
    ))
}

#[test]
fn null_equals_null() {
    let result = compare(&Value::Null, &Value::Null, false).unwrap();
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn null_is_less_than_number() {
    let result = compare(&Value::Null, &Value::Number(1.0), false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn number_is_greater_than_null() {
    let result = compare(&Value::Number(1.0), &Value::Null, false).unwrap();
    assert_eq!(result, Ordering::Greater);
}

#[test]
fn number_less_than() {
    let result = compare(&Value::Number(1.0), &Value::Number(2.0), false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn number_equal() {
    let result = compare(&Value::Number(42.0), &Value::Number(42.0), false).unwrap();
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn number_greater_than() {
    let result = compare(&Value::Number(3.0), &Value::Number(2.0), false).unwrap();
    assert_eq!(result, Ordering::Greater);
}

#[test]
fn number_nan_returns_error() {
    let result = compare(&Value::Number(f64::NAN), &Value::Number(1.0), false);
    assert!(matches!(result, Err(CompareError::NaNFloat)));
}

#[test]
fn string_equal() {
    let s1 = Value::String(String::from_vec(b"abc".to_vec()));
    let s2 = Value::String(String::from_vec(b"abc".to_vec()));
    let result = compare(&s1, &s2, false).unwrap();
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn string_less_than() {
    let s1 = Value::String(String::from_vec(b"abc".to_vec()));
    let s2 = Value::String(String::from_vec(b"abe".to_vec()));
    let result = compare(&s1, &s2, false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn number_vs_parseable_string() {
    let n = Value::Number(10.0);
    let s = Value::String(String::from_vec(b"20".to_vec()));
    let result = compare(&n, &s, false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn string_vs_number_reversed() {
    // string("20") vs num(10.0) should be Greater (reversed from number perspective).
    let s = Value::String(String::from_vec(b"20".to_vec()));
    let n = Value::Number(10.0);
    let result = compare(&s, &n, false).unwrap();
    assert_eq!(result, Ordering::Greater);
}

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `snprintf`")]
fn number_vs_unparseable_string_with_fallback() {
    let n = Value::Number(5.0);
    let s = Value::String(String::from_vec(b"hello".to_vec()));
    // num_to_str(5.0) = "5", byte-wise "5" < "hello"
    let result = compare(&n, &s, true).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn number_vs_unparseable_string_without_fallback() {
    let n = Value::Number(5.0);
    let s = Value::String(String::from_vec(b"hello".to_vec()));
    let result = compare(&n, &s, false);
    assert!(matches!(
        result,
        Err(CompareError::NoNumberToStringFallback)
    ));
}

#[test]
fn ref_ref_delegates_to_inner() {
    let v1 = Value::Ref(SharedValue::new(Value::Number(1.0)));
    let v2 = Value::Ref(SharedValue::new(Value::Number(2.0)));
    let result = compare(&v1, &v2, false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn ref_left_delegates_to_inner() {
    let v1 = Value::Ref(SharedValue::new(Value::Number(5.0)));
    let result = compare(&v1, &Value::Number(5.0), false).unwrap();
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn ref_right_delegates_to_inner() {
    let v2 = Value::Ref(SharedValue::new(Value::Number(5.0)));
    let result = compare(&Value::Number(5.0), &v2, false).unwrap();
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn trio_compares_by_left_element() {
    let t1 = trio(Value::Number(1.0), Value::Number(99.0), Value::Number(99.0));
    let t2 = trio(Value::Number(2.0), Value::Number(0.0), Value::Number(0.0));
    let result = compare(&t1, &t2, false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn array_equal() {
    let a1 = array([Value::Number(1.0), Value::Number(2.0)]);
    let a2 = array([Value::Number(1.0), Value::Number(2.0)]);
    let result = compare(&a1, &a2, false).unwrap();
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn array_differing_element() {
    let a1 = array([Value::Number(1.0), Value::Number(2.0)]);
    let a2 = array([Value::Number(1.0), Value::Number(1.0)]);
    let result = compare(&a1, &a2, false).unwrap();
    assert_eq!(result, Ordering::Greater);
}

#[test]
fn array_shorter_is_less_when_prefix_matches() {
    let a1 = array([Value::Number(1.0)]);
    let a2 = array([Value::Number(1.0), Value::Number(2.0)]);
    let result = compare(&a1, &a2, false).unwrap();
    assert_eq!(result, Ordering::Less);
}

#[test]
fn array_longer_is_greater_when_prefix_matches() {
    let a1 = array([Value::Number(1.0), Value::Number(2.0)]);
    let a2 = array([Value::Number(1.0)]);
    let result = compare(&a1, &a2, false).unwrap();
    assert_eq!(result, Ordering::Greater);
}

#[test]
fn string_empty_vs_array() {
    let s1 = Value::String(String::from_vec(b"".to_vec()));
    let a = Value::Array(Array::new(Box::new([])));
    let result = compare(&s1, &a, false);
    assert_eq!(
        result,
        Err(CompareError::IncompatibleAgainstString(Ordering::Equal))
    );
}

#[test]
fn string_filled_vs_trio() {
    let s1 = Value::String(String::from_vec(b"foo".to_vec()));
    let t = trio(Value::Null, Value::Null, Value::Null);
    let result = compare(&s1, &t, false);
    assert_eq!(
        result,
        Err(CompareError::IncompatibleAgainstString(Ordering::Greater))
    );
}

#[test]
fn map_vs_string_empty() {
    let m = Value::Map(Map::new(Box::new([])));
    let s2 = Value::String(String::from_vec(b"".to_vec()));
    let result = compare(&m, &s2, false);
    assert_eq!(
        result,
        Err(CompareError::IncompatibleAgainstString(Ordering::Equal))
    );
}

#[test]
fn undefined_vs_string_filled() {
    let a = Value::Array(Array::new(Box::new([])));
    let s2 = Value::String(String::from_vec(b"foo".to_vec()));
    let result = compare(&a, &s2, false);
    assert_eq!(
        result,
        Err(CompareError::IncompatibleAgainstString(Ordering::Less))
    );
}

#[test]
fn map_comparison_returns_error() {
    let m1 = Value::Map(Map::new(Box::new([])));
    let m2 = Value::Map(Map::new(Box::new([])));
    let result = compare(&m1, &m2, false);
    assert!(matches!(result, Err(CompareError::MapComparison)));
}

#[test]
fn incompatible_types_returns_error() {
    let t = trio(Value::Null, Value::Null, Value::Null);
    let a = array([Value::Number(1.0)]);
    let result = compare(&t, &a, false);
    assert!(matches!(result, Err(CompareError::IncompatibleTypes)));
}
