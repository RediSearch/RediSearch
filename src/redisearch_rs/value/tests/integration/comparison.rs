/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![expect(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

use query_error::QueryError;
use std::cmp::Ordering;
use value::comparison::{CompareError, cmp_fields, compare};
use value::{Array, Map, RsString, RsValue, RsValueTrio, SharedRsValue};

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
#[cfg_attr(miri, ignore = "Calls FFI function `snprintf`")]
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
fn string_empty_vs_array() {
    let s1 = RsValue::String(RsString::from_vec(b"".to_vec()));
    let a = RsValue::Array(Array::new(Box::new([])));
    let result = compare(&s1, &a, false);
    assert_eq!(
        result,
        Err(CompareError::IncompatibleAgainstString(Ordering::Equal))
    );
}

#[test]
fn string_filled_vs_trio() {
    let s1 = RsValue::String(RsString::from_vec(b"foo".to_vec()));
    let t = trio(RsValue::Null, RsValue::Null, RsValue::Null);
    let result = compare(&s1, &t, false);
    assert_eq!(
        result,
        Err(CompareError::IncompatibleAgainstString(Ordering::Greater))
    );
}

#[test]
fn map_vs_string_empty() {
    let m = RsValue::Map(Map::new(Box::new([])));
    let s2 = RsValue::String(RsString::from_vec(b"".to_vec()));
    let result = compare(&m, &s2, false);
    assert_eq!(
        result,
        Err(CompareError::IncompatibleAgainstString(Ordering::Equal))
    );
}

#[test]
fn undefined_vs_string_filled() {
    let a = RsValue::Array(Array::new(Box::new([])));
    let s2 = RsValue::String(RsString::from_vec(b"foo".to_vec()));
    let result = compare(&a, &s2, false);
    assert_eq!(
        result,
        Err(CompareError::IncompatibleAgainstString(Ordering::Less))
    );
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
    let t = trio(RsValue::Null, RsValue::Null, RsValue::Null);
    let a = array([RsValue::Number(1.0)]);
    let result = compare(&t, &a, false);
    assert!(matches!(result, Err(CompareError::IncompatibleTypes)));
}

#[test]
fn cmp_fields_both_empty_is_equal() {
    let pairs: Vec<(Option<&RsValue>, Option<&RsValue>)> = vec![];
    let ord = cmp_fields(pairs.into_iter(), 0, None);
    assert_eq!(ord, Ordering::Equal);
}

#[test]
fn cmp_fields_all_equal_returns_equal() {
    let a = RsValue::Number(1.0);
    let b = RsValue::Number(1.0);
    let pairs = vec![(Some(&a), Some(&b))];
    let ord = cmp_fields(pairs.into_iter(), 0b1, None);
    assert_eq!(ord, Ordering::Equal);
}

#[test]
fn cmp_fields_ascending_bit_flips_order() {
    let a = RsValue::Number(1.0);
    let b = RsValue::Number(2.0);
    // Ascending bit 0 set => reverse the natural Less into Greater.
    let ord_asc = cmp_fields(vec![(Some(&a), Some(&b))].into_iter(), 0b1, None);
    let ord_desc = cmp_fields(vec![(Some(&a), Some(&b))].into_iter(), 0b0, None);
    assert_eq!(ord_asc, Ordering::Greater);
    assert_eq!(ord_desc, Ordering::Less);
}

#[test]
fn cmp_fields_some_vs_none_is_always_greater_regardless_of_asc() {
    let a = RsValue::Number(1.0);
    let ord_asc = cmp_fields(vec![(Some(&a), None)].into_iter(), 0b1, None);
    let ord_desc = cmp_fields(vec![(Some(&a), None)].into_iter(), 0b0, None);
    assert_eq!(ord_asc, Ordering::Greater);
    assert_eq!(ord_desc, Ordering::Greater);
}

#[test]
fn cmp_fields_none_vs_some_is_always_less_regardless_of_asc() {
    let b = RsValue::Number(1.0);
    let ord_asc = cmp_fields(vec![(None, Some(&b))].into_iter(), 0b1, None);
    let ord_desc = cmp_fields(vec![(None, Some(&b))].into_iter(), 0b0, None);
    assert_eq!(ord_asc, Ordering::Less);
    assert_eq!(ord_desc, Ordering::Less);
}

#[test]
fn cmp_fields_none_pair_falls_through_to_next_key() {
    let a = RsValue::Number(1.0);
    let b = RsValue::Number(2.0);
    // First key: both None => continue. Second key: a < b, ascending => Greater.
    let pairs = vec![(None, None), (Some(&a), Some(&b))];
    let ord = cmp_fields(pairs.into_iter(), 0b11, None);
    assert_eq!(ord, Ordering::Greater);
}

#[test]
fn cmp_fields_equal_pair_falls_through_to_next_key() {
    let a1 = RsValue::Number(1.0);
    let a2 = RsValue::Number(1.0);
    let b = RsValue::Number(1.0);
    let c = RsValue::Number(2.0);
    let pairs = vec![(Some(&a1), Some(&a2)), (Some(&b), Some(&c))];
    let ord = cmp_fields(pairs.into_iter(), 0b00, None);
    assert_eq!(ord, Ordering::Less);
}

#[test]
fn cmp_fields_with_qerr_records_num_to_string_error() {
    // A number vs a non-numeric string with qerr=Some => no fallback, error recorded,
    // key treated as equal; the next key decides.
    let n = RsValue::Number(1.0);
    let s = RsValue::String(RsString::from_vec(b"not-a-number".to_vec()));
    let tie_a = RsValue::Number(5.0);
    let tie_b = RsValue::Number(6.0);
    let mut qerr = QueryError::default();
    let pairs = vec![(Some(&n), Some(&s)), (Some(&tie_a), Some(&tie_b))];
    // Ascending for both keys (bits set) => natural Less(5 < 6) is reversed to Greater.
    let ord = cmp_fields(pairs.into_iter(), 0b11, Some(&mut qerr));
    assert_eq!(ord, Ordering::Greater);
    assert!(!qerr.is_ok());
}

#[test]
fn cmp_fields_without_qerr_uses_num_to_string_fallback() {
    // qerr=None => number is formatted to string and compared byte-wise.
    let n = RsValue::Number(1.0);
    let s = RsValue::String(RsString::from_vec(b"2".to_vec()));
    // "1" < "2" byte-wise, descending (bit 0 clear) => Less.
    let ord = cmp_fields(vec![(Some(&n), Some(&s))].into_iter(), 0b0, None);
    assert_eq!(ord, Ordering::Less);
}
