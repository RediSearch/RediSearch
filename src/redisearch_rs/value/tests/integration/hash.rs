/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fnv::Fnv64;
use std::hash::Hasher;
use value::hash::hash_value;
use value::{Array, Map, SharedValue, String, Trio, Value};

fn hash(value: &Value) -> u64 {
    let mut hasher = Fnv64::default();
    hash_value(value, &mut hasher);
    hasher.finish()
}

#[test]
fn different_strings_produce_different_hashes() {
    let a = Value::String(String::from_vec(b"hello".to_vec()));
    let b = Value::String(String::from_vec(b"world".to_vec()));
    assert_ne!(hash(&a), hash(&b));
}

#[test]
fn same_strings_produce_same_hash() {
    let a = Value::String(String::from_vec(b"hello".to_vec()));
    let b = Value::String(String::from_vec(b"hello".to_vec()));
    assert_eq!(hash(&a), hash(&b));
}

#[test]
fn different_numbers_produce_different_hashes() {
    let a = Value::Number(1.0);
    let b = Value::Number(2.0);
    assert_ne!(hash(&a), hash(&b));
}

#[test]
fn same_numbers_produce_same_hash() {
    let a = Value::Number(42.0);
    let b = Value::Number(42.0);
    assert_eq!(hash(&a), hash(&b));
}

#[test]
fn undefined_resets_hasher_to_zero_basis() {
    // Hashing Undefined should reset the hasher state to offset basis 0,
    // regardless of prior state.
    let mut hasher = Fnv64::default();
    hash_value(&Value::Number(123.0), &mut hasher);
    hash_value(&Value::Undefined, &mut hasher);

    let fresh = Fnv64::with_offset_basis(0);
    // Finish on a fresh hasher with basis 0 should match.
    assert_eq!(hasher.finish(), fresh.finish());
}

#[test]
fn null_advances_hasher_state() {
    // Hashing Null should set offset basis to current hash + 1.
    let mut hasher = Fnv64::default();
    let before = hasher.finish();
    hash_value(&Value::Null, &mut hasher);
    let actual = hasher.finish();

    let expected = Fnv64::with_offset_basis(before + 1).finish();
    assert_eq!(actual, expected);
}

#[test]
fn ref_hashes_same_as_inner_value() {
    let inner = Value::Number(7.0);
    let wrapped = Value::Ref(SharedValue::new(Value::Number(7.0)));
    assert_eq!(hash(&inner), hash(&wrapped));
}

#[test]
fn array_hashes_elements_sequentially() {
    // An array of [x, y] should give the same result as hashing x and y sequentially.
    let arr = Value::Array(Array::new(Box::new([
        SharedValue::new(Value::Number(1.0)),
        SharedValue::new(Value::Number(2.0)),
    ])));

    let mut expected_hasher = Fnv64::default();
    hash_value(&Value::Number(1.0), &mut expected_hasher);
    hash_value(&Value::Number(2.0), &mut expected_hasher);

    assert_eq!(hash(&arr), expected_hasher.finish());
}

#[test]
fn map_hashes_keys_and_values() {
    // A map with one entry {key: val} should give the same result as hashing key and value sequentially.
    let map = Value::Map(Map::new(Box::new([(
        SharedValue::new(Value::Number(3.0)),
        SharedValue::new(Value::Number(4.0)),
    )])));

    let mut expected_hasher = Fnv64::default();
    hash_value(&Value::Number(3.0), &mut expected_hasher);
    hash_value(&Value::Number(4.0), &mut expected_hasher);

    assert_eq!(hash(&map), expected_hasher.finish());
}

#[test]
fn trio_hashes_only_left_value() {
    // Trio should only hash the left value, ignoring middle and right.
    let trio = Value::Trio(Trio::new(
        SharedValue::new(Value::Number(5.0)),
        SharedValue::new(Value::Number(6.0)),
        SharedValue::new(Value::Number(7.0)),
    ));
    assert_eq!(hash(&trio), hash(&Value::Number(5.0)));
}

#[test]
fn trio_ignores_middle_and_right() {
    // Two trios with same left but different middle/right should hash the same.
    let a = Value::Trio(Trio::new(
        SharedValue::new(Value::Number(1.0)),
        SharedValue::new(Value::Number(2.0)),
        SharedValue::new(Value::Number(3.0)),
    ));
    let b = Value::Trio(Trio::new(
        SharedValue::new(Value::Number(1.0)),
        SharedValue::new(Value::Number(99.0)),
        SharedValue::new(Value::Number(100.0)),
    ));
    assert_eq!(hash(&a), hash(&b));
}
