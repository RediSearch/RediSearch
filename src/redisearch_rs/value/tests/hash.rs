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
use value::{Array, Map, RsString, RsValue, RsValueTrio, SharedRsValue};

redis_mock::mock_or_stub_missing_redis_c_symbols!();

#[allow(non_upper_case_globals)]
#[unsafe(no_mangle)]
pub static mut RSDummyContext: *mut redis_mock::ffi::RedisModuleCtx =
    redis_mock::globals::redis_module_ctx();

fn hash(value: &RsValue) -> u64 {
    let mut hasher = Fnv64::default();
    hash_value(value, &mut hasher);
    hasher.finish()
}

#[test]
fn different_strings_produce_different_hashes() {
    let a = RsValue::String(RsString::from_vec(b"hello".to_vec()));
    let b = RsValue::String(RsString::from_vec(b"world".to_vec()));
    assert_ne!(hash(&a), hash(&b));
}

#[test]
fn same_strings_produce_same_hash() {
    let a = RsValue::String(RsString::from_vec(b"hello".to_vec()));
    let b = RsValue::String(RsString::from_vec(b"hello".to_vec()));
    assert_eq!(hash(&a), hash(&b));
}

#[test]
fn different_numbers_produce_different_hashes() {
    let a = RsValue::Number(1.0);
    let b = RsValue::Number(2.0);
    assert_ne!(hash(&a), hash(&b));
}

#[test]
fn same_numbers_produce_same_hash() {
    let a = RsValue::Number(42.0);
    let b = RsValue::Number(42.0);
    assert_eq!(hash(&a), hash(&b));
}

#[test]
fn undefined_resets_hasher_to_zero_basis() {
    // Hashing Undefined should reset the hasher state to offset basis 0,
    // regardless of prior state.
    let mut hasher = Fnv64::default();
    hash_value(&RsValue::Number(123.0), &mut hasher);
    hash_value(&RsValue::Undefined, &mut hasher);

    let fresh = Fnv64::with_offset_basis(0);
    // Finish on a fresh hasher with basis 0 should match.
    assert_eq!(hasher.finish(), fresh.finish());
}

#[test]
fn null_advances_hasher_state() {
    // Hashing Null should set offset basis to current hash + 1.
    let mut hasher = Fnv64::default();
    let before = hasher.finish();
    hash_value(&RsValue::Null, &mut hasher);
    let actual = hasher.finish();

    let expected = Fnv64::with_offset_basis(before + 1).finish();
    assert_eq!(actual, expected);
}

#[test]
fn ref_hashes_same_as_inner_value() {
    let inner = RsValue::Number(7.0);
    let wrapped = RsValue::Ref(SharedRsValue::new(RsValue::Number(7.0)));
    assert_eq!(hash(&inner), hash(&wrapped));
}

#[test]
fn array_hashes_elements_sequentially() {
    // An array of [x, y] should give the same result as hashing x and y sequentially.
    let arr = RsValue::Array(Array::new(Box::new([
        SharedRsValue::new(RsValue::Number(1.0)),
        SharedRsValue::new(RsValue::Number(2.0)),
    ])));

    let mut expected_hasher = Fnv64::default();
    hash_value(&RsValue::Number(1.0), &mut expected_hasher);
    hash_value(&RsValue::Number(2.0), &mut expected_hasher);

    assert_eq!(hash(&arr), expected_hasher.finish());
}

#[test]
fn map_hashes_keys_and_values() {
    // A map with one entry {key: val} should give the same result as hashing key and value sequentially.
    let map = RsValue::Map(Map::new(Box::new([(
        SharedRsValue::new(RsValue::Number(3.0)),
        SharedRsValue::new(RsValue::Number(4.0)),
    )])));

    let mut expected_hasher = Fnv64::default();
    hash_value(&RsValue::Number(3.0), &mut expected_hasher);
    hash_value(&RsValue::Number(4.0), &mut expected_hasher);

    assert_eq!(hash(&map), expected_hasher.finish());
}

#[test]
fn trio_hashes_only_left_value() {
    // Trio should only hash the left value, ignoring middle and right.
    let trio = RsValue::Trio(RsValueTrio::new(
        SharedRsValue::new(RsValue::Number(5.0)),
        SharedRsValue::new(RsValue::Number(6.0)),
        SharedRsValue::new(RsValue::Number(7.0)),
    ));
    assert_eq!(hash(&trio), hash(&RsValue::Number(5.0)));
}

#[test]
fn trio_ignores_middle_and_right() {
    // Two trios with same left but different middle/right should hash the same.
    let a = RsValue::Trio(RsValueTrio::new(
        SharedRsValue::new(RsValue::Number(1.0)),
        SharedRsValue::new(RsValue::Number(2.0)),
        SharedRsValue::new(RsValue::Number(3.0)),
    ));
    let b = RsValue::Trio(RsValueTrio::new(
        SharedRsValue::new(RsValue::Number(1.0)),
        SharedRsValue::new(RsValue::Number(99.0)),
        SharedRsValue::new(RsValue::Number(100.0)),
    ));
    assert_eq!(hash(&a), hash(&b));
}
