/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use value::hash::{hash_stable, hash_value};
use value::{Array, Map, RedisString, SharedValue, String, Trio, Value};

fn hash(value: &Value) -> u64 {
    let mut hasher = DefaultHasher::new();
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
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_CreateString` is not supported by Miri"
)]
fn string_and_redis_string_with_same_bytes_hash_equal() {
    // `compare()` treats `String` and `RedisString` as equal when their bytes
    // match, so they must hash equally too - otherwise GROUPBY/COUNT_DISTINCT
    // would split a single logical value across two hash buckets.
    redis_mock::init_redis_module_mock();

    let bytes: &'static [u8] = b"hello";
    let create_string = unsafe { redis_module::raw::RedisModule_CreateString }
        .expect("mock registers RedisModule_CreateString");
    let raw = unsafe { create_string(std::ptr::null_mut(), bytes.as_ptr().cast(), bytes.len()) };
    let redis_string = Value::RedisString(unsafe { RedisString::from_raw(raw.cast()) });

    let plain_string = Value::String(String::from_vec(bytes.to_vec()));

    assert_eq!(hash(&redis_string), hash(&plain_string));
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
fn undefined_and_null_are_deterministic() {
    assert_eq!(hash(&Value::Undefined), hash(&Value::Undefined));
    assert_eq!(hash(&Value::Null), hash(&Value::Null));
}

#[test]
fn undefined_and_null_do_not_collide() {
    // Undefined and Null carry no payload, but their discriminants keep them
    // distinct from each other and from Number(0.0)'s all-zero bytes.
    assert_ne!(hash(&Value::Undefined), hash(&Value::Null));
    assert_ne!(hash(&Value::Undefined), hash(&Value::Number(0.0)));
    assert_ne!(hash(&Value::Null), hash(&Value::Number(0.0)));
}

#[test]
fn undefined_and_null_do_not_reset_prior_state() {
    // Hashing Undefined/Null no longer wipes out whatever was hashed before
    // them - they just mix their own discriminant into the running state.
    let mut with_undefined = DefaultHasher::new();
    hash_value(&Value::Number(123.0), &mut with_undefined);
    hash_value(&Value::Undefined, &mut with_undefined);

    let mut without_undefined = DefaultHasher::new();
    hash_value(&Value::Number(123.0), &mut without_undefined);

    assert_ne!(with_undefined.finish(), hash(&Value::Undefined));
    assert_ne!(with_undefined.finish(), without_undefined.finish());

    let mut with_null = DefaultHasher::new();
    hash_value(&Value::Number(123.0), &mut with_null);
    hash_value(&Value::Null, &mut with_null);

    assert_ne!(with_null.finish(), hash(&Value::Null));
    assert_ne!(with_null.finish(), without_undefined.finish());
}

#[test]
fn string_sequences_dont_hash_as_concatenated_strings() {
    let arr = |a: &str, b: &str| {
        Value::Array(Array::new(Box::new([
            SharedValue::new(Value::String(String::from_vec(a.into()))),
            SharedValue::new(Value::String(String::from_vec(b.into()))),
        ])))
    };

    assert_ne!(hash(&arr("a", "bc")), hash(&arr("ab", "c")));
}

#[test]
fn ref_hashes_same_as_inner_value() {
    let inner = Value::Number(7.0);
    let wrapped = Value::Ref(SharedValue::new(Value::Number(7.0)));
    assert_eq!(hash(&inner), hash(&wrapped));
}

#[test]
fn array_hash_depends_on_element_order() {
    // Rebuilding the same sequence of elements reproduces the same hash, but
    // swapping the order of distinct elements changes it.
    let arr = |a: f64, b: f64| {
        Value::Array(Array::new(Box::new([
            SharedValue::new(Value::Number(a)),
            SharedValue::new(Value::Number(b)),
        ])))
    };

    assert_eq!(hash(&arr(1.0, 2.0)), hash(&arr(1.0, 2.0)));
    assert_ne!(hash(&arr(1.0, 2.0)), hash(&arr(2.0, 1.0)));
}

#[test]
fn map_hash_depends_on_keys_and_values() {
    // Rebuilding the same {key: val} entry reproduces the same hash, but
    // swapping key and value changes it.
    let map = |k: f64, v: f64| {
        Value::Map(Map::new(Box::new([(
            SharedValue::new(Value::Number(k)),
            SharedValue::new(Value::Number(v)),
        )])))
    };

    assert_eq!(hash(&map(3.0, 4.0)), hash(&map(3.0, 4.0)));
    assert_ne!(hash(&map(3.0, 4.0)), hash(&map(4.0, 3.0)));
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

#[test]
fn hash_stable_does_not_depend_on_the_per_process_seed() {
    // `hash_stable` must be reproducible from the value alone, with no
    // hidden per-process state - unlike `hash`, whose seed differs across
    // processes. Cross-checking against an independently-built
    // `DefaultHasher` (the algorithm `hash_stable` uses) confirms that no
    // per-process secret is mixed in.
    let value = Value::String(String::from_vec(b"hello".to_vec()));
    let hval = 0x5f61767a;

    let actual = hash_stable(&value, hval);

    let mut hasher = DefaultHasher::new();
    hasher.write_u64(hval);
    hash_value(&value, &mut hasher);
    let expected = hasher.finish();

    assert_eq!(actual, expected);
}
