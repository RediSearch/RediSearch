/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

use redis_mock::mock_or_stub_missing_redis_c_symbols;
use value::{RsValue, SharedRsValue};

mock_or_stub_missing_redis_c_symbols!();

#[allow(non_upper_case_globals)]
#[unsafe(no_mangle)]
pub static mut RSDummyContext: *mut redis_mock::ffi::RedisModuleCtx =
    redis_mock::globals::redis_module_ctx();

#[test]
fn null_static_holds_null() {
    let v = SharedRsValue::null_static();
    assert!(matches!(*v, RsValue::Null));
}

#[test]
fn new_number() {
    let v = SharedRsValue::new(RsValue::Number(42.0));
    assert!(matches!(*v, RsValue::Number(42.0)));
}

#[test]
fn new_undefined() {
    let v = SharedRsValue::new(RsValue::Undefined);
    assert!(matches!(*v, RsValue::Undefined));
}

#[test]
fn refcount_starts_at_one() {
    let v = SharedRsValue::new(RsValue::Number(1.0));
    assert_eq!(SharedRsValue::refcount(&v), 1);
}

#[test]
fn clone_increments_refcount() {
    let v = SharedRsValue::new(RsValue::Number(1.0));
    let v2 = v.clone();
    assert_eq!(SharedRsValue::refcount(&v), 2);
    assert_eq!(SharedRsValue::refcount(&v2), 2);
}

#[test]
fn drop_decrements_refcount() {
    let v = SharedRsValue::new(RsValue::Number(1.0));
    let v2 = v.clone();
    assert_eq!(SharedRsValue::refcount(&v), 2);
    drop(v2);
    assert_eq!(SharedRsValue::refcount(&v), 1);
}

#[test]
fn refcount_of_static_is_one() {
    let v = SharedRsValue::null_static();
    assert_eq!(SharedRsValue::refcount(&v), 1);
}

#[test]
fn clone_static_does_not_allocate() {
    let v = SharedRsValue::null_static();
    let v2 = v.clone();
    // Both should point to the same static sentinel.
    assert!(SharedRsValue::ptr_eq(&v, &v2));
    // Refcount remains 1 because static values are not reference-counted.
    assert_eq!(SharedRsValue::refcount(&v), 1);
}

#[test]
fn into_raw_from_raw_round_trip() {
    let v = SharedRsValue::new(RsValue::Number(7.0));
    let ptr = v.into_raw();
    let v2 = unsafe { SharedRsValue::from_raw(ptr) };
    assert!(matches!(*v2, RsValue::Number(7.0)));
    assert_eq!(SharedRsValue::refcount(&v2), 1);
}

#[test]
fn into_raw_does_not_drop() {
    let v = SharedRsValue::new(RsValue::Number(1.0));
    let v2 = v.clone();
    let ptr = v.into_raw();
    // The clone still has refcount 2 because `into_raw` did not decrement.
    assert_eq!(SharedRsValue::refcount(&v2), 2);
    // Reconstruct and drop to clean up.
    drop(unsafe { SharedRsValue::from_raw(ptr) });
    assert_eq!(SharedRsValue::refcount(&v2), 1);
}

#[test]
fn as_ptr_matches_into_raw() {
    let v = SharedRsValue::new(RsValue::Null);
    let ptr = v.as_ptr();
    let raw = v.into_raw();
    assert_eq!(ptr, raw);
    // Clean up.
    drop(unsafe { SharedRsValue::from_raw(raw) });
}

#[test]
fn ptr_eq_same_allocation() {
    let v = SharedRsValue::new(RsValue::Number(1.0));
    let v2 = v.clone();
    assert!(SharedRsValue::ptr_eq(&v, &v2));
}

#[test]
fn ptr_eq_different_allocations() {
    let v1 = SharedRsValue::new(RsValue::Null);
    let v2 = SharedRsValue::new(RsValue::Null);
    assert!(!SharedRsValue::ptr_eq(&v1, &v2));
}

#[test]
fn ptr_eq_static_values() {
    let v1 = SharedRsValue::null_static();
    let v2 = SharedRsValue::null_static();
    assert!(SharedRsValue::ptr_eq(&v1, &v2));
}

#[test]
fn ptr_eq_static_vs_heap() {
    let v_static = SharedRsValue::null_static();
    let v_heap = SharedRsValue::new(RsValue::Null);
    assert!(!SharedRsValue::ptr_eq(&v_static, &v_heap));
}

#[test]
fn set_value_replaces_inner() {
    let mut v = SharedRsValue::new(RsValue::Number(1.0));
    v.set_value(RsValue::Number(2.0));
    assert!(matches!(*v, RsValue::Number(2.0)));
}

#[test]
fn set_value_changes_variant() {
    let mut v = SharedRsValue::new(RsValue::Null);
    v.set_value(RsValue::Number(99.0));
    assert!(matches!(*v, RsValue::Number(99.0)));
}

#[test]
#[should_panic(expected = "Cannot change the value of static NULL")]
fn set_value_panics_on_static() {
    let mut v = SharedRsValue::null_static();
    v.set_value(RsValue::Number(1.0));
}

#[test]
#[should_panic(expected = "Failed to get mutable reference")]
fn set_value_panics_when_shared() {
    let mut v = SharedRsValue::new(RsValue::Number(1.0));
    let _clone = v.clone();
    v.set_value(RsValue::Number(2.0));
}
