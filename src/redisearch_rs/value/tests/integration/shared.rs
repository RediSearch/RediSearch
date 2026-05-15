/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

use value::{SharedValue, Value};

#[test]
fn null_static_holds_null() {
    let v = SharedValue::null_static();
    assert!(matches!(*v, Value::Null));
}

#[test]
fn new_undefined() {
    let v = SharedValue::new(Value::Undefined);
    assert!(matches!(*v, Value::Undefined));
}

#[test]
fn new_number() {
    let v = SharedValue::new_num(42.0);
    assert!(matches!(*v, Value::Number(42.0)));
}

#[test]
fn new_string() {
    let v = SharedValue::new_string(b"Hello".to_vec());
    assert!(matches!(&*v, Value::String(s) if s.as_bytes() == b"Hello"));
}

#[test]
fn new_array() {
    let v = SharedValue::new_array([SharedValue::new_num(1.0), SharedValue::new_num(2.0)]);
    assert!(matches!(&*v, Value::Array(a) if a.len() == 2
        && matches!(*a[0], Value::Number(1.0))
        && matches!(*a[1], Value::Number(2.0))));
}

#[test]
fn new_map() {
    let v = SharedValue::new_map([(
        SharedValue::new_string(b"key".to_vec()),
        SharedValue::new_num(42.0),
    )]);
    assert!(matches!(&*v, Value::Map(m) if m.len() == 1
        && matches!(&*m[0].0, Value::String(s) if s.as_bytes() == b"key")
        && matches!(*m[0].1, Value::Number(42.0))));
}

#[test]
fn new_trio() {
    let v = SharedValue::new_trio(
        SharedValue::new_num(1.0),
        SharedValue::new_num(2.0),
        SharedValue::new_num(3.0),
    );
    assert!(matches!(&*v, Value::Trio(t)
        if matches!(**t.left(), Value::Number(1.0))
        && matches!(**t.middle(), Value::Number(2.0))
        && matches!(**t.right(), Value::Number(3.0))));
}

#[test]
fn refcount_starts_at_one() {
    let v = SharedValue::new(Value::Number(1.0));
    assert_eq!(SharedValue::refcount(&v), 1);
}

#[test]
fn clone_increments_refcount() {
    let v = SharedValue::new(Value::Number(1.0));
    let v2 = v.clone();
    assert_eq!(SharedValue::refcount(&v), 2);
    assert_eq!(SharedValue::refcount(&v2), 2);
}

#[test]
fn drop_decrements_refcount() {
    let v = SharedValue::new(Value::Number(1.0));
    let v2 = v.clone();
    assert_eq!(SharedValue::refcount(&v), 2);
    drop(v2);
    assert_eq!(SharedValue::refcount(&v), 1);
}

#[test]
fn refcount_of_static_is_one() {
    let v = SharedValue::null_static();
    assert_eq!(SharedValue::refcount(&v), 1);
}

#[test]
fn clone_static_does_not_allocate() {
    let v = SharedValue::null_static();
    let v2 = v.clone();
    // Both should point to the same static sentinel.
    assert!(SharedValue::ptr_eq(&v, &v2));
    // Refcount remains 1 because static values are not reference-counted.
    assert_eq!(SharedValue::refcount(&v), 1);
}

#[test]
fn into_raw_from_raw_round_trip() {
    let v = SharedValue::new(Value::Number(7.0));
    let ptr = v.into_raw();
    let v2 = unsafe { SharedValue::from_raw(ptr) };
    assert!(matches!(*v2, Value::Number(7.0)));
    assert_eq!(SharedValue::refcount(&v2), 1);
}

#[test]
fn into_raw_does_not_drop() {
    let v = SharedValue::new(Value::Number(1.0));
    let v2 = v.clone();
    let ptr = v.into_raw();
    // The clone still has refcount 2 because `into_raw` did not decrement.
    assert_eq!(SharedValue::refcount(&v2), 2);
    // Reconstruct and drop to clean up.
    drop(unsafe { SharedValue::from_raw(ptr) });
    assert_eq!(SharedValue::refcount(&v2), 1);
}

#[test]
fn as_ptr_matches_into_raw() {
    let v = SharedValue::new(Value::Null);
    let ptr = v.as_ptr();
    let raw = v.into_raw();
    assert_eq!(ptr, raw);
    // Clean up.
    drop(unsafe { SharedValue::from_raw(raw) });
}

#[test]
fn ptr_eq_same_allocation() {
    let v = SharedValue::new(Value::Number(1.0));
    let v2 = v.clone();
    assert!(SharedValue::ptr_eq(&v, &v2));
}

#[test]
fn ptr_eq_different_allocations() {
    let v1 = SharedValue::new(Value::Null);
    let v2 = SharedValue::new(Value::Null);
    assert!(!SharedValue::ptr_eq(&v1, &v2));
}

#[test]
fn ptr_eq_static_values() {
    let v1 = SharedValue::null_static();
    let v2 = SharedValue::null_static();
    assert!(SharedValue::ptr_eq(&v1, &v2));
}

#[test]
fn ptr_eq_static_vs_heap() {
    let v_static = SharedValue::null_static();
    let v_heap = SharedValue::new(Value::Null);
    assert!(!SharedValue::ptr_eq(&v_static, &v_heap));
}

#[test]
fn set_value_replaces_inner() {
    let mut v = SharedValue::new(Value::Number(1.0));
    v.set_value(Value::Number(2.0));
    assert!(matches!(*v, Value::Number(2.0)));
}

#[test]
fn set_value_changes_variant() {
    let mut v = SharedValue::new(Value::Null);
    v.set_value(Value::Number(99.0));
    assert!(matches!(*v, Value::Number(99.0)));
}

#[test]
#[should_panic(expected = "Cannot change the value of static NULL")]
fn set_value_panics_on_static() {
    let mut v = SharedValue::null_static();
    v.set_value(Value::Number(1.0));
}

#[test]
#[should_panic(expected = "Failed to get mutable reference")]
fn set_value_panics_when_shared() {
    let mut v = SharedValue::new(Value::Number(1.0));
    let _clone = v.clone();
    v.set_value(Value::Number(2.0));
}
