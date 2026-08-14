/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the one-hop reply view ([`value_ffi::view`]).

use redis_mock::mock_or_stub_missing_redis_c_symbols;
use std::ptr;
use value::{RedisString, SharedValue, Value};
use value_ffi::RSValue;
use value_ffi::util::as_rs_value;
use value_ffi::view::{RSValue_GetReplyView, RSValueViewType};

// Force-link the C bundle (and its `RSDummyContext`) in every build configuration,
// like the sibling test binaries; an unused dev-dependency is otherwise dropped.
extern crate redisearch_rs;

mock_or_stub_missing_redis_c_symbols!();

const fn as_value_ptr(value: &Value) -> *const RSValue {
    ptr::from_ref(value).cast()
}

#[test]
fn number_view() {
    let value = Value::Number(3.5);
    // SAFETY: `value` is a live local, so the pointer is a valid `RSValue` for the call.
    let view = unsafe { RSValue_GetReplyView(as_value_ptr(&value)) };
    assert_eq!(view.view_type, RSValueViewType::Number);
    assert_eq!(view.num, 3.5);
    assert_eq!(view.resolved, as_value_ptr(&value));
}

#[test]
fn null_and_undefined_views() {
    for value in [Value::Null, Value::Undefined] {
        // SAFETY: `value` is a live local, so the pointer is a valid `RSValue` for the call.
        let view = unsafe { RSValue_GetReplyView(as_value_ptr(&value)) };
        assert_eq!(view.view_type, RSValueViewType::Null);
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn string_view_preserves_embedded_nul() {
    let value = SharedValue::new_string(b"a\0b".to_vec());
    // SAFETY: `value` is a live local, so the pointer is a valid `RSValue` for the call.
    let view = unsafe { RSValue_GetReplyView(as_rs_value(&value)) };
    assert_eq!(view.view_type, RSValueViewType::String);
    // SAFETY: the view borrows `value`, which outlives the slice; `str_ptr`/`str_len`
    // describe that value's string payload per `RSValue_GetReplyView`'s contract.
    let bytes = unsafe { std::slice::from_raw_parts(view.str_ptr.cast::<u8>(), view.str_len) };
    assert_eq!(bytes, b"a\0b");
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn reference_chain_resolves_to_leaf() {
    let leaf = SharedValue::new_num(7.0);
    let inner = SharedValue::new(Value::Ref(leaf));
    let outer = Value::Ref(inner);
    // SAFETY: `outer` is a live local, so the pointer is a valid `RSValue` for the call.
    let view = unsafe { RSValue_GetReplyView(as_value_ptr(&outer)) };
    assert_eq!(view.view_type, RSValueViewType::Number);
    assert_eq!(view.num, 7.0);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn trio_collapses_to_middle() {
    let trio = SharedValue::new_trio(
        SharedValue::new_num(1.0),
        SharedValue::new_num(2.0),
        SharedValue::new_num(3.0),
    );
    // SAFETY: `trio` is a live local, so the pointer is a valid `RSValue` for the call.
    let view = unsafe { RSValue_GetReplyView(as_rs_value(&trio)) };
    assert_eq!(view.view_type, RSValueViewType::Number);
    assert_eq!(view.num, 2.0);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn array_view_reports_len_and_container() {
    let array = SharedValue::new_array([SharedValue::new_num(1.0), SharedValue::new_num(2.0)]);
    // SAFETY: `array` is a live local, so the pointer is a valid `RSValue` for the call.
    let view = unsafe { RSValue_GetReplyView(as_rs_value(&array)) };
    assert_eq!(view.view_type, RSValueViewType::Array);
    assert_eq!(view.len, 2);
    assert_eq!(view.resolved, as_rs_value(&array));
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn map_view_reports_len_and_container() {
    let map = SharedValue::new_map([(SharedValue::new_num(1.0), SharedValue::new_num(2.0))]);
    // SAFETY: `map` is a live local, so the pointer is a valid `RSValue` for the call.
    let view = unsafe { RSValue_GetReplyView(as_rs_value(&map)) };
    assert_eq!(view.view_type, RSValueViewType::Map);
    assert_eq!(view.len, 1);
    assert_eq!(view.resolved, as_rs_value(&map));
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn redis_string_view_exposes_ptr_len() {
    redis_mock::init_redis_module_mock();
    let raw = redis_mock::string::create_string("hello");
    // SAFETY: `raw` is a fresh string from the mock allocator; ownership moves into
    // `RedisString`, which frees it on drop.
    let value = Value::RedisString(unsafe { RedisString::from_raw(raw) });
    // SAFETY: `value` is a live local, so the pointer is a valid `RSValue` for the call.
    let view = unsafe { RSValue_GetReplyView(as_value_ptr(&value)) };
    assert_eq!(view.view_type, RSValueViewType::String);
    // SAFETY: the view borrows `value`, which outlives the slice; `str_ptr`/`str_len`
    // describe that value's string payload per `RSValue_GetReplyView`'s contract.
    let bytes = unsafe { std::slice::from_raw_parts(view.str_ptr.cast::<u8>(), view.str_len) };
    assert_eq!(bytes, b"hello");
}
