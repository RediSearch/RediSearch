/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_char;

use value::{
    RsValue,
    collection::{RsValueArray, RsValueMap},
    shared::SharedRsValue,
};

/// Creates a heap-allocated `RsValue` by parsing a string as a number.
/// Returns an undefined value if the string cannot be parsed as a valid number.
///
/// # Safety
/// - (1) `str` must be a valid const pointer to a char sequence of `len` bytes.
///
/// @param p The string to parse
/// @param l The length of the string
/// @return A pointer to a heap-allocated `RsValue`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewParsedNumber(
    str: *const c_char,
    len: usize,
) -> *const RsValue {
    if len == 0 {
        return SharedRsValue::new(RsValue::Undefined).into_raw();
    }

    // Safety: caller must ensure (1).
    let str = unsafe { std::slice::from_raw_parts(str as *const u8, len) };
    let Ok(str) = std::str::from_utf8(str) else {
        return SharedRsValue::new(RsValue::Undefined).into_raw();
    };
    let Ok(n) = str.parse() else {
        return SharedRsValue::new(RsValue::Undefined).into_raw();
    };
    let shared_value = SharedRsValue::new(RsValue::Number(n));
    shared_value.into_raw()
}

/// Creates a heap-allocated `RsValue` containing a number from an int64.
/// This operation casts the passed `i64` to an `f64`, possibly losing information.
///
/// @param ii The int64 value to convert and wrap
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewNumberFromInt64(dd: i64) -> *const RsValue {
    let shared_value = SharedRsValue::new(RsValue::Number(dd as f64));
    shared_value.into_raw()
}

/// Creates a heap-allocated `RsValue` array from existing values.
/// Takes ownership of the values (values will be freed when array is freed).
///
/// @param vals The values array to use for the array (ownership is transferred)
/// @param len Number of values
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Array`
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewArray(vals: RsValueArray) -> *const RsValue {
    let shared_value = SharedRsValue::new(RsValue::Array(vals));
    shared_value.into_raw()
}

/// Creates a heap-allocated RsValue of type RsValue_Map from an RsValueMap.
/// Takes ownership of the map structure and all its entries.
///
/// @param map The RsValueMap to wrap (ownership is transferred)
/// @return A pointer to a heap-allocated RsValue of type RsValueType_Map
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewMap(map: RsValueMap) -> *const RsValue {
    let shared_value = SharedRsValue::new(RsValue::Map(map));
    shared_value.into_raw()
}
