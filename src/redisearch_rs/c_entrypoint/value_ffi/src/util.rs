/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use libc::c_char;
use value::RsValue;
// use value::strings::{ConstString, RedisString, RmAllocString};

/// Get a reference to an [`RsValue`] from a pointer.
///
/// Checks for null in debug mode, does an unwrap_unchecked in release mode.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
pub(crate) const unsafe fn expect_value<'a>(value: *const RsValue) -> &'a RsValue {
    // Safety: ensured by caller (1.)
    let value = unsafe { value.as_ref() };

    if cfg!(debug_assertions) {
        value.expect("value must not be null")
    } else {
        // Safety: ensured by caller (1.)
        unsafe { value.unwrap_unchecked() }
    }
}

pub fn rsvalue_any_str(value: &RsValue) -> bool {
    match value {
        RsValue::Undefined => false,
        RsValue::Null => false,
        RsValue::Number(_) => false,
        RsValue::RmAllocString(_) => true,
        RsValue::ConstString(_) => true,
        RsValue::RedisString(_) => true,
        // RsValue::String(_) => true,
        RsValue::String2(_) => true,
        RsValue::Array(_) => false,
        RsValue::Ref(_) => false,
        RsValue::Trio(_) => false,
        RsValue::Map(_) => false,
    }
}

pub fn rsvalue_as_str_ptr_len(value: &RsValue) -> Option<(*const c_char, u32)> {
    match value {
        RsValue::Undefined => None,
        RsValue::Null => None,
        RsValue::Number(_) => None,
        RsValue::RmAllocString(string) => Some(string.as_ptr_len()),
        RsValue::ConstString(string) => Some(string.as_ptr_len()),
        RsValue::RedisString(string) => Some(string.as_ptr_len()),
        // RsValue::String(string) => Some(string.as_bytes()),
        RsValue::String2(string) => Some((string.as_ptr(), string.count_bytes() as u32)),
        RsValue::Array(_) => None,
        RsValue::Ref(_) => None,
        RsValue::Trio(_) => None,
        RsValue::Map(_) => None,
    }
}

pub fn rsvalue_as_byte_slice2(value: &RsValue) -> Option<&[u8]> {
    match value {
        RsValue::Undefined => None,
        RsValue::Null => None,
        RsValue::Number(_) => None,
        RsValue::RmAllocString(string) => Some(string.as_bytes()),
        RsValue::ConstString(string) => Some(string.as_bytes()),
        RsValue::RedisString(string) => Some(string.as_bytes()),
        // RsValue::String(string) => Some(string.as_bytes()),
        RsValue::String2(string) => Some(string.as_bytes()),
        RsValue::Array(_) => None,
        RsValue::Ref(_) => None,
        RsValue::Trio(_) => None,
        RsValue::Map(_) => None,
    }
}

pub fn rsvalue_str_to_float(input: &[u8]) -> Option<f64> {
    std::str::from_utf8(input).ok()?.parse::<f64>().ok()
}

//   if (p) {
//     char *e;
//     errno = 0;
//     *d = fast_float_strtod(p, &e);
//     if ((errno == ERANGE && (*d == HUGE_VAL || *d == -HUGE_VAL)) || (errno != 0 && *d == 0) ||
//         *e != '\0') {
//       return 0;
//     }

//     return 1;
//   }

pub fn rsvalue_num_to_str(number: f64) -> String {
    let mut buf = [0u8; 128];
    let len = value::util::num_to_string_cstyle(number, &mut buf);
    let str_val = std::str::from_utf8(&buf[..(len as usize)]).unwrap();
    str_val.to_owned()
}
