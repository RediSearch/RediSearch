/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::expect_value;
use query_error::{QueryError, QueryErrorCode};
use std::{cmp::Ordering, ffi::c_int};
use value::RsValue;
use value::comparison::{CompareError, compare};

/// Compare two [`RsValue`]s, returning `-1` if `v1 < v2`, `0` if `v1 == v2`,
/// or `1` if `v1 > v2`.
///
/// When `status` is null, mixed number/string comparisons fall back to
/// string-based comparison. When `status` is non-null and string-to-number
/// conversion fails, a [`QueryError`] is written to `status`.
///
/// # Safety
///
/// 1. `v1` and `v2` must be [valid] pointers to [`RsValue`]s.
/// 2. `status`, when non-null, must be a [valid], writable pointer to a [`QueryError`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Cmp(
    v1: *const RsValue,
    v2: *const RsValue,
    status: *mut QueryError,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let v1 = unsafe { expect_value(v1) };
    // SAFETY: ensured by caller (1.)
    let v2 = unsafe { expect_value(v2) };

    match compare(v1, v2, status.is_null()) {
        Ok(Ordering::Less) => -1,
        Ok(Ordering::Equal) => 0,
        Ok(Ordering::Greater) => 1,
        Err(CompareError::NaNFloat) => 0,
        Err(CompareError::MapComparison) => 0,
        Err(CompareError::IncompatibleTypes) => 0,
        Err(CompareError::NoNumberToStringFallback) => {
            // SAFETY: `status` is non-null because `num_to_str_cmp_fallback` was
            // `false` (set from `status.is_null()`), and ensured valid by caller (2.)
            let query_error = unsafe { status.as_mut().unwrap() };
            let message = c"Error converting string".to_owned();
            query_error.set_code_and_message(QueryErrorCode::NumericValueInvalid, Some(message));
            0
        }
    }
}

/// Check whether two [`RsValue`]s are equal, returning `true` if they are and
/// `false` otherwise.
///
/// # Safety
///
/// 1. `v1` and `v2` must be [valid] pointers to [`RsValue`]s.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Equal(
    v1: *const RsValue,
    v2: *const RsValue,
    _status: *mut QueryError,
) -> bool {
    // SAFETY: ensured by caller (1.)
    let v1 = unsafe { expect_value(v1) };
    // SAFETY: ensured by caller (1.)
    let v2 = unsafe { expect_value(v2) };

    match compare(v1, v2, false) {
        Ok(Ordering::Less) => false,
        Ok(Ordering::Equal) => true,
        Ok(Ordering::Greater) => false,
        Err(CompareError::NaNFloat) => true,
        Err(CompareError::MapComparison) => true,
        Err(CompareError::IncompatibleTypes) => true,
        Err(CompareError::NoNumberToStringFallback) => false,
    }
}

/// Test whether an [`RsValue`] is "truthy".
///
/// Returns `true` for non-zero numbers, non-empty strings, and non-empty arrays.
/// All other variants (including [`RsValue::Null`] and [`RsValue::Map`])
/// evaluate to `false`. References are followed via
/// [`RsValue::fully_dereferenced_ref`].
///
/// # Safety
///
/// 1. `value` must be a [valid] pointer to an [`RsValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_BoolTest(value: *const RsValue) -> bool {
    // SAFETY: ensured by caller (1.)
    let value = unsafe { expect_value(value) };
    let value = value.fully_dereferenced_ref();

    match value {
        RsValue::Number(num) => *num != 0.0,
        RsValue::Array(arr) => !arr.is_empty(),
        RsValue::String(string) => !string.as_bytes().is_empty(),
        RsValue::RedisString(string) => !string.as_bytes().is_empty(),
        _ => false,
    }
}
