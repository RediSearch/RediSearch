/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::RSValue;
use crate::util::expect_value;
use query_error::QueryError;
use std::cmp::Ordering;
use std::ffi::c_int;
use value::Value;
use value::comparison::{compare_on_equality_only, compare_with_query_error};

/// Compare two [`RSValue`]s, returning `-1` if `v1 < v2`, `0` if `v1 == v2`,
/// or `1` if `v1 > v2`.
///
/// When `status` is null, mixed number/string comparisons fall back to
/// string-based comparison. When `status` is non-null and string-to-number
/// conversion fails, a [`QueryError`] is written to `status`.
///
/// # Safety
///
/// 1. `v1` and `v2` must be [valid], non-null pointers to [`RSValue`]s.
/// 2. `status`, when non-null, must be a [valid], writable pointer to a [`QueryError`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Cmp(
    v1: *const RSValue,
    v2: *const RSValue,
    status: *mut QueryError,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let v1 = unsafe { expect_value(v1) };
    // SAFETY: ensured by caller (1.)
    let v2 = unsafe { expect_value(v2) };

    // SAFETY: ensured by caller (2.)
    let qerr = unsafe { status.as_mut() };

    match compare_with_query_error(v1, v2, qerr) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

/// Check whether two [`RSValue`]s are equal, returning `true` if they are and
/// `false` otherwise.
///
/// # Safety
///
/// 1. `v1` and `v2` must be [valid], non-null pointers to [`RSValue`]s.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Equal(
    v1: *const RSValue,
    v2: *const RSValue,
    _status: *mut QueryError,
) -> bool {
    // SAFETY: ensured by caller (1.)
    let v1 = unsafe { expect_value(v1) };
    // SAFETY: ensured by caller (1.)
    let v2 = unsafe { expect_value(v2) };

    compare_on_equality_only(v1, v2)
}

/// Test whether an [`RSValue`] is "truthy".
///
/// Returns `true` for non-zero numbers, non-empty strings, and non-empty arrays.
/// All other variants (including [`Value::Null`] and [`Value::Map`])
/// evaluate to `false`. References are followed via
/// [`Value::fully_dereferenced_ref`].
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_BoolTest(value: *const RSValue) -> bool {
    // SAFETY: ensured by caller (1.)
    let value = unsafe { expect_value(value) };
    let value = value.fully_dereferenced_ref();

    match value {
        Value::Number(num) => *num != 0.0,
        Value::Array(arr) => !arr.is_empty(),
        Value::String(string) => !string.as_bytes().is_empty(),
        Value::RedisString(string) => !string.as_bytes().is_empty(),
        _ => false,
    }
}
