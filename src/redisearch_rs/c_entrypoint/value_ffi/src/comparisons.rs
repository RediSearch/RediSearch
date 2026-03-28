/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::expect_value;
use query_error::QueryError;
use std::ffi::c_int;
use value::Value;
use value::comparison::{compare_on_equality_only, compare_with_query_error_to_int};

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
    v1: *const Value,
    v2: *const Value,
    status: *mut QueryError,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let v1 = unsafe { expect_value(v1) };
    // SAFETY: ensured by caller (1.)
    let v2 = unsafe { expect_value(v2) };

    // SAFETY: ensured by caller (2.)
    let qerr = unsafe { status.as_mut() };

    compare_with_query_error_to_int(v1, v2, qerr)
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
    v1: *const Value,
    v2: *const Value,
    _status: *mut QueryError,
) -> bool {
    // SAFETY: ensured by caller (1.)
    let v1 = unsafe { expect_value(v1) };
    // SAFETY: ensured by caller (1.)
    let v2 = unsafe { expect_value(v2) };

    compare_on_equality_only(v1, v2)
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
pub unsafe extern "C" fn RSValue_BoolTest(value: *const Value) -> bool {
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
