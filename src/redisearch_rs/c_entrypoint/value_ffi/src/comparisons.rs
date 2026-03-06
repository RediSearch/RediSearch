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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Cmp(
    v1: *const RsValue,
    v2: *const RsValue,
    status: *mut QueryError,
) -> c_int {
    let v1 = unsafe { expect_value(v1) };
    let v2 = unsafe { expect_value(v2) };

    match compare(v1, v2, status.is_null()) {
        Ok(Ordering::Less) => -1,
        Ok(Ordering::Equal) => 0,
        Ok(Ordering::Greater) => 1,
        Err(CompareError::NaNNumber) => 0,
        Err(CompareError::MapComparison) => 0,
        Err(CompareError::IncompatibleTypes) => 0,
        Err(CompareError::NoNumberToStringFallback) => {
            // SAFETY: Number conversion failed and status was provided.
            let query_error = unsafe { status.as_mut().unwrap() };
            let message = c"Error converting string".to_owned();
            query_error.set_code_and_message(QueryErrorCode::NumericValueInvalid, Some(message));
            // even though we're returning 'equal', the query error code is likely checked for a possible error.
            0
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Equal(
    v1: *const RsValue,
    v2: *const RsValue,
    _status: *mut QueryError,
) -> c_int {
    let v1 = unsafe { expect_value(v1) };
    let v2 = unsafe { expect_value(v2) };

    // For equality, don't fall back to string comparison - conversion failure means not equal
    match compare(v1, v2, false) {
        Ok(Ordering::Less) => 0,
        Ok(Ordering::Equal) => 1,
        Ok(Ordering::Greater) => 0,
        Err(CompareError::NaNNumber) => 1,
        Err(CompareError::MapComparison) => 1,
        Err(CompareError::IncompatibleTypes) => 1,
        Err(CompareError::NoNumberToStringFallback) => 0,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_BoolTest(value: *const RsValue) -> c_int {
    let value = unsafe { expect_value(value) };
    let value = value.fully_dereferenced_ref();

    let result = match value {
        RsValue::Number(num) => *num != 0.0,
        RsValue::Array(arr) => arr.len() != 0,
        RsValue::String(string) => string.as_bytes().len() != 0,
        RsValue::RedisString(string) => string.as_bytes().len() != 0,
        _ => false,
    };

    result as c_int
}
