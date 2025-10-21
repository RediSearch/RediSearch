/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod opaque;

use query_error::QueryError;
use std::ffi::CStr;
use std::os::raw::c_char;

pub use crate::opaque::{OpaqueQueryError, QueryErrorExt};
pub use query_error::QueryErrorCode;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_Default() -> OpaqueQueryError {
    QueryError::default().into_opaque()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_IsOk(query_error: *const OpaqueQueryError) -> bool {
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    query_error.is_ok()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_HasError(query_error: *const OpaqueQueryError) -> bool {
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    !query_error.is_ok()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_Strerror(maybe_code: u8) -> *const c_char {
    let Ok(code) = QueryErrorCode::try_from(maybe_code) else {
        return c"Unknown status code".as_ptr();
    };

    code.to_c_str().as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetError(
    query_error: *mut OpaqueQueryError,
    code: u8,
    message: *const c_char,
) {
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");
    let code = QueryErrorCode::try_from(code).expect("invalid query error code");

    let message = if message.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(message) }.to_owned())
    };

    query_error.set_code_and_info(code, message);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetCode(query_error: *mut OpaqueQueryError, code: u8) {
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");
    let code = QueryErrorCode::try_from(code).expect("invalid query error code");

    query_error.set_code(code);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetDetail(
    query_error: *mut OpaqueQueryError,
    detail: *const c_char,
) {
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");

    let detail = if detail.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(detail) }.to_owned())
    };

    query_error.set_private_info(detail)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_CloneFrom(
    src: *const OpaqueQueryError,
    dest: *mut OpaqueQueryError,
) {
    {
        let dest_query_error =
            unsafe { QueryError::from_opaque_ptr(dest as *const _) }.expect("dest is null");

        if !dest_query_error.is_ok() {
            return;
        }
    }

    let src_query_error = unsafe { QueryError::from_opaque_ptr(src) }.expect("src is null");
    let query_error = src_query_error.clone();

    let query_error_opaque = query_error.into_opaque();

    unsafe { dest.write(query_error_opaque) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_GetUserError(
    query_error: *const OpaqueQueryError,
) -> *const c_char {
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    query_error
        .private_info()
        .unwrap_or_else(|| query_error.code().to_c_str())
        .as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_GetDisplayableError(
    query_error: *const OpaqueQueryError,
    obfuscate: bool,
) -> *const c_char {
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    let info = if obfuscate {
        query_error.public_info()
    } else {
        query_error.private_info()
    };

    info.unwrap_or_else(|| query_error.code().to_c_str())
        .as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_GetCode(
    query_error: *const OpaqueQueryError,
) -> QueryErrorCode {
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    query_error.code()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_ClearError(query_error: *mut OpaqueQueryError) {
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");

    query_error.clear();
}

// Set the code if not previously set. This should be used by code which makes
// use of the ::detail field, and is a placeholder for something like:
// functionWithCharPtr(&status->_detail);
// if (status->_detail && status->_code == QUERY_OK) {
//    status->_code = MYCODE;
// }
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_MaybeSetCode(query_error: *mut OpaqueQueryError, code: u8) {
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");
    let code = QueryErrorCode::try_from(code).expect("invalid query error code");

    if query_error.private_info().is_none() || !query_error.is_ok() {
        return;
    }

    query_error.set_code(code);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_HasReachedMaxPrefixExpansionsWarning(
    query_error: *const OpaqueQueryError,
) -> bool {
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    query_error.warnings().reached_max_prefix_expansions()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetReachedMaxPrefixExpansionsWarning(
    query_error: *mut OpaqueQueryError,
) {
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");

    query_error
        .warnings_mut()
        .set_reached_max_prefix_expansions()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_HasQueryOOMWarning(
    query_error: *const OpaqueQueryError,
) -> bool {
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    query_error.warnings().out_of_memory()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetQueryOOMWarning(query_error: *mut OpaqueQueryError) {
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");

    query_error.warnings_mut().set_out_of_memory()
}
