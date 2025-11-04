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

/// Returns the default [`QueryError`].
#[unsafe(no_mangle)]
pub extern "C" fn QueryError_Default() -> OpaqueQueryError {
    QueryError::default().into_opaque()
}

/// Returns true if `query_error` has no error code set.
///
/// # Safety
///
/// `query_error` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_IsOk(query_error: *const OpaqueQueryError) -> bool {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    query_error.is_ok()
}

/// Returns true if `query_error` has an error code set.
///
/// # Safety
///
/// `query_error` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_HasError(query_error: *const OpaqueQueryError) -> bool {
    // Safety: see safety requirement above.
    unsafe { !QueryError_IsOk(query_error) }
}

/// Returns a human-readable string representing the provided [`QueryErrorCode`].
///
/// This function should always return without a panic for any value provided.
/// It is unique among the `QueryError_*` API as the only function which allows
/// an invalid [`QueryErrorCode`] to be provided.
#[unsafe(no_mangle)]
pub const extern "C" fn QueryError_Strerror(maybe_code: u8) -> *const c_char {
    let Some(code) = QueryErrorCode::from_repr(maybe_code) else {
        return c"Unknown status code".as_ptr();
    };

    code.to_c_str().as_ptr()
}

/// Sets the [`QueryErrorCode`] and error message for a [`QueryError`].
///
/// This does not mutate `query_error` if it already has an error set.
///
/// # Panics
///
/// - `code` must be a valid variant of [`QueryErrorCode`].
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
/// - `message` must be a valid C string or a NULL pointer.
#[unsafe(no_mangle)]
pub extern "C" fn QueryError_GetCodeFromMessage(message: *const c_char) -> QueryErrorCode {
  const TIMED_OUT_ERROR_CSTR: &CStr = QueryErrorCode::TimedOut.to_c_str();
  const OUT_OF_MEMORY_ERROR_CSTR: &CStr = QueryErrorCode::OutOfMemory.to_c_str();

  let message = unsafe { CStr::from_ptr(message) };

  if message == TIMED_OUT_ERROR_CSTR {
    return QueryErrorCode::TimedOut;
  }

  if message == OUT_OF_MEMORY_ERROR_CSTR {
    return QueryErrorCode::OutOfMemory;
  }

  return QueryErrorCode::Generic;
}

/// Sets the [`QueryErrorCode`] and error message for a [`QueryError`].
///
/// This does not mutate `query_error` if it already has an error set.
///
/// # Panics
///
/// - `code` must be a valid variant of [`QueryErrorCode`].
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
/// - `message` must be a valid C string or a NULL pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetError(
    query_error: *mut OpaqueQueryError,
    code: u8,
    message: *const c_char,
) {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");
    let code = QueryErrorCode::from_repr(code).expect("invalid query error code");

    let message = if message.is_null() {
        None
    } else {
        // Safety: see safety requirement above.
        Some(unsafe { CStr::from_ptr(message) }.to_owned())
    };

    query_error.set_code_and_message(code, message);
}

/// Sets the [`QueryErrorCode`] for a [`QueryError`].
///
/// This does not mutate `query_error` if it already has an error set.
///
/// # Panics
///
/// - `code` must be a valid variant of [`QueryErrorCode`].
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetCode(query_error: *mut OpaqueQueryError, code: u8) {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");
    let code = QueryErrorCode::from_repr(code).expect("invalid query error code");

    query_error.set_code(code);
}

/// Always sets the private message for a [`QueryError`].
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
/// - `detail` must be a valid C string or a NULL pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetDetail(
    query_error: *mut OpaqueQueryError,
    detail: *const c_char,
) {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");

    let detail = if detail.is_null() {
        None
    } else {
        // Safety: see safety requirement above.
        Some(unsafe { CStr::from_ptr(detail) }.to_owned())
    };

    query_error.set_private_message(detail)
}

/// Clones the `src` [`QueryError`] into `dest`.
///
/// This does nothing if `dest` already has an error set.
///
/// # Safety
///
/// - `src` must have been created by [`QueryError_Default`].
/// - `dest` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_CloneFrom(
    src: *const OpaqueQueryError,
    dest: *mut OpaqueQueryError,
) {
    {
        // Safety: see safety requirement above.
        let dest_query_error =
            unsafe { QueryError::from_opaque_ptr(dest as *const _) }.expect("dest is null");

        if !dest_query_error.is_ok() {
            return;
        }
    }

    // Safety: see safety requirement above.
    let src_query_error = unsafe { QueryError::from_opaque_ptr(src) }.expect("src is null");
    let query_error = src_query_error.clone();

    let query_error_opaque = query_error.into_opaque();

    // Safety: see safety requirement above.
    unsafe { dest.write(query_error_opaque) };
}

/// Returns the private message set for a [`QueryError`]. If no private message
/// is set, this returns the string error message for the code that is set,
/// like [`QueryError_Strerror`].
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_GetUserError(
    query_error: *const OpaqueQueryError,
) -> *const c_char {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    query_error
        .private_message()
        .unwrap_or_else(|| query_error.code().to_c_str())
        .as_ptr()
}

/// Returns an message of a [`QueryError`].
///
/// This preferentially returns the private message if any, or the public
/// message if any, lastly defaulting to the error code's string error.
///
/// If `obfuscate` is set, the private message is not returned. The public
/// message is returned, if any, defaulting to the error code's string error.
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_GetDisplayableError(
    query_error: *const OpaqueQueryError,
    obfuscate: bool,
) -> *const c_char {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    let message = if obfuscate {
        query_error.public_message()
    } else {
        query_error.private_message()
    };

    message
        .unwrap_or_else(|| query_error.code().to_c_str())
        .as_ptr()
}

/// Returns the [`QueryErrorCode`] set for a [`QueryError`].
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_GetCode(
    query_error: *const OpaqueQueryError,
) -> QueryErrorCode {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    query_error.code()
}

/// Clears any error set on a [`QueryErrorCode`].
///
/// This is equivalent to resetting `query_error` to the value returned by
/// [`QueryError_Default`].
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_ClearError(query_error: *mut OpaqueQueryError) {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");

    query_error.clear();
}

/// Sets the [`QueryErrorCode`] for a [`QueryError`].
///
/// This does not mutate `query_error` if it already has an error set, or
/// if the private message is set. This differs from [`QueryError_SetCode`],
/// as that function does not care if the private message is set.
///
/// # Panics
///
/// - `code` must be a valid variant of [`QueryErrorCode`].
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_MaybeSetCode(query_error: *mut OpaqueQueryError, code: u8) {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");
    let code = QueryErrorCode::from_repr(code).expect("invalid query error code");

    if query_error.private_message().is_none() || !query_error.is_ok() {
        return;
    }

    query_error.set_code(code);
}

/// Returns whether the [`QueryError`] has the `reached_max_prefix_expansions`
/// warning set.
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_HasReachedMaxPrefixExpansionsWarning(
    query_error: *const OpaqueQueryError,
) -> bool {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    query_error.warnings().reached_max_prefix_expansions()
}

/// Sets the `reached_max_prefix_expansions` warning on the [`QueryError`].
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetReachedMaxPrefixExpansionsWarning(
    query_error: *mut OpaqueQueryError,
) {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");

    query_error
        .warnings_mut()
        .set_reached_max_prefix_expansions()
}

/// Returns whether the [`QueryError`] has the `out_of_memory` warning set.
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_HasQueryOOMWarning(
    query_error: *const OpaqueQueryError,
) -> bool {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_ptr(query_error) }.expect("query_error is null");

    query_error.warnings().out_of_memory()
}

/// Sets the `out_of_memory` warning on the [`QueryError`].
///
/// # Safety
///
/// - `query_error` must have been created by [`QueryError_Default`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetQueryOOMWarning(query_error: *mut OpaqueQueryError) {
    // Safety: see safety requirement above.
    let query_error =
        unsafe { QueryError::from_opaque_mut_ptr(query_error) }.expect("query_error is null");

    query_error.warnings_mut().set_out_of_memory()
}
