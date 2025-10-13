/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use query_error::QueryError;
use std::ffi::CStr;
use std::os::raw::c_char;

pub use mimic::Size64Align8;
pub use query_error::QueryErrorCode;

#[repr(C)]
pub struct QueryErrorMimic(Size64Align8);

impl From<QueryErrorMimic> for QueryError {
    fn from(QueryErrorMimic(size_and_align): QueryErrorMimic) -> Self {
        unsafe { Self::from_mimic(size_and_align) }
    }
}

impl From<&QueryErrorMimic> for &QueryError {
    fn from(query_error_mimic: &QueryErrorMimic) -> Self {
        unsafe { std::mem::transmute(query_error_mimic) }
    }
}

impl From<&mut QueryErrorMimic> for &mut QueryError {
    fn from(query_error_mimic: &mut QueryErrorMimic) -> Self {
        unsafe { std::mem::transmute(query_error_mimic) }
    }
}

impl From<QueryError> for QueryErrorMimic {
    fn from(query_error: QueryError) -> Self {
        Self(QueryError::into_mimic(query_error))
    }
}

const _: () = {
    assert!(std::mem::size_of::<QueryErrorMimic>() == std::mem::size_of::<QueryError>());
    assert!(std::mem::align_of::<QueryErrorMimic>() == std::mem::align_of::<QueryError>());
};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_Default() -> QueryErrorMimic {
    QueryError::default().into()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_Strerror(code: QueryErrorCode) -> *const c_char {
    code.to_cstr().as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetError(
    query_error: *mut QueryErrorMimic,
    code: QueryErrorCode,
    message: *const c_char,
) {
    let query_error: &mut QueryError = unsafe { query_error.as_mut() }
        .expect("query_error is null")
        .into();
    let message = if message.is_null() {
        None
    } else {
        Some(
            unsafe { CStr::from_ptr(message) }
                .to_str()
                .expect("message has non-utf8 chars")
                .to_string(),
        )
    };

    query_error.set_error(code, message);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetCode(query_error: *mut QueryErrorMimic, code: QueryErrorCode) {
    let query_error: &mut QueryError = unsafe { query_error.as_mut() }
        .expect("query_error is null")
        .into();

    query_error.set_code(code);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetDetail(
    _status: *mut QueryErrorMimic,
    _code: QueryErrorCode,
    _detail: *const c_char,
) {
    todo!()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_CloneFrom(src: *const QueryErrorMimic, dest: *mut QueryErrorMimic) {
    let src_query_error: &QueryError = unsafe { src.as_ref() }.expect("src is null").into();
    let query_error = src_query_error.clone();
    let query_error_mimic = query_error.into();

    unsafe { dest.write(query_error_mimic) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_GetUserError(_status: *const QueryErrorMimic) -> *const c_char {
    todo!()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_GetDisplayableError(
    _status: *const QueryErrorMimic,
    _obfuscate: bool,
) -> *const c_char {
    todo!()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_GetCode(query_error: *const QueryErrorMimic) -> QueryErrorCode {
    let query_error: &QueryError = unsafe { query_error.as_ref() }
        .expect("query_error is null")
        .into();

    query_error.code()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_ClearError(query_error: *mut QueryErrorMimic) {
    let query_error: &mut QueryError = unsafe { query_error.as_mut() }
        .expect("query_error is null")
        .into();

    query_error.clear();
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_MaybeSetCode(_status: *mut QueryErrorMimic, _code: QueryErrorCode) {
    todo!()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_HasReachedMaxPrefixExpansionsWarning(
    query_error: *const QueryErrorMimic,
) -> bool {
    let query_error: &QueryError = unsafe { query_error.as_ref() }
        .expect("query_error is null")
        .into();

    query_error.warnings().reached_max_prefix_expansions()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetReachedMaxPrefixExpansionsWarning(
    query_error: *mut QueryErrorMimic,
) {
    let query_error: &mut QueryError = unsafe { query_error.as_mut() }
        .expect("query_error is null")
        .into();

    query_error
        .warnings_mut()
        .set_reached_max_prefix_expansions()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_HasQueryOOMWarning(query_error: *const QueryErrorMimic) -> bool {
    let query_error: &QueryError = unsafe { query_error.as_ref() }
        .expect("query_error is null")
        .into();

    query_error.warnings().out_of_memory()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryError_SetQueryOOMWarning(query_error: *mut QueryErrorMimic) {
    let query_error: &mut QueryError = unsafe { query_error.as_mut() }
        .expect("query_error is null")
        .into();

    query_error.warnings_mut().set_out_of_memory()
}
