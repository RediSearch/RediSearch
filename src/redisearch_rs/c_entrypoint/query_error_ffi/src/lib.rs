/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use query_error::QueryError;
use std::os::raw::c_char;

pub use mimic::Size64Align8;
pub use query_error::QueryErrorCode;

#[repr(C)]
pub struct QueryErrorMimic(Size64Align8);

const _: () = {
    assert!(std::mem::size_of::<QueryErrorMimic>() == std::mem::size_of::<QueryError>());
    assert!(std::mem::align_of::<QueryErrorMimic>() == std::mem::align_of::<QueryError>());
};

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_Default() -> QueryErrorMimic {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_Strerror(_code: QueryErrorCode) -> *const c_char {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_SetError(
    _status: *mut QueryErrorMimic,
    _code: QueryErrorCode,
    _message: *const c_char,
) {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_SetCode(_status: *mut QueryErrorMimic, _code: QueryErrorCode) {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_SetMessage(
    _status: *mut QueryErrorMimic,
    _code: QueryErrorCode,
    _message: *const c_char,
) {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_CloneFrom(_src: *const QueryErrorMimic, _dest: *mut QueryErrorMimic) {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_GetUserError(_status: *const QueryErrorMimic) -> *const c_char {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_GetDisplayableError(
    _status: *const QueryErrorMimic,
    _obfuscate: bool,
) -> *const c_char {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_GetCode(_status: *const QueryErrorMimic) -> QueryErrorCode {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_ClearError(_err: *mut QueryErrorMimic) {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_MaybeSetCode(_status: *mut QueryErrorMimic, _code: QueryErrorCode) {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_HasReachedMaxPrefixExpansionsWarning(
    _status: *const QueryErrorMimic,
) -> bool {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_SetReachedMaxPrefixExpansionsWarning(_status: *mut QueryErrorMimic) {
    todo!()
}
