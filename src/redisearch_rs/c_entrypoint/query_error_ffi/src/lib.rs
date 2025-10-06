use std::os::raw::c_char;

pub use query_error::{QueryErrorCode, QueryError};

#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
pub extern "C" fn QueryError_Default() -> QueryError {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_Strerror(_code: QueryErrorCode) -> *const c_char {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_SetError(
    _status: *mut QueryError,
    _code: QueryErrorCode,
    _message: *const c_char,
) {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_SetCode(_status: *mut QueryError, _code: QueryErrorCode) {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_SetMessage(
    _status: *mut QueryError,
    _code: QueryErrorCode,
    _message: *const c_char,
) {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_CloneFrom(_src: *const QueryError, _dest: *mut QueryError) {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_GetUserError(_status: *const QueryError) -> *const c_char {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_GetDisplayableError(
    _status: *const QueryError,
    _obfuscate: bool,
) -> *const c_char {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_GetCode(_status: *const QueryError) -> QueryErrorCode {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_ClearError(_err: *mut QueryError) {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_MaybeSetCode(_status: *mut QueryError, _code: QueryErrorCode) {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_HasReachedMaxPrefixExpansionsWarning(
    _status: *const QueryError,
) -> bool {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn QueryError_SetReachedMaxPrefixExpansionsWarning(_status: *mut QueryError) {
    todo!()
}
