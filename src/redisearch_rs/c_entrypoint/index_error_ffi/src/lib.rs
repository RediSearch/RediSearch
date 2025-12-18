#![allow(non_snake_case)]

use std::os::raw::c_char;

use c_ffi_utils::opaque::IntoOpaque;
use index_error::{IndexError, opaque::OpaqueIndexError};
use libc::timespec;

pub use index_error::RedisModuleString;

/// Initialize the IndexError module globals.
/// This should be called once during module initialization.
///
/// # Safety
/// - `ctx` must be a valid RedisModuleCtx pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_GlobalInit(ctx: *mut redis_module::raw::RedisModuleCtx) {
    // SAFETY: ctx is valid (caller requirement)
    unsafe { index_error::index_error_init(ctx) };
}

/// Returns the default [`IndexError`].
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_Default() -> OpaqueIndexError {
    IndexError::default().into_opaque()
}

/// Returns the number of errors in the IndexError.
///
/// # Safety
/// - `error` must be a valid pointer to an OpaqueIndexError
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_ErrorCount(error: *const OpaqueIndexError) -> usize {
    // SAFETY: error is valid (caller requirement)
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    error.error_count()
}

/// Returns the last error message in the IndexError.
///
/// # Safety
/// - `error` must be a valid pointer to an OpaqueIndexError
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_LastError(error: *const OpaqueIndexError) -> *const c_char {
    // SAFETY: error is valid (caller requirement)
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    error
        .last_error_with_user_data()
        .map(|s| s.as_ptr())
        .unwrap_or(std::ptr::null())
}

/// Returns the last error message in the IndexError, obfuscated.
///
/// # Safety
/// - `error` must be a valid pointer to an OpaqueIndexError
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_LastErrorObfuscated(error: *const OpaqueIndexError) -> *const c_char {
    // SAFETY: error is valid (caller requirement)
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    error
        .last_error_without_user_data()
        .map(|s| s.as_ptr())
        .unwrap_or(std::ptr::null())
}

/// Returns the time of the last error.
///
/// # Safety
/// - `error` must be a valid pointer to an OpaqueIndexError
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_LastErrorTime(
    error: *const OpaqueIndexError) -> timespec {
    // SAFETY: error is valid (caller requirement)
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    error.last_error_time()
}

/// Get the background_indexing_OOM_failure flag.
///
/// # Safety
/// - `error` must be a valid pointer to an OpaqueIndexError
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_HasBackgroundIndexingOOMFailure(error: *const OpaqueIndexError) -> bool {
    // SAFETY: error is valid (caller requirement)
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    error.has_background_indexing_oom_failure()
}

/// Change the background_indexing_OOM_failure flag to true.
///
/// # Safety
/// - `error` must be a valid pointer to an OpaqueIndexError
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_RaiseBackgroundIndexFailureFlag(error: *mut OpaqueIndexError) {
    // SAFETY: error is valid (caller requirement)
    let error = unsafe { IndexError::from_opaque_mut_ptr(error) }.expect("error is null");
    error.raise_background_indexing_oom_failure();
}

/// Initializes an IndexError. The error_count is set to 0 and the last_error is set to NA.
/// Mirrors C function: `IndexError IndexError_Init()`
///
/// # Safety
/// - This function uses RSDummyContext internally, which must be initialized
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_Init() -> OpaqueIndexError {
    // SAFETY: RSDummyContext is initialized at module startup and remains valid
    let ctx = unsafe { ffi::context::redisearch_module_context().cast() };
    // SAFETY: ctx is a valid RedisModuleCtx from RSDummyContext
    unsafe { IndexError::new_with_na(ctx) }.into_opaque()
}

/// Adds an error message to the IndexError. The error_count is incremented and the last_error is set to the error_message.
/// Mirrors C function: `void IndexError_AddError(IndexError *error, ConstErrorMessage withoutUserData, ConstErrorMessage withUserData, RedisModuleString *key)`
///
/// # Safety
/// - `ctx` must be a valid RedisModuleCtx pointer
/// - `error` must be a valid pointer to an OpaqueIndexError
/// - `without_user_data` and `with_user_data` must be valid C strings or null
/// - `key` must be a valid RedisModuleString pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_AddError(
    ctx: *mut redis_module::raw::RedisModuleCtx,
    error: *mut OpaqueIndexError,
    without_user_data: *const c_char,
    with_user_data: *const c_char,
    key: *mut RedisModuleString,
) {
    // SAFETY: error is valid (caller requirement)
    let error = unsafe { IndexError::from_opaque_mut_ptr(error) }.expect("error is null");
    // SAFETY: All pointers are valid (caller requirement)
    unsafe { error.add_error(ctx, without_user_data, with_user_data, key) };
}

/// Returns the key of the document that caused the error.
/// Mirrors C function: `RedisModuleString *IndexError_LastErrorKey(const IndexError *error)`
///
/// # Safety
/// - `error` must be a valid pointer to an OpaqueIndexError
/// - Returns a held reference (incremented refcount) so the caller can always call FreeString
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_LastErrorKey(error: *const OpaqueIndexError) -> *mut RedisModuleString {
    // SAFETY: error is valid (caller requirement)
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    // SAFETY: RSDummyContext is initialized at module startup
    let ctx = unsafe { ffi::context::redisearch_module_context().cast() };
    // SAFETY: ctx is valid, error is valid
    unsafe { error.last_error_key_held(ctx) }
}

/// Clears an IndexError. If the last_error is not NA, it is freed.
/// Mirrors C function: `void IndexError_Clear(IndexError error)`
///
/// # Safety
/// - `ctx` must be a valid RedisModuleCtx pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_Clear(ctx: *mut redis_module::raw::RedisModuleCtx, error: OpaqueIndexError) {
    // SAFETY: error is a valid OpaqueIndexError passed by value
    let mut error = unsafe { IndexError::from_opaque(error) };
    // SAFETY: ctx is valid (caller requirement)
    unsafe { error.clear(ctx) };
    // error is dropped here, freeing all resources
}

/// Clears global variables used in the IndexError module.
/// This function should be called on shutdown.
/// Mirrors C function: `void IndexError_GlobalCleanup()`
///
/// # Safety
/// - `ctx` must be a valid RedisModuleCtx pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_GlobalCleanup(ctx: *mut redis_module::raw::RedisModuleCtx) {
    // SAFETY: ctx is valid (caller requirement)
    unsafe { index_error::index_error_cleanup(ctx) };
}

/// Adds the error message of the other IndexError to the IndexError. The error_count is incremented and the last_error is set to the error_message.
/// This is used when merging errors from different shards in a cluster.
/// Mirrors C function: `void IndexError_Combine(IndexError *error, const IndexError *other)`
///
/// # Safety
/// - `ctx` must be a valid RedisModuleCtx pointer
/// - `error` must be a valid pointer to an OpaqueIndexError
/// - `other` must be a valid pointer to an OpaqueIndexError
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_Combine(
    ctx: *mut redis_module::raw::RedisModuleCtx,
    error: *mut OpaqueIndexError,
    other: *const OpaqueIndexError,
) {
    // SAFETY: error is valid (caller requirement)
    let error = unsafe { IndexError::from_opaque_mut_ptr(error) }.expect("error is null");
    // SAFETY: other is valid (caller requirement)
    let other = unsafe { IndexError::from_opaque_ptr(other) }.expect("other is null");
    // SAFETY: ctx is valid (caller requirement), error and other are valid
    unsafe { error.combine(ctx, other) };
}

