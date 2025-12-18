#![allow(non_snake_case)]

use std::os::raw::c_char;

use c_ffi_utils::opaque::IntoOpaque;
use index_error::{IndexError, opaque::OpaqueIndexError};

pub use index_error::RedisModuleString;

/// C-compatible timespec structure for FFI boundary
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimeSpec {
    pub tv_sec: i64,
    pub tv_nsec: i64,
}

/// Initialize the IndexError module globals.
/// This should be called once during module initialization.
///
/// # Safety
/// - `ctx` must be a valid RedisModuleCtx pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_GlobalInit(ctx: *mut redis_module::raw::RedisModuleCtx) {
    unsafe {
        index_error::index_error_init(ctx);
    }
}

/// Returns the default [`IndexError`].
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_Default() -> OpaqueIndexError {
    IndexError::default().into_opaque()
}

/// Returns the number of errors in the IndexError.
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_ErrorCount(error: *const OpaqueIndexError) -> usize {
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    error.error_count()
}

/// Returns the last error message in the IndexError.
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_LastError(error: *const OpaqueIndexError) -> *const c_char {
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    error
        .last_error_with_user_data()
        .map(|s| s.as_ptr())
        .unwrap_or(std::ptr::null())
}

/// Returns the last error message in the IndexError, obfuscated.
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_LastErrorObfuscated(error: *const OpaqueIndexError) -> *const c_char {
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    error
        .last_error_without_user_data()
        .map(|s| s.as_ptr())
        .unwrap_or(std::ptr::null())
}

/// Returns the time of the last error.
///
/// # Parameters
/// - `error`: Pointer to the IndexError
/// - `tv_sec`: Output pointer for seconds component (can be null)
/// - `tv_nsec`: Output pointer for nanoseconds component (can be null)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexError_LastErrorTime(
    error: *const OpaqueIndexError,
    tv_sec: *mut i64,
    tv_nsec: *mut i64,
) {
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    let duration = error.last_error_time();

    if !tv_sec.is_null() {
        unsafe {
            *tv_sec = duration.as_secs() as i64;
        }
    }

    if !tv_nsec.is_null() {
        unsafe {
            *tv_nsec = duration.subsec_nanos() as i64;
        }
    }
}

/// Get the background_indexing_OOM_failure flag.
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_HasBackgroundIndexingOOMFailure(error: *const OpaqueIndexError) -> bool {
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    error.has_background_indexing_oom_failure()
}

/// Change the background_indexing_OOM_failure flag to true.
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_RaiseBackgroundIndexFailureFlag(error: *mut OpaqueIndexError) {
    let error = unsafe { IndexError::from_opaque_mut_ptr(error) }.expect("error is null");
    error.raise_background_indexing_oom_failure();
}

/// Initializes an IndexError. The error_count is set to 0 and the last_error is set to NA.
/// Mirrors C function: `IndexError IndexError_Init()`
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_Init() -> OpaqueIndexError {
    IndexError::default().into_opaque()
}

/// Adds an error message to the IndexError. The error_count is incremented and the last_error is set to the error_message.
/// Mirrors C function: `void IndexError_AddError(IndexError *error, ConstErrorMessage withoutUserData, ConstErrorMessage withUserData, RedisModuleString *key)`
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_AddError(
    ctx: *mut redis_module::raw::RedisModuleCtx,
    error: *mut OpaqueIndexError,
    without_user_data: *const c_char,
    with_user_data: *const c_char,
    key: *mut RedisModuleString,
) {
    let error = unsafe { IndexError::from_opaque_mut_ptr(error) }.expect("error is null");
    unsafe {
        error.add_error(ctx, without_user_data, with_user_data, key);
    }
}

/// Returns the key of the document that caused the error.
/// Mirrors C function: `RedisModuleString *IndexError_LastErrorKey(const IndexError *error)`
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_LastErrorKey(error: *const OpaqueIndexError) -> *mut RedisModuleString {
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    error.key()
}

/// Returns the key of the document that caused the error, obfuscated.
/// Mirrors C function: `RedisModuleString *IndexError_LastErrorKeyObfuscated(const IndexError *error)`
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_LastErrorKeyObfuscated(error: *const OpaqueIndexError) -> *mut RedisModuleString {
    let error = unsafe { IndexError::from_opaque_ptr(error) }.expect("error is null");
    // TODO: Implement obfuscation logic
    error.key()
}

/// Clears an IndexError. If the last_error is not NA, it is freed.
/// Mirrors C function: `void IndexError_Clear(IndexError error)`
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_Clear(ctx: *mut redis_module::raw::RedisModuleCtx, error: OpaqueIndexError) {
    let mut error = unsafe { IndexError::from_opaque(error) };
    unsafe {
        error.clear(ctx);
    }
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
    unsafe {
        index_error::index_error_cleanup(ctx);
    }
}

/// Adds the error message of the other IndexError to the IndexError. The error_count is incremented and the last_error is set to the error_message.
/// This is used when merging errors from different shards in a cluster.
/// Mirrors C function: `void IndexError_Combine(IndexError *error, const IndexError *other)`
#[unsafe(no_mangle)]
pub extern "C" fn IndexError_Combine(
    ctx: *mut redis_module::raw::RedisModuleCtx,
    error: *mut OpaqueIndexError,
    other: *const OpaqueIndexError,
) {
    let error = unsafe { IndexError::from_opaque_mut_ptr(error) }.expect("error is null");
    let other = unsafe { IndexError::from_opaque_ptr(other) }.expect("other is null");
    unsafe {
        error.combine(ctx, other);
    }
}

