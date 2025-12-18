/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::CString;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

pub use redis_module::raw::{RedisModuleCtx, RedisModuleString};

/// Wrapper for RedisModuleString pointer to allow storage in static OnceLock
///
/// # Safety
/// This wrapper implements Send + Sync to match the C code's behavior where NA_rstr
/// is a plain global variable. The safety relies on:
/// 1. The NA string is initialized once and never freed during normal operation
/// 2. All Redis module API calls (HoldString, FreeString) must be protected by
///    the global Redis lock, which the C code assumes
/// 3. The reference counting in RedisModuleString is NOT thread-safe on its own,
///    so callers MUST hold the appropriate Redis locks when calling HoldString/FreeString
///
/// This matches the C implementation which uses a plain `RedisModuleString* NA_rstr`
/// global without any explicit synchronization.
#[derive(Debug, Clone, Copy)]
struct SyncRedisModuleString(*mut RedisModuleString);

// SAFETY: Matches C code behavior. See struct documentation for safety requirements.
unsafe impl Send for SyncRedisModuleString {}
// SAFETY: Matches C code behavior. See struct documentation for safety requirements.
unsafe impl Sync for SyncRedisModuleString {}

/// Global NA RedisModuleString, equivalent to C's NA_rstr
static NA_RSTR: OnceLock<SyncRedisModuleString> = OnceLock::new();

/// Get current time from monotonic clock
///
/// Uses CLOCK_MONOTONIC_RAW to match the C implementation's use of
/// clock_gettime(CLOCK_MONOTONIC_RAW, ...). This is critical because
/// IndexError_Combine compares timestamps to determine which error is newer.
///
/// Using a monotonic clock ensures timestamps are not affected by NTP
/// adjustments or system time changes, making them suitable for comparison.
fn timespec_monotonic_now() -> libc::timespec {
    let mut ts = std::mem::MaybeUninit::uninit();
    // SAFETY: We have exclusive access to a pointer of the correct type
    let ret = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, ts.as_mut_ptr()) };
    if ret == 0 {
        // SAFETY: ts was initialized by clock_gettime
        unsafe { ts.assume_init() }
    } else {
        panic!(
            "Failed to get monotonic time: clock_gettime returned {}",
            ret
        )
    }
}

/// Compare two timespec values (greater than or equal)
///
/// Matches the C implementation of rs_timer_ge from src/util/timeout.h
const fn timespec_ge(a: &libc::timespec, b: &libc::timespec) -> bool {
    if a.tv_sec == b.tv_sec {
        a.tv_nsec >= b.tv_nsec
    } else {
        a.tv_sec >= b.tv_sec
    }
}

/// Initialize the IndexError module globals
///
/// # Safety
/// - `ctx` must be a valid RedisModuleCtx pointer
/// - Should only be called once during module initialization
pub unsafe fn index_error_init(ctx: *mut RedisModuleCtx) {
    NA_RSTR.get_or_init(|| {
        let na_cstr = c"N/A";
        // SAFETY: RedisModule_CreateString is initialized at module startup
        let create_string = unsafe { redis_module::raw::RedisModule_CreateString.unwrap() };
        // SAFETY: ctx is valid (caller requirement), na_cstr is a valid C string, length is correct
        let na_str = unsafe { create_string(ctx, na_cstr.as_ptr(), 3) };

        // SAFETY: RedisModule_TrimStringAllocation is initialized at module startup
        let trim_allocation =
            unsafe { redis_module::raw::RedisModule_TrimStringAllocation.unwrap() };
        // SAFETY: na_str is a valid RedisModuleString just created
        unsafe { trim_allocation(na_str) };

        SyncRedisModuleString(na_str)
    });
}

/// Get the global NA RedisModuleString
///
/// Returns None if not initialized
pub fn get_na_rstr() -> Option<*mut RedisModuleString> {
    NA_RSTR.get().map(|s| s.0)
}

/// Cleanup the IndexError module globals
///
/// # Safety
/// - `ctx` must be a valid RedisModuleCtx pointer
/// - Should only be called during module shutdown
pub unsafe fn index_error_cleanup(ctx: *mut RedisModuleCtx) {
    if let Some(na_str) = NA_RSTR.get()
        && !na_str.0.is_null()
    {
        // SAFETY: RedisModule_FreeString is initialized at module startup
        let free_string = unsafe { redis_module::raw::RedisModule_FreeString.unwrap() };
        // SAFETY: ctx is valid (caller requirement), na_str.0 is a valid RedisModuleString
        unsafe { free_string(ctx, na_str.0) };
    }
}

// Note: Clone is intentionally NOT derived for IndexError because the `key` field
// is a reference-counted pointer to RedisModuleString. Cloning would require calling
// RedisModule_HoldString to increment the reference count, which requires a RedisModuleCtx
// parameter that the Clone trait doesn't provide. Attempting to clone without proper
// reference counting would result in double-free bugs when both instances call clear().
#[derive(Debug)]
pub struct IndexError {
    // Use AtomicUsize to match C's __atomic_add_fetch behavior
    // The C code explicitly uses atomics "since this might be called when spec is unlocked"
    error_count: AtomicUsize,
    last_error_with_user_data: Option<CString>,
    last_error_without_user_data: Option<CString>,
    key: *mut RedisModuleString,
    // Use libc::timespec to match C's struct timespec
    // Must use CLOCK_MONOTONIC_RAW for compatibility with C code
    last_error_time: libc::timespec,
    background_indexing_oom_failure: bool,
}

impl Default for IndexError {
    fn default() -> Self {
        Self {
            error_count: AtomicUsize::new(0),
            last_error_with_user_data: None,
            last_error_without_user_data: None,
            // Use null pointer for default - proper initialization should use new_with_na()
            // which properly increments the reference count of the NA string.
            key: std::ptr::null_mut(),
            last_error_time: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            background_indexing_oom_failure: false,
        }
    }
}

impl IndexError {
    /// Create a new IndexError with NA_rstr as the key
    ///
    /// # Safety
    /// - `ctx` must be a valid RedisModuleCtx pointer
    /// - NA_rstr must be initialized via `index_error_init`
    pub unsafe fn new_with_na(ctx: *mut RedisModuleCtx) -> Self {
        let na_key = get_na_rstr().unwrap_or_else(|| {
            // SAFETY: ctx is valid (caller requirement)
            unsafe { index_error_init(ctx) };
            get_na_rstr().expect("NA_rstr should be initialized")
        });

        // SAFETY: RedisModule_HoldString is initialized at module startup
        let hold_string = unsafe { redis_module::raw::RedisModule_HoldString.unwrap() };
        // SAFETY: ctx is valid (caller requirement), na_key is a valid RedisModuleString
        let held_key = unsafe { hold_string(ctx, na_key) };

        Self {
            error_count: AtomicUsize::new(0),
            last_error_with_user_data: None,
            last_error_without_user_data: None,
            key: held_key,
            last_error_time: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            background_indexing_oom_failure: false,
        }
    }
}

impl IndexError {
    pub fn error_count(&self) -> usize {
        self.error_count.load(Ordering::Relaxed)
    }

    pub const fn last_error_with_user_data(&self) -> Option<&CString> {
        self.last_error_with_user_data.as_ref()
    }

    pub const fn last_error_without_user_data(&self) -> Option<&CString> {
        self.last_error_without_user_data.as_ref()
    }

    /// Get the raw key pointer without incrementing reference count
    ///
    /// # Safety
    /// The caller must ensure the key is not freed while in use.
    /// For FFI functions that return keys to C callers, use `last_error_key_held` instead.
    pub const fn key(&self) -> *mut RedisModuleString {
        self.key
    }

    /// Get the key with incremented reference count
    ///
    /// This matches the C implementation of IndexError_LastErrorKey which calls
    /// RedisModule_HoldString so the caller can always call FreeString.
    ///
    /// # Safety
    /// - `ctx` must be a valid RedisModuleCtx pointer
    pub unsafe fn last_error_key_held(&self, ctx: *mut RedisModuleCtx) -> *mut RedisModuleString {
        // SAFETY: RedisModule_HoldString is initialized at module startup
        let hold_string = unsafe { redis_module::raw::RedisModule_HoldString.unwrap() };
        // SAFETY: ctx is valid (caller requirement), self.key is a valid RedisModuleString
        unsafe { hold_string(ctx, self.key) }
    }

    pub const fn last_error_time(&self) -> libc::timespec {
        self.last_error_time
    }

    pub const fn has_background_indexing_oom_failure(&self) -> bool {
        self.background_indexing_oom_failure
    }

    pub const fn raise_background_indexing_oom_failure(&mut self) {
        self.background_indexing_oom_failure = true;
    }

    /// Add an error to the IndexError.
    /// This clears any previous error and sets the new error messages and key.
    ///
    /// # Safety
    /// - `ctx` must be a valid RedisModuleCtx pointer
    /// - `without_user_data` and `with_user_data` must be valid C strings or null
    /// - `key` must be a valid RedisModuleString pointer
    pub unsafe fn add_error(
        &mut self,
        ctx: *mut RedisModuleCtx,
        without_user_data: *const std::os::raw::c_char,
        with_user_data: *const std::os::raw::c_char,
        key: *mut RedisModuleString,
    ) {
        // Clear previous error messages
        self.last_error_without_user_data = None;
        self.last_error_with_user_data = None;

        // Free the old key if it exists
        if !self.key.is_null() {
            // SAFETY: RedisModule_FreeString is initialized at module startup
            let free_string = unsafe { redis_module::raw::RedisModule_FreeString.unwrap() };
            // SAFETY: ctx is valid (caller requirement), self.key is a valid RedisModuleString
            unsafe { free_string(ctx, self.key) };
        }

        // Set new error messages (duplicate the C strings if not null)
        if !without_user_data.is_null() {
            // SAFETY: without_user_data is a valid C string (caller requirement)
            let s = unsafe { std::ffi::CStr::from_ptr(without_user_data) }.to_owned();
            self.last_error_without_user_data = Some(s);
        }

        if !with_user_data.is_null() {
            // SAFETY: with_user_data is a valid C string (caller requirement)
            let s = unsafe { std::ffi::CStr::from_ptr(with_user_data) }.to_owned();
            self.last_error_with_user_data = Some(s);
        }

        // Hold the key (increment refcount) and trim allocation
        // SAFETY: RedisModule_HoldString is initialized at module startup
        let hold_string = unsafe { redis_module::raw::RedisModule_HoldString.unwrap() };
        // SAFETY: ctx is valid (caller requirement), key is a valid RedisModuleString (caller requirement)
        let held_key = unsafe { hold_string(ctx, key) };

        // SAFETY: RedisModule_TrimStringAllocation is initialized at module startup
        let trim_allocation =
            unsafe { redis_module::raw::RedisModule_TrimStringAllocation.unwrap() };
        // SAFETY: held_key is a valid RedisModuleString just created
        unsafe { trim_allocation(held_key) };

        self.key = held_key;

        // Atomically increment error count by 1
        // Uses Relaxed ordering to match C's __ATOMIC_RELAXED
        // This is safe because error_count updates don't need to synchronize with other operations
        self.error_count.fetch_add(1, Ordering::Relaxed);

        // Set the current time using CLOCK_MONOTONIC_RAW to match C code
        // The C code uses: clock_gettime(CLOCK_MONOTONIC_RAW, &error->last_error_time)
        // This is critical for IndexError_Combine which compares timestamps
        self.last_error_time = timespec_monotonic_now();
    }

    /// Clear the IndexError and free resources.
    ///
    /// # Safety
    /// - `ctx` must be a valid RedisModuleCtx pointer
    pub unsafe fn clear(&mut self, ctx: *mut RedisModuleCtx) {
        // Clear error messages (Rust will drop the CStrings automatically)
        self.last_error_without_user_data = None;
        self.last_error_with_user_data = None;

        // Free the key
        if !self.key.is_null() {
            // SAFETY: RedisModule_FreeString is initialized at module startup
            let free_string = unsafe { redis_module::raw::RedisModule_FreeString.unwrap() };
            // SAFETY: ctx is valid (caller requirement), self.key is a valid RedisModuleString
            unsafe { free_string(ctx, self.key) };
            self.key = std::ptr::null_mut();
        }
    }

    /// Combine this error with another error.
    /// If the other error is newer, prefer it.
    ///
    /// # Safety
    /// - `ctx` must be a valid RedisModuleCtx pointer
    /// - `other` must be a valid IndexError reference
    pub unsafe fn combine(&mut self, ctx: *mut RedisModuleCtx, other: &IndexError) {
        // If other error is newer (has a later timestamp), prefer it
        // Matches C code: if (!rs_timer_ge(&error->last_error_time, &other->last_error_time))
        if !timespec_ge(&self.last_error_time, &other.last_error_time) {
            // Clear current error messages
            self.last_error_without_user_data = None;
            self.last_error_with_user_data = None;

            // Free the old key
            if !self.key.is_null() {
                // SAFETY: RedisModule_FreeString is initialized at module startup
                let free_string = unsafe { redis_module::raw::RedisModule_FreeString.unwrap() };
                // SAFETY: ctx is valid (caller requirement), self.key is a valid RedisModuleString
                unsafe { free_string(ctx, self.key) };
            }

            // Copy error messages from other
            self.last_error_without_user_data = other.last_error_without_user_data.clone();
            self.last_error_with_user_data = other.last_error_with_user_data.clone();

            // Hold the other's key (RedisModule_HoldString handles null gracefully)
            // This prevents a dangling pointer when self.key was non-null but other.key is null
            // SAFETY: RedisModule_HoldString is initialized at module startup
            let hold_string = unsafe { redis_module::raw::RedisModule_HoldString.unwrap() };
            // SAFETY: ctx is valid (caller requirement), other.key is a valid RedisModuleString
            self.key = unsafe { hold_string(ctx, other.key) };

            // Copy timestamp
            self.last_error_time = other.last_error_time;
        }

        // Add error counts atomically
        self.error_count
            .fetch_add(other.error_count.load(Ordering::Relaxed), Ordering::Relaxed);

        // Combine OOM failure flags
        self.background_indexing_oom_failure |= other.background_indexing_oom_failure;
    }
}

pub mod opaque {
    use super::IndexError;
    use c_ffi_utils::opaque::{Size, Transmute};

    /// An opaque index error which can be passed by value to C.
    ///
    /// The size and alignment of this struct must match the Rust `IndexError`
    /// structure exactly.
    #[repr(C, align(8))]
    pub struct OpaqueIndexError(Size<72>);

    // SAFETY: OpaqueIndexError has the same size and alignment as IndexError
    // This is verified by the c_ffi_utils::opaque! macro below
    unsafe impl Transmute<IndexError> for OpaqueIndexError {}

    c_ffi_utils::opaque!(IndexError, OpaqueIndexError);
}
