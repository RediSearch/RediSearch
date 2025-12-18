use std::ffi::CString;
use std::sync::OnceLock;
use std::time::Duration;

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
unsafe impl Sync for SyncRedisModuleString {}

/// Global NA RedisModuleString, equivalent to C's NA_rstr
static NA_RSTR: OnceLock<SyncRedisModuleString> = OnceLock::new();

/// Initialize the IndexError module globals
///
/// # Safety
/// - `ctx` must be a valid RedisModuleCtx pointer
/// - Should only be called once during module initialization
pub unsafe fn index_error_init(ctx: *mut RedisModuleCtx) {
    NA_RSTR.get_or_init(|| {
        let na_cstr = std::ffi::CStr::from_bytes_with_nul(b"N/A\0").unwrap();
        let na_str = unsafe {
            redis_module::raw::RedisModule_CreateString.unwrap()(
                ctx,
                na_cstr.as_ptr(),
                3,
            )
        };
        unsafe {
            redis_module::raw::RedisModule_TrimStringAllocation.unwrap()(na_str);
        }
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
    if let Some(na_str) = NA_RSTR.get() {
        if !na_str.0.is_null() {
            unsafe {
                redis_module::raw::RedisModule_FreeString.unwrap()(ctx, na_str.0);
            }
        }
    }
}

// Note: Clone is intentionally NOT derived for IndexError because the `key` field
// is a reference-counted pointer to RedisModuleString. Cloning would require calling
// RedisModule_HoldString to increment the reference count, which requires a RedisModuleCtx
// parameter that the Clone trait doesn't provide. Attempting to clone without proper
// reference counting would result in double-free bugs when both instances call clear().
#[derive(Debug)]
pub struct IndexError {
    error_count: usize,
    last_error_with_user_data: Option<CString>,
    last_error_without_user_data: Option<CString>,
    key: *mut RedisModuleString,
    last_error_time: Duration,
    background_indexing_oom_failure: bool,
}

impl Default for IndexError {
    fn default() -> Self {
        Self {
            error_count: 0,
            last_error_with_user_data: None,
            last_error_without_user_data: None,
            // Use null pointer for default - proper initialization should use new_with_na()
            // which properly increments the reference count of the NA string.
            key: std::ptr::null_mut(),
            last_error_time: Duration::default(),
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
            // If NA_rstr is not initialized, create it now
            unsafe { index_error_init(ctx) };
            get_na_rstr().expect("NA_rstr should be initialized")
        });

        // Hold the NA key to increment its refcount
        let held_key = unsafe {
            redis_module::raw::RedisModule_HoldString.unwrap()(ctx, na_key)
        };

        Self {
            error_count: 0,
            last_error_with_user_data: None,
            last_error_without_user_data: None,
            key: held_key,
            last_error_time: Duration::default(),
            background_indexing_oom_failure: false,
        }
    }
}

impl IndexError {
    pub const fn error_count(&self) -> usize {
        self.error_count
    }

    pub fn last_error_with_user_data(&self) -> Option<&CString> {
        self.last_error_with_user_data.as_ref()
    }

    pub fn last_error_without_user_data(&self) -> Option<&CString> {
        self.last_error_without_user_data.as_ref()
    }

    pub const fn key(&self) -> *mut RedisModuleString {
        self.key
    }

    pub const fn last_error_time(&self) -> Duration {
        self.last_error_time
    }

    pub const fn has_background_indexing_oom_failure(&self) -> bool {
        self.background_indexing_oom_failure
    }

    pub fn raise_background_indexing_oom_failure(&mut self) {
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
            unsafe {
                redis_module::raw::RedisModule_FreeString.unwrap()(ctx, self.key);
            }
        }

        // Set new error messages (duplicate the C strings if not null)
        if !without_user_data.is_null() {
            let s = unsafe { std::ffi::CStr::from_ptr(without_user_data) }.to_owned();
            self.last_error_without_user_data = Some(s);
        }

        if !with_user_data.is_null() {
            let s = unsafe { std::ffi::CStr::from_ptr(with_user_data) }.to_owned();
            self.last_error_with_user_data = Some(s);
        }

        // Hold the key (increment refcount) and trim allocation
        self.key = unsafe {
            let held_key = redis_module::raw::RedisModule_HoldString.unwrap()(ctx, key);
            redis_module::raw::RedisModule_TrimStringAllocation.unwrap()(held_key);
            held_key
        };

        // Atomically increment error count
        self.error_count = self.error_count.wrapping_add(1);

        // Set the current time as duration since UNIX_EPOCH
        // This matches the C code's use of clock_gettime
        self.last_error_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
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
            unsafe {
                redis_module::raw::RedisModule_FreeString.unwrap()(ctx, self.key);
            }
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
        if self.last_error_time < other.last_error_time {
            // Clear current error messages
            self.last_error_without_user_data = None;
            self.last_error_with_user_data = None;

            // Free the old key
            if !self.key.is_null() {
                unsafe {
                    redis_module::raw::RedisModule_FreeString.unwrap()(ctx, self.key);
                }
            }

            // Copy error messages from other
            self.last_error_without_user_data = other.last_error_without_user_data.clone();
            self.last_error_with_user_data = other.last_error_with_user_data.clone();

            // Hold the other's key (RedisModule_HoldString handles null gracefully)
            // This prevents a dangling pointer when self.key was non-null but other.key is null
            self.key = unsafe {
                redis_module::raw::RedisModule_HoldString.unwrap()(ctx, other.key)
            };

            // Copy timestamp
            self.last_error_time = other.last_error_time;
        }

        // Add error counts
        self.error_count += other.error_count;

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

    unsafe impl Transmute<IndexError> for OpaqueIndexError {}

    c_ffi_utils::opaque!(IndexError, OpaqueIndexError);
}
