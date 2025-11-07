use std::{
    alloc::{self, Layout},
    fmt,
    mem::MaybeUninit,
    num::NonZeroUsize,
    os::raw::c_char,
    ptr::{NonNull, copy_nonoverlapping},
    slice,
    sync::Arc,
};

use ffi::{
    RedisModule_Alloc, RedisModule_Free, RedisModule_FreeString, RedisModule_RetainString,
    RedisModule_StringPtrLen, RedisModuleString, context::redisearch_module_context,
};
// use redis_module::{
//     Context, RedisModule_Alloc, RedisModule_Free, RedisModule_StringPtrLen, RedisModuleString,
//     RedisString,
// };

/// An owned, `rm_alloc`'d string. The string
/// need not be UTF-8 or null-terminated. It is basically
/// just an array of bytes annotated with its length.
///
/// # Invariants
/// - `str` points to an `rn_alloc`'d sequence of `len` bytes.
/// - The Redis allocator must be initialized before any [`OwnedRmAllocString`]
///   is created, to avoid panicking on `drop`.
/// - [`RedisModule_Alloc`] must not be mutated for the lifetime of the
///   `OwnedRmAllocString`.
#[repr(C)]
pub struct OwnedRmAllocString {
    str: NonNull<c_char>,
    len: u32,
}

impl OwnedRmAllocString {
    /// Create a new [`OwnedRmAllocString`], taking ownership of the
    /// backing `rm_alloc`'d string.
    ///
    /// # Safety
    /// - `str` must not be aliased
    /// - `str` must have been allocated with `rm_alloc`
    /// - [`RedisModule_Alloc`] must not be mutated for the lifetime of the
    ///   `OwnedRmAllocString`.
    pub const unsafe fn take_unchecked(str: NonNull<c_char>, len: u32) -> Self {
        Self { str, len }
    }

    /// Create a new [`OwnedRmAllocString`] by copying `len` bytes
    /// from `str` into a new `rm_alloc`'d space.
    ///
    /// # Panics
    /// Panics if the Redis allocator is not yet initialized.
    ///
    /// # Safety
    /// - `str` must point to a byte sequence and be valid for `len` reads.
    /// - [`RedisModule_Alloc`] must not be mutated for the lifetime of the
    ///   `OwnedRmAllocString`.
    pub unsafe fn copy_from_string(str: *const c_char, len: u32) -> Self {
        let length = len as usize;
        // Safety: caller must ensure `RedisModule_Alloc` is not mutated
        // while this function runs
        let rm_alloc = unsafe { RedisModule_Alloc.expect("Redis allocator not available") };
        // Safety: call to C function
        let buf = unsafe { rm_alloc(length) } as *mut c_char;

        let buf = NonNull::new(buf).expect("Failed to allocate memory");

        // Safety:
        // - `str` is valid for reads of `length` bytes
        // - `buf` is valid for writes of `length + 1` bytes
        // - `buf` points to a fresh allocation that does not overlap with `str`
        unsafe { copy_nonoverlapping(str, buf.as_ptr(), length) };

        Self { str: buf, len }
    }

    /// Get the string's bytes as a slice of `u8`'s.
    pub const fn as_bytes(&self) -> &[u8] {
        // Safety: `self.str` lives as long as `self`, and
        // the invariants of `OwnedRmAllocString` uphold
        // the requirements of `std::slice::from_raw_parts`
        unsafe { slice::from_raw_parts(self.str.as_ptr() as *const u8, self.len as usize) }
    }
}

impl Clone for OwnedRmAllocString {
    fn clone(&self) -> Self {
        // Safety: the invariants of `OwnedRmAllocString`
        // uphold the safety requirements of `Self::copy_from_string`
        unsafe { Self::copy_from_string(self.str.as_ptr(), self.len) }
    }
}

impl fmt::Debug for OwnedRmAllocString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        String::from_utf8_lossy(self.as_bytes()).fmt(f)
    }
}

impl fmt::Display for OwnedRmAllocString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        String::from_utf8_lossy(self.as_bytes()).fmt(f)
    }
}

impl Drop for OwnedRmAllocString {
    fn drop(&mut self) {
        // Safety: the invariants of `OwnedRmAllocString`
        // uphold the safety requirements of accessing `RedisModule_Free`.
        let rm_free = unsafe { RedisModule_Free.expect("Redis allocator not available") };
        // Safety: call to C function
        unsafe { rm_free(self.str.as_ptr() as *mut _) };
    }
}

// Safety: `OwnedRmAllocString` does not hold data that cannot be sent
// to another thread
unsafe impl Send for OwnedRmAllocString {}
// Safety: `OwnedRmAllocString` does not hold data that cannot be referenced
// from another thread
unsafe impl Sync for OwnedRmAllocString {}

/// A reference to a string constant, annotated
/// with its length.
///
/// # Invariants
/// - `str` must not outlive the [`ConstString`] that wraps it
/// - `str` must point to a byte sequence that is valid for reads of `len` bytes.
#[derive(Clone)]
#[repr(C)]
pub struct ConstString {
    str: *const c_char,
    len: u32,
}

impl ConstString {
    /// Create a new [`ConstString`]
    ///
    /// # Safety
    /// - `str` must not outlive the [`ConstString`] that wraps it
    /// - `str` must point to a byte sequence that is valid for reads of `len` bytes.
    pub const unsafe fn new(str: *const c_char, len: u32) -> Self {
        Self { str, len }
    }

    /// Get the string's bytes as a slice of `u8`'s.
    pub const fn as_bytes(&self) -> &[u8] {
        // Safety: the invariants of `ConstString` uphold the safety requirements
        // of `slice::from_raw_parts`
        unsafe { slice::from_raw_parts(self.str as *const u8, self.len as usize) }
    }
}

impl fmt::Debug for ConstString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        String::from_utf8_lossy(self.as_bytes()).fmt(f)
    }
}

impl fmt::Display for ConstString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        String::from_utf8_lossy(self.as_bytes()).fmt(f)
    }
}

// Safety: [`ConstString`] does not hold data that cannot be sent
// to another thread
unsafe impl Send for ConstString {}

// Safety: [`ConstString`] does not hold data that cannot be referenced
// from another thread
unsafe impl Sync for ConstString {}

/// A reference to a [`RedisModuleString`].
///
/// Holds a [`NonNull<RedisModuleString>`], because `RedisModuleString` is
/// reference counted. Instances of this type do not correspond to
/// an increment of the reference count, and thus do not own the
/// `RedisModuleString`.
///
/// # Invariants
/// - `str` must point to a valid `RedisModuleString`.
/// - The reference count of the [`RedisModuleString`] `str` points to
///   must be at least 1 for the lifetime of the [`RedisStringRef`]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct RedisStringRef {
    str: NonNull<RedisModuleString>,
}

impl RedisStringRef {
    /// Create a new [`BorrowedRedisString`] from a borrowed [`RedisString`].
    /// This does not increment the [`RedisString`]'s reference count.
    ///
    /// # Safety
    /// - The passed pointer must be valid for reads.
    /// - The reference count of the [`RedisModuleString`] `str` points to
    ///   must be at least 1 for the lifetime of the created [`RedisStringRef`]
    pub const unsafe fn new_unchecked(str: NonNull<RedisModuleString>) -> Self {
        Self { str }
    }

    /// Converts `self` into an [`OwnedRedisString`] by incrementing
    /// the reference count.
    pub fn retain(self) -> OwnedRedisString {
        // Safety: the invariants of `OwnedRedisString` uphold
        // the safety requirements of `OwnedRedisString::retain`.
        unsafe { OwnedRedisString::retain(self.str) }
    }
}

// Safety: [`RedisStringRef`] does not hold data that cannot be sent
// to another thread
unsafe impl Send for RedisStringRef {}

// Safety: [`RedisStringRef`] does not hold data that cannot be referenced
// from another thread
unsafe impl Sync for RedisStringRef {}

/// An owned [`RedisModuleString`]
///
/// # Invariants
/// - `str` must point to a valid [`RedisModuleString`]
///   with a reference count of at least 1;
/// - The Redis Module must be initialized
#[repr(transparent)]
pub struct OwnedRedisString {
    str: NonNull<RedisModuleString>,
}

impl OwnedRedisString {
    /// Increment the reference count of the [`RedisModuleString`]
    /// `str` points to.
    ///
    /// # Safety
    /// - `str` must point to a valid [`RedisModuleString`]
    ///   with a reference count of at least 1.
    /// - The Redis Module must be initialized
    pub unsafe fn retain(str: NonNull<RedisModuleString>) -> Self {
        // Safety: caller must ensure that the Redis Module is initialized.
        let ctx = unsafe { redisearch_module_context() };
        // Safety: caller must ensure that the Redis Module is initialized.
        let rm_retain_string = unsafe { RedisModule_RetainString };
        let rm_retain_string = rm_retain_string.expect("Redis module not initialized");

        // Safety: caller must ensure `str` points to a valid `RedisModuleString`
        // with a reference count of at least 1
        unsafe { rm_retain_string(ctx, str.as_ptr()) };

        OwnedRedisString { str }
    }

    /// Takes ownership of the passed `RedisModuleString`
    /// and wraps it in an [`OwnedRedisString`]
    ///
    /// # Safety
    /// - `str` must point to a valid [`RedisModuleString`]
    ///   with a reference count of at least 1.
    pub const unsafe fn take(str: NonNull<RedisModuleString>) -> Self {
        Self { str }
    }

    /// Get the string's bytes as a slice of `u8`'s.
    pub fn as_bytes(&self) -> &[u8] {
        let mut len = MaybeUninit::uninit();
        // Safety: invariants of `OwnedRedisString` ensure that the Redis Module is initialized.
        let rm_str_ptr_len = unsafe { RedisModule_StringPtrLen };
        // Safety: invariants of `OwnedRedisString` ensure that the Redis Module is initialized.
        let rm_str_ptr_len = rm_str_ptr_len.expect("Redis module not initialized");

        // Safety: invariants of `OwnedRedisString` ensure that `self.str` points to a valid
        // `RedisModuleString` with a reference count of at least 1.
        let str_ptr = unsafe { rm_str_ptr_len(self.str.as_ptr(), len.as_mut_ptr()) };
        // Safety: `len` was initialized by the previous call.
        let len = unsafe { len.assume_init() };

        // Safety: `str_ptr` is valid for reads of `len` bytes.
        unsafe { slice::from_raw_parts(str_ptr as *const u8, len) }
    }
}

impl Drop for OwnedRedisString {
    fn drop(&mut self) {
        // Safety: invariants of `OwnedRedisString` ensure that the Redis Module is initialized.
        let ctx = unsafe { redisearch_module_context() };
        // Safety: invariants of `OwnedRedisString` ensure that the Redis Module is initialized.
        let free_string = unsafe { RedisModule_FreeString };
        let free_string = free_string.expect("Redis module not initialized");

        // Safety: the invariants of `OwnedRedisString` ensure
        // `self.str` points to a valid `RedisModuleString` with
        // a reference of at least 1.
        unsafe { free_string(ctx, self.str.as_ptr()) };
    }
}

impl Clone for OwnedRedisString {
    fn clone(&self) -> Self {
        // Safety: the invariants of `OwnedRedisString` uphold
        // the safety requirements of `OwnedRedisString::retain`
        unsafe { OwnedRedisString::retain(self.str) }
    }
}

impl fmt::Debug for OwnedRedisString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        String::from_utf8_lossy(self.as_bytes()).fmt(f)
    }
}

impl fmt::Display for OwnedRedisString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        String::from_utf8_lossy(self.as_bytes()).fmt(f)
    }
}

// Safety: [`RedisStringRef`] does not hold data that cannot be sent
// to another thread
unsafe impl Send for OwnedRedisString {}

// Safety: [`RedisStringRef`] does not hold data that cannot be referenced
// from another thread
unsafe impl Sync for OwnedRedisString {}

/// Data container for `RsValueString`,
/// a pointer to which does not become a
/// fat pointer, unlike `Arc<str>`.
///
/// # Invariants
/// - The string is not empty
/// - The string is valid UTF-8
/// - The string is not aliased
/// - The string is allocated using `alloc::alloc`
/// - The length is at most `isize::MAX`. The reason is that
///   the length is encoded as an `u32`, which on e.g. 32-bit systems can
///   exceed `isize::MAX`, in which case
struct RsValueStringData {
    data: *const u8,
    len: NonZeroUsize,
}

impl RsValueStringData {
    /// # Safety:
    /// - The length of `s` must not be 0
    /// - The length of `s` must not exceed `isize::MAX`
    unsafe fn copy_from_str_unchecked(s: &str) -> Self {
        #[cfg(debug_assertions)]
        let len = NonZeroUsize::new(s.len()).expect("s cannot empty");
        #[cfg(not(debug_assertions))]
        // Safety: caller ensures s is not empty.
        let len = unsafe { NonZeroUsize::new_unchecked(s.len()) };
        debug_assert!(s.len() <= isize::MAX as usize);

        // Safety:
        // Caller ensures that the length of `s` is greater than 0,
        // therefore the layout's size is greater than 0.
        let s_cpy = unsafe { alloc::alloc(Self::layout(s.len())) };
        // Safety:
        // - `s_cpy` results from a fresh alloc, and thus does not overlap with
        //   `s`.
        // - `s` is an `&str`, and as such `s.as_ptr()` is valid for reads of `s.len()` bytes
        // - `s_cpy` has been allocated with a layout of `s.len()` size, making it
        //   valid for writing `s.len()` bytes.
        //
        unsafe { s_cpy.copy_from_nonoverlapping(s.as_ptr(), s.len()) };

        Self { len, data: s_cpy }
    }

    /// # Safety:
    /// - `s` must be a valid pointer to a C `char` sequence of `len` length
    /// - `s` must point to a valid UTF-8 byte sequence
    /// - `len` must not be 0
    /// - `len` must not exceed `isize::MAX`
    unsafe fn copy_from_c_chars_unchecked(s: *const c_char, len: usize) -> Self {
        debug_assert!(!s.is_null());
        debug_assert!(len <= isize::MAX as usize);
        #[cfg(debug_assertions)]
        let len = NonZeroUsize::new(len).expect("s cannot empty");
        #[cfg(not(debug_assertions))]
        // Safety: caller ensures s is not empty.
        let len = unsafe { NonZeroUsize::new_unchecked(s.len()) };

        // Validate UTF-8-ness of s only in debug mode
        #[cfg(debug_assertions)]
        // Safety: caller must ensure `s` points to a byte sequence and is valid for reads
        // if `len` bytes.
        str::from_utf8(unsafe { slice::from_raw_parts(s as *const u8, len.get()) })
            .expect("Invalid UTF-8 sequence");

        // Safety: caller must ensure `len > 0`, guaranteeing that the passed layout's
        // size is greater than 0.
        let s_cpy = unsafe { alloc::alloc(Self::layout(len.get())) };
        assert!(!s_cpy.is_null(), "error allocating memory");

        // Safety:
        // - `s_cpy` is valid for writes of `len` bytes;
        // - `s` is valid for reads of `len` bytes;
        // - `s_cpy` was freshly allocated and does not overlap with `s`.
        unsafe { s_cpy.copy_from_nonoverlapping(s as *const u8, len.get()) };

        Self { len, data: s_cpy }
    }

    /// Get the string's bytes as a slice of `u8`'s.
    pub const fn as_bytes(&self) -> &[u8] {
        // Safety: the invariants of `RsValueStringData` uphold
        // the safety requirements of `slice::from_raw_parts`
        unsafe { slice::from_raw_parts(self.data, self.len.get()) }
    }

    pub const fn as_str(&self) -> &str {
        let s = self.as_bytes();
        // Safety:
        // `s` is obtained from an existing `&str`, or by a call to
        // `Self::copy_from_c_chars_unchecked`, in which case the caller
        // validats that the passed string is valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(s) }
    }

    fn layout(len: usize) -> Layout {
        debug_assert!(len <= isize::MAX as usize);
        Layout::array::<u8>(len).expect("Length exceeds `isize::MAX`")
    }
}

impl Drop for RsValueStringData {
    fn drop(&mut self) {
        // Safety:
        // - the invariants of `RsValueStringData`
        //   uphold that `self.data` is not aliased and has been
        //   allocated with `alloc::alloc`;
        // - the passed layout is calculated the same way as it
        //   was upon allocation.
        unsafe {
            alloc::dealloc(
                self.data as *mut u8,
                Layout::array::<u8>(self.len.get()).unwrap(),
            );
        }
    }
}

impl fmt::Debug for RsValueStringData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl fmt::Display for RsValueStringData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

// Safety: [`RsValueStringData`] does not hold data that cannot be sent
// to another thread
unsafe impl Send for RsValueStringData {}

// Safety: [`RsValueStringData`] does not hold data that cannot be referenced
// from another thread
unsafe impl Sync for RsValueStringData {}

/// A string type optimized for use in [`crate::RsValueInternal`],
/// of which variant data can be at most 12 bytes in size.
///
/// Wraps an `Option<Arc<RsValueStringData>>` because:
/// - an `Arc<str>` would be a fat pointer, making `RsValueString` 16 bytes large;
/// - an `Arc<RsValueStringData>` always allocates, even if the string is empty.
///
/// # Invariants
/// - The string is valid UTF-8
/// - The length is at most `isize::MAX`
#[repr(C)]
#[derive(Clone)]
pub struct RsValueString {
    s: Option<Arc<RsValueStringData>>,
}

impl RsValueString {
    /// Copy
    pub fn copy_from_str(s: &str) -> Result<Self, CreateRsValueStringError> {
        if s.is_empty() {
            return Ok(Self { s: None });
        }

        if s.len() > isize::MAX as usize {
            return Err(CreateRsValueStringError::TooLong);
        }

        if s.len() > u32::MAX as usize {
            return Err(CreateRsValueStringError::TooLong);
        }

        // Safety:
        // We've ensured `s.len` is not zero and does not
        // exceed either `isize::MAX` or `u32::MAX`
        let data = unsafe { RsValueStringData::copy_from_str_unchecked(s) };

        Ok(Self::from_data(data))
    }

    /// # Safety
    /// - `s` must be a valid pointer to a C `char` sequence of `len` length
    /// - `s` must point to a valid UTF-8 byte sequence
    /// - `len` must not exceed `isize::MAX`
    pub unsafe fn copy_from_c_chars(
        s: *const c_char,
        len: usize,
    ) -> Result<Self, CreateRsValueStringError> {
        if len == 0 {
            return Ok(Self { s: None });
        }

        if len > isize::MAX as usize {
            return Err(CreateRsValueStringError::TooLong);
        }

        // Safety:
        // `len` is validated to be greater than zero
        // and to not exceed `isize::MAX`.
        // The caller must ensure the other safety requirements are met.
        let data = unsafe { RsValueStringData::copy_from_c_chars_unchecked(s, len) };

        Ok(Self::from_data(data))
    }

    fn from_data(data: RsValueStringData) -> Self {
        let data = Arc::new(data);
        Self { s: Some(data) }
    }

    pub fn as_str(&self) -> &str {
        if let Some(data) = &self.s {
            // Safety:
            // `ptr` originates from a call to either
            // `Self::copy_from_str` or `copy_from_c_chars` which
            // both either guarantee or require that they result
            // in a valid `RsValueStringData` being owned by `Self`
            data.as_str()
        } else {
            ""
        }
    }
}

impl fmt::Debug for RsValueString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl fmt::Display for RsValueString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, thiserror::Error)]
pub enum CreateRsValueStringError {
    #[error("The input string is too long")]
    TooLong,
}
