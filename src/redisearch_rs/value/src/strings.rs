/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

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
/// - (1) `str` points to an `rn_alloc`'d sequence of `len` bytes.
/// - (2) The Redis allocator must be initialized before any [`OwnedRmAllocString`]
///   is created, to avoid panicking on `drop`.
/// - (3) [`RedisModule_Alloc`] must not be mutated for the lifetime of the
///   `OwnedRmAllocString`.
#[repr(C, packed)]
pub struct OwnedRmAllocString {
    str: NonNull<c_char>,
    len: u32,
}

impl OwnedRmAllocString {
    /// Create a new [`OwnedRmAllocString`], taking ownership of the
    /// backing `rm_alloc`'d string.
    ///
    /// # Safety
    /// - (1) `len` must match the length of `str`;
    /// - (2) `str` must point to a valid, C string with a length of at most `u32::MAX` bytes;
    /// - (3) `str` must not be aliased.
    /// - (4) `str` must have been allocated with `rm_alloc`
    /// - (5) [`RedisModule_Alloc`] must not be mutated for the lifetime of the
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
    /// - (1) `str` must point to a byte sequence and be valid for `len` reads.
    /// - (2) [`RedisModule_Alloc`] must not be mutated for the lifetime of the
    ///   `OwnedRmAllocString`.
    pub unsafe fn copy_from_string(str: *const c_char, len: u32) -> Self {
        let length = len as usize;
        // Safety: caller must ensure (2)
        let rm_alloc = unsafe { RedisModule_Alloc.expect("Redis allocator not available") };
        // Safety: call to C function
        let buf = unsafe { rm_alloc(length) } as *mut c_char;

        let buf = NonNull::new(buf).expect("Failed to allocate memory");

        // Safety:
        // - `str` is valid for reads of `length` bytes (1)
        // - `buf` is valid for writes of `length + 1` bytes
        // - `buf` points to a fresh allocation that does not overlap with `str`
        unsafe { copy_nonoverlapping(str, buf.as_ptr(), length) };

        Self { str: buf, len }
    }

    /// Get the string's bytes as a slice of `u8`'s.
    pub const fn as_bytes(&self) -> &[u8] {
        // Safety: `self.str` lives as long as `self`, and
        // invariant (3) and `self.str` being `NonNull` uphold
        // the requirements of `std::slice::from_raw_parts`
        unsafe { slice::from_raw_parts(self.str.as_ptr() as *const u8, self.len as usize) }
    }
}

impl Clone for OwnedRmAllocString {
    fn clone(&self) -> Self {
        // Safety: invariants (3) and (4)
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
        // Safety: invariants (1) and (2)
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
/// - (1) `str` must not outlive the [`ConstString`] that wraps it
/// - (2) `str` must point to a byte sequence that is valid for reads of `len` bytes.
#[derive(Clone)]
#[repr(C, packed)]
pub struct ConstString {
    str: *const c_char,
    len: u32,
}

impl ConstString {
    /// Create a new [`ConstString`]
    ///
    /// # Safety
    /// - (1) `str` must not outlive the [`ConstString`] that wraps it
    /// - (2) `str` must point to a byte sequence that is valid for reads of `len` bytes.
    pub const unsafe fn new(str: *const c_char, len: u32) -> Self {
        Self { str, len }
    }

    /// Get the string's bytes as a slice of `u8`'s.
    pub const fn as_bytes(&self) -> &[u8] {
        // Safety: invariants (1) and (2) uphold the safety requirements
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
/// - (1) `str` must point to a valid `RedisModuleString`.
/// - (2) The reference count of the [`RedisModuleString`] `str` points to
///   must be at least 1 for the lifetime of the [`RedisStringRef`]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct RedisStringRef {
    str: NonNull<RedisModuleString>,
}

impl RedisStringRef {
    /// Create a new [`RedisStringRef`] from a borrowed [`RedisModuleString`].
    /// This does not increment the [`RedisModuleString`]'s reference count.
    ///
    /// # Safety
    /// - (1) The passed pointer must be valid for reads.
    /// - (2) The reference count of the [`RedisModuleString`] `str` points to
    ///   must be at least 1 for the lifetime of the created [`RedisStringRef`]
    pub const unsafe fn new_unchecked(str: NonNull<RedisModuleString>) -> Self {
        Self { str }
    }

    /// Converts `self` into an [`OwnedRedisString`] by incrementing
    /// the reference count.
    pub fn retain(self) -> OwnedRedisString {
        // Safety: invariants (1) and (2) uphold
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
/// - (1) `str` must point to a valid [`RedisModuleString`]
///   with a reference count of at least 1;
/// - (2) The Redis Module must be initialized
#[repr(transparent)]
pub struct OwnedRedisString {
    str: NonNull<RedisModuleString>,
}

impl OwnedRedisString {
    /// Increment the reference count of the [`RedisModuleString`]
    /// `str` points to.
    ///
    /// # Safety
    /// - (1) `str` must point to a valid [`RedisModuleString`]
    ///   with a reference count of at least 1.
    /// - (2) The Redis Module must be initialized
    pub unsafe fn retain(str: NonNull<RedisModuleString>) -> Self {
        // Safety: caller must ensure (2)
        let ctx = unsafe { redisearch_module_context() };
        // Safety: caller must ensure (2)
        let rm_retain_string = unsafe { RedisModule_RetainString };
        let rm_retain_string = rm_retain_string.expect("Redis module not initialized");

        // Safety: caller must ensure (1)
        unsafe { rm_retain_string(ctx, str.as_ptr()) };

        OwnedRedisString { str }
    }

    /// Takes ownership of the passed `RedisModuleString`
    /// and wraps it in an [`OwnedRedisString`]
    ///
    /// # Safety
    /// - (1) `str` must point to a valid [`RedisModuleString`]
    ///   with a reference count of at least 1.
    pub const unsafe fn take(str: NonNull<RedisModuleString>) -> Self {
        Self { str }
    }

    /// Get the string's bytes as a slice of `u8`'s.
    pub fn as_bytes(&self) -> &[u8] {
        let mut len = MaybeUninit::uninit();
        // Safety: invariant (2).
        let rm_str_ptr_len = unsafe { RedisModule_StringPtrLen };
        // Safety: invariant (2).
        let rm_str_ptr_len = rm_str_ptr_len.expect("Redis module not initialized");

        // Safety: invariant (1).
        let str_ptr = unsafe { rm_str_ptr_len(self.str.as_ptr(), len.as_mut_ptr()) };
        // Safety: `len` was initialized by the previous call.
        let len = unsafe { len.assume_init() };

        // Safety: `str_ptr` is valid for reads of `len` bytes.
        unsafe { slice::from_raw_parts(str_ptr as *const u8, len) }
    }
}

impl Drop for OwnedRedisString {
    fn drop(&mut self) {
        // Safety: invariant (1).
        let ctx = unsafe { redisearch_module_context() };
        // Safety: invariant (1).
        let free_string = unsafe { RedisModule_FreeString };
        let free_string = free_string.expect("Redis module not initialized");

        // Safety: invariant (2).
        unsafe { free_string(ctx, self.str.as_ptr()) };
    }
}

impl Clone for OwnedRedisString {
    fn clone(&self) -> Self {
        // Safety: invariants (1) and (2) uphold
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
/// - (1) The string is not empty, i.e. `len > 0` and `data` is valid for
///   reads of `len` bytes.
/// - (2) The string is valid UTF-8
/// - (3) The string is not aliased
/// - (4) The string is allocated using `alloc::alloc`
/// - (5) The length is at most `isize::MAX`. The reason is that
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
    /// - (1) `s` must be a valid pointer to a C `char` sequence of `len` length
    /// - (2) `s` must point to a valid UTF-8 byte sequence
    /// - (3) `len` must not be 0
    /// - (4) `len` must not exceed `isize::MAX`
    unsafe fn copy_from_c_chars_unchecked(s: *const c_char, len: usize) -> Self {
        debug_assert!(!s.is_null());
        debug_assert!(len <= isize::MAX as usize);
        #[cfg(debug_assertions)]
        let len = NonZeroUsize::new(len).expect("s cannot empty");
        #[cfg(not(debug_assertions))]
        // Safety: caller ensures (3).
        let len = unsafe { NonZeroUsize::new_unchecked(len) };

        // Validate UTF-8-ness of s only in debug mode
        #[cfg(debug_assertions)]
        // Safety: caller must ensure (1)
        str::from_utf8(unsafe { slice::from_raw_parts(s as *const u8, len.get()) })
            .expect("Invalid UTF-8 sequence");

        // Safety: caller must ensure (3), guaranteeing that the passed layout's
        // size is greater than 0.
        let s_cpy = unsafe { alloc::alloc(Self::layout(len.get())) };
        assert!(!s_cpy.is_null(), "error allocating memory");

        // Safety:
        // - `s_cpy` is valid for writes of `len` bytes;
        // - `s` is valid for reads of `len` bytes (1);
        // - `s_cpy` was freshly allocated and does not overlap with `s`.
        unsafe { s_cpy.copy_from_nonoverlapping(s as *const u8, len.get()) };

        Self { len, data: s_cpy }
    }

    /// Get the string's bytes as a slice of `u8`'s.
    pub const fn as_bytes(&self) -> &[u8] {
        // Safety: invariant (1) upholds
        // the safety requirements of `slice::from_raw_parts`
        unsafe { slice::from_raw_parts(self.data, self.len.get()) }
    }

    pub const fn as_str(&self) -> &str {
        let s = self.as_bytes();
        // Safety: invariant (2)
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
        // - the invariant (3) and (4)
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
/// - (1) The string is valid UTF-8
/// - (2) The length is at most `isize::MAX`]
#[repr(C)]
#[derive(Clone)]
pub struct RsValueString {
    s: Option<Arc<RsValueStringData>>,
}

impl RsValueString {
    /// Copy a `str` into a new `RsValueString`.
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
    /// - (1) `s` must be a valid pointer to a C `char` sequence of `len` length
    /// - (2) `s` must point to a valid UTF-8 byte sequence
    /// - (3) `len` must not exceed `isize::MAX`
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

    /// Returns the string as a `&str`.
    pub fn as_str(&self) -> &str {
        if let Some(data) = &self.s {
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
