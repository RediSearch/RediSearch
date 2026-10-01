/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrappers around C fat arrays: `arrayof(T)` values allocated by
//! [`ffi::array_new_sz`] and its siblings, whose length and element size are
//! kept in an [`ffi::array_hdr_t`] stored just before the elements.
//!
//! The pointer C hands out, and which these wrappers take, is the *head* of the
//! array: its first element, not the start of the allocation.

use std::{marker::PhantomData, ptr::NonNull};

/// The largest element alignment a fat array can honor.
///
/// The elements start right after the [`ffi::array_hdr_t`], so their
/// alignment is the smaller of the allocation's alignment and the header's
/// size. The Redis allocator, and the mock one used in tests, align every
/// allocation to at least the header's size, which leaves the header's size as
/// the bound.
const MAX_ELEM_ALIGN: usize = size_of::<ffi::array_hdr_t>();

/// Check, in debug builds, that the fat array headed by `ptr` holds `T`s.
///
/// # Safety
///
/// `ptr` must be the head of a C fat array.
unsafe fn debug_check_layout<T>(ptr: *const T) {
    const { assert!(align_of::<T>() <= MAX_ELEM_ALIGN) };
    debug_assert!(ptr.is_aligned(), "fat array head is misaligned for T");
    // SAFETY: the header sits immediately before the head, in the same
    // allocation, per the caller's guarantee.
    let hdr = unsafe { ptr.cast::<ffi::array_hdr_t>().sub(1) };
    // SAFETY: `hdr` points to the initialized header of a live fat array.
    let elem_sz = unsafe { (*hdr).elem_sz };
    debug_assert_eq!(
        usize::from(elem_sz),
        size_of::<T>(),
        "fat array element size does not match T"
    );
}

/// An owned C fat array, freed with [`ffi::array_free`] on drop.
///
/// `T` is bound by [`Copy`] because dropping the array releases only its
/// storage: no element destructor ever runs. Pointers held by the elements are
/// left untouched.
pub struct FatArray<T: Copy> {
    /// The array's head.
    ptr: NonNull<T>,
    _owner: PhantomData<T>,
}

impl<T: Copy> FatArray<T> {
    /// Take ownership of a C fat array.
    ///
    /// # Safety
    ///
    /// - `ptr` must be the head of a C fat array freeable by
    ///   [`ffi::array_free`], and ownership of that array passes to the
    ///   returned value.
    /// - The array's element size must be `size_of::<T>()` and every element
    ///   must be a valid `T`.
    /// - `ptr` must be aligned for `T`.
    pub unsafe fn new(ptr: NonNull<T>) -> Self {
        // SAFETY: `ptr` is a fat-array head, per the caller.
        unsafe { debug_check_layout(ptr.as_ptr()) };
        Self {
            ptr,
            _owner: PhantomData,
        }
    }

    /// Borrow the owned array.
    pub fn as_array_ref(&self) -> FatArrayRef<'_, T> {
        // SAFETY: `self` holds a fat array of valid, aligned `T`s, per `new`,
        // which lives and stays unmodified for as long as it is borrowed.
        unsafe { FatArrayRef::new(self.ptr.as_ptr()) }
    }
}

impl<T: Copy> Drop for FatArray<T> {
    fn drop(&mut self) {
        // SAFETY: `self` uniquely owns a fat array freeable by `array_free`,
        // per `new`.
        unsafe { ffi::array_free(self.ptr.as_ptr().cast()) };
    }
}

/// A C fat array borrowed for `'a`, owned by someone else. A null array is
/// treated as empty.
#[derive(Clone, Copy)]
pub struct FatArrayRef<'a, T> {
    /// The array's head, or null for an empty array.
    ptr: *const T,
    _borrow: PhantomData<&'a [T]>,
}

impl<'a, T> FatArrayRef<'a, T> {
    /// Borrow a C fat array.
    ///
    /// # Safety
    ///
    /// Unless `ptr` is null:
    /// - `ptr` must be the head of a C fat array which stays alive and
    ///   unmodified for `'a`.
    /// - The array's element size must be `size_of::<T>()` and every element
    ///   must be a valid `T`.
    /// - `ptr` must be aligned for `T`.
    pub unsafe fn new(ptr: *const T) -> Self {
        if !ptr.is_null() {
            // SAFETY: a non-null `ptr` is a fat-array head, per the caller.
            unsafe { debug_check_layout(ptr) };
        }
        Self {
            ptr,
            _borrow: PhantomData,
        }
    }

    /// View the array as a slice, valid for the whole borrow.
    ///
    /// The length is read from the array's header with
    /// [`ffi::array_len_func`].
    pub fn as_slice(self) -> &'a [T] {
        if self.ptr.is_null() {
            return &[];
        }
        // SAFETY: a non-null `ptr` is a fat-array head, per `new`.
        let len = unsafe { ffi::array_len_func(self.ptr.cast_mut().cast()) } as usize;
        // SAFETY: a fat array stores `len` contiguous elements, which are valid,
        // aligned `T`s that stay alive and unmodified for `'a`, per `new`.
        unsafe { std::slice::from_raw_parts(self.ptr, len) }
    }
}
