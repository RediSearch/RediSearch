/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ops::{Index, IndexMut};

use ffi::RSValue;
use libc::c_char;

/// A [`RSSortingVector`] is a vector of [`RSValue`] pointers.
pub struct RSSortingVector {
    values: Box<[*mut ffi::RSValue]>,
}

impl RSSortingVector {
    pub fn new(len: usize) -> Self {
        Self {
            // Safety: The C side is responsible for allocating the memory for the RSValue instances,
            // and we initialize them with null values of the C side.
            values: vec![unsafe { ffi::RS_NullVal() }; len].into_boxed_slice(),
        }
    }

    /// Checks if the index is valid and decrements the reference count of previous RSValue instances.
    /// Returns `true` if the index is valid , `false` otherwise.
    ///
    /// Safety:
    /// If the function returns `true`, the caller must ensure that the RSValue at the index is replaced with a new value.
    fn check_and_cleanup(&self, idx: usize) -> bool {
        if idx >= self.values.len() {
            return false;
        }

        let value = self.values[idx];
        // Safety: We have a valid pointer to an RSValue, we can safely decrement the refcount.
        // We need to ensure that the value will get replaced after calling this.
        unsafe { ffi::RSValue_Decref(value) };
        true
    }

    /// Set a number (double) at the given index
    pub fn put_num(&mut self, idx: usize, num: f64) {
        if !self.check_and_cleanup(idx) {
            return;
        }

        // Safety: We are creating a new RSValue with a number, we need to ensure that
        // We ensure the previous value is decremented in `check_and_cleanup`.
        self.values[idx] = unsafe { ffi::RS_NumVal(num) };
    }

    pub fn put_string(&mut self, idx: usize, str: &str) {
        if self.check_and_cleanup(idx) {
            return;
        }

        let (ptr, len) = alloc_c_string(str);

        // Safety: We are creating a new RSValue with a number, we need to ensure that
        // We ensure the previous value is decremented in `check_and_cleanup`.
        self.values[idx] = unsafe { ffi::RS_StringVal(ptr as *mut std::ffi::c_char, len as u32) };
    }

    pub fn put_string_and_normalize(&mut self, idx: usize, str: &str) {
        if !self.check_and_cleanup(idx) {
            return;
        }

        // todo: This uses TWO allocations and will be therefore slower than the C implementation.
        let normalized = str.to_lowercase();
        let (ptr, len) = alloc_c_string(&normalized);

        // Safety: We are creating a new RSValue with a number, we need to ensure that
        // We ensure the previous value is decremented in `check_and_cleanup`.
        self.values[idx] = unsafe { ffi::RS_StringVal(ptr as *mut std::ffi::c_char, len as u32) };
    }

    pub fn put_val(&mut self, idx: usize, value: *mut ffi::RSValue) {
        if self.check_and_cleanup(idx) {
            return;
        }

        self.values[idx] = value;
    }

    pub fn put_null(&mut self, idx: usize) {
        if !self.check_and_cleanup(idx) {
            return;
        }

        // Safety: We are creating a new RSValue with a null value, we need to ensure that
        // We ensure the previous value is decremented in `check_and_cleanup`.
        self.values[idx] = unsafe { ffi::RS_NullVal() };
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get_memory_size(&self) -> usize {
        // Each RSValue is a pointer, so we multiply by the size of a pointer
        let mut sz = self.values.len() * std::mem::size_of::<*mut ffi::RSValue>();

        for idx in 0..self.values.len() {
            // Safety: The c-side is using a global pointer to identify the RSValue that is null.
            if unsafe { ffi::RSValue_IsNull(self.values[idx]) } == 1 {
                continue;
            }

            // Each RSValue has a refcount and a value, so we add the size of those
            sz += std::mem::size_of::<ffi::RSValue>();

            let value = walk_down_rsvalue_ref_chain(self.values[idx]);

            // Safety: During creation in ['RSSortingVector::new'], we ensure that all values are valid pointers.
            let value = unsafe { &*value };

            if value.t() == ffi::RSValueType_RSValue_String {
                // Safety: The union field `strval` is valid for `RSValueType_RSValue_String`.
                let str: *mut std::os::raw::c_char = unsafe { value.__bindgen_anon_1.strval.str_ };

                // Safety: We can safely use `libc::strlen` here because we are guaranteed that the string is null-terminated.
                let str_len = unsafe { libc::strlen(str) } as usize;
                sz += str_len;
            }
        }
        sz
    }
}

/// We use that to ensure that the allocation of the strings is with the C allocator,
fn alloc_c_string(s: &str) -> (*mut c_char, libc::size_t) {
    let len = s.len();

    // Safety: len is a valid size for allocation, and we allocate enough space for the string plus a null terminator.
    let ptr: *mut u8 = unsafe { libc::calloc(1, len + 1) }.cast();

    if ptr.is_null() {
        panic!("Failed to allocate memory");
    }

    // Safety: `ptr` is valid and large enough.
    unsafe {
        std::ptr::copy_nonoverlapping(s.as_ptr(), ptr, len);
    }

    // Safety: we know that `ptr` has length `len + 1`, so we can safely add `len` to it.
    let ptr_last = unsafe {
        ptr.add(len) // Ensure null termination
    };

    // Safety: We can safely write a null terminator at the end of the string.
    unsafe {
        *ptr_last = 0; // Null-terminate the string
    }

    (ptr as *mut c_char, len)
}

/// Walks down the reference chain of a `RSValue` until it reaches a non-reference value.
fn walk_down_rsvalue_ref_chain(origin: *mut ffi::RSValue) -> *mut RSValue {
    // Safety: The `origin` pointer must be a valid pointer to an `RSValue`.
    let mut origin = unsafe { &*origin };
    while origin.t() == ffi::RSValueType_RSValue_Reference {
        // Safety: The union field `ref_` is valid for `RSValueType_RSValue_Reference`.
        let tmp = unsafe { origin.__bindgen_anon_1.ref_ };

        // Safety: We can safely dereference `tmp` because it is a valid pointer to an `RSValue`.
        origin = unsafe { &*tmp };
    }
    origin as *const RSValue as *mut RSValue
}

impl<I> Index<I> for RSSortingVector
where
    I: std::slice::SliceIndex<[*mut ffi::RSValue]>,
{
    type Output = <[*mut ffi::RSValue] as std::ops::Index<I>>::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.values.index(index)
    }
}

impl<I> IndexMut<I> for RSSortingVector
where
    I: std::slice::SliceIndex<[*mut ffi::RSValue]>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.values.index_mut(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rssortingvector_creation() {
        let vector = RSSortingVector::new(10);
        assert_eq!(vector.len(), 10);
    }

    #[test]
    fn test_normlize_in_c_equals_rust_impl() {
        let cstr = std::ffi::CString::new("Hello, World!").unwrap();
        let rstr = cstr.to_str().unwrap();

        let rimpl = rstr.to_lowercase();
        // todo: fix linker error:
        //let cimpl = unsafe { ffi::normalizeStr(cstr.as_ptr().cast_mut()) };
    }
}

mod c_layer {
    use std::{ffi::c_char, ptr::NonNull};

    use super::RSSortingVector;

    pub const RS_SORTABLES_MAX: usize = 1024;

    /// Gets a RSValue from the sorting vector at the given index.
    ///
    /// Safety:
    /// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`].
    #[unsafe(no_mangle)]
    unsafe extern "C" fn RSSortingVector_Get(
        mut vec: NonNull<RSSortingVector>,
        idx: libc::size_t,
    ) -> *mut ffi::RSValue {
        // Safety: Caller must ensure 1. --> Deref is safe
        let vec = unsafe { vec.as_mut() };
        if idx >= vec.len() {
            return std::ptr::null_mut();
        }

        vec.values[idx]
    }

    /// Returns the length of the sorting vector.
    ///
    /// Safety:
    /// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`] or null.
    #[unsafe(no_mangle)]
    unsafe extern "C" fn RSSortingVector_GetLength(vec: *const RSSortingVector) -> libc::size_t {
        if vec.is_null() {
            return 0;
        }

        // Safety: Caller must ensure 1. --> Deref is safe, we checked for null above
        unsafe { vec.as_ref() }.unwrap().len() as libc::size_t
    }

    /// Returns the memory size of the sorting vector.
    ///  
    /// Safety:
    /// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`].
    #[unsafe(no_mangle)]
    unsafe extern "C" fn RSSortingVector_GetMemorySize(
        vector: NonNull<RSSortingVector>,
    ) -> libc::size_t {
        // Safety: Caller must ensure 1. --> Deref is safe
        unsafe { vector.as_ref() }.get_memory_size() as libc::size_t
    }

    /// Puts a number (double) at the given index in the sorting vector.
    ///
    /// Safety:
    /// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`].
    /// 2. The `idx` must be a valid index within the bounds of the sorting vector.
    #[unsafe(no_mangle)]
    unsafe extern "C" fn RSSortingVector_PutNum(
        mut vec: NonNull<RSSortingVector>,
        idx: libc::size_t,
        num: f64,
    ) {
        // Safety: Caller must ensure 1. --> Deref is safe
        unsafe { vec.as_mut() }.put_num(idx, num);
    }

    /// Puts a string at the given index in the sorting vector.
    ///
    /// This function will normalize the string to lowercase and use utf normalization for sorting if `is_normalized` is true.
    ///
    /// Internally it uses `libc` functions to allocate and copy the string, ensuring that string allocation and deallocation
    /// all happen on the C side for now.
    ///
    /// Safety:
    /// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`].
    /// 2. The `str` pointer must point to a valid C string (null-terminated).
    /// 3. The `idx` must be a valid index within the bounds of the sorting vector.
    #[unsafe(no_mangle)]
    unsafe extern "C" fn RSSortingVector_PutStr(
        mut vec: NonNull<RSSortingVector>,
        idx: libc::size_t,
        str: *const c_char,
        is_normalized: bool,
    ) {
        // Safety: Caller must ensure 1. --> Deref is safe
        let vec = unsafe { vec.as_mut() };
        // ffi-side impl ensures to allocate strings from the c allocator, so we can safely use
        if !vec.check_and_cleanup(idx) {
            return;
        }

        if is_normalized {
            // Safety: Caller must ensure 2. --> We have a valid C string pointer for duplication
            let dupl = unsafe { libc::strdup(str) };

            // Safety: dupl is a valid pointer to a C string -> we can safely call `libc::strlen`
            let strlen = unsafe { libc::strlen(dupl) };

            // Safety: THe constructor of RString Value with a c str and the right string length
            vec.values[idx] = unsafe { ffi::RS_StringVal(dupl, strlen as u32) };
        } else {
            // Safety: Caller must ensure 2. --> We have a valid C string pointer to create a CStr
            let c_str = unsafe { std::ffi::CStr::from_ptr(str) };
            let str_slice = c_str.to_str().unwrap_or("");
            vec.put_string_and_normalize(idx, str_slice);
        }
    }

    /// Puts a value at the given index in the sorting vector.
    ///
    /// Safety:
    /// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`].
    /// 2. The `val` pointer must point to a valid `RSValue` instance.
    /// 3. The `idx` must be a valid index within the bounds of the sorting vector.
    #[unsafe(no_mangle)]
    unsafe extern "C" fn RSSortingVector_PutRSVal(
        mut vec: NonNull<RSSortingVector>,
        idx: libc::size_t,
        val: *mut ffi::RSValue,
    ) {
        // Safety: Caller must ensure 1. --> Deref is safe
        unsafe {
            vec.as_mut().put_val(idx, val);
        }
    }

    #[unsafe(no_mangle)]
    unsafe extern "C" fn NewSortingVector(len: libc::size_t) -> *mut RSSortingVector {
        if len > RS_SORTABLES_MAX {
            return std::ptr::null_mut();
        }

        let vector = RSSortingVector::new(len);
        Box::into_raw(Box::new(vector))
    }

    /// Reduces the refcount of every value and frees the memory allocated for an `RSSortingVector`.
    /// Called by the C code to deallocate the vector.
    ///
    /// Safety:
    /// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`].
    /// 2. The pointer must not have been freed before this call to avoid double free.
    #[unsafe(no_mangle)]
    unsafe extern "C" fn SortingVector_Free(mut vector: NonNull<RSSortingVector>) {
        // Safety: Caller must ensure 1. --> Deref is safe
        let vector = unsafe { vector.as_mut() };

        for idx in 0..vector.len() {
            // Safety: We have valid RSValue pointers in the vector as the vector owns them.
            unsafe {
                ffi::RSValue_Decref(vector.values[idx]);
            }
        }

        // Safety:
        // Condition 1 --> Ensures this is a valid pointer to an RSSortingVector created by RSSortingVector_New
        // Condition 2 --> Ensures that there is no double free
        let _ = unsafe { Box::from_raw(vector) };
    }
}
