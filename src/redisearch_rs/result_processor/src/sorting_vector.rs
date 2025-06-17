/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ops::{Deref, Index, IndexMut};

use libc::c_char;

/// A new-type for RSValue that ensures that the reference count is managed correctly.
pub struct RSValue(pub *mut ffi::RSValue);

impl RSValue {
    pub fn as_num(&self) -> Option<f64> {
        if self.is_number() {
            // Safety: We are accessing the num field of an RSValue, which is safe as long as the type matches.
            Some(unsafe { (*self.0).__bindgen_anon_1.numval })
        } else {
            None
        }
    }

    pub fn as_string(&self) -> Option<String> {
        if self.is_string() {
            // Safety: Access to strval is safe as the type is string (checked above)
            let str_ptr = unsafe { (*self.0).__bindgen_anon_1.strval.str_ };
            // Safety: We can safely convert a C string to a Rust string.
            Some(unsafe {
                std::ffi::CStr::from_ptr(str_ptr)
                    .to_string_lossy()
                    .into_owned()
            })
        } else {
            None
        }
    }

    pub fn as_ref(&self) -> Option<RSValue> {
        if self.is_reference() {
            // Safety: We are accessing the ref_ field of an RSValue, which is safe as long as the type matches.
            let ref_ptr = unsafe { (*self.0).__bindgen_anon_1.ref_ };
            Some(RSValue(ref_ptr))
        } else {
            None
        }
    }

    pub fn is_null(&self) -> bool {
        // Safety: We are checking if the RSValue is null, which is safe as the pointer is valid.
        unsafe { ffi::RSValue_IsNull(self.0) == 1 }
    }

    pub fn is_number(&self) -> bool {
        // Safety: Inner pointer is valid as long as the RSValue is living
        let value = unsafe { &*self.0 };
        value.t() == ffi::RSValueType_RSValue_Number
    }

    pub fn is_string(&self) -> bool {
        // Safety: Inner pointer is valid as long as the RSValue is living
        let value = unsafe { &*self.0 };
        value.t() == ffi::RSValueType_RSValue_String
    }

    pub fn is_reference(&self) -> bool {
        // Safety: Inner pointer is valid as long as the RSValue is living
        let value = unsafe { &*self.0 };
        value.t() == ffi::RSValueType_RSValue_Reference
    }

    pub fn value_type(&self) -> ffi::RSValueType {
        // Safety: We are accessing the type of an RSValue, which is safe as long as the pointer is valid.
        unsafe { (*self.0).t() }
    }
}

impl Clone for RSValue {
    fn clone(&self) -> Self {
        // Safety: We are cloning a pointer to an RSValue, which is safe as long as we increment the refcount.
        unsafe { ffi::RSValue_IncrRef(self.0) };
        RSValue(self.0)
    }
}

impl Drop for RSValue {
    fn drop(&mut self) {
        // Safety: We are decrementing the refcount of an RSValue, which is safe as long as we don't use it after this.
        unsafe { ffi::RSValue_Decref(self.0) };
    }
}

impl Deref for RSValue {
    type Target = ffi::RSValue;

    fn deref(&self) -> &Self::Target {
        // Safety: We are dereferencing a pointer to an RSValue, which is safe as long as the pointer is valid.
        unsafe { &*self.0 }
    }
}

/// A [`RSSortingVector`] is a vector of [`RSValue`] pointers.
pub struct RSSortingVector {
    pub values: Box<[RSValue]>,
}

impl RSSortingVector {
    pub fn new(len: usize) -> Self {
        Self {
            // Safety: We are creating a new RSSortingVector with a given length, all values are initialized to null and allocated by C.
            values: vec![RSValue(unsafe { ffi::RS_NullVal() }); len].into_boxed_slice(),
        }
    }

    /// Checks if the index is valid and decrements the reference count of previous RSValue instances.
    /// Returns `true` if the index is valid , `false` otherwise.
    ///
    /// Safety:
    /// If the function returns `true`, the caller must ensure that the RSValue at the index is replaced with a new value.
    pub fn check(&self, idx: usize) -> bool {
        if idx >= self.values.len() {
            return false;
        }

        true
    }

    /// Set a number (double) at the given index
    pub fn put_num(&mut self, idx: usize, num: f64) {
        if !self.check(idx) {
            return;
        }

        // Safety: We are creating a new RSValue with a number, we need to ensure that
        // We ensure the previous value is decremented in `check_and_cleanup`.
        self.values[idx] = unsafe { RSValue(ffi::RS_NumVal(num)) };
    }

    pub fn put_string(&mut self, idx: usize, str: &str) {
        if self.check(idx) {
            return;
        }

        let (ptr, len) = alloc_c_string(str);

        // Safety: We are creating a new RSValue with a number, we need to ensure that
        // We ensure the previous value is decremented in `check_and_cleanup`.
        self.values[idx] =
            unsafe { RSValue(ffi::RS_StringVal(ptr as *mut std::ffi::c_char, len as u32)) };
    }

    pub fn put_string_and_normalize(&mut self, idx: usize, str: &str) {
        if !self.check(idx) {
            return;
        }

        // todo: This uses TWO allocations and will be therefore slower than the C implementation.
        let normalized = str.to_lowercase();
        let (ptr, len) = alloc_c_string(&normalized);

        // Safety: We are creating a new RSValue with a number, we need to ensure that
        // We ensure the previous value is decremented in `check_and_cleanup`.
        self.values[idx] =
            unsafe { RSValue(ffi::RS_StringVal(ptr as *mut std::ffi::c_char, len as u32)) };
    }

    pub fn put_val(&mut self, idx: usize, value: RSValue) {
        if self.check(idx) {
            return;
        }

        self.values[idx] = value;
    }

    pub fn put_null(&mut self, idx: usize) {
        if !self.check(idx) {
            return;
        }

        // Safety: We are creating a new RSValue with a null value, we need to ensure that
        // We ensure the previous value is decremented in `check_and_cleanup`.
        self.values[idx] = unsafe { RSValue(ffi::RS_NullVal()) };
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
            if unsafe { ffi::RSValue_IsNull(self.values[idx].0) } == 1 {
                continue;
            }

            // Each RSValue has a refcount and a value, so we add the size of those
            sz += std::mem::size_of::<ffi::RSValue>();

            let value = walk_down_rsvalue_ref_chain(self.values[idx].0);

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
    origin as *const ffi::RSValue as *mut RSValue
}

impl<I> Index<I> for RSSortingVector
where
    I: std::slice::SliceIndex<[RSValue]>,
{
    type Output = <I as std::slice::SliceIndex<[RSValue]>>::Output;
    fn index(&self, index: I) -> &Self::Output {
        self.values.index(index)
    }
}

impl<I> IndexMut<I> for RSSortingVector
where
    I: std::slice::SliceIndex<[RSValue]>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.values.index_mut(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ffi::c_void;
    redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

    #[test]
    fn test_rssortingvector_creation() {
        let vector = RSSortingVector::new(10);
        assert_eq!(vector.len(), 10);

        // Ensure all values are initialized to null
        for value in vector.values.iter() {
            assert_eq!(unsafe { ffi::RSValue_IsNull(value.0) }, 1);
        }
    }

    #[test]
    fn test_put_num() {
        let mut vector = RSSortingVector::new(3);
        vector.put_num(0, 42.0);
        assert_eq!(vector.values[0].as_num().unwrap(), 42.0);

        // Ensure the rest are still null
        for i in 1..vector.len() {
            assert_eq!(unsafe { ffi::RSValue_IsNull(vector.values[i].0) }, 1);
        }
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
