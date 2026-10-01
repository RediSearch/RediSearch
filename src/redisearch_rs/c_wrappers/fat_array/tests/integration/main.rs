/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
use fat_array::FatArrayRef;

// Install the mock Redis allocator so the C array functions can allocate, and
// force the combined C bundle to be linked into the test binary.
redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate redisearch_rs;

#[test]
fn borrowed_null_array_is_empty() {
    // SAFETY: a null array is explicitly allowed.
    let borrowed = unsafe { FatArrayRef::new(std::ptr::null::<u32>()) };
    assert!(borrowed.as_slice().is_empty());
}

// These tests call the C array functions, so they cannot run under miri.
#[cfg(not(miri))]
mod c_arrays {
    use super::*;
    use fat_array::FatArray;
    use std::ptr::NonNull;

    /// Build a C fat array holding a copy of `values`.
    fn new_c_array<T: Copy>(values: &[T]) -> NonNull<T> {
        new_c_array_with_spare(values, 0)
    }

    /// Build a C fat array holding a copy of `values`, with room for `spare` more
    /// elements.
    fn new_c_array_with_spare<T: Copy>(values: &[T], spare: u16) -> NonNull<T> {
        let elem_sz = u16::try_from(size_of::<T>()).expect("element too large for a fat array");
        let len = u32::try_from(values.len()).expect("too many elements for a fat array");
        // SAFETY: the element size and length fit the header's fields, as checked
        // above.
        let raw = unsafe { ffi::array_new_sz(elem_sz, spare, len) };
        let ptr = NonNull::new(raw.cast::<T>()).expect("array_new_sz must allocate");
        // SAFETY: the array was just allocated with room for at least `values.len()`
        // elements of `T`, and does not overlap `values`.
        unsafe {
            ptr.as_ptr()
                .copy_from_nonoverlapping(values.as_ptr(), values.len())
        };
        ptr
    }

    /// An element whose size and alignment differ from those of the other
    /// tests, and which contains padding.
    #[derive(Clone, Copy, Debug, PartialEq)]
    #[repr(C)]
    struct Pair {
        a: u8,
        b: u32,
    }

    #[test]
    fn owned_array_exposes_its_elements() {
        // SAFETY: the array is freshly built from `u32`s and owned by no one
        // else.
        let owned = unsafe { FatArray::new(new_c_array(&[1u32, 2, 3])) };
        assert_eq!(owned.as_array_ref().as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn owned_empty_array_is_empty() {
        // SAFETY: the array is freshly built from `u32`s and owned by no one
        // else.
        let owned = unsafe { FatArray::new(new_c_array::<u32>(&[])) };
        assert!(owned.as_array_ref().as_slice().is_empty());
    }

    #[test]
    fn byte_elements_are_not_widened() {
        // SAFETY: the array is freshly built from `u8`s and owned by no one
        // else.
        let owned = unsafe { FatArray::new(new_c_array(&[1u8, 2, 3, 4, 5])) };
        assert_eq!(owned.as_array_ref().as_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn struct_elements_keep_their_stride() {
        let values = [
            Pair { a: 1, b: 10 },
            Pair { a: 2, b: 20 },
            Pair { a: 3, b: 30 },
        ];
        // SAFETY: the array is freshly built from `Pair`s and owned by no one
        // else.
        let owned = unsafe { FatArray::new(new_c_array(&values)) };
        assert_eq!(owned.as_array_ref().as_slice(), &values);
    }

    #[test]
    fn spare_capacity_is_not_exposed() {
        // SAFETY: the array is freshly built from `u32`s and owned by no one
        // else.
        let owned = unsafe { FatArray::new(new_c_array_with_spare(&[8u32, 9], 4)) };
        assert_eq!(owned.as_array_ref().as_slice(), &[8, 9]);
    }

    #[test]
    fn borrowed_c_owned_array_exposes_its_elements() {
        let head = new_c_array(&[4u32, 5]);
        {
            // SAFETY: `head` is a fat array of `u32`s which is freed only after
            // `borrowed` is last used.
            let borrowed = unsafe { FatArrayRef::new(head.as_ptr()) };
            assert_eq!(borrowed.as_slice(), &[4, 5]);
        }
        // SAFETY: `head` is a fat array which nothing else owns or borrows.
        unsafe { ffi::array_free(head.as_ptr().cast()) };
    }

    #[test]
    fn borrowed_copies_share_the_array() {
        // SAFETY: the array is freshly built from `u32`s and owned by no one
        // else.
        let owned = unsafe { FatArray::new(new_c_array(&[6u32, 7])) };
        let borrowed = owned.as_array_ref();
        let copy = borrowed;
        assert_eq!(borrowed.as_slice().as_ptr(), copy.as_slice().as_ptr());
        assert_eq!(copy.as_slice(), &[6, 7]);
    }

    #[cfg(debug_assertions)]
    #[test]
    fn mismatched_element_size_is_caught() {
        let head = new_c_array(&[1u16, 2, 3, 4]);
        let result = std::panic::catch_unwind(|| {
            // SAFETY: this breaks only the element-size requirement, which
            // `new` checks before anything relies on it.
            unsafe { FatArrayRef::new(head.as_ptr().cast::<u32>()) };
        });
        // SAFETY: `head` is a fat array which nothing else owns or borrows.
        unsafe { ffi::array_free(head.as_ptr().cast()) };
        assert!(result.is_err(), "a mismatched element size must panic");
    }

    #[cfg(debug_assertions)]
    #[test]
    fn misaligned_head_is_caught() {
        let head = new_c_array(&[0u8; 8]);
        let result = std::panic::catch_unwind(|| {
            // SAFETY: this breaks only the alignment requirement, which `new`
            // checks before anything relies on it.
            unsafe { FatArrayRef::new(head.as_ptr().wrapping_add(1).cast::<u16>()) };
        });
        // SAFETY: `head` is a fat array which nothing else owns or borrows.
        unsafe { ffi::array_free(head.as_ptr().cast()) };
        assert!(result.is_err(), "a misaligned head must panic");
    }
}
