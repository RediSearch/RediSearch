/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::alloc::{Layout, alloc, alloc_zeroed, dealloc, realloc};
use std::ffi::c_void;
use std::ptr;

const ALIGNMENT: usize = 2 * std::mem::align_of::<usize>();
const HEADER_SIZE: usize = 2 * std::mem::size_of::<usize>();

#[inline]
fn layout_for(total: usize) -> Layout {
    debug_assert!(total > 0);
    debug_assert!(total < (isize::MAX / 2) as usize);
    Layout::from_size_align(total, ALIGNMENT).unwrap()
}

/// Allocates the required memory of `size` bytes for the caller usage. The caller must invoke `free_shim`
/// to free the memory when it is no longer needed.
///
/// The allocator includes an additional header to keep track of the requested size.
/// That size information will be required, later on, if reallocating or deallocating the
/// buffer that this function returns.
///
/// If the reallocation fails and the size is non-zero, it panics.
///
/// The pointer returned by this function points right after the header slot, which is
/// invisible to the caller. Or it returns a null pointer if the size is zero.
///
/// Safety:
/// 1. The caller must ensure that the pointer returned by this function is freed using `free_shim` and
///    not another free function.
pub extern "C" fn alloc_shim(size: usize) -> *mut c_void {
    if size == 0 {
        return ptr::null_mut();
    }

    let alloc_size = match size.checked_add(HEADER_SIZE) {
        Some(n) => n,
        None => return ptr::null_mut(),
    };

    // Safety: `alloc` is called with a valid `Layout` of non zero size and returns
    // a pointer to `alloc_size` bytes aligned to `ALIGNMENT` or null on OOM.
    let base = unsafe { alloc(layout_for(alloc_size)) };
    if base.is_null() {
        return ptr::null_mut();
    }

    let h0 = base as *mut usize;
    // Safety: `h0` points into the allocated block and we write exactly one `usize`
    // that fits within the first `HEADER_SIZE` bytes we reserved for the header.
    unsafe { *h0 = alloc_size };

    let h1 = base.wrapping_add(std::mem::size_of::<usize>()) as *mut usize;
    // Safety: `h1` is within the allocated header region and disjoint from `h0`.
    // Since the header is two words-long, and we only need one word
    // for required metadata (i.e. the actual allocated space),
    // we can use the second word to keep track of additional information
    // that can be handy when troubleshooting, such as the size
    // requested by the user.
    unsafe { *h1 = size };

    // Compute user pointer just after the header while preserving alignment.
    base.wrapping_add(HEADER_SIZE) as *mut c_void
}

/// Allocates the required memory of `size`*`count` bytes for the caller usage. The caller must invoke `free_shim`
/// to free the memory when it is no longer needed.
///
/// The function behaves like [alloc_shim] but besides the header slots initializes the allocated memory to zero.
///
/// That also implicates that if either `size` or `count` is zero, the function returns a null pointer.
///
/// Safety:
/// 1. The caller must ensure that pointers returned by this function are freed using `free_shim`.
pub extern "C" fn calloc_shim(count: usize, size: usize) -> *mut c_void {
    let req = match count.checked_mul(size) {
        Some(n) if n > 0 => n,
        _ => return ptr::null_mut(),
    };

    let alloc_size = match req.checked_add(HEADER_SIZE) {
        Some(n) => n,
        None => return ptr::null_mut(),
    };

    // Safety: `alloc_zeroed` is called with a valid non zero `Layout` and returns
    // a zero initialized block aligned to `ALIGNMENT` or null on OOM.
    let base = unsafe { alloc_zeroed(layout_for(alloc_size)) };
    if base.is_null() {
        return ptr::null_mut();
    }

    let h0 = base as *mut usize;
    // Safety: `h0` is within the allocated header region.
    unsafe { *h0 = alloc_size };

    let h1 = base.wrapping_add(std::mem::size_of::<usize>()) as *mut usize;
    // Safety: `h1` is within the allocated header region and disjoint from `h0`.
    // Since the header is two words-long, and we only need one word
    // for required metadata (i.e. the actual allocated space),
    // we can use the second word to keep track of additional information
    // that can be handy when troubleshooting, such as the size
    // requested by the user.
    unsafe { *h1 = req };

    // pointer after header, alignment preserved since HEADER_SIZE is a multiple of ALIGNMENT.
    base.wrapping_add(HEADER_SIZE) as *mut c_void
}

/// Frees the memory allocated by the `alloc_shim`, `calloc_shim` or `realloc_shim` functions.
///
/// It retrieves the original pointer by subtracting the header size from the given pointer.
/// The function then retrieves the size from the first 8 bytes of the original pointer.
/// Finally, it deallocates the memory using Rust's `dealloc` function, which uses free from libc.
/// The function does nothing if the pointer is null.
///
/// Safety:
/// 1. The caller must ensure that the pointer is valid and was allocated by `alloc_shim`.
pub extern "C" fn free_shim(ptr_user: *mut c_void) {
    if ptr_user.is_null() {
        return;
    }

    let user = ptr_user as *mut u8;
    let base = user.wrapping_sub(HEADER_SIZE);

    let h0 = base as *mut usize;

    // Safety: `h0` points to the header we wrote at allocation time.
    let alloc_size = unsafe { *h0 };

    // Safety: We pass the exact `Layout` used to allocate `base`, satisfying the allocator contract.
    unsafe { dealloc(base, layout_for(alloc_size)) }
}

/// Reallocates the required memory of `size` bytes for the caller usage. The `ptr` must be created
/// via `alloc_shim`, `calloc_shim` or `realloc_shim` functions. The caller must invoke `free_shim`
/// to free the memory when it is no longer needed.
///
/// If this is called with a null pointer, it behaves like `alloc_shim`. If the size is zero, the function
/// frees the memory and returns a null pointer.
///
/// It retrieves the original pointer by subtracting the header size from the given pointer.
/// The function then retrieves the size from the first 8 bytes of the original pointer.
///
/// The pointer returned by this function points right after the header slot, which is
/// invisible to the caller.
///
/// If the pointer is null, it behaves like `alloc_shim`.
/// If the reallocation fails and the size is non-zero, it panics.
///
/// Safety:
/// 1. The caller must ensure that the pointer is valid and was allocated by `alloc_shim`.
///
/// If the pointer is null the function behaves like `alloc_shim`, that means if size is zero
/// the function returns a null pointer.
pub extern "C" fn realloc_shim(ptr_user: *mut c_void, new_size: usize) -> *mut c_void {
    if ptr_user.is_null() {
        return alloc_shim(new_size);
    }
    if new_size == 0 {
        free_shim(ptr_user);
        return ptr::null_mut();
    }

    let user = ptr_user as *mut u8;
    let base = user.wrapping_sub(HEADER_SIZE);

    let h0_old = base as *mut usize;

    // Safety: `h0_old` points to the header we wrote for this allocation.
    let old_alloc_size = unsafe { *h0_old };
    let old_layout = layout_for(old_alloc_size);

    let new_alloc_size = match new_size.checked_add(HEADER_SIZE) {
        Some(n) => n,
        None => return ptr::null_mut(),
    };

    // Safety: `realloc` is called with the exact old layout and the new total size.
    // On success it returns a valid pointer to `new_alloc_size` bytes aligned to `ALIGNMENT`.
    let new_base = unsafe { realloc(base, old_layout, new_alloc_size) };
    if new_base.is_null() {
        return ptr::null_mut();
    }

    let h0_new = new_base as *mut usize;
    // Safety: `h0_new` points to the start of the new header block.
    unsafe { *h0_new = new_alloc_size };

    let h1_new = new_base.wrapping_add(std::mem::size_of::<usize>()) as *mut usize;
    // Safety: `h1_new` is within the allocated header region and disjoint from `h0_new`.
    // Since the header is two words-long, and we only need one word
    // for required metadata (i.e. the actual allocated space),
    // we can use the second word to keep track of additional information
    // that can be handy when troubleshooting, such as the size
    // requested by the user.
    unsafe { *h1_new = new_size };

    // pointer just after the header
    new_base.wrapping_add(HEADER_SIZE) as *mut c_void
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal_allocs() {
        let size = 100;
        let ptr = alloc_shim(size);
        assert!(!ptr.is_null());
        free_shim(ptr);

        let ptr = calloc_shim(10, size);
        assert!(!ptr.is_null());
        free_shim(ptr);
    }

    #[test]
    fn test_realloc() {
        let size = 128;
        let ptr = alloc_shim(size);
        assert!(!ptr.is_null());

        let new_size = 64;
        let new_ptr = realloc_shim(ptr, new_size);
        assert!(!new_ptr.is_null());
        free_shim(new_ptr);

        let ptr = realloc_shim(std::ptr::null_mut(), 100);
        assert!(!ptr.is_null());
        free_shim(ptr);
    }

    #[test]
    fn test_zero_sizes() {
        let ptr = alloc_shim(0);
        assert!(ptr.is_null());

        let ptr = calloc_shim(0, 100);
        assert!(ptr.is_null());

        let ptr = calloc_shim(10, 0);
        assert!(ptr.is_null());

        let ptr = alloc_shim(32);
        let ptr = realloc_shim(ptr, 0);
        assert!(ptr.is_null());
        free_shim(ptr);
    }
}
