/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Alternative implementations of Redis' functions for heap allocation.
use std::{
    alloc::{Layout, alloc, alloc_zeroed, dealloc, realloc},
    ffi::c_void,
};

const HEADER_SIZE: usize = std::mem::size_of::<usize>();
const ALIGNMENT: usize = std::mem::align_of::<usize>();

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
/// invisible to the caller.
///
/// Safety:
/// 1. The caller must ensure that neither is non-zero as the behavior with size == 0 is implementation defined
/// 2. See [generic_shim] for more details.
///
/// If size is zero, the behavior is implementation defined (null pointer may be returned,
/// or some non-null pointer may be returned that shall not be dereferenced).
pub extern "C" fn alloc_shim(size: usize) -> *mut c_void {
    #[cfg(debug_assertions)]
    {
        // Check if size is zero
        if size == 0 {
            panic!("alloc_shim called with size 0");
        }
    }

    // Safety:
    // The caller will have to guarantee that `total_size` is > 0.
    let alloc_ = |total_size| unsafe {
        alloc(Layout::from_size_align(total_size, ALIGNMENT).unwrap()) as *mut c_void
    };
    // Safety:
    // 1. --> We know size > 0
    // A. total_size = size + HEADER_SIZE (see [generic_shim])
    unsafe { generic_shim(size, alloc_) }
}

/// Allocates the required memory of `size`*`count` bytes for the caller usage. The caller must invoke `free_shim`
/// to free the memory when it is no longer needed.
///
/// The function behaves like [alloc_shim] but besides the header slots initializes the allocated memory to zero.
///
/// Safety:
/// 1. The caller must ensure that neither size nor count is non-zero.
/// 2. See [generic_shim] for more details.
pub extern "C" fn calloc_shim(count: usize, size: usize) -> *mut c_void {
    #[cfg(debug_assertions)]
    {
        // Check if size is zero
        if size == 0 || count == 0 {
            panic!(
                "calloc_shim called with size={} and count={}, both must be > 0",
                size, count
            );
        }
    }

    // ensure no overflow
    let size_without_header = count.checked_mul(size).unwrap();
    // Safety:
    // The caller will have to guarantee that `total_size` is > 0.
    let alloc_zeroed = |total_size| unsafe {
        alloc_zeroed(Layout::from_size_align(total_size, ALIGNMENT).unwrap()) as *mut c_void
    };
    // Safety:
    // 1. --> We know count > 0, size > 0 --> size_without_header > 0
    unsafe { generic_shim(size_without_header, alloc_zeroed) }
}

/// Frees the memory allocated by the `alloc_shim`, `calloc_shim` or `realloc_shim` functions.
/// It retrieves the original pointer by subtracting the header size from the given pointer.
/// The function then retrieves the size from the first 8 bytes of the original pointer.
/// Finally, it deallocates the memory using Rust's `dealloc` function, which uses free from libc.
/// The function does nothing if the pointer is null.
///
/// Safety:
/// 1. The caller must ensure that the pointer is valid and was allocated by `alloc_shim`.
pub extern "C" fn free_shim(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }

    let ptr = ptr as *mut u8;

    // Safety:
    // 1. --> We know the ptr is valid and has `HEADER_SIZE` header slot prefixed because
    // it was allocated by `alloc_shim`.
    let ptr = unsafe { ptr.sub(HEADER_SIZE) };

    // Safety:
    // We just moved to the begin of the header slot
    let size = unsafe { *(ptr as *mut usize) };

    // Safety:
    // 1. --> We know the pointer is valid because it was allocated by `alloc_shim`.
    unsafe { dealloc(ptr, Layout::from_size_align(size, ALIGNMENT).unwrap()) }
}

/// Reallocates the required memory of `size` bytes for the caller usage. The `ptr` must be created
/// via `alloc_shim`, `calloc_shim` or `realloc_shim` functions. The caller must invoke `free_shim`
/// to free the memory when it is no longer needed.
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
/// 2. The caller must ensure that the size is non-zero if the pointer is not null.
pub extern "C" fn realloc_shim(ptr: *mut c_void, size: usize) -> *mut c_void {
    if ptr.is_null() {
        // Safety:
        // 1. --> We know size > 0
        return alloc_shim(size);
    }

    let ptr = ptr as *mut u8;

    // Safety:
    // 1. --> We know the ptr is valid and has `HEADER_SIZE` header slot prefixed because
    // it was allocated by `alloc_shim`.
    let ptr = unsafe { ptr.sub(HEADER_SIZE) };

    // Safety:
    // We just moved to the begin of the header slot
    let old_size = unsafe { *(ptr as *mut usize) };

    let old_layout = Layout::from_size_align(old_size, ALIGNMENT).unwrap();

    // Safety:
    // The caller will have to guarantee that `total_size` is > 0.
    let realloc_ = |total_size| unsafe { realloc(ptr, old_layout, total_size) as *mut c_void };
    // SAFETY:
    // 1. The caller guarantees that the size is greater than 0 (safety requirement #1)
    unsafe { generic_shim(size, realloc_) }
}

/// Allocates the required memory.
///
/// The allocator includes an additional header to keep track of the requested size.
/// That size information will be required, later on, if reallocating or deallocating the
/// buffer that this function returns.
///
/// The pointer returned by this function points right after the header slot, which is
/// invisible to the caller.
///
/// Safety:
/// 1. The caller must ensure that the size is non-zero.
unsafe fn generic_shim<F: FnOnce(usize) -> *mut c_void>(
    size: usize,
    allocation_func: F,
) -> *mut c_void {
    let size = size + HEADER_SIZE;

    // Safety:
    // 1 --> We know size > 0
    // A. allocation_fun is called with size >= HEADER_SIZE + 1
    let mem: *mut c_void = allocation_func(size);
    if mem.is_null() {
        panic!("Allocation of {size} bytes failed, out of memory?");
    }

    // Safety:
    // We know the memory is valid because we just allocated it.
    unsafe {
        *(mem as *mut usize) = size;
    }

    // Safety:
    // 1. --> We know there is at least one byte after the header
    unsafe { mem.add(HEADER_SIZE) }
}
