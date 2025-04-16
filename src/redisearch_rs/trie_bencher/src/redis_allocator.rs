#![allow(non_upper_case_globals)]
//! We redirect the `RedisModule_Alloc`, `RedisModule_Realloc`, and `RedisModule_Free` functions
//! to Rust's `alloc`, `realloc`, and `dealloc` functions.
//!
//! This forces both implementations to use the same memory allocator.
use std::{
    alloc::{Layout, alloc, alloc_zeroed, dealloc, realloc},
    ffi::c_void,
};

#[unsafe(no_mangle)]
pub static mut RedisModule_Alloc: ::std::option::Option<
    unsafe extern "C" fn(bytes: usize) -> *mut ::std::os::raw::c_void,
> = Some(alloc_shim);

#[unsafe(no_mangle)]
pub static mut RedisModule_Realloc: ::std::option::Option<
    unsafe extern "C" fn(
        ptr: *mut ::std::os::raw::c_void,
        bytes: usize,
    ) -> *mut ::std::os::raw::c_void,
> = Some(realloc_shim);

#[unsafe(no_mangle)]
pub static mut RedisModule_Free: ::std::option::Option<
    unsafe extern "C" fn(ptr: *mut ::std::os::raw::c_void),
> = Some(free_shim);

#[unsafe(no_mangle)]
pub static mut RedisModule_Calloc: ::std::option::Option<
    unsafe extern "C" fn(count: usize, size: usize) -> *mut ::std::os::raw::c_void,
> = Some(calloc_shim);

/// size header size
const HEADER_SIZE: usize = std::mem::size_of::<usize>();
const ALIGNMENT: usize = std::mem::align_of::<usize>();

/// Allocates memory for a given size and stores the size in the first 8 bytes of the allocated memory.
/// This is a shim function that redirects redis allocations to Rust's `alloc` function, which uses malloc from libc.
/// The size is increased by the size of the header to accommodate the size storage.
/// The function returns a pointer to the allocated memory which is offset by the header size.
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
    // A. ftor is called with size >= HEADER_SIZE + 1
    let mem = allocation_func(size);

    // Safety:
    // We know the memory is valid because we just allocated it.
    unsafe {
        *(mem as *mut usize) = size;
    }

    // Safety:
    // 1. --> We know there is at least one byte after the header
    unsafe { mem.add(HEADER_SIZE) as *mut c_void }
}

/// Allocates memory for a given size and stores the size in the first 8 bytes of the allocated memory.
/// This is a shim function that redirects redis allocations to Rust's `alloc` function, which uses malloc from libc.
/// The size is increased by the size of the header to accommodate the size storage.
/// The function returns a pointer to the allocated memory which is offset by the header size.
///
/// Safety:
/// 1. The caller must ensure that neither is non-zero as the behavior with size == 0 is implementation defined (either nullptr).
/// See [generic_shim] for more details.
///
/// If size is zero, the behavior is implementation defined (null pointer may be returned, or some non-null pointer may be returned that may not be used to access storage).
extern "C" fn alloc_shim(size: usize) -> *mut c_void {
    #[cfg(debug_assertions)]
    {
        // Check if size is zero
        if size == 0 {
            panic!("alloc_shim called with size 0");
        }
    }

    // Safety:
    // 1. --> We know size > 0
    // A. total_size = size + HEADER_SIZE (see [generic_shim])
    unsafe {
        generic_shim(size, |total_size| {
            alloc(Layout::from_size_align(total_size, ALIGNMENT).unwrap()) as *mut c_void
        })
    }
}

/// Allocates memory for a given size and stores the size in the first 8 bytes of the allocated memory.
/// This is a shim function that redirects redis allocations to Rust's `alloc` function, which uses malloc from libc.
/// The size is increased by the size of the header to accommodate the size storage.
/// The function returns a pointer to the allocated memory which is offset by the header size.
/// The function behaves like [alloc_shim] but also initializes the allocated memory besides the size header to zero.
///
/// Safety:
/// 1. The caller must ensure that neither size nor count is non-zero.
/// See [generic_shim] for more details.
extern "C" fn calloc_shim(count: usize, size: usize) -> *mut c_void {
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
    // 1. --> We know count > 0, size > 0 --> size_without_header > 0
    // A. total_size = size_without_header + HEADER_SIZE (see [generic_shim])
    unsafe {
        generic_shim(size_without_header, |total_size| {
            alloc_zeroed(Layout::from_size_align(total_size, ALIGNMENT).unwrap()) as *mut c_void
        })
    }
}

/// Frees the memory allocated by the `alloc_shim` function.
/// It retrieves the original pointer by subtracting the header size from the given pointer.
/// The function then retrieves the size from the first 8 bytes of the original pointer.
/// Finally, it deallocates the memory using Rust's `dealloc` function, which uses free from libc.
/// The function does nothing if the pointer is null.
///
/// Safety:
/// 1. The caller must ensure that the pointer is valid and was allocated by `alloc_shim`.
extern "C" fn free_shim(ptr: *mut c_void) {
    if !ptr.is_null() {
        let ptr = ptr as *mut u8;
        // Safety:
        // 1. --> We know the ptr is valid and has its size prefixed because it was allocated by `alloc_shim`.
        let (ptr, size) = unsafe {
            let ptr = ptr.sub(HEADER_SIZE);
            let size = *(ptr as *mut usize);
            (ptr, size)
        };

        // Safety:
        // 1. --> We know the pointer is valid because it was allocated by `alloc_shim`.
        unsafe {
            dealloc(
                ptr as *mut u8,
                Layout::from_size_align(size, ALIGNMENT).unwrap(),
            )
        }
    }
}

/// Reallocates memory for a given pointer and size.
/// It retrieves the original pointer by subtracting the header size from the given pointer.
/// The function then retrieves the size from the first 8 bytes of the original pointer.
/// It reallocates the memory using Rust's `realloc` function, which uses realloc from libc.
/// The function returns a pointer to the reallocated memory which is offset by the header size.
/// If the pointer is null, it behaves like `alloc_shim`.
/// If the reallocation fails and the size is non-zero, it panics.
///
/// Safety:
/// 1. The caller must ensure that the pointer is valid and was allocated by `alloc_shim`.
/// 2. The caller must ensure that the size is non-zero if the pointer is not null.
extern "C" fn realloc_shim(ptr: *mut c_void, size: usize) -> *mut c_void {
    if ptr.is_null() {
        // Safety:
        // 1. --> We know size > 0
        return unsafe { RedisModule_Alloc.unwrap()(size) };
    }

    let ptr = ptr as *mut u8;

    // Safety:
    // 1. --> We know the ptr is valid and has its size prefixed because it was allocated by `alloc_shim`.
    let (ptr, old_size) = unsafe {
        let ptr = ptr.sub(HEADER_SIZE);
        let old_size = *(ptr as *mut usize);
        (ptr, old_size)
    };

    let old_layout = Layout::from_size_align(old_size, ALIGNMENT).unwrap();
    let new_size = size + HEADER_SIZE;

    // Safety:
    // A. We just adapted ptr to the begin of the allocated memory in the last unsafe block
    // 1. + A --> We know ptr is valid and is now pointing to the address previously used by alloc
    // 1. + A --> We know old_layout is valid because it used size from the size header
    let new_ptr = unsafe { realloc(ptr, old_layout, new_size) };

    // Safety:
    // 2. --> We know new_ptr is valid because realloc returns a valid pointer or null
    unsafe {
        *(new_ptr as *mut usize) = new_size;
    }

    if new_ptr.is_null() && size > 0 {
        // Handle allocation failure, if realloc returns null and the size is non-zero
        panic!("Reallocation failed!");
    }

    // Safety:
    // We just allocated new_ptr and prefixed it with the size header
    // 2. --> We know there is at least one byte after the header
    (unsafe { new_ptr.add(HEADER_SIZE) } as *mut c_void)
}
