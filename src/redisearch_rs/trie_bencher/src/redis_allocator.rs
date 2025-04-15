//! We redirect the `RedisModule_Alloc`, `RedisModule_Realloc`, and `RedisModule_Free` functions
//! to Rust's `alloc`, `realloc`, and `dealloc` functions.
//!
//! This forces both implementations to use the same memory allocator.
use std::{
    alloc::{alloc, dealloc, realloc, Layout}, collections::BTreeMap, ffi::c_void
};

use crate::ffi::max_align_t;

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
    unsafe extern "C" fn(nmemb: usize, size: usize) -> *mut ::std::os::raw::c_void,
> = Some(calloc_shim);

/// size header size
const SHS: usize = 8;

extern "C" fn alloc_shim(size: usize) -> *mut c_void {
    unsafe {
        let size = size + SHS;
        println!("Allocating {} bytes", size);
        let mem = alloc(Layout::from_size_align(size, 8).unwrap());
        // Store the size in the first 8 bytes (as usize)
        *(mem as *mut usize) = size;
        mem.offset(SHS as isize) as *mut c_void
    }
}

extern "C" fn free_shim(ptr: *mut c_void) {
    if !ptr.is_null() {
        let ptr = ptr as *mut u8;
        let ptr = unsafe { ptr.offset(-(SHS as isize)) };
        let size = unsafe { *(ptr as *mut usize) };
        println!("Freeing {} bytes", size);

        // Cast the pointer back to `*mut u8` and deallocate it
        unsafe { dealloc(ptr as *mut u8, Layout::from_size_align(size, SHS).unwrap()) }
    }
}

extern "C" fn realloc_shim(ptr: *mut c_void, size: usize) -> *mut c_void {
    if ptr.is_null() {
        return unsafe { RedisModule_Alloc.unwrap()(size) };
    }

    let ptr = ptr as *mut u8;
    let ptr = unsafe { ptr.offset(-(SHS as isize)) };

    // Reallocate using Rust's realloc function
    unsafe {
        let size = size + SHS;
        let layout = Layout::from_size_align(size, SHS).unwrap();

        // If the pointer is valid, try reallocating
        let new_ptr = realloc(ptr, layout, size);
        *(new_ptr as *mut usize) = size;

        if new_ptr.is_null() && size > 0 {
            // Handle allocation failure, if realloc returns null and the size is non-zero
            panic!("Reallocation failed!");
        }

        new_ptr.offset(SHS as isize) as *mut c_void
    }
}

extern "C" fn calloc_shim(nmemb: usize, size: usize) -> *mut ::std::os::raw::c_void {
    if nmemb == 0 || size == 0 {
        return std::ptr::null_mut();
    }

    let size = size+SHS;
    let total_size = nmemb.checked_mul(size).unwrap();
    let layout = Layout::from_size_align(total_size, std::mem::align_of::<u8>()).unwrap();

    unsafe {
        let ptr = alloc(layout);
        if ptr.is_null() {
            panic!("Allocation failed!");
        }
        *(ptr as *mut usize) = size;

        ptr.offset(SHS as isize) as *mut c_void
    }
}
