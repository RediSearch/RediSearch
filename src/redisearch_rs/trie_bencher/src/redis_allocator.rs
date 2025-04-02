//! We redirect the `RedisModule_Alloc`, `RedisModule_Realloc`, and `RedisModule_Free` functions
//! to Rust's `alloc`, `realloc`, and `dealloc` functions.
//!
//! This forces both implementations to use the same memory allocator.
use std::{
    alloc::{alloc, dealloc, realloc, Layout},
    ffi::{c_char, c_void},
};

use crate::ffi::{mstime_t, RedisModuleCtx, RedisModuleTimerID, RedisModuleTimerProc};

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

extern "C" fn alloc_shim(size: usize) -> *mut c_void {
    unsafe {
        alloc(Layout::from_size_align(size, std::mem::align_of::<u8>()).unwrap()) as *mut c_void
    }
}

extern "C" fn free_shim(ptr: *mut c_void) {
    if !ptr.is_null() {
        // Cast the pointer back to `*mut u8` and deallocate it
        unsafe { dealloc(ptr as *mut u8, Layout::new::<u8>()) }
    }
}

extern "C" fn realloc_shim(ptr: *mut c_void, size: usize) -> *mut c_void {
    if ptr.is_null() {
        return unsafe { RedisModule_Alloc.unwrap()(size) };
    }

    // Reallocate using Rust's realloc function
    unsafe {
        let layout = Layout::from_size_align(size, std::mem::align_of::<u8>()).unwrap();

        // If the pointer is valid, try reallocating
        let new_ptr = realloc(ptr as *mut u8, layout, size);

        if new_ptr.is_null() && size > 0 {
            // Handle allocation failure, if realloc returns null and the size is non-zero
            panic!("Reallocation failed!");
        }

        new_ptr as *mut c_void
    }
}

extern "C" fn calloc_shim(nmemb: usize, size: usize) -> *mut ::std::os::raw::c_void {
    if nmemb == 0 || size == 0 {
        return std::ptr::null_mut();
    }

    let total_size = nmemb.checked_mul(size).unwrap();
    let layout = Layout::from_size_align(total_size, std::mem::align_of::<u8>()).unwrap();

    unsafe {
        let ptr = alloc(layout);
        if ptr.is_null() {
            panic!("Allocation failed!");
        }

        ptr as *mut c_void
    }
}

#[unsafe(no_mangle)]    
pub extern "C" fn Wildcard_MatchChar(pattern: *mut c_char, p_len: usize, str: *mut c_char, str_len: usize) -> u32 { 0 }

#[unsafe(no_mangle)]
pub static mut RedisModule_CreateTimer: ::std::option::Option<
    unsafe extern "C" fn(
        ctx: *mut RedisModuleCtx,
        period: mstime_t,
        callback: RedisModuleTimerProc,
        data: *mut ::std::os::raw::c_void,
    ) -> RedisModuleTimerID
> = None;
