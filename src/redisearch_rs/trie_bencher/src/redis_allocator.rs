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
    unsafe extern "C" fn(nmemb: usize, size: usize) -> *mut ::std::os::raw::c_void,
> = Some(calloc_shim);

/// size header size
const HEADER_SIZE: usize = std::mem::size_of::<usize>();
const ALIGNMENT: usize = std::mem::align_of::<usize>();

extern "C" fn alloc_shim(size: usize) -> *mut c_void {
    unsafe {
        let size = size + HEADER_SIZE;
        //println!("Allocating {} bytes", size);
        let mem = alloc(Layout::from_size_align(size, ALIGNMENT).unwrap());
        // Store the size in the first 8 bytes (as usize)
        *(mem as *mut usize) = size;
        mem.add(HEADER_SIZE) as *mut c_void
    }
}

extern "C" fn free_shim(ptr: *mut c_void) {
    if !ptr.is_null() {
        let ptr = ptr as *mut u8;
        let ptr = unsafe { ptr.sub(HEADER_SIZE) };
        let size = unsafe { *(ptr as *mut usize) };
        //println!("Freeing {} bytes", size);

        // Cast the pointer back to `*mut u8` and deallocate it
        unsafe {
            dealloc(
                ptr as *mut u8,
                Layout::from_size_align(size, ALIGNMENT).unwrap(),
            )
        }
    }
}

extern "C" fn realloc_shim(ptr: *mut c_void, size: usize) -> *mut c_void {
    if ptr.is_null() {
        return unsafe { RedisModule_Alloc.unwrap()(size) };
    }

    let ptr = ptr as *mut u8;
    let ptr = unsafe { ptr.sub(HEADER_SIZE) };

    // Reallocate using Rust's realloc function
    unsafe {
        let old_size = *(ptr as *mut usize);
        let old_layout = Layout::from_size_align(old_size, ALIGNMENT).unwrap();
        let new_size = size + HEADER_SIZE;
        //println!("Reallocating from {} bytes to {} bytes", old_size, new_size);
        let new_ptr = realloc(ptr, old_layout, new_size);
        *(new_ptr as *mut usize) = new_size;

        if new_ptr.is_null() && size > 0 {
            // Handle allocation failure, if realloc returns null and the size is non-zero
            panic!("Reallocation failed!");
        }

        new_ptr.add(HEADER_SIZE) as *mut c_void
    }
}

extern "C" fn calloc_shim(nmemb: usize, size: usize) -> *mut ::std::os::raw::c_void {
    if nmemb == 0 || size == 0 {
        return std::ptr::null_mut();
    }

    let total_size = nmemb.checked_mul(size).unwrap();
    let total_size = total_size + HEADER_SIZE;
    let layout = Layout::from_size_align(total_size, ALIGNMENT).unwrap();

    unsafe {
        let ptr = alloc_zeroed(layout);
        if ptr.is_null() {
            panic!("Allocation failed!");
        }
        *(ptr as *mut usize) = total_size;

        ptr.add(HEADER_SIZE) as *mut c_void
    }
}
