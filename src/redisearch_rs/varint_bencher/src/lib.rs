/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types and functions for benchmarking varint operations.
//!
//! This crate benchmarks the performance of Rust varint implementation
//! against the original C implementation to validate performance characteristics.

#![allow(non_upper_case_globals)]

use std::ffi::c_void;

// Force the compiler to link the symbols defined in `redis_mock`,
// since they are required by `libvarint.a`.
extern crate redis_mock;

// Provide function pointer variables that the C library can link to.
// The C library expects these as function pointers, not actual functions.
#[unsafe(no_mangle)]
pub static mut RedisModule_Alloc: unsafe extern "C" fn(usize) -> *mut c_void =
    redis_module_alloc_impl;

#[unsafe(no_mangle)]
pub static mut RedisModule_Free: unsafe extern "C" fn(*mut c_void) = redis_module_free_impl;

#[unsafe(no_mangle)]
pub static mut RedisModule_Realloc: unsafe extern "C" fn(*mut c_void, usize) -> *mut c_void =
    redis_module_realloc_impl;

#[unsafe(no_mangle)]
pub static mut RedisModule_Calloc: unsafe extern "C" fn(usize, usize) -> *mut c_void =
    redis_module_calloc_impl;

unsafe extern "C" fn redis_module_alloc_impl(bytes: usize) -> *mut c_void {
    if bytes == 0 {
        return std::ptr::null_mut();
    }
    redis_mock::allocator::alloc_shim(bytes)
}

unsafe extern "C" fn redis_module_free_impl(ptr: *mut c_void) {
    redis_mock::allocator::free_shim(ptr)
}

unsafe extern "C" fn redis_module_realloc_impl(ptr: *mut c_void, bytes: usize) -> *mut c_void {
    if bytes == 0 {
        if !ptr.is_null() {
            redis_mock::allocator::free_shim(ptr);
        }
        return std::ptr::null_mut();
    }
    redis_mock::allocator::realloc_shim(ptr, bytes)
}

unsafe extern "C" fn redis_module_calloc_impl(count: usize, size: usize) -> *mut c_void {
    if count == 0 || size == 0 {
        return std::ptr::null_mut();
    }
    redis_mock::allocator::calloc_shim(count, size)
}

pub use bencher::VarintBencher;

pub mod bencher;
pub mod c_varint;
pub mod ffi;

// Convenient type aliases for varint operations.
pub type FieldMask = encode_decode::FieldMask;

// Re-export C functions from FFI module for easier access in benchmarks
pub use ffi::{
    NewVarintVectorWriter, VVW_Free, VVW_Truncate, VVW_Write, WriteVarint, WriteVarintFieldMask,
};

// Export C types
pub use ffi::VarintVectorWriter;
