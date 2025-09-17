/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI layer to access, from C, the varint encoding machinery implemented in Rust.

use fxhash::{FxHasher32, FxHasher64};
use std::ffi::c_void;
use std::hash::Hasher;

/// Returns the 32-bit [fxhash] of `buf` of length `len` bytes using an initial hash of `hval`.
///
/// # Safety
///
/// 1. `buf` must point to a valid region of memory of `len` bytes.
///
/// [fxhash]: https://docs.rs/fxhash
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fxhash_32_incremental(buf: *const c_void, len: usize, hval: u32) -> u32 {
    // Safety: see safety point 1 above.
    let bytes = unsafe { std::slice::from_raw_parts(buf as *const u8, len) };
    let mut hasher = FxHasher32::default();

    hasher.write_u32(hval);
    hasher.write(bytes);

    hasher.finish() as u32
}

/// Returns the 32-bit [fxhash] of `buf` of length `len`.
///
/// # Safety
///
/// 1. `buf` must point to a valid region of memory of `len` bytes.
///
/// [fxhash]: https://docs.rs/fxhash
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fxhash_32(buf: *const c_void, len: usize) -> u32 {
    // Safety: see safety point 1 above.
    let bytes = unsafe { std::slice::from_raw_parts(buf as *const u8, len) };

    fxhash::hash32(bytes)
}

/// Returns the 64-bit [fxhash] of `buf` of length `len` bytes using an initial hash of `hval`.
///
/// # Safety
///
/// 1. `buf` must point to a valid region of memory of `len` bytes.
///
/// [fxhash]: https://docs.rs/fxhash
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fxhash_64_incremental(buf: *const c_void, len: usize, hval: u64) -> u64 {
    // Safety: see safety point 1 above.
    let bytes = unsafe { std::slice::from_raw_parts(buf as *const u8, len) };
    let mut hasher = FxHasher64::default();

    hasher.write_u64(hval);
    hasher.write(bytes);

    hasher.finish()
}

/// Returns the 64-bit [fxhash] of `buf` of length `len`.
///
/// # Safety
///
/// 1. `buf` must point to a valid region of memory of `len` bytes.
///
/// [fxhash]: https://docs.rs/fxhash
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fxhash_64(buf: *const c_void, len: usize) -> u64 {
    // Safety: see safety point 1 above.
    let bytes = unsafe { std::slice::from_raw_parts(buf as *const u8, len) };

    fxhash::hash64(bytes)
}
