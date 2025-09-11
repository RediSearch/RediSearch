/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI layer to access, from C, the varint encoding machinery implemented in Rust.

use fnv::{Fnv32, Fnv64};
use std::ffi::c_void;
use std::hash::Hasher;

/// Returns the 32-bit [FNV-1a hash] of `buf` of length `len` using an [offset basis] `hval`.
///
/// # Safety
///
/// 1. `buf` must point to a valid region of memory of length `len`.
///
/// [FNV-1a hash]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-1a
/// [offset basis]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rs_fnv_32a_buf(buf: *const c_void, len: usize, hval: u32) -> u32 {
    // Safety: see safety point 1 above.
    let bytes = unsafe { std::slice::from_raw_parts(buf as *const u8, len) };
    let mut fnv = Fnv32::with_offset_basis(hval);

    fnv.write(bytes);

    fnv.finish() as u32
}

/// Returns the 64-bit [FNV-1a hash] of `buf` of length `len` using an [offset basis] `hval`.
///
/// # Safety
///
/// 1. `buf` must point to a valid region of memory of length `len`.
///
/// [FNV-1a hash]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-1a
/// [offset basis]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fnv_64a_buf(buf: *const c_void, len: usize, hval: u64) -> u64 {
    // Safety: see safety point 1 above.
    let bytes = unsafe { std::slice::from_raw_parts(buf as *const u8, len) };
    let mut fnv = Fnv64::with_offset_basis(hval);

    fnv.write(bytes);

    fnv.finish()
}
