/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    alloc::{Layout, realloc},
    ffi::c_char,
};

/// Mock implementation of Buffer_Grow for tests
#[allow(non_snake_case)]
pub unsafe fn Buffer_Grow(buffer: *mut ffi::Buffer, extra_len: usize) -> usize {
    // Safety: buffer is a valid pointer to a Buffer.
    let buffer = unsafe { &mut *buffer };
    let old_capacity = buffer.cap;

    // Double the capacity or add extra_len, whichever is greater
    let new_capacity = std::cmp::max(buffer.cap * 2, buffer.cap + extra_len);

    let layout = Layout::array::<c_char>(old_capacity).unwrap();
    let new_data = unsafe { realloc(buffer.data as *mut _, layout, new_capacity) };
    buffer.data = new_data as *mut c_char;
    buffer.cap = new_capacity;

    // Return bytes added
    new_capacity - old_capacity
}
