/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::*;
use std::alloc::{Layout, alloc};

/// Mock implementation of Buffer_Grow for tests
#[allow(non_snake_case)]
pub unsafe fn Buffer_Grow(mut b: NonNull<Buffer>, extra_len: usize) -> usize {
    let buffer = unsafe { b.as_mut() };
    let old_capacity = buffer.capacity;

    // Double the capacity or add extra_len, whichever is greater
    let new_capacity = std::cmp::max(buffer.capacity * 2, buffer.capacity + extra_len);

    let layout = Layout::array::<u8>(new_capacity).unwrap();
    let new_data = unsafe { alloc(layout) };
    let new_data_ptr = NonNull::new(new_data).unwrap();

    // Copy existing data to new buffer
    unsafe {
        std::ptr::copy_nonoverlapping(buffer.data.as_ptr(), new_data_ptr.as_ptr(), buffer.len);
    }

    // Free old buffer
    let old_layout = Layout::array::<u8>(old_capacity).unwrap();
    unsafe { std::alloc::dealloc(buffer.data.as_ptr(), old_layout) };

    // Update buffer
    buffer.data = new_data_ptr;
    buffer.capacity = new_capacity;

    // Return bytes added
    new_capacity - old_capacity
}
