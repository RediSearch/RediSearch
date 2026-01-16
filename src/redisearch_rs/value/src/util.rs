/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use libc::snprintf;

pub fn num_to_string_cstyle(float: f64, buf: &mut [u8]) -> i32 {
    let integer = float as i64;
    if integer as f64 == float {
        unsafe {
            snprintf(
                buf.as_mut_ptr() as *mut i8,
                buf.len(),
                c"%lld".as_ptr(),
                integer,
            )
        }
    } else {
        unsafe {
            snprintf(
                buf.as_mut_ptr() as *mut i8,
                buf.len(),
                c"%.12g".as_ptr(),
                float,
            )
        }
    }
}
