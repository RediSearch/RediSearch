/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use libc::snprintf;
use std::ffi::c_char;

pub fn str_to_float(input: &[u8]) -> Option<f64> {
    std::str::from_utf8(input).ok()?.parse::<f64>().ok()
}

pub fn num_to_str(float: f64, buf: &mut [u8]) -> Option<usize> {
    let integer = float as i64;
    let result = if integer as f64 == float {
        unsafe {
            snprintf(
                buf.as_mut_ptr() as *mut c_char,
                buf.len(),
                c"%lld".as_ptr(),
                integer,
            )
        }
    } else {
        unsafe {
            snprintf(
                buf.as_mut_ptr() as *mut c_char,
                buf.len(),
                c"%.12g".as_ptr(),
                float,
            )
        }
    };

    if result < 0 {
        None
    } else if (result as usize) < buf.len() {
        Some(result as usize)
    } else {
        None
    }
}
