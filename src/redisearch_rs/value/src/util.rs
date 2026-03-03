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

/// Converts a string into a float, returning `None` if it failed.
pub fn str_to_float(input: &[u8]) -> Option<f64> {
    std::str::from_utf8(input).ok()?.parse::<f64>().ok()
}

/// Converts a float into a string using the c `snprintf` function for compatibility with
/// expected output. A float is rendered as an integer if possible, else it's rendered
/// with up to 12 decimal points, else it's rendered in scientific notation.
///
/// Returns the amount of bytes written as per `snprint`: if that is >= `buf.len()` then
/// it didn't fit (`snprintf` always places a nul-terminator at the end).
pub fn num_to_str(num: f64, buf: &mut [u8]) -> usize {
    let representable_as_integer =
        num.fract() == 0.0 && num >= i64::MIN as f64 && num < i64::MAX as f64;

    let result = if representable_as_integer {
        // Safety: buf is valid by definition, formatting string and arguments match up.
        unsafe {
            snprintf(
                buf.as_mut_ptr() as *mut c_char,
                buf.len(),
                c"%lld".as_ptr(),
                num as i64,
            )
        }
    } else {
        // Safety: buf is valid by definition, formatting string and arguments match up.
        unsafe {
            snprintf(
                buf.as_mut_ptr() as *mut c_char,
                buf.len(),
                c"%.12g".as_ptr(),
                num,
            )
        }
    };

    if result < 0 {
        panic!("snprintf failed")
    }

    result as usize
}
