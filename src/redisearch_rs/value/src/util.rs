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

/// Converts a float into a string. A float is rendered as an integer if possible (via
/// [`itoa`], byte-identical to C's `%lld`), else with the c `snprintf` function for
/// compatibility with expected output: up to 12 significant digits, or scientific notation.
///
/// Returns the amount of bytes written, excluding the NUL terminator that always follows
/// them: C callers read `buf` as a C string.
pub fn num_to_str(num: f64, buf: &mut [u8; 32]) -> usize {
    let representable_as_integer =
        num.fract() == 0.0 && num >= i64::MIN as f64 && num < i64::MAX as f64;

    let result = if representable_as_integer {
        let mut digits = itoa::Buffer::new();
        let s = digits.format(num as i64);
        buf[..s.len()].copy_from_slice(s.as_bytes());
        buf[s.len()] = 0;
        s.len() as i32
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
