/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`num_to_str`] must stay byte-identical to the `snprintf` rendering it replaced.

use std::ffi::c_char;
use value::util::num_to_str;

fn render(num: f64) -> String {
    let mut buf = [0u8; 32];
    let len = num_to_str(num, &mut buf);
    String::from_utf8(buf[..len].to_vec()).unwrap()
}

fn snprintf_lld(num: f64) -> String {
    let mut buf = [0u8; 32];
    // Safety: buf is valid, the format string takes exactly one long long.
    let len = unsafe {
        libc::snprintf(
            buf.as_mut_ptr() as *mut c_char,
            buf.len(),
            c"%lld".as_ptr(),
            num as i64,
        )
    };
    String::from_utf8(buf[..len as usize].to_vec()).unwrap()
}

#[test]
fn integral_values_match_the_c_rendering() {
    let values = [
        0.0,
        -0.0,
        1.0,
        -1.0,
        42.0,
        -1234567890.0,
        1e15,
        -1e15,
        9007199254740992.0, // 2^53
        i64::MIN as f64,
        // Just below the i64::MAX exclusion; still integral and in range.
        9223372036854774784.0,
    ];
    for v in values {
        assert_eq!(render(v), snprintf_lld(v), "value {v}");
    }
}

#[test]
fn fractional_and_out_of_range_values_keep_the_g_rendering() {
    assert_eq!(render(0.5), "0.5");
    assert_eq!(render(0.1 + 0.2), "0.3");
    assert_eq!(render(1.0 / 3.0), "0.333333333333");
    assert_eq!(render(1e20), "1e+20");
    assert_eq!(render(i64::MAX as f64), "9.22337203685e+18");
    assert_eq!(render(f64::INFINITY), "inf");
    assert_eq!(render(f64::NAN), "nan");
}
