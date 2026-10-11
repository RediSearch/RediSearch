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
    // Pre-filled with non-zero bytes: C callers read the buffer as a C string, so the
    // terminator must come from num_to_str itself.
    let mut buf = [0xffu8; 32];
    let len = num_to_str(num, &mut buf);
    assert_eq!(buf[len], 0, "missing NUL terminator for {num}");
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

const INTEGRAL_VALUES: [f64; 11] = [
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

#[test]
fn integral_values_render_as_decimal_integers() {
    let expected = [
        "0",
        "0",
        "1",
        "-1",
        "42",
        "-1234567890",
        "1000000000000000",
        "-1000000000000000",
        "9007199254740992",
        "-9223372036854775808",
        "9223372036854774784",
    ];
    for (v, e) in INTEGRAL_VALUES.iter().zip(expected) {
        assert_eq!(render(*v), e, "value {v}");
    }
}

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `snprintf`")]
fn integral_values_match_the_c_rendering() {
    for v in INTEGRAL_VALUES {
        assert_eq!(render(v), snprintf_lld(v), "value {v}");
    }
}

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `snprintf`")]
fn fractional_and_out_of_range_values_keep_the_g_rendering() {
    assert_eq!(render(0.5), "0.5");
    assert_eq!(render(0.1 + 0.2), "0.3");
    assert_eq!(render(1.0 / 3.0), "0.333333333333");
    assert_eq!(render(1e20), "1e+20");
    assert_eq!(render(i64::MAX as f64), "9.22337203685e+18");
    assert_eq!(render(f64::INFINITY), "inf");
    assert_eq!(render(f64::NAN), "nan");
}
