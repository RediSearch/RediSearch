/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`format_g`].

use rqe_iterators::profile_print::format_g;

#[test]
fn zero() {
    assert_eq!(format_g(0.0), "0");
}

#[test]
fn negative_zero() {
    assert_eq!(format_g(-0.0), "-0");
}

#[test]
fn integers() {
    assert_eq!(format_g(1.0), "1");
    assert_eq!(format_g(123456.0), "123456");
}

#[test]
fn decimals() {
    assert_eq!(format_g(3.14159), "3.14159");
    assert_eq!(format_g(0.5), "0.5");
    assert_eq!(format_g(0.001), "0.001");
}

#[test]
fn small_values() {
    // Boundary: exponent == -4 stays in fixed notation.
    assert_eq!(format_g(0.0001), "0.0001");
    // Exponent < -4 switches to scientific notation.
    assert_eq!(format_g(0.00001), "1e-05");
}

#[test]
fn large_values() {
    // Boundary: exponent == 5 stays in fixed notation.
    assert_eq!(format_g(999999.0), "999999");
    // Exponent >= 6 switches to scientific notation.
    assert_eq!(format_g(1000000.0), "1e+06");
    assert_eq!(format_g(1234567.0), "1.23457e+06");
}

#[test]
fn trailing_zeros_trimmed() {
    assert_eq!(format_g(1.5), "1.5");
    assert_eq!(format_g(1.50), "1.5");
    assert_eq!(format_g(100.0), "100");
}

#[test]
fn non_finite() {
    assert_eq!(format_g(f64::NAN), "nan");
    assert_eq!(format_g(f64::INFINITY), "inf");
    assert_eq!(format_g(f64::NEG_INFINITY), "-inf");
}

#[test]
fn negative() {
    assert_eq!(format_g(-1.0), "-1");
    assert_eq!(format_g(-0.00001), "-1e-05");
}

/// Call C's `snprintf(buf, len, "%g", value)` and return the result as a [`String`].
fn c_format_g(value: f64) -> String {
    let mut buf = [0u8; 64];
    // SAFETY: buf is a valid buffer, format string and argument match.
    let n = unsafe {
        libc::snprintf(
            buf.as_mut_ptr().cast::<libc::c_char>(),
            buf.len(),
            c"%g".as_ptr(),
            value,
        )
    };
    assert!(n >= 0 && (n as usize) < buf.len(), "snprintf overflow");
    std::str::from_utf8(&buf[..n as usize])
        .expect("C produced non-UTF-8")
        .to_string()
}

proptest::proptest! {
    #[test]
    fn matches_c(value: f64) {
        let rust = format_g(value);
        let c = c_format_g(value);
        proptest::prop_assert_eq!(rust, c, "mismatch for {:?} (bits={:#018x})", value, value.to_bits());
    }
}
