/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Aggregate value adapter; registration remains in the C expression registry.

use geo::{GeoDistanceError, GeoDistanceValue};
use query_error::opaque::OpaqueQueryError;
use std::ffi::c_int;
use value::Value;
use value_ffi::{RSValue, setters::RSValue_SetNumber, util::try_value};

/// Keep field classification distinct from reference-and-trio numeric coercion.
struct ValueView<'a>(Option<&'a Value>);

impl GeoDistanceValue for ValueView<'_> {
    fn geo_string(&self) -> Option<&[u8]> {
        self.0?.fully_dereferenced_ref().as_str_bytes()
    }

    fn geo_number(&self) -> Option<f64> {
        self.0?.fully_dereferenced_ref().as_num()
    }

    fn to_number(&self) -> Option<f64> {
        self.0?.to_number()
    }
}

/// Evaluate the registered aggregate callback with [`geo::geodistance`].
///
/// Returns expression-evaluation success even on conversion failure, writing NaN.
/// Only coordinate-string parse errors set `status`.
///
/// # Safety
/// `argv` must be [valid] for `argc` pointers, each null or pointing to a live [`RSValue`].
/// `argc` must satisfy the registry's two-to-four argument bounds. `result` must be a
/// [valid], uniquely referenced writable value and must not alias an argument.
/// `status` must be a [valid], initialized, exclusively borrowed [`OpaqueQueryError`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoDistance_Compute(
    argv: *const *mut RSValue,
    argc: usize,
    result: *mut RSValue,
    status: *mut OpaqueQueryError,
) -> c_int {
    assert!((2..=4).contains(&argc));
    // SAFETY: the registry supplies an array of argc argument pointers.
    let argv = unsafe { std::slice::from_raw_parts(argv, argc) };
    let values: [_; 4] = std::array::from_fn(|i| {
        ValueView(argv.get(i).and_then(|&value| {
            // SAFETY: each argument is null or a live borrowed value for this evaluation.
            unsafe { try_value(value) }
        }))
    });
    let distance = match geo::geodistance(&values[..argc]) {
        Ok(distance) => distance,
        Err(error) => {
            if let GeoDistanceError::Parse(error) = error {
                // SAFETY: the caller supplies an initialized error output.
                unsafe { super::set_parse_error(status, error) };
            }
            f64::NAN
        }
    };
    // SAFETY: result is uniquely owned and all argument borrows have ended.
    unsafe { RSValue_SetNumber(result, distance) };
    1
}
