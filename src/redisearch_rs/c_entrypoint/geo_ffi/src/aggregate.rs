/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{DistanceArgument, GeoValue};
use query_error::opaque::OpaqueQueryError;
use value::Value;
use value_ffi::RSValue;

struct Argument<'a>(&'a Value);

impl DistanceArgument for Argument<'_> {
    fn field(&self) -> GeoValue<'_> {
        let value = self.0.fully_dereferenced_ref();
        match value {
            Value::Number(number) => GeoValue::Number(*number),
            _ => value
                .as_str_bytes()
                .map_or(GeoValue::Other, GeoValue::String),
        }
    }

    fn to_number(&self) -> Option<f64> {
        self.0.to_number()
    }
}

/// Evaluate aggregate geodistance, returning NaN on conversion failure.
/// Only malformed coordinate strings update the query error.
///
/// # Safety
/// `argv` must contain `argc` live non-null value pointers, with arity checked by
/// the function registry. `status` must point to an initialized writable error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_GeoDistance_Evaluate(
    argv: *const *mut RSValue,
    argc: usize,
    status: *mut OpaqueQueryError,
) -> f64 {
    assert!((2..=4).contains(&argc));
    // SAFETY: caller supplies argc live pointers.
    let argv = unsafe { std::slice::from_raw_parts(argv, argc) };
    // A fixed-size stack buffer avoids allocating per result row.
    let mut args = [const { Argument(&Value::Undefined) }; 4];
    for (arg, ptr) in args.iter_mut().zip(argv) {
        // SAFETY: caller guarantees each value is live for this call.
        *arg = Argument(unsafe { value_ffi::util::try_value(*ptr) }.expect("value is null"));
    }
    let result = geo::geodistance(&args[..argc]);
    if let Some(error) = result.error {
        // SAFETY: caller supplies an initialized writable query error.
        unsafe { crate::set_parse_error(status, error) };
    }
    result.distance
}
