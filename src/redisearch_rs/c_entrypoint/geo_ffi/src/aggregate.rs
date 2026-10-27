/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Aggregate geodistance, using the existing value conversions.

use std::ffi::c_int;
use value_ffi::{
    RSValue,
    conversions::RSValue_ToNumber,
    getters::{RSValue_Number_Get, RSValue_StringPtrLen},
    setters::RSValue_SetNumber,
    shared::RSValue_Dereference,
    value_type::{RSValue_IsNumber, RSValue_IsString},
};

/// Parse a string coordinate pair or encoded numeric geohash.
///
/// # Safety
/// The value and status must remain live and exclusively accessible for this call.
unsafe fn parse_field(
    value: *const RSValue,
    status: *mut query_error::opaque::OpaqueQueryError,
) -> Option<[f64; 2]> {
    // SAFETY: the evaluator supplies a live value.
    let value = unsafe { RSValue_Dereference(value) };
    let mut xy = [0.0; 2];
    // SAFETY: the dereferenced value is live.
    if unsafe { RSValue_IsString(value) } {
        let mut len = 0;
        // SAFETY: value is a string; len is writable.
        let data = unsafe { RSValue_StringPtrLen(value, &mut len) };
        // SAFETY: the string buffer and both outputs are valid; status belongs to the evaluator.
        if unsafe { crate::parseGeo(data.cast(), len, &mut xy[0], &mut xy[1], status) } != 0 {
            return None;
        }
    // SAFETY: the dereferenced value is live.
    } else if unsafe { RSValue_IsNumber(value) } {
        // SAFETY: value is numeric and xy contains two writable doubles.
        let bits = unsafe { RSValue_Number_Get(value) };
        // SAFETY: xy contains two writable doubles.
        unsafe { crate::decodeGeo(bits, xy.as_mut_ptr()) };
    } else {
        return None;
    }
    Some(xy)
}

/// Convert two evaluator values, retaining short-circuit conversion order.
///
/// # Safety
/// Both arguments must point to live evaluator values.
unsafe fn parse_lon_lat(lon: *const RSValue, lat: *const RSValue) -> Option<[f64; 2]> {
    let mut xy = [0.0; 2];
    // SAFETY: the evaluator supplies live values and xy has writable outputs.
    if !unsafe { RSValue_ToNumber(lon, &mut xy[0]) } {
        return None;
    }
    // SAFETY: the latitude value is live and xy has a writable output.
    unsafe { RSValue_ToNumber(lat, &mut xy[1]) }.then_some(xy)
}

/// Evaluate aggregate geodistance with two, three or four arguments.
///
/// # Safety
/// 1. `ctx` must be a [valid] evaluator with an initialized, writable error.
/// 2. `argv` must contain `argc` live value pointers; `argc` must be between two and four.
/// 3. `result` must be a [valid] writable value, disjoint from the arguments.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn geofunc_distance(
    ctx: *mut ffi::ExprEval,
    argv: *mut *mut RSValue,
    argc: usize,
    result: *mut RSValue,
) -> c_int {
    debug_assert!((2..=4).contains(&argc));
    debug_assert!(!ctx.is_null() && !argv.is_null() && !result.is_null());
    // SAFETY: the caller guarantees the argument array (2).
    let args = unsafe { std::slice::from_raw_parts(argv, argc) };
    // SAFETY: the caller guarantees a live evaluator (1).
    let status = unsafe { (*ctx).err.cast() };
    let field = |value| {
        // SAFETY: all arguments and the evaluator's error are live (1, 2).
        unsafe { parse_field(value, status) }
    };
    let pair = |lon, lat| {
        // SAFETY: both arguments are live evaluator values (2).
        unsafe { parse_lon_lat(lon, lat) }
    };
    let points = (|| {
        match args {
            [a, b] => Some((field(*a)?, field(*b)?)),
            [a, b, c] => {
                let mut dummy = 0.0;
                // SAFETY: a is a live evaluator value and dummy is writable (2).
                if unsafe { RSValue_ToNumber(*a, &mut dummy) } {
                    Some((pair(*a, *b)?, field(*c)?))
                } else {
                    Some((field(*a)?, pair(*b, *c)?))
                }
            }
            [a, b, c, d] => Some((pair(*a, *b)?, pair(*c, *d)?)),
            _ => None,
        }
    })();
    let distance = points.map_or(f64::NAN, |(a, b)| {
        geo::aggregate_distance(a[0], a[1], b[0], b[1])
    });
    // SAFETY: result is exclusively writable (3).
    unsafe { RSValue_SetNumber(result, distance) };
    ffi::EXPR_EVAL_OK as c_int
}

unsafe extern "C" fn distance_callback(
    ctx: *mut ffi::ExprEval,
    argv: *mut *mut ffi::RSValue,
    argc: usize,
    result: *mut ffi::RSValue,
) -> c_int {
    // SAFETY: the registry invokes this with the evaluator's live values and checked argument count.
    unsafe { geofunc_distance(ctx, argv.cast(), argc, result.cast()) }
}

/// Register geodistance with the historical return-type metadata and argument bounds.
///
/// # Safety
/// The function registry must be initialized and exclusively accessible.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RegisterGeoFunctions() {
    // SAFETY: the caller grants exclusive registry access; callback matches its ABI.
    unsafe {
        ffi::RSFunctionRegistry_RegisterFunction(
            c"geodistance".as_ptr(),
            Some(distance_callback),
            ffi::RSValueType_RSValueType_String,
            2,
            4,
        )
    };
}
