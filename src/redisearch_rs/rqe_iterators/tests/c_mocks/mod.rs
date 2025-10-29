/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains mock implementations of C functions that are used in tests. Linking to a
//! C static file with these implementations would have been a overkill.
//!
//! The integration tests can use these as is if they don't add anything to the metrics or set
//! any of the term record type's internals. Using these only requires the following:
//!
//! ```rust
//! mod c_mocks;
//! ```

use ffi::{RSQueryTerm, RSValueType_RSValueType_Number};
use ffi::{array_clear_func, array_free, array_len_func};
use inverted_index::{RSIndexResult, RSTermRecord};
use std::ffi::c_void;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

fn free_rs_values(metrics: *mut ffi::RSYieldableMetric) {
    let len = unsafe { array_len_func(metrics as *mut c_void) };
    let mut metric = metrics;
    unsafe {
        for _ in 0..len {
            RSValue_Free((*metric).value);
            metric = metric.add(1);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Free(metrics: *mut ffi::RSYieldableMetric) {
    if metrics.is_null() {
        return;
    }
    free_rs_values(metrics);
    unsafe {
        array_free(metrics as *mut c_void);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Reset_func(result: *mut ffi::RSIndexResult) {
    let metrics: *mut ffi::RSYieldableMetric = unsafe { (*result).metrics };
    if metrics.is_null() {
        return;
    }
    free_rs_values(metrics);
    unsafe {
        array_clear_func(
            metrics as *mut c_void,
            std::mem::size_of::<ffi::RSYieldableMetric>() as u16,
        );
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn IndexResult_ConcatMetrics(
    _parent: *mut RSIndexResult,
    _child: *const RSIndexResult,
) {
    // Do nothing since the code will call this
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Offset_Data_Free(_tr: *mut RSTermRecord) {
    panic!("Nothing should have copied the term record to require this call");
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Free(t: *mut RSQueryTerm) {
    if !t.is_null() {
        panic!("No test created a term record");
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_Number(val: f64) -> ffi::RSValue {
    ffi::RSValue {
        __bindgen_anon_1: ffi::RSValue__bindgen_ty_1 {
            _numval: val, // Store the number value in the union
        },
        _refcount: 1,
        _bitfield_align_1: [0u8; 0],
        _bitfield_1: ffi::__BindgenBitfieldUnit::new([RSValueType_RSValueType_Number as u8; 1]),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewNumber(val: f64) -> *mut ffi::RSValue {
    let rs_val = RSValue_Number(val);
    Box::into_raw(Box::new(rs_val))
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_DecrRef() {}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_Free(val: *mut ffi::RSValue) {
    if val.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(val)) }
}
