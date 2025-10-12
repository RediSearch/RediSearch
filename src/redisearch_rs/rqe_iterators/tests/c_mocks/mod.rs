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

use ffi::RSQueryTerm;
use ffi::{array_ensure_append_n_func, array_clear_func, array_free};
use inverted_index::{RSIndexResult, RSTermRecord};
use std::ffi::c_void;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Free(metrics: *mut ffi::RSYieldableMetric) {
    if metrics.is_null() {
        return;
    }
    unsafe {
        array_free(metrics as *mut c_void);   
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
pub extern "C" fn RSValue_NewNumber(val: f64) -> *mut ffi::RSValue {
    // Allocate the f64 value and cast the pointer
    let mock_value = Box::new(val);
    Box::into_raw(mock_value) as *mut ffi::RSValue
}

#[allow(dead_code)]
pub fn get_mock_number_value(ptr: *const ffi::RSValue) -> Option<f64> {
    if ptr.is_null() {
        return None;
    }
    
    // Cast back to f64 pointer and read the value
    unsafe {
        Some(*(ptr as *const f64))
    }
}

#[unsafe(no_mangle)]
#[allow(unused_assignments)]
pub extern "C" fn RSYieldableMetric_Concat(
    metrics: *mut *mut ffi::RSYieldableMetric,
    new_metric: *const ffi::RSYieldableMetric,
) {
    if new_metric.is_null() {
        return;
    }
    unsafe {
        let elem_sz = std::mem::size_of::<ffi::RSYieldableMetric>();
        // array_ensure_append_n_func returns a new array pointer, but we don't need to use it in this mock
        *metrics = array_ensure_append_n_func(
            *metrics as *mut c_void, 
            new_metric as *mut c_void, 
            1, 
            elem_sz) as *mut ffi::RSYieldableMetric ;
        
        // array_clear_func returns a new array pointer, but we don't need to use it in this mock
        let _ = array_clear_func(new_metric as *mut c_void, elem_sz);
    }
}