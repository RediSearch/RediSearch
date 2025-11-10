/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(
    clippy::undocumented_unsafe_blocks,
    clippy::missing_safety_doc,
    clippy::multiple_unsafe_ops_per_block
)]

pub mod benchers;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn ResultMetrics_Free(metrics: *mut ::ffi::RSYieldableMetric) {
    if metrics.is_null() {
        return;
    }

    panic!(
        "did not expect any benchmark to set metrics, but got: {:?}",
        unsafe { *metrics }
    );
}

#[unsafe(no_mangle)]
pub const extern "C" fn Term_Free(_t: *mut ::ffi::RSQueryTerm) {
    // RSQueryTerm used by the benchers are created on the stack so we don't need to free them.
}

#[unsafe(no_mangle)]
pub extern "C" fn isWithinRadius(
    _gf: *const ::ffi::GeoFilter,
    _d: f64,
    _distance: *mut f64,
) -> bool {
    panic!("isWithinRadius should not be called by any of the benchmarks");
}

#[unsafe(no_mangle)]
pub extern "C" fn DocTable_Exists(_dt: *const ::ffi::DocTable, _d: ::ffi::t_docId) -> bool {
    panic!("DocTable_Exists should not be called by any of the benchmarks");
}
