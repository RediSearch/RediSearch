/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::{c_char, c_void};

pub mod benchers;
pub mod ffi;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RSGlobalConfig: *const c_void = std::ptr::null();

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RSDummyContext: *const c_void = std::ptr::null();

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RedisModule_Log(
    _ctx: *mut redis_module::RedisModuleCtx,
    _level: *const c_char,
    _fmt: *const c_char,
) {
}

#[unsafe(no_mangle)]
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
pub extern "C" fn Term_Free(_t: *mut ::ffi::RSQueryTerm) {
    // RSQueryTerm used by the benchers are created on the stack so we don't need to free them.
}
