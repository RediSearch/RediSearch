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

use ::ffi::{
    __BindgenBitfieldUnit, RSIndexResult, RSValue, RSValue__bindgen_ty_1,
    RSValueType_RSValueType_Number, RSYieldableMetric, array_clear_func, array_free,
    array_len_func,
};
use std::ffi::c_void;

pub mod benchers;
pub mod ffi;

// Need those symbols to be defined for the benchers to run
pub use inverted_index_ffi::NewInvertedIndex_Ex;
pub use triemap_ffi::TRIEMAP_NOTFOUND;
pub use types_ffi::NewVirtualResult;
pub use varint_ffi::WriteVarint;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

// symbols required by the C code we need to redefine
#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RSGlobalConfig: *const c_void = std::ptr::null();

fn free_rs_values(metrics: *mut RSYieldableMetric) {
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
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn ResultMetrics_Free(metrics: *mut ::ffi::RSYieldableMetric) {
    if metrics.is_null() {
        return;
    }
    free_rs_values(metrics);
    unsafe {
        array_free(metrics as *mut c_void);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Reset_func(result: *mut RSIndexResult) {
    let metrics: *mut RSYieldableMetric = unsafe { (*result).metrics };
    if metrics.is_null() {
        return;
    }
    free_rs_values(metrics);
    unsafe {
        array_clear_func(
            metrics as *mut c_void,
            std::mem::size_of::<RSYieldableMetric>() as u16,
        );
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_Number(val: f64) -> RSValue {
    RSValue {
        __bindgen_anon_1: RSValue__bindgen_ty_1 {
            _numval: val, // Store the number value in the union
        },
        _refcount: 1,
        _bitfield_align_1: [0u8; 0],
        _bitfield_1: __BindgenBitfieldUnit::new([RSValueType_RSValueType_Number as u8; 1]),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewNumber(val: f64) -> *mut RSValue {
    let rs_val = RSValue_Number(val);
    Box::into_raw(Box::new(rs_val))
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_Free(val: *mut RSValue) {
    if val.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(val));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_DecrRef() {}

#[unsafe(no_mangle)]
pub const extern "C" fn Term_Free(_t: *mut ::ffi::RSQueryTerm) {
    // RSQueryTerm used by the benchers are created on the stack so we don't need to free them.
}

/// Define an empty stub function for each given symbols.
/// This is used to define C functions the linker requires but which are not actually used by the benchers.
macro_rules! stub_c_fn {
    ($($fn_name:ident),* $(,)?) => {
        $(
            #[unsafe(no_mangle)]
            pub extern "C" fn $fn_name() {
                panic!(concat!(stringify!($fn_name), " should not be called by any of the benchmarks"));
            }
        )*
    };
}

// Those C symbols are required for the c benchmarking code to build and run.
// They have been added by adding them until it runs fine.
stub_c_fn! {
    DocTable_Exists,
    IndexSpec_GetFormattedKey,
    RS_dictFetchValue,
    RedisModule_CreateStringFromLongDouble,
    RedisModule_GetSwapKeyMetadata,
    RedisModule_IsKeyInRam,
    RedisModule_LoadDefaultConfigs,
    RedisModule_LoadLongDouble,
    RedisModule_ReplyWithLongDouble,
    RedisModule_SaveLongDouble,
    RedisModule_SetDataTypeExtensions,
    RedisModule_SetSwapKeyMetadata,
    RedisModule_ShardingGetKeySlot,
    RedisModule_ShardingGetSlotRange,
    RedisModule_StringToLongDouble,
    RedisModule_SwapPrefetchKey,
    Redis_OpenInvertedIndex,
    TagIndex_OpenIndex,
    TimeToLiveTable_VerifyDocAndField,
    TimeToLiveTable_VerifyDocAndFieldMask,
    TimeToLiveTable_VerifyDocAndWideFieldMask,
    fast_float_strtod,
    isWithinRadius,
    nu_bytenlen,
    nu_strtransformnlen,
    nu_tolower,
    nu_utf8_write,
    nu_writenstr,
    openNumericKeysDict,
}
