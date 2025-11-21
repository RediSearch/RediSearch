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

use std::ffi::c_void;

pub mod benchers;
pub mod ffi;

// Need these symbols to be defined for the benchers to run
pub use types_ffi::NewVirtualResult;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

// symbols required by the C code we need to redefine
#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RSGlobalConfig: *const c_void = std::ptr::null();

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RSDummyContext: *const c_void = std::ptr::null();

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
