/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Mock implementations or stubs of C symbol that aren't provided
//! by the static C libraries we are linking against in build.rs.
use std::ffi::c_void;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

// `types_ffi` brings in some of the C symbols we need, even if it
// isn't used directly.
#[expect(unused_imports)]
pub use types_ffi;

// symbols required by the C code we need to redefine
#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RSGlobalConfig: *const c_void = std::ptr::null();

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RSDummyContext: *const c_void = std::ptr::null();

/// Define an empty stub function for each given symbols.
/// This is used to define C functions the linker requires but which are not actually used by the tests.
macro_rules! stub_c_fn {
    ($($fn_name:ident),* $(,)?) => {
        $(
            #[unsafe(no_mangle)]
            pub extern "C" fn $fn_name() {
                panic!(concat!(stringify!($fn_name), " should not be called by any of the tests"));
            }
        )*
    };
}

// On macOS, we can tell the loader to ignore undefined symbols via `-undefined=dynamic_lookup`.
// As long as they are not invoked at runtime, everything will work out.
// On Linux, there is no way equivalent option. We can tell the _linker_ to ignore undefined
// symbols at link time, but we will still get a runtime error if there *strong* references
// to undefined symbols.
// The simplest workaround is to provide stubs for those symbols: dummy entries that make the loader
// happy but are actually going to fail catastrophically if invoked at runtime. You wouldn't want
// to do it for production builds, but for tests it's fine.
//
// What symbols should we stub?
// One approach is to "fix when the loader complains". You try to run tests, you get a "symbol lookup error",
// you add a stub for that symbol. Tedious, it can require tens of iterations depending on what's being linked.
// Luckily enough, we can be more scientific!
// Start by building the test binary you care about. In this case:
//
// ```bash
// cargo test --no-run --test integration
// ```
//
// `cargo` will output the path to the newly compiled test binary in its output. For example,
// `../../bin/redisearch_rs/debug/deps/integration-fbaef160d4b407a6`.
//
// You then extract the undefined symbols with a strong reference from the compiler artifact via:
//
// ```bash
// readelf -Ws --dyn-syms ../../bin/redisearch_rs/debug/deps/integration-fbaef160d4b407a6 | \
// awk '$5 == "GLOBAL" && $7 == "UND" && $4 == "NOTYPE" { print $8 }' | sort -u
// ```
//
// You add those symbols to the list below... and you're done!
// Still tedious, but faster.
//
// # Caveats
//
// The required list of symbols may vary depending on how the linked C libraries are compiled.
// In particular, coverage runs may require extra symbols!
// Kick off coverage via `./build.sh COV=1 FORCE RUN_RUST_TESTS` and then repeat the process
// above using the corresponding test binaries.
stub_c_fn! {
    fast_float_strtod,
    Obfuscate_Number,
    Obfuscate_Text,
    QueryError_SetWithUserDataFmt,
    RSIndexResult_IterateOffsets,
    sdscat,
    sdscatfmt,
    sdscatlen,
    nu_bytenlen,
    nu_strtransformnlen,
    nu_tolower,
    nu_utf8_write,
    nu_writenstr,
    RedisModule_ClusterCanAccessKeysInSlot,
    RedisModule_ClusterFreeSlotRanges,
    RedisModule_ClusterGetLocalSlotRanges,
    RedisModule_ClusterKeySlotC,
    RedisModule_ClusterPropagateForSlotMigration,
    RedisModule_ConfigGet,
    RedisModule_ConfigGetBool,
    RedisModule_ConfigGetEnum,
    RedisModule_ConfigGetNumeric,
    RedisModule_ConfigGetType,
    RedisModule_ConfigIteratorCreate,
    RedisModule_ConfigIteratorNext,
    RedisModule_ConfigIteratorRelease,
    RedisModule_ConfigSet,
    RedisModule_ConfigSetBool,
    RedisModule_ConfigSetEnum,
    RedisModule_ConfigSetNumeric,
    RedisModule_CreateStringFromLongDouble,
    RedisModule_DefragRedisModuleDict,
    RedisModule_GetSwapKeyMetadata,
    RedisModule_IsKeyInRam,
    RedisModule_LoadDefaultConfigs,
    RedisModule_LoadLongDouble,
    RedisModule_RegisterDefragFunc2,
    RedisModule_ReplyWithLongDouble,
    RedisModule_SaveLongDouble,
    RedisModule_SetDataTypeExtensions,
    RedisModule_SetSwapKeyMetadata,
    RedisModule_ShardingGetKeySlot,
    RedisModule_ShardingGetSlotRange,
    RedisModule_StringToLongDouble,
    RedisModule_SwapPrefetchKey,
    RedisModule_UnsubscribeFromKeyspaceEvents,
}
