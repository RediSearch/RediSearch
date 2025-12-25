//! This module contains mock implementations of C functions that are used in tests. Linking to a
//! C static file with these implementations would have been a overkill.
//!
//! The integration tests can use these as is if they don't add anything to the metrics or set
//! any of the term record type's internals. Using these only requires the following:
//!
//! ```rust
//! mod c_mocks;
//! ```

//! Mock implementations or stubs of C symbol that aren't provided
//! by the static C libraries we are linking against in build.rs.

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

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
// On Linux, there is no equivalent option. We can tell the _linker_ to ignore undefined
// symbols at link time, but we will still get a runtime error if there are *strong* references
// in the compiled binary to undefined symbols.
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
// cargo test --no-run --test inverted_index
// ```
//
// `cargo` will output the path to the newly compiled test binary in its output. For example,
// `target/debug/deps/inverted_index-9605816e60530866`.
//
// You then extract the undefined symbols with a strong reference from the compiler artifact via:
//
// ```bash
// readelf -Ws --dyn-syms target/debug/deps/inverted_index-9605816e60530866 | \
// awk '$5 == "GLOBAL" && $7 == "UND" && $4 == "NOTYPE" { print $8 }' | sort -u
// ```
//
// You add those symbols to the list below... and you're done!
// Still tedious, but faster.
stub_c_fn! {
    HiddenString_Compare,
    HiddenString_Duplicate,
    HiddenString_Free,
    HiddenString_GetUnsafe,
    RedisModule_ClusterCanAccessKeysInSlot,
    RedisModule_ClusterDisableTrim,
    RedisModule_ClusterEnableTrim,
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
    RedisModule_GetApi,
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
    ResultMetrics_Free,
    Term_Free,
}
