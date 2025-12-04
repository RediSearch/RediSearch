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

// Those ffi crates bring in C symbols we need, even if they are not used directly.
#[expect(unused_imports)]
pub use inverted_index_ffi;
#[expect(unused_imports)]
pub use triemap_ffi;
#[expect(unused_imports)]
pub use types_ffi;

// symbols required by the C code we need to redefine
#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RSGlobalConfig: *const c_void = std::ptr::null();

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
    AREQ_Free,
    AddDocumentCtx_Submit,
    Array_Add,
    Array_InitEx,
    Array_Resize,
    Buffer_Wrap,
    ConcurrentSearchPool_HighPriorityPendingJobsCount,
    ConcurrentSearchPool_WorkingThreadCount,
    CurrentThread_ClearIndexSpec,
    CurrentThread_SetIndexSpec,
    Dictionary_Clear,
    Document_Free,
    Document_Init,
    Document_LoadSchemaFieldHash,
    Document_LoadSchemaFieldJson,
    GCContext_CreateGC,
    GCContext_OnDelete,
    GCContext_Start,
    GCContext_StopMock,
    GenericAofRewrite_DisabledHandler,
    GeometryApi_Get,
    GeometryIndex_RemoveId,
    IncrementBgIndexYieldCounter,
    IndexerYieldWhileLoading,
    Initialize_KeyspaceNotifications,
    IsMaster,
    JSONParse_error,
    LoadByteOffsets,
    LogCallback,
    MRReply_ArrayElement,
    MRReply_ArrayToMap,
    MRReply_Integer,
    MRReply_Length,
    MRReply_MapElement,
    MRReply_String,
    MRReply_StringEquals,
    MRReply_Type,
    NewAddDocumentCtx,
    NewEmptyIterator,
    NewMetricIteratorSortedById,
    NewMetricIteratorSortedByScore,
    NewSortedIdListIterator,
    NewUnsortedIdListIterator,
    NewWildcardIterator_NonOptimized,
    Param_DictGet,
    QueryError_ClearError,
    QueryError_Default,
    QueryError_GetDisplayableError,
    QueryError_HasError,
    QueryError_IsOk,
    QueryError_SetCode,
    QueryError_SetError,
    QueryError_SetWithUserDataFmt,
    QueryError_SetWithoutUserDataFmt,
    QueryError_Strerror,
    QueryParam_Resolve,
    RPTypeToString,
    RSByteOffsets_Free,
    RSIndexResult_IterateOffsets,
    RSSortingVector_GetMemorySize,
    RedisMemory_GetUsedMemoryRatioUnified,
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
    RedisModule_EndReply,
    RedisModule_GetSwapKeyMetadata,
    RedisModule_IsKeyInRam,
    RedisModule_LoadDefaultConfigs,
    RedisModule_LoadLongDouble,
    RedisModule_RegisterDefragFunc2,
    RedisModule_ReplyKV_Array,
    RedisModule_ReplyKV_LongLong,
    RedisModule_ReplyKV_Map,
    RedisModule_ReplyKV_SimpleString,
    RedisModule_ReplyKV_String,
    RedisModule_ReplyWithLongDouble,
    RedisModule_Reply_Array,
    RedisModule_Reply_ArrayEnd,
    RedisModule_Reply_LongLong,
    RedisModule_Reply_Map,
    RedisModule_Reply_MapEnd,
    RedisModule_Reply_Null,
    RedisModule_Reply_Set,
    RedisModule_Reply_SetEnd,
    RedisModule_Reply_SimpleString,
    RedisModule_Reply_SimpleStringf,
    RedisModule_Reply_StringBuffer,
    RedisModule_SaveLongDouble,
    RedisModule_SetDataTypeExtensions,
    RedisModule_SetSwapKeyMetadata,
    RedisModule_ShardingGetKeySlot,
    RedisModule_ShardingGetSlotRange,
    RedisModule_StringToLongDouble,
    RedisModule_SwapPrefetchKey,
    RedisModule_UnsubscribeFromKeyspaceEvents,
    SearchResult_Clear,
    SortingVector_Free,
    SortingVector_RdbLoad,
    SynonymMap_Free,
    SynonymMap_New,
    SynonymMap_RdbLoad,
    SynonymMap_RdbSave,
    TagIndex_FormatName,
    TagIndex_Free,
    TagIndex_GetOverhead,
    TagIndex_OpenIndex,
    ThreadPoolAPI_SubmitIndexJobs,
    VecSimBatchIterator_Free,
    VecSimBatchIterator_HasNext,
    VecSimBatchIterator_New,
    VecSimBatchIterator_Next,
    VecSimIndex_BasicInfo,
    VecSimIndex_DeleteVector,
    VecSimIndex_EstimateElementSize,
    VecSimIndex_EstimateInitialSize,
    VecSimIndex_Free,
    VecSimIndex_GetDistanceFrom_Unsafe,
    VecSimIndex_IndexSize,
    VecSimIndex_New,
    VecSimIndex_PreferAdHocSearch,
    VecSimIndex_RangeQuery,
    VecSimIndex_ResolveParams,
    VecSimIndex_TopKQuery,
    VecSimQueryReply_Free,
    VecSimQueryReply_GetCode,
    VecSimQueryReply_GetIterator,
    VecSimQueryReply_IteratorFree,
    VecSimQueryReply_IteratorHasNext,
    VecSimQueryReply_IteratorNext,
    VecSimQueryReply_Len,
    VecSimQueryResult_GetId,
    VecSimQueryResult_GetScore,
    VecSimTieredIndex_AcquireSharedLocks,
    VecSimTieredIndex_GC,
    VecSimTieredIndex_ReleaseSharedLocks,
    VecSim_Normalize,
    VecSim_SetWriteMode,
    Wildcard_MatchRune,
    decodeGeo,
    escapeSimpleString,
    fast_float_strtod,
    fmtRedisGeometryIndexKey,
    g_isLoading,
    geohashGetDistance,
    getRedisConfigValue,
    globalDebugCtx,
    heap_cb_root,
    heap_clear,
    heap_count,
    heap_free,
    heap_init,
    heap_offerx,
    heap_peek,
    heap_poll,
    heap_replace,
    heap_size,
    heap_sizeof,
    isWithinRadius,
    japi,
    mmh_clear,
    mmh_exchange_max,
    mmh_free,
    mmh_init_with_size,
    mmh_insert,
    mmh_peek_max,
    mmh_pop_min,
    parseGeo,
    pathParse,
    rs_fnv_32a_buf,
    suffixTrie_freeCallback,
}
