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
pub mod ffi;

// Some of the missing C symbols are actually Rust-provided.
pub use redisearch_rs;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

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
    ClusterConfig_RegisterTriggers,
    DEBUG_RSExecDistAggregate,
    DetectClusterType,
    GeometryApi_Get,
    GeometryCoordsToName,
    GeometryIndexFactory,
    GetClusterConfigOptions,
    InfoReplyReducer,
    InitRedisTopologyUpdater,
    MRCommand_Append,
    MRCommand_Free,
    MRCommand_Insert,
    MRCommand_PrepareForSlotInfo,
    MRCommand_ReplaceArg,
    MRCommand_ReplaceArgSubstring,
    MRCommand_SetPrefix,
    MRCommand_SetProtocol,
    MRCtx_Free,
    MRCtx_GetBlockedClient,
    MRCtx_GetNumReplied,
    MRCtx_GetPrivData,
    MRCtx_GetRedisCtx,
    MRCtx_GetReplies,
    MRCtx_RequestCompleted,
    MRCtx_SetReduceFunction,
    MRReply_ArrayElement,
    MRReply_ArrayToMap,
    MRReply_Double,
    MRReply_Integer,
    MRReply_Length,
    MRReply_MapElement,
    MRReply_String,
    MRReply_StringEquals,
    MRReply_ToDouble,
    MRReply_Type,
    MR_CreateCtx,
    MR_Fanout,
    MR_FreeCluster,
    MR_GetLocalNodeId,
    MR_Init,
    MR_InitLocalNodeId,
    MR_NewCommandFromRedisStrings,
    MR_ReleaseLocalNodeIdReadLock,
    MR_ReplyWithMRReply,
    MR_SetLocalNodeId,
    MR_UpdateTopology,
    MR_uvReplyClusterInfo,
    RPCounter_New,
    RSExecDistAggregate,
    RSExecDistHybrid,
    RedisEnterprise_ParseTopology,
    RedisModule_ReplyKV_MRReply,
    RedisTopologyUpdater_StopAndRescheduleImmediately,
    RegisterClusterModuleConfig,
    RegisterCoordDebugCommands,
    Sha1_Compute,
    Sha1_FormatIntoBuffer,
    TracingRedisModule_Init,
    UpdateTopology,
    VecSimBatchIterator_Free,
    VecSimBatchIterator_HasNext,
    VecSimBatchIterator_New,
    VecSimBatchIterator_Next,
    VecSimDebugInfoIterator_Free,
    VecSimDebugInfoIterator_HasNextField,
    VecSimDebugInfoIterator_NextField,
    VecSimDebugInfoIterator_NumberOfFields,
    VecSimDebug_GetElementNeighborsInHNSWGraph,
    VecSimDebug_ReleaseElementNeighborsInHNSWGraph,
    VecSimIndex_AddVector,
    VecSimIndex_BasicInfo,
    VecSimIndex_DebugInfoIterator,
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
    VecSimIndex_StatsInfo,
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
    VecSim_SetLogCallbackFunction,
    VecSim_SetMemoryFunctions,
    VecSim_SetTimeoutCallbackFunction,
    VecSim_SetWriteMode,
    clusterConfig,
    fnv_64a_buf,
    hiredisSetAllocators,
    parseProfileArgs,
    processResultFormat,
    rs_fnv_32a_buf,
    sdsAllocSize,
    sdscat,
    sdscatfmt,
    sdscatlen,
    sdscatprintf,
    sdscmp,
    sdsdup,
    sdsempty,
    sdsfree,
    sdsjoin,
    sdsnew,
    sdsnewlen,
    sdstolower,
    spellCheckReducer_resp2,
    spellCheckReducer_resp3,
    uv_replace_allocator,
}
