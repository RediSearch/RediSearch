/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/vec_sim.h"
#include "VecSim/query_results.h"
#include "VecSim/query_result_definitions.h"
#include "VecSim/utils/vec_utils.h"
#include "VecSim/spaces/spaces.h"
#include "VecSim/index_factories/index_factory.h"
#include "VecSim/vec_sim_index.h"
#include "VecSim/vec_sim_adhoc_bf_ctx.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/algorithms/svs/svs_utils.h"
#include <cassert>
#include <cstdarg>
#include <cstdio>
#include "memory.h"

// Ensure labelType is compatible with size_t for C API boundary.
// The C API functions (VecSimIndex_AddVector, VecSimIndex_DeleteVector, etc.)
// use size_t for labels, which are passed directly to C++ methods expecting labelType.
static_assert(std::is_same_v<labelType, size_t>,
              "labelType must be size_t for C API compatibility");

extern "C" void VecSim_SetTimeoutCallbackFunction(timeoutCallbackFunction callback) {
    VecSimIndex::setTimeoutCallbackFunction(callback);
}

extern "C" void VecSim_SetLogCallbackFunction(logCallbackFunction callback) {
    VecSimIndex::setLogCallbackFunction(callback);
}

extern "C" void VecSim_SetWriteMode(VecSimWriteMode mode) { VecSimIndex::setWriteMode(mode); }

extern "C" void VecSim_UpdateThreadPoolSize(size_t new_size) {
    if (new_size == 0) {
        VecSimIndex::setWriteMode(VecSim_WriteInPlace);
    } else {
        VecSimIndex::setWriteMode(VecSim_WriteAsync);
    }
    // Resize the shared SVS pool. Clamped to a minimum of 1. OS threads are spawned
    // lazily on first SVS index creation; once an index exists this resizes the
    // shared pool immediately (cooperating with the deferred-shrink protocol).
    VecSimSVSThreadPool::resize(new_size);
}

namespace {
// Written by SetResolveErr() and read back through the *err_msg it hands out; both happen on the
// same thread within a single VecSimIndex_ResolveParams call, before any other resolve call can
// overwrite it.
constexpr size_t RESOLVE_ERR_MSG_MAX_LEN = 256;
thread_local char resolve_err_msg_buf[RESOLVE_ERR_MSG_MAX_LEN];

__attribute__((format(printf, 3, 4))) VecSimResolveCode SetResolveErr(const char **err_msg,
                                                                      VecSimResolveCode code,
                                                                      const char *fmt, ...) {
    if (err_msg) {
        va_list args;
        va_start(args, fmt);
        vsnprintf(resolve_err_msg_buf, sizeof(resolve_err_msg_buf), fmt, args);
        va_end(args);
        *err_msg = resolve_err_msg_buf;
    }
    return code;
}
} // namespace

static VecSimResolveCode _ResolveParams_EFRuntime(VecSimAlgo index_type, VecSimRawParam rparam,
                                                  VecSimQueryParams *qparams,
                                                  VecsimQueryType query_type,
                                                  const char **err_msg) {
    long long num_val;
    // EF_RUNTIME is a valid parameter only in HNSW algorithm.
    if (index_type != VecSimAlgo_HNSWLIB) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_UnknownParam,
                             "%s is only valid for HNSW indexes",
                             VecSimCommonStrings::HNSW_EF_RUNTIME_STRING);
    }
    // EF_RUNTIME is invalid for range query
    if (query_type == QUERY_TYPE_RANGE) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_UnknownParam,
                             "%s is not a valid parameter for range queries",
                             VecSimCommonStrings::HNSW_EF_RUNTIME_STRING);
    }
    if (qparams->hnswRuntimeParams.efRuntime != 0) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_AlreadySet,
                             "%s was specified more than once",
                             VecSimCommonStrings::HNSW_EF_RUNTIME_STRING);
    }
    if (validate_positive_integer_param(rparam, &num_val) != VecSimParamResolver_OK) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_BadValue,
                             "%s must be a positive integer",
                             VecSimCommonStrings::HNSW_EF_RUNTIME_STRING);
    }

    qparams->hnswRuntimeParams.efRuntime = (size_t)num_val;
    return VecSimParamResolver_OK;
}

static VecSimResolveCode _ResolveParams_SearchWS(VecSimAlgo index_type, VecSimRawParam rparam,
                                                 VecSimQueryParams *qparams, const char **err_msg) {
    long long num_val;
    // SEARCH_WS is a valid parameter only in SVS algorithm.
    if (index_type != VecSimAlgo_SVS) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_UnknownParam,
                             "%s is only valid for SVS indexes",
                             VecSimCommonStrings::SVS_SEARCH_WS_STRING);
    }
    if (qparams->svsRuntimeParams.windowSize != 0) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_AlreadySet,
                             "%s was specified more than once",
                             VecSimCommonStrings::SVS_SEARCH_WS_STRING);
    }
    if (validate_positive_integer_param(rparam, &num_val) != VecSimParamResolver_OK) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_BadValue,
                             "%s must be a positive integer",
                             VecSimCommonStrings::SVS_SEARCH_WS_STRING);
    }

    qparams->svsRuntimeParams.windowSize = (size_t)num_val;
    return VecSimParamResolver_OK;
}

static VecSimResolveCode _ResolveParams_SearchBC(VecSimAlgo index_type, VecSimRawParam rparam,
                                                 VecSimQueryParams *qparams, const char **err_msg) {
    long long num_val;
    // SEARCH_BC is a valid parameter only in SVS algorithm.
    if (index_type != VecSimAlgo_SVS) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_UnknownParam,
                             "%s is only valid for SVS indexes",
                             VecSimCommonStrings::SVS_SEARCH_BC_STRING);
    }
    if (qparams->svsRuntimeParams.bufferCapacity != 0) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_AlreadySet,
                             "%s was specified more than once",
                             VecSimCommonStrings::SVS_SEARCH_BC_STRING);
    }
    if (validate_positive_integer_param(rparam, &num_val) != VecSimParamResolver_OK) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_BadValue,
                             "%s must be a positive integer",
                             VecSimCommonStrings::SVS_SEARCH_BC_STRING);
    }

    qparams->svsRuntimeParams.bufferCapacity = (size_t)num_val;
    return VecSimParamResolver_OK;
}
static VecSimResolveCode _ResolveParams_UseSearchHistory(VecSimAlgo index_type,
                                                         VecSimRawParam rparam,
                                                         VecSimQueryParams *qparams,
                                                         const char **err_msg) {
    VecSimOptionMode bool_val;
    // USE_SEARCH_HISTORY is a valid parameter only in SVS algorithm.
    if (index_type != VecSimAlgo_SVS) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_UnknownParam,
                             "%s is only valid for SVS indexes",
                             VecSimCommonStrings::SVS_USE_SEARCH_HISTORY_STRING);
    }
    if (qparams->svsRuntimeParams.searchHistory != 0) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_AlreadySet,
                             "%s was specified more than once",
                             VecSimCommonStrings::SVS_USE_SEARCH_HISTORY_STRING);
    }
    if (validate_vecsim_bool_param(rparam, &bool_val) != VecSimParamResolver_OK) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_BadValue,
                             "%s must be one of: ON, OFF, AUTO",
                             VecSimCommonStrings::SVS_USE_SEARCH_HISTORY_STRING);
    }

    qparams->svsRuntimeParams.searchHistory = bool_val;
    return VecSimParamResolver_OK;
}

static VecSimResolveCode _ResolveParams_BatchSize(VecSimRawParam rparam, VecSimQueryParams *qparams,
                                                  VecsimQueryType query_type,
                                                  const char **err_msg) {
    long long num_val;
    if (query_type != QUERY_TYPE_HYBRID) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_InvalidPolicy_NHybrid,
                             "%s is only valid for hybrid queries",
                             VecSimCommonStrings::BATCH_SIZE_STRING);
    }
    if (qparams->batchSize != 0) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_AlreadySet,
                             "%s was specified more than once",
                             VecSimCommonStrings::BATCH_SIZE_STRING);
    }
    if (validate_positive_integer_param(rparam, &num_val) != VecSimParamResolver_OK) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_BadValue,
                             "%s must be a positive integer",
                             VecSimCommonStrings::BATCH_SIZE_STRING);
    }
    qparams->batchSize = (size_t)num_val;
    return VecSimParamResolver_OK;
}

static VecSimResolveCode _ResolveParams_Epsilon(VecSimAlgo index_type, VecSimRawParam rparam,
                                                VecSimQueryParams *qparams,
                                                VecsimQueryType query_type, const char **err_msg) {
    double num_val;
    // EPSILON is a valid parameter only in HNSW or SVS algorithms.
    if (index_type != VecSimAlgo_HNSWLIB && index_type != VecSimAlgo_SVS) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_UnknownParam,
                             "%s is only valid for HNSW or SVS indexes",
                             VecSimCommonStrings::EPSILON_STRING);
    }
    if (query_type != QUERY_TYPE_RANGE) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_InvalidPolicy_NRange,
                             "%s is only valid for range queries",
                             VecSimCommonStrings::EPSILON_STRING);
    }
    auto &epsilon_ref = index_type == VecSimAlgo_HNSWLIB ? qparams->hnswRuntimeParams.epsilon
                                                         : qparams->svsRuntimeParams.epsilon;
    if (epsilon_ref != 0) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_AlreadySet,
                             "%s was specified more than once",
                             VecSimCommonStrings::EPSILON_STRING);
    }
    if (validate_positive_double_param(rparam, &num_val) != VecSimParamResolver_OK) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_BadValue,
                             "%s must be a positive number", VecSimCommonStrings::EPSILON_STRING);
    }
    epsilon_ref = num_val;
    return VecSimParamResolver_OK;
}

// Zero-initialize qparams and explicitly set fields whose "absent" state is not
// representable by all-zeros (e.g. tristate enums where 0 maps to a valid value).
static void _InitQueryParams(VecSimQueryParams *qparams, VecSimAlgo index_type, bool is_disk) {
    memset(qparams, 0, sizeof(VecSimQueryParams));
    if (index_type == VecSimAlgo_HNSWLIB && is_disk) {
        qparams->hnswDiskRuntimeParams.shouldRerank = VecSimBool_UNSET;
    }
}

static VecSimResolveCode _ResolveParams_Rerank(VecSimAlgo index_type, bool is_disk,
                                               VecSimRawParam rparam, VecSimQueryParams *qparams,
                                               const char **err_msg) {
    // RERANK is valid only for disk-based HNSW indexes.
    if (index_type != VecSimAlgo_HNSWLIB || !is_disk) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_UnknownParam,
                             "%s is only valid for disk-based HNSW indexes",
                             VecSimCommonStrings::HNSW_RERANK_STRING);
    }
    if (qparams->hnswDiskRuntimeParams.shouldRerank != VecSimBool_UNSET) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_AlreadySet,
                             "%s was specified more than once",
                             VecSimCommonStrings::HNSW_RERANK_STRING);
    }
    VecSimBool bool_val;
    if (validate_vecsim_tristate_bool_param(rparam, &bool_val) != VecSimParamResolver_OK) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_BadValue, "%s must be TRUE or FALSE",
                             VecSimCommonStrings::HNSW_RERANK_STRING);
    }
    qparams->hnswDiskRuntimeParams.shouldRerank = bool_val;
    return VecSimParamResolver_OK;
}

static VecSimResolveCode _ResolveParams_HybridPolicy(VecSimRawParam rparam,
                                                     VecSimQueryParams *qparams,
                                                     VecsimQueryType query_type,
                                                     const char **err_msg) {
    if (query_type != QUERY_TYPE_HYBRID) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_InvalidPolicy_NHybrid,
                             "%s is only valid for hybrid queries",
                             VecSimCommonStrings::HYBRID_POLICY_STRING);
    }
    if (qparams->searchMode != 0) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_AlreadySet,
                             "%s was specified more than once",
                             VecSimCommonStrings::HYBRID_POLICY_STRING);
    }
    if (!strcasecmp(rparam.value, VECSIM_POLICY_BATCHES)) {
        qparams->searchMode = HYBRID_BATCHES;
    } else if (!strcasecmp(rparam.value, VECSIM_POLICY_ADHOC_BF)) {
        qparams->searchMode = HYBRID_ADHOC_BF;
    } else {
        return SetResolveErr(err_msg, VecSimParamResolverErr_InvalidPolicy_NExits,
                             "invalid value '%s' for %s: expected '%s' or '%s'", rparam.value,
                             VecSimCommonStrings::HYBRID_POLICY_STRING, VECSIM_POLICY_BATCHES,
                             VECSIM_POLICY_ADHOC_BF);
    }
    return VecSimParamResolver_OK;
}

extern "C" VecSimIndex *VecSimIndex_New(const VecSimParams *params) {
    return VecSimFactory::NewIndex(params);
}

extern "C" size_t VecSimIndex_EstimateInitialSize(const VecSimParams *params) {
    try {
        return VecSimFactory::EstimateInitialSize(params);
    } catch (...) {
        return SIZE_MAX;
    }
}

extern "C" int VecSimIndex_AddVector(VecSimIndex *index, const void *blob, size_t label) {
    return index->addVector(blob, label);
}

extern "C" int VecSimIndex_DeleteVector(VecSimIndex *index, size_t label) {
    return index->deleteVector(label);
}

extern "C" VecSimRelabelCode VecSimIndex_RelabelVector(VecSimIndex *index, size_t old_label,
                                                       size_t new_label) {
    return index->relabelVector(old_label, new_label);
}

extern "C" VecSimUpdateCode VecSimIndex_UpdateVectors(VecSimIndex *index, size_t label,
                                                      const void *new_blobs, size_t n) {
    return index->updateVectors(label, new_blobs, n);
}

extern "C" double VecSimIndex_GetDistanceFrom_Unsafe(VecSimIndex *index, size_t label,
                                                     const void *blob) {
    return index->getDistanceFrom_Unsafe(label, blob);
}

extern "C" size_t VecSimIndex_EstimateElementSize(const VecSimParams *params) {
    return VecSimFactory::EstimateElementSize(params);
}

extern "C" void VecSim_Normalize(void *blob, size_t dim, VecSimType type) {
    if (type == VecSimType_FLOAT32) {
        spaces::GetNormalizeFunc<float>()(blob, dim);
    } else if (type == VecSimType_FLOAT64) {
        spaces::GetNormalizeFunc<double>()(blob, dim);
    } else if (type == VecSimType_BFLOAT16) {
        spaces::GetNormalizeFunc<vecsim_types::bfloat16>()(blob, dim);
    } else if (type == VecSimType_FLOAT16) {
        spaces::GetNormalizeFunc<vecsim_types::float16>()(blob, dim);
    } else if (type == VecSimType_INT8) {
        // assuming blob is large enough to store the norm at the end of the vector
        spaces::GetNormalizeFunc<int8_t>()(blob, dim);
    } else if (type == VecSimType_UINT8) {
        // assuming blob is large enough to store the norm at the end of the vector
        spaces::GetNormalizeFunc<uint8_t>()(blob, dim);
    }
}

extern "C" size_t VecSimParams_GetQueryBlobSize(VecSimType type, size_t dim, VecSimMetric metric) {
    // Assert all supported types are covered
    assert(type == VecSimType_FLOAT32 || type == VecSimType_FLOAT64 ||
           type == VecSimType_BFLOAT16 || type == VecSimType_FLOAT16 || type == VecSimType_INT8 ||
           type == VecSimType_UINT8);
    size_t blobSize = VecSimType_sizeof(type) * dim;
    if (metric == VecSimMetric_Cosine && (type == VecSimType_INT8 || type == VecSimType_UINT8)) {
        blobSize += sizeof(float); // For the norm
    }
    return blobSize;
}

extern "C" size_t VecSimIndex_IndexSize(VecSimIndex *index) { return index->indexSize(); }

extern "C" VecSimResolveCode VecSimIndex_ResolveParams(VecSimIndex *index, VecSimRawParam *rparams,
                                                       int paramNum, VecSimQueryParams *qparams,
                                                       VecsimQueryType query_type,
                                                       const char **err_msg) {
    if (err_msg) {
        *err_msg = NULL;
    }
    if (!qparams || (!rparams && (paramNum != 0))) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_NullParam,
                             "Query parameters were not provided");
    }
    VecSimIndexBasicInfo info = index->basicInfo();
    VecSimAlgo index_type = info.algo;
    bool is_disk = info.isDisk;

    _InitQueryParams(qparams, index_type, is_disk);
    auto res = VecSimParamResolver_OK;
    for (int i = 0; i < paramNum; i++) {
        if (!strcasecmp(rparams[i].name, VecSimCommonStrings::HNSW_EF_RUNTIME_STRING)) {
            if ((res = _ResolveParams_EFRuntime(index_type, rparams[i], qparams, query_type,
                                                err_msg)) != VecSimParamResolver_OK) {
                return res;
            }
        } else if (!strcasecmp(rparams[i].name, VecSimCommonStrings::EPSILON_STRING)) {
            if ((res = _ResolveParams_Epsilon(index_type, rparams[i], qparams, query_type,
                                              err_msg)) != VecSimParamResolver_OK) {
                return res;
            }
        } else if (!strcasecmp(rparams[i].name, VecSimCommonStrings::HNSW_RERANK_STRING)) {
            if ((res = _ResolveParams_Rerank(index_type, is_disk, rparams[i], qparams, err_msg)) !=
                VecSimParamResolver_OK) {
                return res;
            }
        } else if (!strcasecmp(rparams[i].name, VecSimCommonStrings::BATCH_SIZE_STRING)) {
            if ((res = _ResolveParams_BatchSize(rparams[i], qparams, query_type, err_msg)) !=
                VecSimParamResolver_OK) {
                return res;
            }
        } else if (!strcasecmp(rparams[i].name, VecSimCommonStrings::HYBRID_POLICY_STRING)) {
            if ((res = _ResolveParams_HybridPolicy(rparams[i], qparams, query_type, err_msg)) !=
                VecSimParamResolver_OK) {
                return res;
            }
        } else if (!strcasecmp(rparams[i].name, VecSimCommonStrings::SVS_SEARCH_WS_STRING)) {
            if ((res = _ResolveParams_SearchWS(index_type, rparams[i], qparams, err_msg)) !=
                VecSimParamResolver_OK) {
                return res;
            }
        } else if (!strcasecmp(rparams[i].name, VecSimCommonStrings::SVS_SEARCH_BC_STRING)) {
            if ((res = _ResolveParams_SearchBC(index_type, rparams[i], qparams, err_msg)) !=
                VecSimParamResolver_OK) {
                return res;
            }
        } else if (!strcasecmp(rparams[i].name,
                               VecSimCommonStrings::SVS_USE_SEARCH_HISTORY_STRING)) {
            if ((res = _ResolveParams_UseSearchHistory(index_type, rparams[i], qparams, err_msg)) !=
                VecSimParamResolver_OK) {
                return res;
            }
        } else {
            return SetResolveErr(err_msg, VecSimParamResolverErr_UnknownParam,
                                 "Unknown query parameter: '%s'", rparams[i].name);
        }
    }
    // The combination of AD-HOC with batch_size is invalid, as there are no batches in this policy.
    if (qparams->searchMode == HYBRID_ADHOC_BF && qparams->batchSize > 0) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_InvalidPolicy_AdHoc_With_BatchSize,
                             "%s is irrelevant for the '%s' hybrid policy",
                             VecSimCommonStrings::BATCH_SIZE_STRING, VECSIM_POLICY_ADHOC_BF);
    }
    // Also, 'ef_runtime' is meaning less in AD-HOC policy, since it doesn't involve search in HNSW
    // graph.
    if (qparams->searchMode == HYBRID_ADHOC_BF && index_type == VecSimAlgo_HNSWLIB &&
        qparams->hnswRuntimeParams.efRuntime > 0) {
        return SetResolveErr(err_msg, VecSimParamResolverErr_InvalidPolicy_AdHoc_With_EfRuntime,
                             "%s is irrelevant for the '%s' hybrid policy",
                             VecSimCommonStrings::HNSW_EF_RUNTIME_STRING, VECSIM_POLICY_ADHOC_BF);
    }
    if (qparams->searchMode != 0) {
        index->setLastSearchMode(qparams->searchMode);
    }
    return res;
}

extern "C" VecSimQueryReply *VecSimIndex_TopKQuery(VecSimIndex *index, const void *queryBlob,
                                                   size_t k, VecSimQueryParams *queryParams,
                                                   VecSimQueryReply_Order order) {
    assert((order == BY_ID || order == BY_SCORE) &&
           "Possible order values are only 'BY_ID' or 'BY_SCORE'");
    VecSimQueryReply *results;
    results = index->topKQuery(queryBlob, k, queryParams);

    if (order == BY_ID) {
        sort_results_by_id(results);
    }
    return results;
}

extern "C" VecSimQueryReply *VecSimIndex_RangeQuery(VecSimIndex *index, const void *queryBlob,
                                                    double radius, VecSimQueryParams *queryParams,
                                                    VecSimQueryReply_Order order) {
    if (order != BY_ID && order != BY_SCORE) {
        throw std::runtime_error("Possible order values are only 'BY_ID' or 'BY_SCORE'");
    }
    if (radius < 0) {
        throw std::runtime_error("radius must be non-negative");
    }
    return index->rangeQuery(queryBlob, radius, queryParams, order);
}

extern "C" void VecSimIndex_Free(VecSimIndex *index) {
    std::shared_ptr<VecSimAllocator> allocator =
        index->getAllocator(); // Save allocator so it will not deallocate itself
    delete index;
}

extern "C" VecSimIndexDebugInfo VecSimIndex_DebugInfo(VecSimIndex *index) {
    return index->debugInfo();
}

extern "C" VecSimDebugInfoIterator *VecSimIndex_DebugInfoIterator(VecSimIndex *index) {
    VecSimDebugInfoIterator *infoIterator = index->debugInfoIterator();
    // Append the process-wide shared memory total. This field is not emitted by
    // any algorithm's own debugInfoIterator(); it is injected here at the C API
    // boundary so that every caller (regardless of algorithm) can account for
    // memory not tied to any single index without special-casing SVS internals.
    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::SHARED_MEMORY_STRING,
                         .fieldType = INFOFIELD_UINT64,
                         .fieldValue = {FieldValue{.uintegerValue = VecSim_GetSharedMemory()}}});
    return infoIterator;
}

extern "C" size_t VecSim_GetSharedMemory(void) {
    return VecSimSVSThreadPool::getSharedAllocationSize();
}

extern "C" VecSimIndexBasicInfo VecSimIndex_BasicInfo(VecSimIndex *index) {
    return index->basicInfo();
}

extern "C" VecSimIndexStatsInfo VecSimIndex_StatsInfo(VecSimIndex *index) {
    return index->statisticInfo();
}

extern "C" VecSimBatchIterator *VecSimBatchIterator_New(VecSimIndex *index, const void *queryBlob,
                                                        VecSimQueryParams *queryParams) {
    return index->newBatchIterator(queryBlob, queryParams);
}

extern "C" void VecSimTieredIndex_GC(VecSimIndex *index) {
    if (index->basicInfo().isTiered) {
        index->runGC();
    }
}

extern "C" void VecSimTieredIndex_AcquireSharedLocks(VecSimIndex *index) {
    index->acquireSharedLocks();
}

extern "C" void VecSimTieredIndex_ReleaseSharedLocks(VecSimIndex *index) {
    index->releaseSharedLocks();
}

extern "C" void VecSim_SetMemoryFunctions(VecSimMemoryFunctions memoryfunctions) {
    VecSimAllocator::setMemoryFunctions(memoryfunctions);
}

extern "C" bool VecSimIndex_PreferAdHocSearch(VecSimIndex *index, size_t subsetSize, size_t k,
                                              bool initial_check) {
    return index->preferAdHocSearch(subsetSize, k, initial_check);
}

// Ad-hoc Brute Force Context API - delegates to virtual methods on the index/context
extern "C" VecSimAdhocBfCtx *VecSimIndex_AdhocBfCtx_New(VecSimIndex *index, const void *queryBlob) {
    return index->newAdhocBfCtx(queryBlob);
}

extern "C" void VecSimIndex_AdhocBfCtx_Free(VecSimAdhocBfCtx *ctx) {
    std::shared_ptr<VecSimAllocator> allocator =
        ctx->getAllocator(); // Save allocator so it will not deallocate itself
    delete ctx;              // Virtual destructor handles derived class cleanup
}

extern "C" double VecSimIndex_AdhocBfCtx_GetDistanceFrom(VecSimAdhocBfCtx *ctx, size_t label) {
    return ctx->getDistanceFrom(label);
}

extern "C" void VecSimIndex_AdhocBfCtx_GetExactDistances(VecSimAdhocBfCtx *ctx,
                                                         const size_t *labels,
                                                         double *distances_out, size_t count) {
    ctx->getExactDistances(labels, distances_out, count);
}
