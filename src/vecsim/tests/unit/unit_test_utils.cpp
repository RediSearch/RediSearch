/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "unit_test_utils.h"
#include "gtest/gtest.h"
#include "VecSim/utils/vec_utils.h"
#include "VecSim/memory/vecsim_malloc.h"
#include "VecSim/utils/vecsim_stl.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"
#include "VecSim/algorithms/hnsw/hnsw_tiered.h"

using bfloat16 = vecsim_types::bfloat16;
using float16 = vecsim_types::float16;

// Expected number of fields per debug iterator, as emitted by index->debugInfoIterator()
// (the C++ method). The C API VecSimIndex_DebugInfoIterator appends one extra
// SHARED_MEMORY field on top — top-level compare functions add +1 to these counts.
namespace DebugInfoIteratorFieldCount {
constexpr size_t FLAT = 11;
constexpr size_t HNSW = 18;
constexpr size_t SVS = 25;
constexpr size_t TIERED_HNSW = 18;
constexpr size_t TIERED_SVS = 20;
} // namespace DebugInfoIteratorFieldCount

static void chooseCompareIndexInfoToIterator(VecSimIndexDebugInfo info,
                                             VecSimDebugInfoIterator *infoIter);

VecsimQueryType test_utils::query_types[4] = {QUERY_TYPE_NONE, QUERY_TYPE_KNN, QUERY_TYPE_HYBRID,
                                              QUERY_TYPE_RANGE};

static bool allUniqueResults(VecSimQueryReply *res) {
    size_t len = VecSimQueryReply_Len(res);
    auto it1 = VecSimQueryReply_GetIterator(res);
    for (size_t i = 0; i < len; i++) {
        auto ei = VecSimQueryReply_IteratorNext(it1);
        auto it2 = VecSimQueryReply_GetIterator(res);
        for (size_t j = 0; j < i; j++) {
            auto ej = VecSimQueryReply_IteratorNext(it2);
            if (VecSimQueryResult_GetId(ei) == VecSimQueryResult_GetId(ej)) {
                VecSimQueryReply_IteratorFree(it2);
                VecSimQueryReply_IteratorFree(it1);
                return false;
            }
        }
        VecSimQueryReply_IteratorFree(it2);
    }
    VecSimQueryReply_IteratorFree(it1);
    return true;
}

/*
 * helper function to run Top K search and iterate over the results. ResCB is a callback that takes
 * the id, score and index of a result, and performs test-specific logic for each.
 */

VecSimQueryParams CreateQueryParams(const HNSWRuntimeParams &RuntimeParams) {
    VecSimQueryParams QueryParams = {.hnswRuntimeParams = RuntimeParams};
    return QueryParams;
}

VecSimQueryParams CreateQueryParams(const SVSRuntimeParams &RuntimeParams) {
    VecSimQueryParams QueryParams = {.svsRuntimeParams = RuntimeParams};
    return QueryParams;
}

static bool is_async_index(VecSimIndex *index) {
    return dynamic_cast<VecSimTieredIndex<float, float> *>(index) != nullptr ||
           dynamic_cast<VecSimTieredIndex<bfloat16, float> *>(index) != nullptr ||
           dynamic_cast<VecSimTieredIndex<float16, float> *>(index) != nullptr ||
           dynamic_cast<VecSimTieredIndex<uint8_t, float> *>(index) != nullptr ||
           dynamic_cast<VecSimTieredIndex<int8_t, float> *>(index) != nullptr ||
           dynamic_cast<VecSimTieredIndex<double, double> *>(index) != nullptr;
}

void validateTopKSearchTest(VecSimIndex *index, VecSimQueryReply *res, size_t k,
                            std::function<void(size_t, double, size_t)> ResCB) {
    const size_t expected_num_res = std::min(index->indexLabelCount(), k);
    if (is_async_index(index)) {
        // Async index may return more or less than the expected number of results,
        // depending on the number of results that were available at the time of the query.
        // We can estimate the number of results that should be returned to be roughly
        // `expected_num_res` +- number of threads in the pool of the job queue.

        // for now, lets only check that the number of results is not greater than k.
        ASSERT_LE(VecSimQueryReply_Len(res), k);
    } else {
        ASSERT_EQ(VecSimQueryReply_Len(res), expected_num_res);
    }
    ASSERT_TRUE(allUniqueResults(res)) << *res;
    VecSimQueryReply_Iterator *iterator = VecSimQueryReply_GetIterator(res);
    int res_ind = 0;
    while (VecSimQueryReply_IteratorHasNext(iterator)) {
        VecSimQueryResult *item = VecSimQueryReply_IteratorNext(iterator);
        int id = (int)VecSimQueryResult_GetId(item);
        double score = VecSimQueryResult_GetScore(item);
        ResCB(id, score, res_ind++);
    }
    VecSimQueryReply_IteratorFree(iterator);
    VecSimQueryReply_Free(res);
}

void runTopKSearchTest(VecSimIndex *index, const void *query, size_t k,
                       std::function<void(size_t, double, size_t)> ResCB, VecSimQueryParams *params,
                       VecSimQueryReply_Order order) {
    VecSimQueryReply *res = VecSimIndex_TopKQuery(index, query, k, params, order);
    validateTopKSearchTest(index, res, k, ResCB);
}

/*
 * helper function to run batch search iteration, and iterate over the results. ResCB is a callback
 * that takes the id, score and index of a result, and performs test-specific logic for each.
 */
void runBatchIteratorSearchTest(VecSimBatchIterator *batch_iterator, size_t n_res,
                                std::function<void(size_t, double, size_t)> ResCB,
                                VecSimQueryReply_Order order, size_t expected_n_res) {
    if (expected_n_res == SIZE_MAX)
        expected_n_res = n_res;
    VecSimQueryReply *res = VecSimBatchIterator_Next(batch_iterator, n_res, order);
    ASSERT_EQ(VecSimQueryReply_Len(res), expected_n_res);
    ASSERT_TRUE(allUniqueResults(res));
    VecSimQueryReply_Iterator *iterator = VecSimQueryReply_GetIterator(res);
    int res_ind = 0;
    while (VecSimQueryReply_IteratorHasNext(iterator)) {
        VecSimQueryResult *item = VecSimQueryReply_IteratorNext(iterator);
        int id = (int)VecSimQueryResult_GetId(item);
        double score = VecSimQueryResult_GetScore(item);
        ResCB(id, score, res_ind++);
    }
    ASSERT_EQ(res_ind, expected_n_res);
    VecSimQueryReply_IteratorFree(iterator);
    VecSimQueryReply_Free(res);
}

void compareCommonInfo(CommonInfo info1, CommonInfo info2) {
    ASSERT_EQ(info1.basicInfo.dim, info2.basicInfo.dim);
    ASSERT_EQ(info1.basicInfo.metric, info2.basicInfo.metric);
    ASSERT_EQ(info1.indexSize, info2.indexSize);
    ASSERT_EQ(info1.basicInfo.type, info2.basicInfo.type);
    ASSERT_EQ(info1.memory, info2.memory);
    ASSERT_EQ(info1.basicInfo.blockSize, info2.basicInfo.blockSize);
    ASSERT_EQ(info1.basicInfo.isMulti, info2.basicInfo.isMulti);
    ASSERT_EQ(info1.lastMode, info2.lastMode);
    ASSERT_EQ(info1.indexLabelCount, info2.indexLabelCount);
}
void compareFlatInfo(bfInfoStruct info1, bfInfoStruct info2) {}

void compareHNSWInfo(hnswInfoStruct info1, hnswInfoStruct info2) {
    ASSERT_EQ(info1.efConstruction, info2.efConstruction);
    ASSERT_EQ(info1.efRuntime, info2.efRuntime);
    ASSERT_EQ(info1.entrypoint, info2.entrypoint);
    ASSERT_EQ(info1.epsilon, info2.epsilon);
    ASSERT_EQ(info1.M, info2.M);
    ASSERT_EQ(info1.max_level, info2.max_level);
    ASSERT_EQ(info1.visitedNodesPoolSize, info2.visitedNodesPoolSize);
}

void compareSVSInfo(svsInfoStruct info1, svsInfoStruct info2) {
    ASSERT_EQ(info1.alpha, info2.alpha);
    ASSERT_EQ(info1.constructionWindowSize, info2.constructionWindowSize);
    ASSERT_EQ(info1.graphMaxDegree, info2.graphMaxDegree);
    ASSERT_EQ(info1.maxCandidatePoolSize, info2.maxCandidatePoolSize);
    ASSERT_EQ(info1.pruneTo, info2.pruneTo);
    ASSERT_EQ(info1.quantBits, info2.quantBits);
    ASSERT_EQ(info1.searchWindowSize, info2.searchWindowSize);
    ASSERT_EQ(info1.searchBufferCapacity, info2.searchBufferCapacity);
    ASSERT_EQ(info1.leanvecDim, info2.leanvecDim);
    ASSERT_EQ(info1.useSearchHistory, info2.useSearchHistory);
    ASSERT_EQ(info1.epsilon, info2.epsilon);
    ASSERT_EQ(info1.numThreads, info2.numThreads);
    ASSERT_EQ(info1.numberOfMarkedDeletedNodes, info2.numberOfMarkedDeletedNodes);
}

#if HAVE_SVS
#include "VecSim/algorithms/svs/svs_utils.h"

void validateSVSIndexAttributesInfo(svsInfoStruct info, SVSParams params) {
    ASSERT_EQ(info.constructionWindowSize,
              svs_details::getOrDefault(params.construction_window_size,
                                        SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE));
    ASSERT_EQ(info.graphMaxDegree, svs_details::getOrDefault(params.graph_max_degree,
                                                             SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE));
    ASSERT_EQ(
        info.maxCandidatePoolSize,
        svs_details::getOrDefault(params.max_candidate_pool_size, info.constructionWindowSize * 3));
    ASSERT_EQ(info.pruneTo, svs_details::getOrDefault(params.prune_to, info.graphMaxDegree - 4));
    ASSERT_EQ(info.quantBits, get<0>(svs_details::isSVSQuantBitsSupported(params.quantBits)));
    ASSERT_EQ(info.searchWindowSize,
              svs_details::getOrDefault(params.search_window_size,
                                        SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE));
    ASSERT_EQ(info.searchBufferCapacity,
              svs_details::getOrDefault(params.search_buffer_capacity, info.searchWindowSize));
    ASSERT_EQ(info.leanvecDim,
              svs_details::getOrDefault(params.leanvec_dim, SVS_VAMANA_DEFAULT_LEANVEC_DIM));
    ASSERT_EQ(info.epsilon, svs_details::getOrDefault(params.epsilon, SVS_VAMANA_DEFAULT_EPSILON));
    ASSERT_EQ(info.numThreads,
              std::max(size_t{SVS_VAMANA_DEFAULT_NUM_THREADS}, params.num_threads));

    float expected_alpha = params.metric == VecSimMetric_L2 ? SVS_VAMANA_DEFAULT_ALPHA_L2
                                                            : SVS_VAMANA_DEFAULT_ALPHA_IP;
    ASSERT_EQ(info.alpha, svs_details::getOrDefault(params.alpha, expected_alpha));
    bool expected_search_history = params.use_search_history == VecSimOption_AUTO
                                       ? SVS_VAMANA_DEFAULT_USE_SEARCH_HISTORY
                                       : params.use_search_history == VecSimOption_ENABLE;
    ASSERT_EQ(info.useSearchHistory, expected_search_history);
}
#endif

/*
 * helper function to run range query and iterate over the results. ResCB is a callback that takes
 * the id, score and index of a result, and performs test-specific logic for each.
 */
void validateRangeQueryTest(VecSimQueryReply *res,
                            const std::function<void(size_t, double, size_t)> &ResCB,
                            size_t expected_res_num) {

    EXPECT_EQ(VecSimQueryReply_Len(res), expected_res_num);
    EXPECT_TRUE(allUniqueResults(res));
    VecSimQueryReply_Iterator *iterator = VecSimQueryReply_GetIterator(res);
    int res_ind = 0;
    while (VecSimQueryReply_IteratorHasNext(iterator)) {
        VecSimQueryResult *item = VecSimQueryReply_IteratorNext(iterator);
        int id = (int)VecSimQueryResult_GetId(item);
        double score = VecSimQueryResult_GetScore(item);
        ResCB(id, score, res_ind++);
    }
    EXPECT_EQ(res_ind, expected_res_num);
    VecSimQueryReply_IteratorFree(iterator);
    VecSimQueryReply_Free(res);
}

void runRangeQueryTest(VecSimIndex *index, const void *query, double radius,
                       const std::function<void(size_t, double, size_t)> &ResCB,
                       size_t expected_res_num, VecSimQueryReply_Order order,
                       VecSimQueryParams *params) {
    VecSimQueryReply *res =
        VecSimIndex_RangeQuery(index, (const void *)query, radius, params, order);
    validateRangeQueryTest(res, ResCB, expected_res_num);
}

void compareFlatIndexInfoToIterator(VecSimIndexDebugInfo info, VecSimDebugInfoIterator *infoIter,
                                    bool expect_shared_memory) {
    size_t extra = expect_shared_memory ? 1 : 0;
    ASSERT_EQ(DebugInfoIteratorFieldCount::FLAT + extra,
              VecSimDebugInfoIterator_NumberOfFields(infoIter));
    while (VecSimDebugInfoIterator_HasNextField(infoIter)) {
        VecSim_InfoField *infoField = VecSimDebugInfoIterator_NextField(infoIter);
        if (!strcmp(infoField->fieldName, VecSimCommonStrings::ALGORITHM_STRING)) {
            // Algorithm type.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimAlgo_ToString(info.commonInfo.basicInfo.algo));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::TYPE_STRING)) {
            // Vector type.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimType_ToString(info.commonInfo.basicInfo.type));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::DIMENSION_STRING)) {
            // Vector dimension.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.dim);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::METRIC_STRING)) {
            // Metric.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimMetric_ToString(info.commonInfo.basicInfo.metric));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SEARCH_MODE_STRING)) {
            // Search mode.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimSearchMode_ToString(info.commonInfo.lastMode));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::INDEX_SIZE_STRING)) {
            // Index size.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.indexSize);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::INDEX_LABEL_COUNT_STRING)) {
            // Index label count.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.indexLabelCount);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::IS_MULTI_STRING)) {
            // Is the index multi value.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.isMulti);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::IS_DISK_STRING)) {
            // Is the index disk-based.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.isDisk);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::BLOCK_SIZE_STRING)) {
            // Block size.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.blockSize);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::MEMORY_STRING)) {
            // Memory.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.memory);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SHARED_MEMORY_STRING)) {
            // Process-wide shared allocation appended by VecSimIndex_DebugInfoIterator.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, VecSim_GetSharedMemory());
        } else {
            FAIL();
        }
    }
}

void compareHNSWIndexInfoToIterator(VecSimIndexDebugInfo info, VecSimDebugInfoIterator *infoIter,
                                    bool expect_shared_memory) {
    size_t extra = expect_shared_memory ? 1 : 0;
    ASSERT_EQ(DebugInfoIteratorFieldCount::HNSW + extra,
              VecSimDebugInfoIterator_NumberOfFields(infoIter));
    while (VecSimDebugInfoIterator_HasNextField(infoIter)) {
        VecSim_InfoField *infoField = VecSimDebugInfoIterator_NextField(infoIter);
        if (!strcmp(infoField->fieldName, VecSimCommonStrings::ALGORITHM_STRING)) {
            // Algorithm type.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimAlgo_ToString(info.commonInfo.basicInfo.algo));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::TYPE_STRING)) {
            // Vector type.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimType_ToString(info.commonInfo.basicInfo.type));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::DIMENSION_STRING)) {
            // Vector dimension.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.dim);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::METRIC_STRING)) {
            // Metric.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimMetric_ToString(info.commonInfo.basicInfo.metric));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SEARCH_MODE_STRING)) {
            // Search mode.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimSearchMode_ToString(info.commonInfo.lastMode));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::INDEX_SIZE_STRING)) {
            // Index size.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.indexSize);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::INDEX_LABEL_COUNT_STRING)) {
            // Index label count.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.indexLabelCount);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::IS_MULTI_STRING)) {
            // Is the index multi value.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.isMulti);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::IS_DISK_STRING)) {
            // Is the index disk-based.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.isDisk);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::HNSW_EF_CONSTRUCTION_STRING)) {
            // EF construction.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.hnswInfo.efConstruction);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::HNSW_EF_RUNTIME_STRING)) {
            // EF runtime.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.hnswInfo.efRuntime);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::EPSILON_STRING)) {
            // Epsilon.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_FLOAT64);
            ASSERT_EQ(infoField->fieldValue.floatingPointValue, info.hnswInfo.epsilon);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::HNSW_M_STRING)) {
            // M.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.hnswInfo.M);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::HNSW_MAX_LEVEL)) {
            // Levels.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.hnswInfo.max_level);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::HNSW_ENTRYPOINT)) {
            // Entrypoint.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.hnswInfo.entrypoint);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::MEMORY_STRING)) {
            // Memory.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.memory);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::BLOCK_SIZE_STRING)) {
            // Block size.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.blockSize);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::NUM_MARKED_DELETED)) {
            // Number of marked deleted.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue,
                      info.hnswInfo.numberOfMarkedDeletedNodes);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SHARED_MEMORY_STRING)) {
            // Process-wide shared allocation appended by VecSimIndex_DebugInfoIterator.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, VecSim_GetSharedMemory());
        } else {
            FAIL();
        }
    }
}

void compareTieredIndexInfoToIterator(VecSimIndexDebugInfo info,
                                      VecSimIndexDebugInfo frontendIndexInfo,
                                      VecSimIndexDebugInfo backendIndexInfo,
                                      VecSimDebugInfoIterator *infoIterator) {
    // +1 for the SHARED_MEMORY field appended by VecSimIndex_DebugInfoIterator (C API).
    VecSimAlgo backendAlgo = backendIndexInfo.commonInfo.basicInfo.algo;
    if (backendAlgo == VecSimAlgo_HNSWLIB) {
        ASSERT_EQ(DebugInfoIteratorFieldCount::TIERED_HNSW + 1,
                  VecSimDebugInfoIterator_NumberOfFields(infoIterator));
    } else if (backendAlgo == VecSimAlgo_SVS) {
        ASSERT_EQ(DebugInfoIteratorFieldCount::TIERED_SVS + 1,
                  VecSimDebugInfoIterator_NumberOfFields(infoIterator));
    } else {
        FAIL() << "Unsupported backend algorithm";
    }
    while (infoIterator->hasNext()) {
        VecSim_InfoField *infoField = VecSimDebugInfoIterator_NextField(infoIterator);

        if (!strcmp(infoField->fieldName, VecSimCommonStrings::ALGORITHM_STRING)) {
            // Algorithm type.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue, VecSimCommonStrings::TIERED_STRING);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::TYPE_STRING)) {
            // Vector type.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimType_ToString(info.commonInfo.basicInfo.type));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::DIMENSION_STRING)) {
            // Vector dimension.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.dim);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::METRIC_STRING)) {
            // Metric.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimMetric_ToString(info.commonInfo.basicInfo.metric));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SEARCH_MODE_STRING)) {
            // Search mode.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimSearchMode_ToString(info.commonInfo.lastMode));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::INDEX_SIZE_STRING)) {
            // Index size.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.indexSize);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::INDEX_LABEL_COUNT_STRING)) {
            // Index label count.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.indexLabelCount);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::IS_MULTI_STRING)) {
            // Is the index multi value.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.isMulti);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::IS_DISK_STRING)) {
            // Is the index disk-based.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.isDisk);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::MEMORY_STRING)) {
            // Memory.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.memory);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::TIERED_MANAGEMENT_MEMORY_STRING)) {
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.tieredInfo.management_layer_memory);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::TIERED_BACKGROUND_INDEXING_STRING)) {
            ASSERT_EQ(infoField->fieldType, INFOFIELD_INT64);
            ASSERT_EQ(infoField->fieldValue.integerValue, info.tieredInfo.backgroundIndexing);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::FRONTEND_INDEX_STRING)) {
            ASSERT_EQ(infoField->fieldType, INFOFIELD_ITERATOR);
            compareFlatIndexInfoToIterator(frontendIndexInfo, infoField->fieldValue.iteratorValue,
                                           /*expect_shared_memory=*/false);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::BACKEND_INDEX_STRING)) {
            ASSERT_EQ(infoField->fieldType, INFOFIELD_ITERATOR);
            chooseCompareIndexInfoToIterator(backendIndexInfo, infoField->fieldValue.iteratorValue);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::TIERED_BUFFER_LIMIT_STRING)) {
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.tieredInfo.bufferLimit);
            // HNSW specific fields
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::TIERED_HNSW_SWAP_JOBS_THRESHOLD_STRING)) {
            ASSERT_EQ(backendAlgo, VecSimAlgo_HNSWLIB);
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(
                infoField->fieldValue.uintegerValue,
                info.tieredInfo.specificTieredBackendInfo.hnswTieredInfo.pendingSwapJobsThreshold);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::TIERED_HNSW_DEFRAG_RUNS_STRING)) {
            ASSERT_EQ(backendAlgo, VecSimAlgo_HNSWLIB);
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue,
                      info.tieredInfo.specificTieredBackendInfo.hnswTieredInfo.defragRuns);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::TIERED_HNSW_DEFRAG_TIME_NS_STRING)) {
            ASSERT_EQ(backendAlgo, VecSimAlgo_HNSWLIB);
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue,
                      info.tieredInfo.specificTieredBackendInfo.hnswTieredInfo.defragTimeNs);
            // SVS specific fields
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::TIERED_SVS_TRAINING_THRESHOLD_STRING)) {
            ASSERT_EQ(backendAlgo, VecSimAlgo_SVS);
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(
                infoField->fieldValue.uintegerValue,
                info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.trainingTriggerThreshold);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::TIERED_SVS_UPDATE_THRESHOLD_STRING)) {
            ASSERT_EQ(backendAlgo, VecSimAlgo_SVS);
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(
                infoField->fieldValue.uintegerValue,
                info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.updateTriggerThreshold);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::TIERED_SVS_THREADS_RESERVE_TIMEOUT_STRING)) {
            ASSERT_EQ(backendAlgo, VecSimAlgo_SVS);
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue,
                      info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.updateJobWaitTime);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::TIERED_SVS_DEFRAG_RUNS_STRING)) {
            ASSERT_EQ(backendAlgo, VecSimAlgo_SVS);
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue,
                      info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.defragRuns);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::TIERED_SVS_DEFRAG_TIME_NS_STRING)) {
            ASSERT_EQ(backendAlgo, VecSimAlgo_SVS);
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue,
                      info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.defragTimeNs);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SHARED_MEMORY_STRING)) {
            // Process-wide shared allocation appended by VecSimIndex_DebugInfoIterator.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, VecSim_GetSharedMemory());
        } else {
            FAIL();
        }
    }
}

static void chooseCompareIndexInfoToIterator(VecSimIndexDebugInfo info,
                                             VecSimDebugInfoIterator *infoIter) {
    // These are nested C++ iterators (not from the C API), so SHARED_MEMORY is
    // not appended — pass expect_shared_memory=false.
    switch (info.commonInfo.basicInfo.algo) {
    case VecSimAlgo_HNSWLIB:
        compareHNSWIndexInfoToIterator(info, infoIter, /*expect_shared_memory=*/false);
        break;
#if HAVE_SVS
    case VecSimAlgo_SVS:
        compareSVSIndexInfoToIterator(info, infoIter, /*expect_shared_memory=*/false);
        break;
#endif
    default:
        FAIL();
    }
}

#if HAVE_SVS
void compareSVSIndexInfoToIterator(VecSimIndexDebugInfo info, VecSimDebugInfoIterator *infoIter,
                                   bool expect_shared_memory) {
    size_t extra = expect_shared_memory ? 1 : 0;
    ASSERT_EQ(DebugInfoIteratorFieldCount::SVS + extra,
              VecSimDebugInfoIterator_NumberOfFields(infoIter));
    while (VecSimDebugInfoIterator_HasNextField(infoIter)) {
        VecSim_InfoField *infoField = VecSimDebugInfoIterator_NextField(infoIter);
        if (!strcmp(infoField->fieldName, VecSimCommonStrings::ALGORITHM_STRING)) {
            // Algorithm type.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimAlgo_ToString(info.commonInfo.basicInfo.algo));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::TYPE_STRING)) {
            // Vector type.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimType_ToString(info.commonInfo.basicInfo.type));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::DIMENSION_STRING)) {
            // Vector dimension.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.dim);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::METRIC_STRING)) {
            // Metric.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimMetric_ToString(info.commonInfo.basicInfo.metric));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SEARCH_MODE_STRING)) {
            // Search mode.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimSearchMode_ToString(info.commonInfo.lastMode));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::INDEX_SIZE_STRING)) {
            // Index size.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.indexSize);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::INDEX_LABEL_COUNT_STRING)) {
            // Index label count.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.indexLabelCount);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::IS_MULTI_STRING)) {
            // Is the index multi value.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.isMulti);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::IS_DISK_STRING)) {
            // Is the index disk-based.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.isDisk);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::MEMORY_STRING)) {
            // Memory.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.memory);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::BLOCK_SIZE_STRING)) {
            // Block size.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.commonInfo.basicInfo.blockSize);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SVS_QUANT_BITS_STRING)) {
            // SVS quantization bits.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_STRING);
            ASSERT_STREQ(infoField->fieldValue.stringValue,
                         VecSimQuantBits_ToString(info.svsInfo.quantBits));
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SVS_ALPHA_STRING)) {
            // SVS alpha parameter.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_FLOAT64);
            ASSERT_EQ(infoField->fieldValue.floatingPointValue, info.svsInfo.alpha);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::SVS_GRAPH_MAX_DEGREE_STRING)) {
            // SVS graph max degree.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.svsInfo.graphMaxDegree);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SVS_CONSTRUCTION_WS_STRING)) {
            // SVS construction window size.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.svsInfo.constructionWindowSize);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::SVS_MAX_CANDIDATE_POOL_SIZE_STRING)) {
            // SVS max candidate pool size.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.svsInfo.maxCandidatePoolSize);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SVS_PRUNE_TO_STRING)) {
            // SVS prune to parameter.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.svsInfo.pruneTo);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::SVS_USE_SEARCH_HISTORY_STRING)) {
            // SVS use search history.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.svsInfo.useSearchHistory);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SVS_NUM_THREADS_STRING)) {
            // SVS number of threads.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.svsInfo.numThreads);
        } else if (!strcmp(infoField->fieldName,
                           VecSimCommonStrings::SVS_LAST_RESERVED_THREADS_STRING)) {
            // SVS number of last reserved threads.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.svsInfo.lastReservedThreads);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::NUM_MARKED_DELETED)) {
            // SVS number of marked deleted nodes.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.svsInfo.numberOfMarkedDeletedNodes);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SVS_SEARCH_WS_STRING)) {
            // SVS search window size.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.svsInfo.searchWindowSize);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SVS_SEARCH_BC_STRING)) {
            // SVS search buffer capacity.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.svsInfo.searchBufferCapacity);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SVS_LEANVEC_DIM_STRING)) {
            // SVS leanvec dimension.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, info.svsInfo.leanvecDim);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::EPSILON_STRING)) {
            // SVS epsilon parameter.
            ASSERT_EQ(infoField->fieldType, INFOFIELD_FLOAT64);
            ASSERT_EQ(infoField->fieldValue.floatingPointValue, info.svsInfo.epsilon);
        } else if (!strcmp(infoField->fieldName, VecSimCommonStrings::SHARED_MEMORY_STRING)) {
            // Process-wide shared allocation appended by VecSimIndex_DebugInfoIterator.
            ASSERT_TRUE(expect_shared_memory);
            ASSERT_EQ(infoField->fieldType, INFOFIELD_UINT64);
            ASSERT_EQ(infoField->fieldValue.uintegerValue, VecSim_GetSharedMemory());
        } else {
            FAIL();
        }
    }
}
#endif // HAVE_SVS

size_t getLabelsLookupNodeSize() {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    auto dummy_lookup = vecsim_stl::unordered_map<size_t, unsigned int>(1, allocator);
    size_t memory_before = allocator->getAllocationSize();
    dummy_lookup.insert({1, 1}); // Insert a dummy {key, value} element pair.
    size_t memory_after = allocator->getAllocationSize();
    return memory_after - memory_before;
}
namespace test_utils {
size_t CalcVectorDataSize(VecSimIndex *index, VecSimType data_type) {
    switch (data_type) {
    case VecSimType_FLOAT32: {
        VecSimIndexAbstract<float, float> *abs_index =
            dynamic_cast<VecSimIndexAbstract<float, float> *>(index);
        assert(abs_index &&
               "dynamic_cast failed: can't convert index to VecSimIndexAbstract<float, float>");
        return abs_index->getStoredDataSize();
    }
    case VecSimType_FLOAT64: {
        VecSimIndexAbstract<double, double> *abs_index =
            dynamic_cast<VecSimIndexAbstract<double, double> *>(index);
        assert(abs_index &&
               "dynamic_cast failed: can't convert index to VecSimIndexAbstract<double, double>");
        return abs_index->getStoredDataSize();
    }
    case VecSimType_BFLOAT16: {
        VecSimIndexAbstract<vecsim_types::bfloat16, float> *abs_index =
            dynamic_cast<VecSimIndexAbstract<vecsim_types::bfloat16, float> *>(index);
        assert(abs_index && "dynamic_cast failed: can't convert index to "
                            "VecSimIndexAbstract<vecsim_types::bfloat16, float>");
        return abs_index->getStoredDataSize();
    }
    case VecSimType_FLOAT16: {
        VecSimIndexAbstract<vecsim_types::float16, float> *abs_index =
            dynamic_cast<VecSimIndexAbstract<vecsim_types::float16, float> *>(index);
        assert(abs_index && "dynamic_cast failed: can't convert index to "
                            "VecSimIndexAbstract<vecsim_types::float16, float>");
        return abs_index->getStoredDataSize();
    }
    case VecSimType_INT8: {
        VecSimIndexAbstract<int8_t, float> *abs_index =
            dynamic_cast<VecSimIndexAbstract<int8_t, float> *>(index);
        assert(abs_index &&
               "dynamic_cast failed: can't convert index to VecSimIndexAbstract<int8_t, float>");
        return abs_index->getStoredDataSize();
    }
    case VecSimType_UINT8: {
        VecSimIndexAbstract<uint8_t, float> *abs_index =
            dynamic_cast<VecSimIndexAbstract<uint8_t, float> *>(index);
        assert(abs_index &&
               "dynamic_cast failed: can't convert index to VecSimIndexAbstract<uint8_t, float>");
        return abs_index->getStoredDataSize();
    }
    default:
        return 0;
    }
}

TieredIndexParams CreateTieredParams(VecSimParams &primary_params,
                                     tieredIndexMock &mock_thread_pool) {
    TieredIndexParams tiered_params = {.jobQueue = &mock_thread_pool.jobQ,
                                       .jobQueueCtx = mock_thread_pool.ctx,
                                       .submitCb = tieredIndexMock::submit_callback,
                                       .flatBufferLimit = SIZE_MAX,
                                       .primaryIndexParams = &primary_params,
                                       .specificParams = {TieredHNSWParams{.swapJobThreshold = 0}}};

    return tiered_params;
}

namespace test_debug_info_iterator_order {
std::vector<std::string> getCommonFields() {
    return {
        VecSimCommonStrings::TYPE_STRING,              // 1. TYPE
        VecSimCommonStrings::DIMENSION_STRING,         // 2. DIMENSION
        VecSimCommonStrings::METRIC_STRING,            // 3. METRIC
        VecSimCommonStrings::IS_MULTI_STRING,          // 4. IS_MULTI
        VecSimCommonStrings::IS_DISK_STRING,           // 5. IS_DISK
        VecSimCommonStrings::INDEX_SIZE_STRING,        // 6. INDEX_SIZE
        VecSimCommonStrings::INDEX_LABEL_COUNT_STRING, // 7. INDEX_LABEL_COUNT
        VecSimCommonStrings::MEMORY_STRING,            // 8. MEMORY
        VecSimCommonStrings::SEARCH_MODE_STRING        // 9. SEARCH_MODE
    };
}

// Each of these field-list functions mirrors the output of VecSimIndex_DebugInfoIterator (C API),
// which appends SHARED_MEMORY after the algorithm's own debugInfoIterator() output.
std::vector<std::string> getFlatFields() {
    std::vector<std::string> fields;
    fields.push_back(VecSimCommonStrings::ALGORITHM_STRING); // ALGORITHM
    auto commonFields = getCommonFields();
    fields.insert(fields.end(), commonFields.begin(), commonFields.end());
    fields.push_back(VecSimCommonStrings::BLOCK_SIZE_STRING); // BLOCK_SIZE
    fields.push_back(VecSimCommonStrings::SHARED_MEMORY_STRING);
    return fields;
}

// Imitates HNSWIndex<DataType, DistType>::debugInfoIterator() + C API SHARED_MEMORY field.
std::vector<std::string> getHNSWFields() {
    std::vector<std::string> fields;
    fields.push_back(VecSimCommonStrings::ALGORITHM_STRING); // ALGORITHM
    auto commonFields = getCommonFields();
    fields.insert(fields.end(), commonFields.begin(), commonFields.end());
    // Then HNSW-specific fields:
    fields.push_back(VecSimCommonStrings::BLOCK_SIZE_STRING);
    fields.push_back(VecSimCommonStrings::HNSW_M_STRING);
    fields.push_back(VecSimCommonStrings::HNSW_EF_CONSTRUCTION_STRING);
    fields.push_back(VecSimCommonStrings::HNSW_EF_RUNTIME_STRING);
    fields.push_back(VecSimCommonStrings::HNSW_MAX_LEVEL);
    fields.push_back(VecSimCommonStrings::HNSW_ENTRYPOINT);
    fields.push_back(VecSimCommonStrings::EPSILON_STRING);
    fields.push_back(VecSimCommonStrings::NUM_MARKED_DELETED);
    fields.push_back(VecSimCommonStrings::SHARED_MEMORY_STRING);
    return fields;
}

// Imitates SVSIndex<DataType, DistType>::debugInfoIterator() + C API SHARED_MEMORY field.
std::vector<std::string> getSVSFields() {
    std::vector<std::string> fields;
    fields.push_back(VecSimCommonStrings::ALGORITHM_STRING); // ALGORITHM
    auto commonFields = getCommonFields();
    fields.insert(fields.end(), commonFields.begin(), commonFields.end());
    // Then SVS-specific fields:
    fields.push_back(VecSimCommonStrings::BLOCK_SIZE_STRING);
    fields.push_back(VecSimCommonStrings::SVS_QUANT_BITS_STRING);
    fields.push_back(VecSimCommonStrings::SVS_ALPHA_STRING);
    fields.push_back(VecSimCommonStrings::SVS_GRAPH_MAX_DEGREE_STRING);
    fields.push_back(VecSimCommonStrings::SVS_CONSTRUCTION_WS_STRING);
    fields.push_back(VecSimCommonStrings::SVS_MAX_CANDIDATE_POOL_SIZE_STRING);
    fields.push_back(VecSimCommonStrings::SVS_PRUNE_TO_STRING);
    fields.push_back(VecSimCommonStrings::SVS_USE_SEARCH_HISTORY_STRING);
    fields.push_back(VecSimCommonStrings::SVS_NUM_THREADS_STRING);
    fields.push_back(VecSimCommonStrings::SVS_LAST_RESERVED_THREADS_STRING);
    fields.push_back(VecSimCommonStrings::NUM_MARKED_DELETED);
    fields.push_back(VecSimCommonStrings::SVS_SEARCH_WS_STRING);
    fields.push_back(VecSimCommonStrings::SVS_SEARCH_BC_STRING);
    fields.push_back(VecSimCommonStrings::SVS_LEANVEC_DIM_STRING);
    fields.push_back(VecSimCommonStrings::EPSILON_STRING);
    fields.push_back(VecSimCommonStrings::SHARED_MEMORY_STRING);
    return fields;
}

// Imitates VecSimTieredIndex<DataType, DistType>::debugInfoIterator()
std::vector<std::string> getTieredCommonFields() {
    std::vector<std::string> fields;
    fields.push_back(VecSimCommonStrings::ALGORITHM_STRING); // ALGORITHM (set to "TIERED")
    auto commonFields = getCommonFields();
    fields.insert(fields.end(), commonFields.begin(),
                  commonFields.end()); // backendIndex->addCommonInfoToIterator()
    // Then tiered-specific fields:
    fields.push_back(VecSimCommonStrings::TIERED_MANAGEMENT_MEMORY_STRING);
    fields.push_back(VecSimCommonStrings::TIERED_BACKGROUND_INDEXING_STRING);
    fields.push_back(VecSimCommonStrings::TIERED_BUFFER_LIMIT_STRING);
    fields.push_back(VecSimCommonStrings::FRONTEND_INDEX_STRING);
    fields.push_back(VecSimCommonStrings::BACKEND_INDEX_STRING);
    return fields;
}

// Imitates TieredSVSIndex<DataType, DistType>::debugInfoIterator() + C API SHARED_MEMORY field.
std::vector<std::string> getTieredSVSFields() {
    auto fields = getTieredCommonFields();
    // Add SVS tiered-specific fields:
    fields.push_back(VecSimCommonStrings::TIERED_SVS_TRAINING_THRESHOLD_STRING);
    fields.push_back(VecSimCommonStrings::TIERED_SVS_UPDATE_THRESHOLD_STRING);
    fields.push_back(VecSimCommonStrings::TIERED_SVS_THREADS_RESERVE_TIMEOUT_STRING);
    fields.push_back(VecSimCommonStrings::TIERED_SVS_DEFRAG_RUNS_STRING);
    fields.push_back(VecSimCommonStrings::TIERED_SVS_DEFRAG_TIME_NS_STRING);
    fields.push_back(VecSimCommonStrings::SHARED_MEMORY_STRING);
    return fields;
}

// Imitates TieredHNSWIndex<DataType, DistType>::debugInfoIterator() + C API SHARED_MEMORY field.
std::vector<std::string> getTieredHNSWFields() {
    auto fields = getTieredCommonFields();
    // Add HNSW tiered-specific field:
    fields.push_back(
        VecSimCommonStrings::
            TIERED_HNSW_SWAP_JOBS_THRESHOLD_STRING); // 15. TIERED_HNSW_SWAP_JOBS_THRESHOLD
    fields.push_back(VecSimCommonStrings::TIERED_HNSW_DEFRAG_RUNS_STRING);    // 16.
    fields.push_back(VecSimCommonStrings::TIERED_HNSW_DEFRAG_TIME_NS_STRING); // 17.
    fields.push_back(VecSimCommonStrings::SHARED_MEMORY_STRING);
    return fields;
}

void testDebugInfoIteratorFieldOrder(VecSimDebugInfoIterator *infoIterator,
                                     const std::vector<std::string> &expectedFieldOrder) {
    // Verify the total number of fields matches expected
    ASSERT_EQ(VecSimDebugInfoIterator_NumberOfFields(infoIterator), expectedFieldOrder.size());

    // Iterate through the fields and verify the order
    size_t fieldIndex = 0;
    while (VecSimDebugInfoIterator_HasNextField(infoIterator)) {
        VecSim_InfoField *infoField = VecSimDebugInfoIterator_NextField(infoIterator);
        ASSERT_LT(fieldIndex, expectedFieldOrder.size())
            << "More fields than expected. Field index: " << fieldIndex;

        ASSERT_STREQ(infoField->fieldName, expectedFieldOrder[fieldIndex].c_str())
            << "Field order mismatch at index " << fieldIndex
            << ". Expected: " << expectedFieldOrder[fieldIndex]
            << ", Got: " << infoField->fieldName;

        fieldIndex++;
    }

    // Verify we processed all expected fields
    ASSERT_EQ(fieldIndex, expectedFieldOrder.size())
        << "Fewer fields than expected. Processed: " << fieldIndex
        << ", Expected: " << expectedFieldOrder.size();
}
} // namespace test_debug_info_iterator_order
} // namespace test_utils
