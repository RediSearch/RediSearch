/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "vec_utils.h"
#include "VecSim/query_result_definitions.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"
#include <cmath>
#include <cerrno>
#include <climits>
#include <float.h>
#include <algorithm>

using bfloat16 = vecsim_types::bfloat16;
using float16 = vecsim_types::float16;

const char *VecSimCommonStrings::ALGORITHM_STRING = "ALGORITHM";
const char *VecSimCommonStrings::FLAT_STRING = "FLAT";
const char *VecSimCommonStrings::HNSW_STRING = "HNSW";
const char *VecSimCommonStrings::TIERED_STRING = "TIERED";
const char *VecSimCommonStrings::SVS_STRING = "SVS";

const char *VecSimCommonStrings::TYPE_STRING = "TYPE";
const char *VecSimCommonStrings::FLOAT32_STRING = "FLOAT32";
const char *VecSimCommonStrings::FLOAT64_STRING = "FLOAT64";
const char *VecSimCommonStrings::BFLOAT16_STRING = "BFLOAT16";
const char *VecSimCommonStrings::FLOAT16_STRING = "FLOAT16";
const char *VecSimCommonStrings::INT8_STRING = "INT8";
const char *VecSimCommonStrings::UINT8_STRING = "UINT8";
const char *VecSimCommonStrings::INT32_STRING = "INT32";
const char *VecSimCommonStrings::INT64_STRING = "INT64";

const char *VecSimCommonStrings::METRIC_STRING = "METRIC";
const char *VecSimCommonStrings::COSINE_STRING = "COSINE";
const char *VecSimCommonStrings::IP_STRING = "IP";
const char *VecSimCommonStrings::L2_STRING = "L2";

const char *VecSimCommonStrings::DIMENSION_STRING = "DIMENSION";
const char *VecSimCommonStrings::INDEX_SIZE_STRING = "INDEX_SIZE";
const char *VecSimCommonStrings::INDEX_LABEL_COUNT_STRING = "INDEX_LABEL_COUNT";
const char *VecSimCommonStrings::IS_MULTI_STRING = "IS_MULTI_VALUE";
const char *VecSimCommonStrings::IS_DISK_STRING = "IS_DISK";
const char *VecSimCommonStrings::MEMORY_STRING = "MEMORY";

const char *VecSimCommonStrings::HNSW_EF_RUNTIME_STRING = "EF_RUNTIME";
const char *VecSimCommonStrings::HNSW_M_STRING = "M";
const char *VecSimCommonStrings::HNSW_EF_CONSTRUCTION_STRING = "EF_CONSTRUCTION";
const char *VecSimCommonStrings::EPSILON_STRING = "EPSILON";
const char *VecSimCommonStrings::HNSW_MAX_LEVEL = "MAX_LEVEL";
const char *VecSimCommonStrings::HNSW_ENTRYPOINT = "ENTRYPOINT";
const char *VecSimCommonStrings::NUM_MARKED_DELETED = "NUMBER_OF_MARKED_DELETED";
const char *VecSimCommonStrings::HNSW_RERANK_STRING = "RERANK";

const char *VecSimCommonStrings::SVS_SEARCH_WS_STRING = "SEARCH_WINDOW_SIZE";
const char *VecSimCommonStrings::SVS_CONSTRUCTION_WS_STRING = "CONSTRUCTION_WINDOW_SIZE";
const char *VecSimCommonStrings::SVS_SEARCH_BC_STRING = "SEARCH_BUFFER_CAPACITY";
const char *VecSimCommonStrings::SVS_USE_SEARCH_HISTORY_STRING = "USE_SEARCH_HISTORY";
const char *VecSimCommonStrings::SVS_ALPHA_STRING = "ALPHA";
const char *VecSimCommonStrings::SVS_QUANT_BITS_STRING = "QUANT_BITS";
const char *VecSimCommonStrings::SVS_GRAPH_MAX_DEGREE_STRING = "GRAPH_MAX_DEGREE";
const char *VecSimCommonStrings::SVS_MAX_CANDIDATE_POOL_SIZE_STRING = "MAX_CANDIDATE_POOL_SIZE";
const char *VecSimCommonStrings::SVS_PRUNE_TO_STRING = "PRUNE_TO";
const char *VecSimCommonStrings::SVS_NUM_THREADS_STRING = "NUM_THREADS";
const char *VecSimCommonStrings::SVS_LAST_RESERVED_THREADS_STRING = "LAST_RESERVED_NUM_THREADS";
const char *VecSimCommonStrings::SVS_LEANVEC_DIM_STRING = "LEANVEC_DIMENSION";

const char *VecSimCommonStrings::BLOCK_SIZE_STRING = "BLOCK_SIZE";
const char *VecSimCommonStrings::SEARCH_MODE_STRING = "LAST_SEARCH_MODE";
const char *VecSimCommonStrings::HYBRID_POLICY_STRING = "HYBRID_POLICY";
const char *VecSimCommonStrings::BATCH_SIZE_STRING = "BATCH_SIZE";

const char *VecSimCommonStrings::TIERED_MANAGEMENT_MEMORY_STRING = "MANAGEMENT_LAYER_MEMORY";
const char *VecSimCommonStrings::TIERED_BACKGROUND_INDEXING_STRING = "BACKGROUND_INDEXING";
const char *VecSimCommonStrings::TIERED_BUFFER_LIMIT_STRING = "TIERED_BUFFER_LIMIT";
const char *VecSimCommonStrings::FRONTEND_INDEX_STRING = "FRONTEND_INDEX";
const char *VecSimCommonStrings::BACKEND_INDEX_STRING = "BACKEND_INDEX";
// Tiered HNSW specific
const char *VecSimCommonStrings::TIERED_HNSW_SWAP_JOBS_THRESHOLD_STRING =
    "TIERED_HNSW_SWAP_JOBS_THRESHOLD";
const char *VecSimCommonStrings::TIERED_HNSW_DEFRAG_RUNS_STRING = "TIERED_HNSW_DEFRAG_RUNS";
const char *VecSimCommonStrings::TIERED_HNSW_DEFRAG_TIME_NS_STRING = "TIERED_HNSW_DEFRAG_TIME_NS";
// Tiered SVS specific
const char *VecSimCommonStrings::TIERED_SVS_TRAINING_THRESHOLD_STRING =
    "TIERED_SVS_TRAINING_THRESHOLD";
const char *VecSimCommonStrings::TIERED_SVS_UPDATE_THRESHOLD_STRING = "TIERED_SVS_UPDATE_THRESHOLD";
const char *VecSimCommonStrings::TIERED_SVS_DEFRAG_RUNS_STRING = "TIERED_SVS_DEFRAG_RUNS";
const char *VecSimCommonStrings::TIERED_SVS_DEFRAG_TIME_NS_STRING = "TIERED_SVS_DEFRAG_TIME_NS";
const char *VecSimCommonStrings::TIERED_SVS_THREADS_RESERVE_TIMEOUT_STRING =
    "TIERED_SVS_THREADS_RESERVE_TIMEOUT";

const char *VecSimCommonStrings::SHARED_MEMORY_STRING = "SHARED_MEMORY";

// Log levels
const char *VecSimCommonStrings::LOG_DEBUG_STRING = "debug";
const char *VecSimCommonStrings::LOG_VERBOSE_STRING = "verbose";
const char *VecSimCommonStrings::LOG_NOTICE_STRING = "notice";
const char *VecSimCommonStrings::LOG_WARNING_STRING = "warning";

void sort_results_by_id(VecSimQueryReply *rep) {
    std::sort(rep->results.begin(), rep->results.end(),
              [](const VecSimQueryResult &a, const VecSimQueryResult &b) { return a.id < b.id; });
}

void sort_results_by_score(VecSimQueryReply *rep) {
    std::sort(
        rep->results.begin(), rep->results.end(),
        [](const VecSimQueryResult &a, const VecSimQueryResult &b) { return a.score < b.score; });
}

void sort_results_by_score_then_id(VecSimQueryReply *rep) {
    std::sort(rep->results.begin(), rep->results.end(),
              [](const VecSimQueryResult &a, const VecSimQueryResult &b) {
                  if (a.score == b.score) {
                      return a.id < b.id;
                  }
                  return a.score < b.score;
              });
}

void sort_results(VecSimQueryReply *rep, VecSimQueryReply_Order order) {
    switch (order) {
    case BY_ID:
        return sort_results_by_id(rep);
    case BY_SCORE:
        return sort_results_by_score(rep);
    case BY_SCORE_THEN_ID:
        return sort_results_by_score_then_id(rep);
    }
}

VecSimResolveCode validate_positive_integer_param(VecSimRawParam rawParam, long long *val) {
    char *ep; // For checking that strtoll used all rawParam.valLen chars.
    errno = 0;
    *val = strtoll(rawParam.value, &ep, 0);
    // Here we verify that val is positive and strtoll was successful.
    // The last test checks that the entire rawParam.value was used.
    // We catch here inputs like "3.14", "123text" and so on.
    if (*val <= 0 || *val == LLONG_MAX || errno != 0 || (rawParam.value + rawParam.valLen) != ep) {
        return VecSimParamResolverErr_BadValue;
    }
    return VecSimParamResolver_OK;
}

VecSimResolveCode validate_positive_double_param(VecSimRawParam rawParam, double *val) {
    char *ep; // For checking that strtold used all rawParam.valLen chars.
    errno = 0;
    *val = strtod(rawParam.value, &ep);
    // Here we verify that val is positive and strtod was successful.
    // The last test checks that the entire rawParam.value was used.
    // We catch here inputs like "-3.14", "123text" and so on.
    if (*val <= 0 || *val == DBL_MAX || errno != 0 || (rawParam.value + rawParam.valLen) != ep) {
        return VecSimParamResolverErr_BadValue;
    }
    return VecSimParamResolver_OK;
}

VecSimResolveCode validate_vecsim_bool_param(VecSimRawParam rawParam, VecSimOptionMode *val) {
    // Here we verify that given value is strictly ON or OFF
    std::string value(rawParam.value, rawParam.valLen);
    std::transform(value.begin(), value.end(), value.begin(), ::toupper);
    if (value == "ON") {
        *val = VecSimOption_ENABLE;
    } else if (value == "OFF") {
        *val = VecSimOption_DISABLE;
    } else if (value == "AUTO") {
        *val = VecSimOption_AUTO;
    } else {
        return VecSimParamResolverErr_BadValue;
    }
    return VecSimParamResolver_OK;
}

VecSimResolveCode validate_vecsim_tristate_bool_param(VecSimRawParam rawParam, VecSimBool *val) {
    std::string value(rawParam.value, rawParam.valLen);
    std::transform(value.begin(), value.end(), value.begin(), ::toupper);
    if (value == "TRUE") {
        *val = VecSimBool_TRUE;
    } else if (value == "FALSE") {
        *val = VecSimBool_FALSE;
    } else {
        return VecSimParamResolverErr_BadValue;
    }
    return VecSimParamResolver_OK;
}

const char *VecSimAlgo_ToString(VecSimAlgo vecsimAlgo) {
    switch (vecsimAlgo) {
    case VecSimAlgo_BF:
        return VecSimCommonStrings::FLAT_STRING;
    case VecSimAlgo_HNSWLIB:
        return VecSimCommonStrings::HNSW_STRING;
    case VecSimAlgo_TIERED:
        return VecSimCommonStrings::TIERED_STRING;
    case VecSimAlgo_SVS:
        return VecSimCommonStrings::SVS_STRING;
    }
    return NULL;
}
const char *VecSimType_ToString(VecSimType vecsimType) {
    switch (vecsimType) {
    case VecSimType_FLOAT32:
        return VecSimCommonStrings::FLOAT32_STRING;
    case VecSimType_FLOAT64:
        return VecSimCommonStrings::FLOAT64_STRING;
    case VecSimType_BFLOAT16:
        return VecSimCommonStrings::BFLOAT16_STRING;
    case VecSimType_FLOAT16:
        return VecSimCommonStrings::FLOAT16_STRING;
    case VecSimType_INT8:
        return VecSimCommonStrings::INT8_STRING;
    case VecSimType_UINT8:
        return VecSimCommonStrings::UINT8_STRING;
    case VecSimType_INT32:
        return VecSimCommonStrings::INT32_STRING;
    case VecSimType_INT64:
        return VecSimCommonStrings::INT64_STRING;
    }
    return NULL;
}

const char *VecSimMetric_ToString(VecSimMetric vecsimMetric) {
    switch (vecsimMetric) {
    case VecSimMetric_Cosine:
        return "COSINE";
    case VecSimMetric_IP:
        return "IP";
    case VecSimMetric_L2:
        return "L2";
    }
    return NULL;
}

const char *VecSimSearchMode_ToString(VecSearchMode vecsimSearchMode) {
    switch (vecsimSearchMode) {
    case EMPTY_MODE:
        return "EMPTY_MODE";
    case STANDARD_KNN:
        return "STANDARD_KNN";
    case HYBRID_ADHOC_BF:
        return "HYBRID_ADHOC_BF";
    case HYBRID_BATCHES:
        return "HYBRID_BATCHES";
    case HYBRID_BATCHES_TO_ADHOC_BF:
        return "HYBRID_BATCHES_TO_ADHOC_BF";
    case RANGE_QUERY:
        return "RANGE_QUERY";
    }
    return NULL;
}

const char *VecSimQuantBits_ToString(VecSimSvsQuantBits quantBits) {
    switch (quantBits) {
    case VecSimSvsQuant_NONE:
        return "NONE";
    case VecSimSvsQuant_Scalar:
        return "Scalar";
    case VecSimSvsQuant_4:
        return "4";
    case VecSimSvsQuant_8:
        return "8";
    case VecSimSvsQuant_4x4:
        return "4x4";
    case VecSimSvsQuant_4x8:
        return "4x8";
    case VecSimSvsQuant_4x8_LeanVec:
        return "4x8_LeanVec";
    case VecSimSvsQuant_8x8_LeanVec:
        return "8x8_LeanVec";
    }
    return NULL;
}

size_t VecSimType_sizeof(VecSimType type) {
    switch (type) {
    case VecSimType_FLOAT32:
        return sizeof(float);
    case VecSimType_FLOAT64:
        return sizeof(double);
    case VecSimType_BFLOAT16:
        return sizeof(bfloat16);
    case VecSimType_FLOAT16:
        return sizeof(float16);
    case VecSimType_INT8:
        return sizeof(int8_t);
    case VecSimType_UINT8:
        return sizeof(uint8_t);
    case VecSimType_INT32:
        return sizeof(int32_t);
    case VecSimType_INT64:
        return sizeof(int64_t);
    }
    return 0;
}

size_t VecSimParams_GetStoredDataSize(VecSimType type, size_t dim, VecSimMetric metric) {
    size_t storedDataSize = VecSimType_sizeof(type) * dim;
    if (metric == VecSimMetric_Cosine && (type == VecSimType_INT8 || type == VecSimType_UINT8)) {
        storedDataSize += sizeof(float); // For the norm
    }
    return storedDataSize;
}
