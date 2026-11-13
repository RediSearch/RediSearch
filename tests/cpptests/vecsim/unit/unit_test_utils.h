/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <functional>
#include <cmath>
#include <cstring>
#include <exception>
#include <thread>

#include "VecSim/vec_sim.h"
#include "VecSim/algorithms/hnsw/hnsw_tiered.h"
#include "VecSim/types/sq8.h"
#include "mock_thread_pool.h"
#include "gtest/gtest.h"

using sq8 = vecsim_types::sq8;

// IndexType is used to define indices unit tests
template <VecSimType type, typename DataType, typename DistType = DataType>
struct IndexType {
    static VecSimType get_index_type() { return type; }
    typedef DataType data_t;
    typedef DistType dist_t;
};

#define TEST_DATA_T typename TypeParam::data_t
#define TEST_DIST_T typename TypeParam::dist_t

using DataTypeSet = ::testing::Types<IndexType<VecSimType_FLOAT32, float>
#ifdef FP64_TESTS
                                     ,
                                     IndexType<VecSimType_FLOAT64, double>
#endif
                                     >;

// Define index type for tests that can be automatically generated for single and multi.
template <VecSimType type, bool IsMulti, typename DataType, typename DistType = DataType>
struct IndexTypeExtended {
    static VecSimType get_index_type() { return type; }
    static bool isMulti() { return IsMulti; }
    typedef DataType data_t;
    typedef DistType dist_t;
};

using DataTypeSetExtended = ::testing::Types<IndexTypeExtended<VecSimType_FLOAT32, false, float>,
                                             IndexTypeExtended<VecSimType_FLOAT32, true, float>
#ifdef FP64_TESTS
                                             ,
                                             IndexTypeExtended<VecSimType_FLOAT64, false, double>,
                                             IndexTypeExtended<VecSimType_FLOAT64, true, double>
#endif
                                             >;

template <typename data_t>
static void GenerateVector(data_t *output, size_t dim, data_t value = 1.0) {
    for (size_t i = 0; i < dim; i++) {
        output[i] = (data_t)value;
    }
}

// use std::type_identity to force explicit template specification.
template <typename data_t>
int GenerateAndAddVector(VecSimIndex *index, size_t dim, size_t id,
                         typename std::type_identity<data_t>::type value = 1.0) {
    data_t v[dim];
    GenerateVector(v, dim, value);
    return VecSimIndex_AddVector(index, v, id);
}

template <typename data_t>
void CompareVectors(const data_t *v1, const data_t *v2, size_t dim) {
    auto print_vector = [](const data_t *v, size_t dim) {
        std::ostringstream oss;
        oss << "[";
        for (size_t i = 0; i < dim; i++) {
            oss << +v[i]; // unary + promotes uint8_t to int for printing
            if (i < dim - 1)
                oss << ", ";
        }
        oss << "]";
        return oss.str();
    };
    for (size_t i = 0; i < dim; i++) {
        ASSERT_EQ(v1[i], v2[i]) << "Vectors are not equal at index " << i
                                << "\nv1: " << print_vector(v1, dim)
                                << "\nv2: " << print_vector(v2, dim);
    }
}

inline VecSimParams CreateParams(const HNSWParams &hnsw_params) {
    VecSimParams params{.algo = VecSimAlgo_HNSWLIB,
                        .algoParams = {.hnswParams = HNSWParams{hnsw_params}}};
    return params;
}

inline VecSimParams CreateParams(const BFParams &bf_params) {
    VecSimParams params{.algo = VecSimAlgo_BF, .algoParams = {.bfParams = BFParams{bf_params}}};
    return params;
}

inline VecSimParams CreateParams(const TieredIndexParams &tiered_params) {
    VecSimParams params{.algo = VecSimAlgo_TIERED,
                        .algoParams = {.tieredParams = TieredIndexParams{tiered_params}}};
    return params;
}

inline VecSimParams CreateParams(const SVSParams &svs_params) {
    VecSimParams params{.algo = VecSimAlgo_SVS, .algoParams = {.svsParams = svs_params}};
    return params;
}

namespace test_utils {
template <typename IndexParams>
inline VecSimIndex *CreateNewIndex(IndexParams &index_params, VecSimType type,
                                   bool is_multi = false) {
    index_params.type = type;
    index_params.multi = is_multi;
    VecSimParams params = CreateParams(index_params);
    return VecSimIndex_New(&params);
}

TieredIndexParams CreateTieredParams(VecSimParams &primary_params,
                                     tieredIndexMock &mock_thread_pool);
template <typename backend_index_params>
VecSimIndex *CreateNewTieredVecSimIndex(const backend_index_params &hnsw_params,
                                        tieredIndexMock &mock_thread_pool) {
    VecSimParams primary_params = CreateParams(hnsw_params);
    auto tiered_params = CreateTieredParams(primary_params, mock_thread_pool);
    VecSimParams params = CreateParams(tiered_params);
    VecSimIndex *index = VecSimIndex_New(&params);
    mock_thread_pool.ctx->index_strong_ref.reset(index);

    return index;
}

extern VecsimQueryType query_types[4];

} // namespace test_utils

template <typename IndexParams>
size_t EstimateInitialSize(const IndexParams &index_params) {
    VecSimParams params = CreateParams(index_params);
    return VecSimIndex_EstimateInitialSize(&params);
}

template <typename IndexParams>
size_t EstimateElementSize(const IndexParams &index_params) {
    VecSimParams params = CreateParams(index_params);
    return VecSimIndex_EstimateElementSize(&params);
}

VecSimQueryParams CreateQueryParams(const HNSWRuntimeParams &RuntimeParams);
VecSimQueryParams CreateQueryParams(const SVSRuntimeParams &RuntimeParams);

inline void ASSERT_TYPE_EQ(double arg1, double arg2) { ASSERT_DOUBLE_EQ(arg1, arg2); }

inline void ASSERT_TYPE_EQ(float arg1, float arg2) { ASSERT_FLOAT_EQ(arg1, arg2); }

void runTopKSearchTest(VecSimIndex *index, const void *query, size_t k,
                       std::function<void(size_t, double, size_t)> ResCB,
                       VecSimQueryParams *params = nullptr,
                       VecSimQueryReply_Order order = BY_SCORE);

void runBatchIteratorSearchTest(VecSimBatchIterator *batch_iterator, size_t n_res,
                                std::function<void(size_t, double, size_t)> ResCB,
                                VecSimQueryReply_Order order = BY_SCORE,
                                size_t expected_n_res = -1);

void compareCommonInfo(CommonInfo info1, CommonInfo info2);
void compareFlatInfo(bfInfoStruct info1, bfInfoStruct info2);
void compareHNSWInfo(hnswInfoStruct info1, hnswInfoStruct info2);
void compareSVSInfo(svsInfoStruct info1, svsInfoStruct info2);

void validateSVSIndexAttributesInfo(svsInfoStruct info, SVSParams params);

// When called with a C API iterator (VecSimIndex_DebugInfoIterator), pass the
// default expect_shared_memory=true so the SHARED_MEMORY field is accounted for.
// Pass false when comparing nested sub-iterators built by the C++ debugInfoIterator()
// method directly (e.g. inside compareTieredIndexInfoToIterator).
void compareFlatIndexInfoToIterator(VecSimIndexDebugInfo info, VecSimDebugInfoIterator *infoIter,
                                    bool expect_shared_memory = true);

void compareHNSWIndexInfoToIterator(VecSimIndexDebugInfo info, VecSimDebugInfoIterator *infoIter,
                                    bool expect_shared_memory = true);

void compareTieredIndexInfoToIterator(VecSimIndexDebugInfo info,
                                      VecSimIndexDebugInfo frontendIndexInfo,
                                      VecSimIndexDebugInfo backendIndexInfo,
                                      VecSimDebugInfoIterator *infoIterator);

#if HAVE_SVS
void compareSVSIndexInfoToIterator(VecSimIndexDebugInfo info, VecSimDebugInfoIterator *infoIter,
                                   bool expect_shared_memory = true);
#endif

void runRangeQueryTest(VecSimIndex *index, const void *query, double radius,
                       const std::function<void(size_t, double, size_t)> &ResCB,
                       size_t expected_res_num, VecSimQueryReply_Order order = BY_ID,
                       VecSimQueryParams *params = nullptr);

size_t getLabelsLookupNodeSize();

inline double GetInfVal(VecSimType type) {
    if (type == VecSimType_FLOAT64) {
        return exp(500);
    } else if (type == VecSimType_FLOAT32) {
        return exp(60);
    } else {
        throw std::invalid_argument("This type is not supported");
    }
}
/**
 * Computes SQ8 quantization for a float vector.
 * Output layout: [quantized_values[dim] | min_val | delta | sum | sum_squares]
 *
 * @param original_blob Input float vector
 * @param dim           Dimension of the vector
 * @param output        Output buffer (must be at least dim + 4*sizeof(float) bytes)
 */
inline void ComputeSQ8Quantization(const float *original_blob, size_t dim, uint8_t *output) {
    // Find min and max values
    float min_val = original_blob[0];
    float max_val = original_blob[0];
    for (size_t i = 1; i < dim; i++) {
        min_val = std::min(min_val, original_blob[i]);
        max_val = std::max(max_val, original_blob[i]);
    }

    // Calculate delta
    float diff = max_val - min_val;
    float delta = (diff == 0.0f) ? 1.0f : diff / 255.0f;

    // Quantize, accumulating the bytes as exact integers, then derive the sums over the
    // reconstruction min + delta * a[i], which is what the kernel algebra is written in terms of.
    // See QuantPreprocessor::quantize.
    // 64-bit for the squares: 255^2 * dim passes UINT32_MAX just above dim 66051, and a
    // wrapped counter would corrupt the stored norm rather than round it. Mirrors
    // QuantPreprocessor::quantize.
    uint32_t q_sum = 0;
    uint64_t q_sum_squares = 0;
    for (size_t i = 0; i < dim; i++) {
        float normalized = (original_blob[i] - min_val) / delta;
        const uint32_t q = static_cast<uint8_t>(std::round(normalized));
        output[i] = static_cast<uint8_t>(q);
        q_sum += q;
        q_sum_squares += q * q;
    }

    const double d_min = min_val, d_delta = delta, d_dim = static_cast<double>(dim);
    const float sum = static_cast<float>(d_dim * d_min + d_delta * q_sum);
    const float sum_squares = static_cast<float>(
        d_dim * d_min * d_min + 2.0 * d_min * d_delta * q_sum + d_delta * d_delta * q_sum_squares);

    // Store metadata: min_val, delta, sum, sum_squares. Use memcpy because the metadata region
    // (output + dim) is not guaranteed to be 4-byte aligned for arbitrary dim values.
    const float metadata[4] = {min_val, delta, sum, sum_squares};
    std::memcpy(output + dim, metadata, sizeof(metadata));
}

// TODO: Move all test_utils to this namespace
namespace test_utils {
size_t CalcVectorDataSize(VecSimIndex *index, VecSimType data_type);

template <typename data_t, typename dist_t>
TieredHNSWIndex<data_t, dist_t> *cast_to_tiered_index(VecSimIndex *index) {
    return dynamic_cast<TieredHNSWIndex<data_t, dist_t> *>(index);
}

namespace test_debug_info_iterator_order {
// Test debug info iterator field order (general function for all index types)
void testDebugInfoIteratorFieldOrder(VecSimDebugInfoIterator *infoIterator,
                                     const std::vector<std::string> &expectedFieldOrder);

// Helper function: imitates addCommonInfoToIterator() from vec_sim_index.h (8 fields)
std::vector<std::string> getCommonFields();

// Get common expected field order for all tiered indexes (14 fields: imitates
// VecSimTieredIndex::debugInfoIterator)
std::vector<std::string> getTieredCommonFields();

// Get expected field order for flat/brute force index (10 fields)
std::vector<std::string> getFlatFields();

// Get expected field order for HNSW index (17 fields)
std::vector<std::string> getHNSWFields();

// Get expected field order for SVS index (23 fields)
std::vector<std::string> getSVSFields();

// Get expected field order for tiered SVS index
std::vector<std::string> getTieredSVSFields();

// Get expected field order for tiered HNSW index
std::vector<std::string> getTieredHNSWFields();
} // namespace test_debug_info_iterator_order
} // namespace test_utils

// Test a specific exception type is thrown and prints the right message.
#define ASSERT_EXCEPTION_MESSAGE(VALUE, EXCEPTION_TYPE, MESSAGE)                                   \
    try {                                                                                          \
        VALUE;                                                                                     \
        FAIL() << "exception '" << MESSAGE << "' not thrown at all!";                              \
    } catch (const EXCEPTION_TYPE &e) {                                                            \
        EXPECT_EQ(std::string(MESSAGE), e.what())                                                  \
            << " exception message is incorrect. Expected the following "                          \
               "message:\n\n"                                                                      \
            << MESSAGE << "\n";                                                                    \
    } catch (...) {                                                                                \
        FAIL() << "exception '" << MESSAGE << "' not thrown with expected type '"                  \
               << #EXCEPTION_TYPE << "'!";                                                         \
    }
