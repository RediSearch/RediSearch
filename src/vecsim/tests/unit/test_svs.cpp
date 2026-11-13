/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "VecSim/vec_sim.h"
#include "VecSim/vec_sim_index.h"
#include "unit_test_utils.h"
#include <array>
#include <cmath>
#include <filesystem>
#include <random>
#include <vector>
#if HAVE_SVS
#include <sstream>
#include "spdlog/sinks/ostream_sink.h"
#include "VecSim/algorithms/svs/svs.h"
#include "VecSim/index_factories/svs_factory.h"

// There are possible cases when SVS Index cannot be created with the requested quantization mode
// due to platform and/or hardware limitations or combination of requested 'compression' modes.
// This assert handle those cases and skip a test if the mode is not supported.
// Elsewhere, test will fail if the index creation failed with no reason explained above.
#define ASSERT_INDEX(index)                                                                        \
    if (index == nullptr) {                                                                        \
        if (std::get<1>(svs_details::isSVSQuantBitsSupported(TypeParam::get_quant_bits()))) {      \
            GTEST_FAIL() << "Failed to create SVS index";                                          \
        } else {                                                                                   \
            GTEST_SKIP() << "SVS LVQ is not supported.";                                           \
        }                                                                                          \
    }

// Expected allocation for one block of `block_size` element slots, of which only
// `num_elements` are actually populated.
//
static size_t EstimateBlockSize(const SVSParams &params, size_t block_size, size_t num_elements) {
    const size_t reverse_edges_per_slot = SVSGraphBuilder<uint32_t>::reverse_edges_element_size();
    const size_t reverse_edges_slots = svs::lib::SegmentedVector<uint8_t>(num_elements).capacity();
    return (EstimateElementSize(params) - reverse_edges_per_slot) * block_size +
           reverse_edges_per_slot * reverse_edges_slots;
}

// Log callback function to print non-debug log messages
static void svsTestLogCallBackNoDebug(void *ctx, const char *level, const char *message) {
    if (level == nullptr || message == nullptr) {
        return; // Skip null messages
    }
    if (std::string_view{level} == VecSimCommonStrings::LOG_DEBUG_STRING) {
        return; // Skip debug messages
    }
    // Print other log levels
    std::cout << level << ": " << message << std::endl;
}
template <typename index_type_t>
class SVSTest : public ::testing::Test {
public:
    using data_t = typename index_type_t::data_t;

protected:
    void SetTypeParams(SVSParams &params) {
        params.quantBits = params.quantBits == VecSimSvsQuant_NONE ? index_type_t::get_quant_bits()
                                                                   : params.quantBits;
        params.type = index_type_t::get_index_type();
        params.multi = false;
    }

    VecSimIndex *CreateNewIndex(const VecSimParams &index_params) {
        return VecSimIndex_New(&index_params);
    }

    VecSimIndex *CreateNewIndex(SVSParams &params) {
        SetTypeParams(params);
        VecSimParams index_params = CreateParams(params);
        return CreateNewIndex(index_params);
    }

    SVSIndexBase *CastToSVS(VecSimIndex *index) {
        auto indexBase = dynamic_cast<SVSIndexBase *>(index);
        assert(indexBase != nullptr);
        return indexBase;
    }

    void SetUp() override {
        // Limit VecSim log level to avoid printing too much information
        VecSimIndexInterface::setLogCallbackFunction(svsTestLogCallBackNoDebug);
    }

    // Check if the test is running in fallback mode to scalar quantization.
    bool isFallbackToSQ() const {
        // Get the fallback quantization mode and compare it to the scalar quantization mode.
        return VecSimSvsQuant_Scalar ==
               std::get<0>(svs_details::isSVSQuantBitsSupported(index_type_t::get_quant_bits()));
    }

    // Use svsInfoStruct for parameter validation (ignoring additional runtime fields)
    using ExpectedSVSValues = svsInfoStruct;

    // Helper method to validate SVS parameters using debugInfo
    static void validateSVSParameters(VecSimIndex *index, const ExpectedSVSValues &expected) {
        // Get debug info to validate all parameters
        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);

        // Validate basic index properties
        EXPECT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
        compareSVSInfo(info.svsInfo, expected);
    }
};

// TEST_DATA_T and TEST_DIST_T are defined in test_utils.h

template <VecSimType type, typename DataType, VecSimSvsQuantBits quantBits>
struct SVSIndexType {
    static constexpr VecSimType get_index_type() { return type; }
    static constexpr VecSimSvsQuantBits get_quant_bits() { return quantBits; }
    typedef DataType data_t;
};

// clang-format off
using SVSDataTypeSet = ::testing::Types<SVSIndexType<VecSimType_FLOAT32, float, VecSimSvsQuant_NONE>
                                       ,SVSIndexType<VecSimType_FLOAT32, float, VecSimSvsQuant_8>
                                       ,SVSIndexType<VecSimType_FLOAT32, float, VecSimSvsQuant_8x8_LeanVec>
                                        >;
// clang-format on

TYPED_TEST_SUITE(SVSTest, SVSDataTypeSet);

TYPED_TEST(SVSTest, svs_vector_add_test) {

    size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_IP,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    EXPECT_EQ(VecSimIndex_IndexSize(index), 0);

    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1);

    EXPECT_EQ(VecSimIndex_IndexSize(index), 1);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, svs_vector_update_test) {
    size_t dim = 4;
    size_t n = 1;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_IP,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto *svs_index = this->CastToSVS(index);

    EXPECT_EQ(VecSimIndex_IndexSize(index), 0);

    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1);

    EXPECT_EQ(VecSimIndex_IndexSize(index), 1);

    // Prepare new vector data and call addVector with the same id, different data.
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 2.0);

    // Now we have one deleted (not empty) slot
    // and one live slot
    EXPECT_EQ(VecSimIndex_IndexSize(index), 2);
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), 1);

    index->runGC();
    // GC consolidated and compacted deleted slot.
    EXPECT_EQ(VecSimIndex_IndexSize(index), 1);

    // Delete the last vector.
    VecSimIndex_DeleteVector(index, 1);
    // Still have a single "deleted" slot
    EXPECT_EQ(VecSimIndex_IndexSize(index), 1);
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), 1);

    index->runGC();
    EXPECT_EQ(VecSimIndex_IndexSize(index), 0);
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, svs_vector_search_by_id_test) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t n = 100;
    size_t k = 11;
    size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    TEST_DATA_T query[] = {50, 50, 50, 50};
    auto verify_res = [&](size_t id, double score, size_t index) { EXPECT_EQ(id, (index + 45)); };
    runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, svs_bulk_vectors_add_delete_test) {
    size_t n = 256;
    size_t k = 11;
    const size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index); // CAST_TO_SVS(index, svs::distance::DistanceL2);

    std::vector<std::array<TEST_DATA_T, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        GenerateVector<TEST_DATA_T>(v[i].data(), dim, i);
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    svs_index->addVectors(v.data(), ids.data(), n);

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    TEST_DATA_T query[] = {50, 50, 50, 50};
    auto verify_res = [&](size_t id, double score, size_t index) { EXPECT_EQ(id, (index + 45)); };
    runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

    // Delete almost all vectors
    const size_t keep_num = 1;
    ASSERT_EQ(svs_index->deleteVectors(ids.data(), n - keep_num), n - keep_num);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), keep_num);
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), n - keep_num);

    // Delete rest of the vectors
    // num_marked_deleted should reset.
    ASSERT_EQ(svs_index->deleteVectors(ids.data() + n - keep_num, keep_num), keep_num);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), 0);
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), n);
    index->runGC();
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), 0);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, two_stage_initialization_test) {
    size_t n = 256;
    size_t k = 11;
    const size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);

    std::vector<std::array<TEST_DATA_T, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        GenerateVector<TEST_DATA_T>(v[i].data(), dim, i);
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    // 2-stage initialization
    // initialization with null should fail
    EXPECT_THROW(svs_index->setImpl(nullptr), std::logic_error);

    // initialization with data should succeed
    auto impl = svs_index->createImpl(v.data(), ids.data(), n);
    svs_index->setImpl(std::move(impl));

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    TEST_DATA_T query[] = {50, 50, 50, 50};
    auto verify_res = [&](size_t id, double score, size_t index) { EXPECT_EQ(id, (index + 45)); };
    runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

    // Try to re-initialize with the same data.
    impl = svs_index->createImpl(v.data(), ids.data(), n);
    // Should fail because the index is not empty.
    EXPECT_THROW(svs_index->setImpl(std::move(impl)), std::logic_error);

    // Index should remain unchanged.
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

    // Delete almost all vectors
    const size_t keep_num = 1;
    ASSERT_EQ(svs_index->deleteVectors(ids.data(), n - keep_num), n - keep_num);
    // setImpl() should fail again because the index is not empty.
    impl = svs_index->createImpl(v.data(), ids.data(), n);
    EXPECT_THROW(svs_index->setImpl(std::move(impl)), std::logic_error);

    // Delete rest of the vectors - index should be empty, but impl still alive.
    ASSERT_EQ(svs_index->deleteVectors(ids.data() + n - keep_num, keep_num), keep_num);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Re-initialization should fail again.
    impl = svs_index->createImpl(v.data(), ids.data(), n);
    EXPECT_THROW(svs_index->setImpl(std::move(impl)), std::logic_error);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, svs_get_distance) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t n = 4;
    size_t dim = 2;
    size_t numIndex = 3;
    VecSimIndex *index[numIndex];
    std::vector<double> distances;

    TEST_DATA_T v1[] = {M_PI, M_PI};
    TEST_DATA_T v2[] = {M_E, M_E};
    TEST_DATA_T v3[] = {M_PI, M_E};
    TEST_DATA_T v4[] = {M_SQRT2, -M_SQRT2};

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    for (size_t i = 0; i < numIndex; i++) {
        params.metric = (VecSimMetric)i;
        index[i] = this->CreateNewIndex(params);
        ASSERT_INDEX(index[i]);
        VecSimIndex_AddVector(index[i], v1, 1);
        VecSimIndex_AddVector(index[i], v2, 2);
        VecSimIndex_AddVector(index[i], v3, 3);
        VecSimIndex_AddVector(index[i], v4, 4);
        ASSERT_EQ(VecSimIndex_IndexSize(index[i]), 4);
    }

    TEST_DATA_T *query = v1;
    TEST_DATA_T *norm = v2;                   // {e, e}
    VecSim_Normalize(norm, dim, params.type); // now {1/sqrt(2), 1/sqrt(2)}

    ASSERT_TYPE_EQ(norm[0], TEST_DATA_T(1.0 / sqrt(2.0)));
    ASSERT_TYPE_EQ(norm[1], TEST_DATA_T(1.0 / sqrt(2.0)));
    double dist;

    auto qbits = TypeParam::get_quant_bits();
    double relative_err = qbits ? 1e-2 : 1.e-5;

    // VecSimMetric_L2
    distances = {0, 0.3583844006061554, 0.1791922003030777, 23.739208221435547};
    for (size_t i = 0; i < n; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_L2], i + 1, query);
        EXPECT_NEAR(dist, distances[i], std::abs(distances[i] * relative_err));
    }

    // VecSimMetric_IP
    distances = {-18.73921012878418, -16.0794677734375, -17.409339904785156, 1};
    for (size_t i = 0; i < n; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_IP], i + 1, query);
        EXPECT_NEAR(dist, distances[i], std::abs(distances[i] * relative_err));
    }

    // VecSimMetric_Cosine
    distances = {5.9604644775390625e-08, 5.9604644775390625e-08, 0.0025991201400756836, 1};
    for (size_t i = 0; i < n; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_Cosine], i + 1, norm);
        EXPECT_NEAR(dist, distances[i], std::abs(distances[i] * relative_err));
    }

    // Bad values
    dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_Cosine], 0, norm);
    EXPECT_TRUE(std::isnan(dist));
    dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_L2], 46, query);
    EXPECT_TRUE(std::isnan(dist));

    // Clean-up.
    for (size_t i = 0; i < numIndex; i++) {
        VecSimIndex_Free(index[i]);
    }
}

TYPED_TEST(SVSTest, svs_indexing_same_vector) {
    const size_t n = 100;
    const size_t k = 10;
    const size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    std::vector<std::array<TEST_DATA_T, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        GenerateVector<TEST_DATA_T>(v[i].data(), dim,
                                    i / 10); // i / 10 is in integer (take the "floor" value).
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Run a query where all the results are supposed to be {5,5,5,5} (different ids).
    TEST_DATA_T query[] = {4.9, 4.95, 5.05, 5.1};
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_TRUE(id >= 50 && id < 60 && score <= 1);
    };
    runTopKSearchTest(index, query, k, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, svs_reindexing_same_vector) {
    const size_t n = 100;
    const size_t k = 10;
    const size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    std::vector<std::array<TEST_DATA_T, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        // i / 10 is in integer (take the "floor" value).
        GenerateVector<TEST_DATA_T>(v[i].data(), dim, i / 10);
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Run a query where all the results are supposed to be {5,5,5,5} (different ids).
    TEST_DATA_T query[] = {4.9, 4.95, 5.05, 5.1};
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_TRUE(id >= 50 && id < 60 && score <= 1);
    };
    runTopKSearchTest(index, query, k, verify_res);

    // Delete almost all vectors - keeping SVS index implementation alive.
    for (size_t i = 0; i < n - 1; i++) {
        VecSimIndex_DeleteVector(index, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), 1);
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), n - 1);

    // Reinsert the same vectors under the same ids.
    for (size_t i = 0; i < n; i++) {
        // i / 10 is in integer (take the "floor value).
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i / 10);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), 2 * n);
    ASSERT_EQ(index->indexLabelCount(), n);
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), n);

    // Run the same query again.
    runTopKSearchTest(index, query, k, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, svs_reindexing_same_vector_different_id) {
    const size_t n = 100;
    const size_t k = 10;
    const size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    std::vector<std::array<TEST_DATA_T, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        GenerateVector<TEST_DATA_T>(v[i].data(), dim,
                                    i / 10); // i / 10 is in integer (take the "floor" value).
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Run a query where all the results are supposed to be {5,5,5,5} (different ids).
    TEST_DATA_T query[] = {4.9, 4.95, 5.05, 5.1};
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_TRUE(id >= 50 && id < 60 && score <= 1);
    };
    runTopKSearchTest(index, query, k, verify_res);

    for (size_t i = 0; i < n - 1; i++) {
        VecSimIndex_DeleteVector(index, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), 1);
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), n - 1);

    // Reinsert the same vectors under different ids than before.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i + 10,
                                          i / 10); // i / 10 is in integer (take the "floor" value).
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), 2 * n);
    ASSERT_EQ(index->indexLabelCount(), n);
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), n);

    // Run the same query again.
    auto verify_res_different_id = [&](size_t id, double score, size_t index) {
        ASSERT_TRUE(id >= 60 && id < 70 && score <= 1);
    };
    runTopKSearchTest(index, query, k, verify_res_different_id);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, svs_batch_iterator) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;

    // run the test twice - for index of size 100, every iteration will run select-based search,
    // as the number of results is 5, which is more than 0.1% of the index size. for index of size
    // 10000, we will run the heap-based search until we return 5000 results, and then switch to
    // select-based search.
    for (size_t n : {100, 1000}) {
        SVSParams params = {
            .dim = dim,
            .metric = VecSimMetric_L2,
            /* SVS-Vamana specifics */
            .alpha = 1.2,
            .graph_max_degree = 64,
            .construction_window_size = 20,
            .max_candidate_pool_size = 1024,
            .prune_to = 60,
            .use_search_history = VecSimOption_ENABLE,
        };

        VecSimIndex *index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        for (size_t i = 0; i < n; i++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(index), n);

        // Query for (n,n,...,n) vector (recall that n is the largest id in te index).
        TEST_DATA_T query[dim];
        GenerateVector<TEST_DATA_T>(query, dim, n);

        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
        size_t iteration_num = 0;

        // Get the 10 vectors whose ids are the maximal among those that hasn't been returned yet,
        // in every iteration. The order should be from the largest to the lowest id.
        size_t n_res = 20;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            std::vector<size_t> expected_ids(n_res);
            for (size_t i = 0; i < n_res; i++) {
                expected_ids[i] = (n - iteration_num * n_res - i - 1);
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_EQ(id, expected_ids[index]);
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
            iteration_num++;
        }
        ASSERT_EQ(iteration_num, n / n_res);
        VecSimBatchIterator_Free(batchIterator);

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(SVSTest, svs_batch_iterator_non_unique_scores) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;

    // Run the test twice - for index of size 100, every iteration will run select-based search,
    // as the number of results is 5, which is more than 0.1% of the index size. for index of size
    // 10000, we will run the heap-based search until we return 5000 results, and then switch to
    // select-based search.
    for (size_t n : {100, 1000}) {
        SVSParams params = {
            .dim = dim,
            .metric = VecSimMetric_L2,
            /* SVS-Vamana specifics */
            .alpha = 1.2,
            .graph_max_degree = 64,
            .construction_window_size = 20,
            .max_candidate_pool_size = 1024,
            .prune_to = 60,
            .use_search_history = VecSimOption_ENABLE,
        };

        VecSimIndex *index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        for (size_t i = 0; i < n; i++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i / 10);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(index), n);

        // Query for (n,n,...,n) vector (recall that n is the largest id in te index).
        TEST_DATA_T query[dim];
        GenerateVector<TEST_DATA_T>(query, dim, n);

        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
        size_t iteration_num = 0;

        // Get the 5 vectors whose ids are the maximal among those that hasn't been returned yet, in
        // every iteration. there are n/10 groups of 10 different vectors with the same score.
        size_t n_res = 5;
        bool even_iteration = false;
        std::set<size_t> expected_ids;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            // Insert the maximal 10 ids in every odd iteration.
            if (!even_iteration) {
                for (size_t i = 1; i <= 2 * n_res; i++) {
                    expected_ids.insert(n - iteration_num * n_res - i);
                }
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_TRUE(expected_ids.find(id) != expected_ids.end());
                expected_ids.erase(id);
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
            // Make sure that the expected ids set is empty after two iterations.
            if (even_iteration) {
                ASSERT_TRUE(expected_ids.empty());
            }
            iteration_num++;
            even_iteration = !even_iteration;
        }
        ASSERT_EQ(iteration_num, n / n_res);
        VecSimBatchIterator_Free(batchIterator);

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(SVSTest, svs_batch_iterator_reset) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t n = 10000;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i / 10);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Query for (n,n,...,n) vector (recall that n is the largest id in te index).
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);

    // Get the 100 vectors whose ids are the maximal among those that hasn't been returned yet, in
    // every iteration. run this flow for 5 times, each time for 10 iteration, and reset the
    // iterator.
    size_t n_res = 100;
    size_t total_iteration = 5;
    size_t re_runs = 3;

    for (size_t take = 0; take < re_runs; take++) {
        size_t iteration_num = 0;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            std::set<size_t> expected_ids;
            for (size_t i = 1; i <= n_res * 2; i++) {
                expected_ids.insert(n - iteration_num * n_res - i);
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_TRUE(expected_ids.find(id) != expected_ids.end());
                expected_ids.erase(id);
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
            iteration_num++;
            if (iteration_num == total_iteration) {
                break;
            }
        }
        VecSimBatchIterator_Reset(batchIterator);
    }
    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, svs_batch_iterator_corner_cases) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t n = 1000;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // Query for (n,n,...,n) vector (recall that n is the largest id in te index).
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);

    // Create batch iterator for empty index.
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    // Try to get more results even though there are no.
    VecSimQueryReply *res = VecSimBatchIterator_Next(batchIterator, 1, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);
    // Retry to get results.
    VecSimBatchIterator_Reset(batchIterator);
    res = VecSimBatchIterator_Next(batchIterator, 1, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);

    // Check if depleted
    ASSERT_FALSE(VecSimBatchIterator_HasNext(batchIterator));
    VecSimBatchIterator_Free(batchIterator);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    batchIterator = VecSimBatchIterator_New(index, query, nullptr);

    // Ask for zero results.
    res = VecSimBatchIterator_Next(batchIterator, 0, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);

    // Get all in first iteration, expect to use select search.
    size_t n_res = n;
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_TRUE(id == n - 1 - index);
    };
    runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
    ASSERT_FALSE(VecSimBatchIterator_HasNext(batchIterator));

    // Try to get more results even though there are no.
    res = VecSimBatchIterator_Next(batchIterator, n_res, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);

    // Reset, and run in batches, but the final batch is partial.
    VecSimBatchIterator_Reset(batchIterator);
    res = VecSimBatchIterator_Next(batchIterator, n_res / 2, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), n / 2);
    VecSimQueryReply_Free(res);
    res = VecSimBatchIterator_Next(batchIterator, n_res / 2 + 1, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), n / 2);
    VecSimQueryReply_Free(res);
    ASSERT_FALSE(VecSimBatchIterator_HasNext(batchIterator));

    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

// Add up to capacity.
TYPED_TEST(SVSTest, resizeIndex) {
    size_t dim = 4;
    size_t n = 10;
    size_t bs = 4;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = bs};

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // Add up to n.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }

    // Initial capacity is rounded up to the block size.
    size_t extra_cap = n % bs == 0 ? 0 : bs - n % bs;
    auto quantBits = TypeParam::get_quant_bits();
    // Get the fallback quantization mode
    quantBits = std::get<0>(svs_details::isSVSQuantBitsSupported(quantBits));
    if (quantBits != VecSimSvsQuant_NONE) {
        // LVQDataset does not provide a capacity method
        extra_cap = 0;
    }
    // The size (+extra) and the capacity should be equal.
    ASSERT_EQ(index->indexCapacity(), VecSimIndex_IndexSize(index) + extra_cap);
    ASSERT_EQ(index->indexMetaDataCapacity(), index->indexCapacity());
    // The capacity shouldn't be changed.
    ASSERT_EQ(index->indexCapacity(), n + extra_cap);

    VecSimIndex_Free(index);
}

// Test empty index edge cases.
TYPED_TEST(SVSTest, svs_empty_index) {
    size_t dim = 4;
    size_t n = 20;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Try to remove from an empty index - should fail because label doesn't exist.
    VecSimIndex_DeleteVector(index, 0);

    // Add one vector.
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 1.7);

    // Size equals 1.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 1);

    // Try to remove it.
    VecSimIndex_DeleteVector(index, 1);

    // Size equals 0.
    index->runGC();
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Even an empty index holds the first block for nonquantized data
    // ASSERT_EQ(index->indexCapacity(), 0);
    ASSERT_EQ(index->indexMetaDataCapacity(), index->indexCapacity());

    // Try to remove it again.
    VecSimIndex_DeleteVector(index, 1);
    // Nor the size.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, test_delete_vector) {
    size_t k = 5;
    size_t dim = 2;
    size_t block_size = 3;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        .blockSize = block_size,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // Delete from empty index
    ASSERT_EQ(VecSimIndex_DeleteVector(index, 111), 0);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    size_t n = 6;
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Here the shift should happen.
    VecSimIndex_DeleteVector(index, 1);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n - 1);

    TEST_DATA_T query[] = {0.0, 0.0};
    auto verify_res = [&](size_t id, double score, size_t index) {
        if (index == 0) {
            ASSERT_EQ(id, index);
        } else {
            ASSERT_EQ(id, index + 1);
        }
    };
    runTopKSearchTest(index, query, k, verify_res);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, sanity_reinsert_1280) {
    size_t n = 5;
    size_t d = 1280;
    size_t k = 5;

    SVSParams params = {
        .dim = d,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto *vectors = new TEST_DATA_T[n * d];

    // Generate random vectors in every iteration and inert them under different ids.
    for (size_t iter = 1; iter <= 3; iter++) {
        for (size_t i = 0; i < n; i++) {
            for (size_t j = 0; j < d; j++) {
                (vectors + i * d)[j] = (TEST_DATA_T)rand() / (TEST_DATA_T)(RAND_MAX) / 100;
            }
        }
        auto expected_ids = std::set<size_t>();
        for (size_t i = 0; i < n; i++) {
            VecSimIndex_AddVector(index, (vectors + i * d), i * iter);
            expected_ids.insert(i * iter);
        }
        auto verify_res = [&](size_t id, double score, size_t index) {
            ASSERT_TRUE(expected_ids.find(id) != expected_ids.end());
            expected_ids.erase(id);
        };

        // Send arbitrary vector (the first) and search for top k. This should return all the
        // vectors that were inserted in this iteration - verify their ids.
        runTopKSearchTest(index, vectors, k, verify_res);

        // Remove vectors form current iteration.
        for (size_t i = 0; i < n; i++) {
            VecSimIndex_DeleteVector(index, i * iter);
        }
    }
    delete[] vectors;
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, test_svs_info) {
    size_t n = 100;
    size_t d = 128;

    // Build with default args.

    SVSParams params = {
        .dim = d,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, d);
    ASSERT_FALSE(info.commonInfo.basicInfo.isMulti);
    // Default args.
    ASSERT_EQ(info.commonInfo.indexSize, 0);
    VecSimIndex_Free(index);

    d = 1280;
    params.dim = d;
    params.blockSize = 1;

    index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, d);
    ASSERT_FALSE(info.commonInfo.basicInfo.isMulti);
    ASSERT_FALSE(info.commonInfo.basicInfo.isTiered);

    // User args.
    ASSERT_EQ(info.commonInfo.basicInfo.blockSize, 1);
    ASSERT_EQ(info.commonInfo.indexSize, 0);

    // Validate that Static info returns the right restricted info as well.
    VecSimIndexBasicInfo s_info = VecSimIndex_BasicInfo(index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, s_info.algo);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, s_info.dim);
    ASSERT_EQ(info.commonInfo.basicInfo.blockSize, s_info.blockSize);
    ASSERT_EQ(info.commonInfo.basicInfo.type, s_info.type);
    ASSERT_EQ(info.commonInfo.basicInfo.isMulti, s_info.isMulti);
    ASSERT_EQ(info.commonInfo.basicInfo.type, s_info.type);
    ASSERT_EQ(info.commonInfo.basicInfo.isTiered, s_info.isTiered);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, test_basic_svs_info_iterator) {
    size_t n = 100;
    size_t d = 128;
    VecSimMetric metrics[3] = {VecSimMetric_Cosine, VecSimMetric_IP, VecSimMetric_L2};

    for (size_t i = 0; i < 3; i++) {

        // Build with default args.
        SVSParams params = {
            .dim = d,
            .metric = metrics[i],
            /* SVS-Vamana specifics */
            .alpha = 1.2,
            .graph_max_degree = 64,
            .construction_window_size = 20,
            .max_candidate_pool_size = 1024,
            .prune_to = 60,
            .use_search_history = VecSimOption_ENABLE,
        };

        VecSimIndex *index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        VecSimDebugInfoIterator *infoIter = VecSimIndex_DebugInfoIterator(index);
        compareSVSIndexInfoToIterator(info, infoIter);
        VecSimDebugInfoIterator_Free(infoIter);
        VecSimIndex_Free(index);
    }
}

TYPED_TEST(SVSTest, test_dynamic_svs_info_iterator) {
    if (TypeParam::get_quant_bits() == VecSimSvsQuant_8 ||
        TypeParam::get_quant_bits() == VecSimSvsQuant_8x8_LeanVec) {
        GTEST_SKIP() << "Already included in the test loop.";
    }
    size_t d = 128;
    for (auto quant_bits : {VecSimSvsQuant_NONE, VecSimSvsQuant_Scalar, VecSimSvsQuant_8,
                            VecSimSvsQuant_4, VecSimSvsQuant_4x4, VecSimSvsQuant_4x8,
                            VecSimSvsQuant_4x8_LeanVec, VecSimSvsQuant_8x8_LeanVec}) {

        SVSParams params = {
            .dim = d,
            .metric = VecSimMetric_L2,
            .blockSize = 1,
            /* SVS-Vamana specifics */
            .quantBits = quant_bits,
            .alpha = 1.2,
            .graph_max_degree = 64,
            .construction_window_size = 20,
            .max_candidate_pool_size = 1024,
            .prune_to = 60,
            .use_search_history = VecSimOption_ENABLE,
        };
        VecSimIndex *index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        VecSimDebugInfoIterator *infoIter = VecSimIndex_DebugInfoIterator(index);
        ASSERT_EQ(1, info.commonInfo.basicInfo.blockSize);
        ASSERT_EQ(0, info.commonInfo.indexSize);
        compareSVSIndexInfoToIterator(info, infoIter);
        VecSimDebugInfoIterator_Free(infoIter);

        TEST_DATA_T v[d];
        for (size_t i = 0; i < d; i++) {
            v[i] = (TEST_DATA_T)i;
        }
        // Add vector.
        VecSimIndex_AddVector(index, v, 0);
        info = VecSimIndex_DebugInfo(index);
        infoIter = VecSimIndex_DebugInfoIterator(index);
        ASSERT_EQ(1, info.commonInfo.indexSize);
        compareSVSIndexInfoToIterator(info, infoIter);
        VecSimDebugInfoIterator_Free(infoIter);

        // Delete vector.
        VecSimIndex_DeleteVector(index, 0);
        index->runGC();
        info = VecSimIndex_DebugInfo(index);
        infoIter = VecSimIndex_DebugInfoIterator(index);
        ASSERT_EQ(0, info.commonInfo.indexSize);
        compareSVSIndexInfoToIterator(info, infoIter);
        VecSimDebugInfoIterator_Free(infoIter);

        // Perform (or simulate) Search in all modes.
        VecSimIndex_AddVector(index, v, 0);
        auto res = VecSimIndex_TopKQuery(index, v, 1, nullptr, BY_SCORE);
        VecSimQueryReply_Free(res);
        info = VecSimIndex_DebugInfo(index);
        infoIter = VecSimIndex_DebugInfoIterator(index);
        ASSERT_EQ(STANDARD_KNN, info.commonInfo.lastMode);
        compareSVSIndexInfoToIterator(info, infoIter);
        VecSimDebugInfoIterator_Free(infoIter);

        res = VecSimIndex_RangeQuery(index, v, 1, nullptr, BY_SCORE);
        VecSimQueryReply_Free(res);
        info = VecSimIndex_DebugInfo(index);
        infoIter = VecSimIndex_DebugInfoIterator(index);
        ASSERT_EQ(RANGE_QUERY, info.commonInfo.lastMode);
        compareSVSIndexInfoToIterator(info, infoIter);
        VecSimDebugInfoIterator_Free(infoIter);

        ASSERT_TRUE(VecSimIndex_PreferAdHocSearch(index, 1, 1, true));
        info = VecSimIndex_DebugInfo(index);
        infoIter = VecSimIndex_DebugInfoIterator(index);
        ASSERT_EQ(HYBRID_ADHOC_BF, info.commonInfo.lastMode);
        compareSVSIndexInfoToIterator(info, infoIter);
        VecSimDebugInfoIterator_Free(infoIter);

        // Simulate the case where another call to the heuristics is done after realizing that
        // the subset size is smaller, and change the policy as a result.
        ASSERT_TRUE(VecSimIndex_PreferAdHocSearch(index, 1, 1, false));
        info = VecSimIndex_DebugInfo(index);
        infoIter = VecSimIndex_DebugInfoIterator(index);
        ASSERT_EQ(HYBRID_BATCHES_TO_ADHOC_BF, info.commonInfo.lastMode);
        compareSVSIndexInfoToIterator(info, infoIter);
        VecSimDebugInfoIterator_Free(infoIter);

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(SVSTest, debugInfoIteratorFieldOrder) {
    namespace expected_output = test_utils::test_debug_info_iterator_order;

    size_t d = 4;
    SVSParams params = {.dim = d, .metric = VecSimMetric_L2, .blockSize = 1};
    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // Add a vector to ensure the index is not empty
    GenerateAndAddVector<TEST_DATA_T>(index, d, 1, 1);

    VecSimDebugInfoIterator *infoIterator = VecSimIndex_DebugInfoIterator(index);

    // Test the field order using the common function
    expected_output::testDebugInfoIteratorFieldOrder(infoIterator, expected_output::getSVSFields());

    VecSimDebugInfoIterator_Free(infoIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, svs_vector_search_test_ip) {
    const size_t dim = 4;
    const size_t n = 10;
    const size_t k = 5;

    for (size_t blocksize : {1, 12, DEFAULT_BLOCK_SIZE}) {

        SVSParams params = {.dim = dim,
                            .metric = VecSimMetric_IP,
                            .blockSize = blocksize,
                            /* SVS-Vamana specifics */
                            .alpha = 0.9,
                            .graph_max_degree = 64,
                            .construction_window_size = 20,
                            .max_candidate_pool_size = 1024,
                            .prune_to = 60,
                            .use_search_history = VecSimOption_ENABLE,
                            .leanvec_dim = dim / 4};

        VecSimIndex *index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
        ASSERT_EQ(info.commonInfo.basicInfo.blockSize, blocksize);

        auto svs_index = this->CastToSVS(index);
        ASSERT_NE(svs_index, nullptr);

        std::vector<std::array<TEST_DATA_T, dim>> v(n);
        for (size_t i = 0; i < n; i++) {
            GenerateVector<TEST_DATA_T>(v[i].data(), dim, i);
        }

        std::vector<size_t> ids(n);
        std::iota(ids.begin(), ids.end(), 0);

        svs_index->addVectors(v.data(), ids.data(), n);
        ASSERT_EQ(VecSimIndex_IndexSize(index), n);

        TEST_DATA_T query[] = {50, 50, 50, 50};
        std::set<size_t> expected_ids;
        for (size_t i = n - 1; i > n - 1 - k; i--) {
            expected_ids.insert(i);
        }
        auto verify_res = [&](size_t id, double score, size_t index) {
            ASSERT_TRUE(expected_ids.find(id) != expected_ids.end());
            expected_ids.erase(id);
        };
        runTopKSearchTest(index, query, k, verify_res);
        VecSimIndex_Free(index);
    }
}

TYPED_TEST(SVSTest, svs_vector_search_test_l2) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t n = 100;
    size_t k = 11;

    for (size_t blocksize : {1, 12, DEFAULT_BLOCK_SIZE}) {

        SVSParams params = {.dim = dim,
                            .metric = VecSimMetric_L2,
                            .blockSize = blocksize,
                            /* SVS-Vamana specifics */
                            .alpha = 1.2,
                            .graph_max_degree = 64,
                            .construction_window_size = 20,
                            .max_candidate_pool_size = 1024,
                            .prune_to = 60,
                            .use_search_history = VecSimOption_ENABLE,
                            .leanvec_dim = dim / 4};

        VecSimIndex *index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
        ASSERT_EQ(info.commonInfo.basicInfo.blockSize, blocksize);

        for (size_t i = 0; i < n; i++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(index), n);

        auto verify_res = [&](size_t id, double score, size_t index) {
            size_t diff_id = (id > 50) ? (id - 50) : (50 - id);
            ASSERT_EQ(diff_id, (index + 1) / 2);
            ASSERT_EQ(score, (4 * ((index + 1) / 2) * ((index + 1) / 2)));
        };
        TEST_DATA_T query[] = {50, 50, 50, 50};
        runTopKSearchTest(index, query, k, verify_res);
        runTopKSearchTest(index, query, 0, verify_res); // For sanity, search for nothing

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(SVSTest, svs_search_empty_index) {
    size_t dim = 4;
    size_t n = 100;
    size_t k = 11;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        .blockSize = 1,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    TEST_DATA_T query[] = {50, 50, 50, 50};

    // We do not expect any results.
    VecSimQueryReply *res = VecSimIndex_TopKQuery(index, query, k, NULL, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Iterator *it = VecSimQueryReply_GetIterator(res);
    ASSERT_EQ(VecSimQueryReply_IteratorNext(it), nullptr);
    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(res);

    res = VecSimIndex_RangeQuery(index, query, 1.0, NULL, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);

    // Add some vectors and remove them all from index, so it will be empty again.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    for (size_t i = 0; i < n; i++) {
        VecSimIndex_DeleteVector(index, i);
    }
    index->runGC();
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Again - we do not expect any results.
    res = VecSimIndex_TopKQuery(index, query, k, NULL, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    it = VecSimQueryReply_GetIterator(res);
    ASSERT_EQ(VecSimQueryReply_IteratorNext(it), nullptr);
    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(res);

    res = VecSimIndex_RangeQuery(index, query, 1.0, NULL, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, svs_test_inf_score) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t n = 4;
    size_t k = 4;
    size_t dim = 2;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        .blockSize = 1,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    TEST_DATA_T inf_val = GetInfVal(params.type);
    ASSERT_FALSE(std::isinf(inf_val));

    TEST_DATA_T query[] = {M_PI, M_PI};
    TEST_DATA_T v1[] = {M_PI, M_PI};
    TEST_DATA_T v2[] = {inf_val, inf_val};
    TEST_DATA_T v3[] = {M_E, M_E};
    TEST_DATA_T v4[] = {-inf_val, -inf_val};

    VecSimIndex_AddVector(index, v1, 1);
    VecSimIndex_AddVector(index, v2, 2);
    VecSimIndex_AddVector(index, v3, 3);
    VecSimIndex_AddVector(index, v4, 4);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 4);

    auto verify_res = [&](size_t id, double score, size_t index) {
        if (index == 0) {
            ASSERT_EQ(1, id);
        } else if (index == 1) {
            ASSERT_EQ(3, id);
        } else {
            ASSERT_TRUE(id == 2 || id == 4);
            ASSERT_TRUE(std::isinf(score));
        }
    };
    runTopKSearchTest(index, query, k, verify_res);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, preferAdHocOptimization) {
    // Save the expected ratio which is the threshold between ad-hoc and batches mode
    // for every combination of index size and dim.
    // std::map<std::pair<size_t, size_t>, float> threshold;
    // threshold[{1000, 4}] = threshold[{1000, 80}] = threshold[{1000, 350}] = threshold[{1000,
    // 780}] =
    //     1.0;
    // threshold[{6000, 4}] = 0.2;
    // threshold[{6000, 80}] = 0.4;
    // threshold[{6000, 350}] = 0.6;
    // threshold[{6000, 780}] = 0.8;
    // threshold[{600000, 4}] = threshold[{600000, 80}] = 0.2;
    // threshold[{600000, 350}] = 0.6;
    // threshold[{600000, 780}] = 0.8;

    for (size_t index_size : {1000, 6000, 600000}) {
        for (size_t dim : {4, 80, 350, 780}) {
            // Create index and check for the expected output of "prefer ad-hoc".

            SVSParams params = {
                .dim = dim,
                .metric = VecSimMetric_IP,
                .blockSize = 5,
                /* SVS-Vamana specifics */
                .alpha = 1.2,
                .graph_max_degree = 64,
                .construction_window_size = 20,
                .max_candidate_pool_size = 1024,
                .prune_to = 60,
                .use_search_history = VecSimOption_ENABLE,
            };

            VecSimIndex *index = this->CreateNewIndex(params);
            ASSERT_INDEX(index);

            // Set the index size artificially to be the required one.
            // (this->CastToBF(index))->count = index_size;
            // ASSERT_EQ(VecSimIndex_IndexSize(index), index_size);
            for (float r : {0.1f, 0.3f, 0.5f, 0.7f, 0.9f}) {
                bool res = VecSimIndex_PreferAdHocSearch(index, (size_t)(r * index_size), 50, true);
                // If r is below the threshold for this specific configuration of (index_size, dim),
                // expect that result will be ad-hoc (i.e., true), and otherwise, batches (i.e.,
                // false)
                // bool expected_res = r < threshold[{index_size, dim}];
                bool expected_res = true;
                ASSERT_EQ(res, expected_res);
            }
            VecSimIndex_Free(index);
        }
    }
    // Corner cases - empty index.

    SVSParams params = {
        .dim = 4,
        .metric = VecSimMetric_IP,
        .blockSize = 5,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    ASSERT_TRUE(VecSimIndex_PreferAdHocSearch(index, 0, 50, true));

    // Corner cases - subset size is greater than index size.
    ASSERT_EQ(VecSimIndex_PreferAdHocSearch(index, 42, 50, true),
              VecSimIndex_PreferAdHocSearch(index, 0, 50, true));

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, test_svs_parameter_combinations_and_defaults) {
    size_t dim = 4;

    // Test structure to hold parameter combinations
    struct ParamTestCase {
        std::string name;
        SVSParams params;
        // Expected values after applying defaults
        typename TestFixture::ExpectedSVSValues expected;
    };

    // Define test cases covering all parameter combinations
    std::vector<ParamTestCase> testCases = {
        // Test: All default parameters (zeros/unset)
        {"all_defaults",
         {
             .dim = dim, .metric = VecSimMetric_L2,
             // All other parameters left as default (0/unset)
         },
         {.quantBits = get<0>(svs_details::isSVSQuantBitsSupported(TypeParam::get_quant_bits())),
          .alpha = SVS_VAMANA_DEFAULT_ALPHA_L2,
          .graphMaxDegree = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE,
          .constructionWindowSize = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE,
          .maxCandidatePoolSize = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE * 3,
          .pruneTo = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE - 4,
          .useSearchHistory = SVS_VAMANA_DEFAULT_USE_SEARCH_HISTORY,
          .numThreads = SVS_VAMANA_DEFAULT_NUM_THREADS,
          .numberOfMarkedDeletedNodes = 0,
          .searchWindowSize = SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE,
          .searchBufferCapacity = SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE,
          .leanvecDim = SVS_VAMANA_DEFAULT_LEANVEC_DIM,
          .epsilon = SVS_VAMANA_DEFAULT_EPSILON}},

        // Test: Cosine metric with defaults
        {"cosine_metric_defaults",
         {
             .dim = dim,
             .metric = VecSimMetric_Cosine,
         },
         {.quantBits = get<0>(svs_details::isSVSQuantBitsSupported(TypeParam::get_quant_bits())),
          .alpha = SVS_VAMANA_DEFAULT_ALPHA_IP, // Cosine uses same as IP
          .graphMaxDegree = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE,
          .constructionWindowSize = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE,
          .maxCandidatePoolSize = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE * 3,
          .pruneTo = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE - 4,
          .useSearchHistory = SVS_VAMANA_DEFAULT_USE_SEARCH_HISTORY,
          .numThreads = SVS_VAMANA_DEFAULT_NUM_THREADS,
          .numberOfMarkedDeletedNodes = 0,
          .searchWindowSize = SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE,
          .searchBufferCapacity = SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE,
          .leanvecDim = SVS_VAMANA_DEFAULT_LEANVEC_DIM,
          .epsilon = SVS_VAMANA_DEFAULT_EPSILON}},

        // Test: Custom alpha parameter
        {"custom_alpha",
         {
             .dim = dim,
             .metric = VecSimMetric_L2,
             .alpha = 1.5f,
         },
         {.quantBits = get<0>(svs_details::isSVSQuantBitsSupported(TypeParam::get_quant_bits())),
          .alpha = 1.5f,
          .graphMaxDegree = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE,
          .constructionWindowSize = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE,
          .maxCandidatePoolSize = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE * 3,
          .pruneTo = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE - 4,
          .useSearchHistory = SVS_VAMANA_DEFAULT_USE_SEARCH_HISTORY,
          .numThreads = SVS_VAMANA_DEFAULT_NUM_THREADS,
          .numberOfMarkedDeletedNodes = 0,
          .searchWindowSize = SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE,
          .searchBufferCapacity = SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE,
          .leanvecDim = SVS_VAMANA_DEFAULT_LEANVEC_DIM,
          .epsilon = SVS_VAMANA_DEFAULT_EPSILON}},

        // Test: Custom graph parameters
        {"custom_graph_params",
         {
             .dim = dim,
             .metric = VecSimMetric_L2,
             .graph_max_degree = 48,
             .construction_window_size = 150,
         },
         {.quantBits = get<0>(svs_details::isSVSQuantBitsSupported(TypeParam::get_quant_bits())),
          .alpha = SVS_VAMANA_DEFAULT_ALPHA_L2,
          .graphMaxDegree = 48,
          .constructionWindowSize = 150,
          .maxCandidatePoolSize = 150 * 3, // Should be construction_window_size * 3
          .pruneTo = 48 - 4,               // Should be graph_max_degree - 4
          .useSearchHistory = SVS_VAMANA_DEFAULT_USE_SEARCH_HISTORY,
          .numThreads = SVS_VAMANA_DEFAULT_NUM_THREADS,
          .numberOfMarkedDeletedNodes = 0,

          .searchWindowSize = SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE,
          .searchBufferCapacity = SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE,
          .leanvecDim = SVS_VAMANA_DEFAULT_LEANVEC_DIM,
          .epsilon = SVS_VAMANA_DEFAULT_EPSILON}},

        // Test: All custom parameters
        {"all_custom_params",
         {
             .dim = dim,
             .metric = VecSimMetric_IP,
             .alpha = 0.8f,
             .graph_max_degree = 64,
             .construction_window_size = 100,
             .max_candidate_pool_size = 500,
             .prune_to = 55,
             .use_search_history = VecSimOption_DISABLE,
             .num_threads = 4,
             .search_window_size = 20,
             .search_buffer_capacity = 40,
             .leanvec_dim = dim / 2,
             .epsilon = 0.05,
         },
         {.quantBits = get<0>(svs_details::isSVSQuantBitsSupported(TypeParam::get_quant_bits())),
          .alpha = 0.8f,
          .graphMaxDegree = 64,
          .constructionWindowSize = 100,
          .maxCandidatePoolSize = 500,
          .pruneTo = 55,
          .useSearchHistory = false,                    // VecSimOption_DISABLE
          .numThreads = SVS_VAMANA_DEFAULT_NUM_THREADS, // Deprecated, expect default to be used
          .numberOfMarkedDeletedNodes = 0,
          .searchWindowSize = 20,
          .searchBufferCapacity = 40,
          .leanvecDim = dim / 2,
          .epsilon = 0.05}},

        // Test: Search history AUTO mode
        {"search_history_auto",
         {
             .dim = dim,
             .metric = VecSimMetric_L2,
             .use_search_history = VecSimOption_AUTO,
         },
         {.quantBits = get<0>(svs_details::isSVSQuantBitsSupported(TypeParam::get_quant_bits())),
          .alpha = SVS_VAMANA_DEFAULT_ALPHA_L2,
          .graphMaxDegree = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE,
          .constructionWindowSize = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE,
          .maxCandidatePoolSize = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE * 3,
          .pruneTo = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE - 4,
          .useSearchHistory = SVS_VAMANA_DEFAULT_USE_SEARCH_HISTORY, // AUTO resolves to default
          .numThreads = SVS_VAMANA_DEFAULT_NUM_THREADS,
          .numberOfMarkedDeletedNodes = 0,
          .searchWindowSize = SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE,
          .searchBufferCapacity = SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE,
          .leanvecDim = SVS_VAMANA_DEFAULT_LEANVEC_DIM,
          .epsilon = SVS_VAMANA_DEFAULT_EPSILON}},
        // Test: Search history AUTO mode
        {"search_history_enable",
         {
             .dim = dim,
             .metric = VecSimMetric_L2,
             .use_search_history = VecSimOption_ENABLE,
         },
         {.quantBits = get<0>(svs_details::isSVSQuantBitsSupported(TypeParam::get_quant_bits())),
          .alpha = SVS_VAMANA_DEFAULT_ALPHA_L2,
          .graphMaxDegree = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE,
          .constructionWindowSize = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE,
          .maxCandidatePoolSize = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE * 3,
          .pruneTo = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE - 4,
          .useSearchHistory = true,
          .numThreads = SVS_VAMANA_DEFAULT_NUM_THREADS,
          .numberOfMarkedDeletedNodes = 0,
          .searchWindowSize = SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE,
          .searchBufferCapacity = SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE,
          .leanvecDim = SVS_VAMANA_DEFAULT_LEANVEC_DIM,
          .epsilon = SVS_VAMANA_DEFAULT_EPSILON}}};

    // Run tests for each parameter combination
    for (const auto &testCase : testCases) {
        SCOPED_TRACE("Testing parameter combination: " + testCase.name);

        // Create index with the test parameters
        VecSimIndex *index = this->CreateNewIndex(const_cast<SVSParams &>(testCase.params));
        ASSERT_INDEX(index);

        // Validate basic index properties
        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        EXPECT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
        EXPECT_EQ(info.commonInfo.basicInfo.dim, dim);
        EXPECT_EQ(info.commonInfo.basicInfo.metric, testCase.params.metric);

        // Verify all parameters using debugInfo
        this->validateSVSParameters(index, testCase.expected);

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(SVSTest, test_svs_parameter_consistency_across_metrics) {
    size_t dim = 4;

    // Test that default parameters work consistently across different metrics and types.
    std::vector<VecSimMetric> metrics = {VecSimMetric_L2, VecSimMetric_IP, VecSimMetric_Cosine};

    for (auto metric : metrics) {
        SCOPED_TRACE("Testing metric: " + std::to_string(static_cast<int>(metric)));

        // Create index with default parameters for this metric
        SVSParams params = {
            .dim = dim, .metric = metric,
            // All other parameters use defaults
        };

        VecSimIndex *index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        // Verify debug info shows correct metric and default parameters
        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        EXPECT_EQ(info.commonInfo.basicInfo.metric, metric);
        EXPECT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
        EXPECT_EQ(info.commonInfo.basicInfo.dim, dim);

        // Verify that default parameters are correctly applied based on metric
        if (metric == VecSimMetric_L2) {
            EXPECT_FLOAT_EQ(info.svsInfo.alpha, SVS_VAMANA_DEFAULT_ALPHA_L2);
        } else { // IP or Cosine
            EXPECT_FLOAT_EQ(info.svsInfo.alpha, SVS_VAMANA_DEFAULT_ALPHA_IP);
        }

        // Verify other default parameters
        EXPECT_EQ(info.svsInfo.graphMaxDegree, SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE);
        EXPECT_EQ(info.svsInfo.constructionWindowSize, SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE);
        EXPECT_EQ(info.svsInfo.searchWindowSize, SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE);
        EXPECT_EQ(info.svsInfo.searchBufferCapacity, SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE);
        EXPECT_DOUBLE_EQ(info.svsInfo.leanvecDim, SVS_VAMANA_DEFAULT_LEANVEC_DIM);
        EXPECT_DOUBLE_EQ(info.svsInfo.epsilon, SVS_VAMANA_DEFAULT_EPSILON);
        EXPECT_EQ(info.svsInfo.numThreads, SVS_VAMANA_DEFAULT_NUM_THREADS);
        EXPECT_EQ(info.svsInfo.useSearchHistory, SVS_VAMANA_DEFAULT_USE_SEARCH_HISTORY);

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(SVSTest, batchIteratorSwapIndices) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t n = 10000;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    TEST_DATA_T close_vec[] = {1.0, 1.0, 1.0, 1.0};
    TEST_DATA_T further_vec[] = {2.0, 2.0, 2.0, 2.0};
    VecSimIndex_AddVector(index, further_vec, 0);
    VecSimIndex_AddVector(index, close_vec, 1);
    VecSimIndex_AddVector(index, further_vec, 2);
    VecSimIndex_AddVector(index, close_vec, 3);
    VecSimIndex_AddVector(index, close_vec, 4);
    VecSimIndex_AddVector(index, close_vec, 5);
    for (size_t i = 6; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Query for (1,1,1,1) vector.
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 1.0);
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);

    // Get first batch - expect to get ids 1,3,4,5.
    VecSimQueryReply *res = VecSimBatchIterator_Next(batchIterator, 4, BY_ID);
    ASSERT_EQ(VecSimQueryReply_Len(res), 4);
    VecSimQueryReply_Iterator *iterator = VecSimQueryReply_GetIterator(res);
    int res_ind = 0;
    size_t expected_res[] = {1, 3, 4, 5};
    while (VecSimQueryReply_IteratorHasNext(iterator)) {
        VecSimQueryResult *item = VecSimQueryReply_IteratorNext(iterator);
        int id = (int)VecSimQueryResult_GetId(item);
        ASSERT_EQ(expected_res[res_ind++], id);
    }
    VecSimQueryReply_IteratorFree(iterator);
    VecSimQueryReply_Free(res);

    // Get another batch - expect to get ids 0,2,6,7. Make sure that ids 0,2 swapped properly.
    res = VecSimBatchIterator_Next(batchIterator, 4, BY_ID);
    ASSERT_EQ(VecSimQueryReply_Len(res), 4);
    iterator = VecSimQueryReply_GetIterator(res);
    res_ind = 0;
    size_t expected_res_2[] = {0, 2, 6, 7};
    while (VecSimQueryReply_IteratorHasNext(iterator)) {
        VecSimQueryResult *item = VecSimQueryReply_IteratorNext(iterator);
        int id = (int)VecSimQueryResult_GetId(item);
        ASSERT_EQ(expected_res_2[res_ind++], id);
    }
    VecSimQueryReply_IteratorFree(iterator);
    VecSimQueryReply_Free(res);

    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

// This test verifies a bug fix where zero vectors would cause division by zero during
// normalization (vector/norm where norm=0), resulting in NaN/Inf values that crashed the SVS
// library. The fix was introduced in PR #752, bumping up SVS library to version that includes the
// fix.
TYPED_TEST(SVSTest, test_index_zeros_vector_cosine) {
    const size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_Cosine,
    };
    this->SetTypeParams(params);
    VecSimParams index_params = CreateParams(params);
    VecSimIndex *index = this->CreateNewIndex(index_params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    GenerateAndAddVector<TEST_DATA_T>(index, dim, 0, 0);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 1);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, svs_vector_search_test_cosine) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    const size_t dim = 128;
    const size_t n = 50;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_Cosine,
        /* SVS-Vamana specifics */
        .alpha = 0.9,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // To meet accurary in LVQ case we have to add bulk of vectors at once.
    std::vector<std::array<TEST_DATA_T, dim>> v(n);
    for (size_t i = 1; i <= n; i++) {
        auto &f = v[i - 1];
        f[0] = (TEST_DATA_T)i / n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 1);

    auto svs_index = this->CastToSVS(index);
    svs_index->addVectors(v.data(), ids.data(), n);

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 1.0);

    // topK search will normalize the query so we keep the original data to
    // avoid normalizing twice.
    TEST_DATA_T normalized_query[dim];
    memcpy(normalized_query, query, dim * sizeof(TEST_DATA_T));
    VecSim_Normalize(normalized_query, dim, params.type);

    auto verify_res = [&](size_t id, double score, size_t result_rank) {
        ASSERT_EQ(id, (n - result_rank));
        TEST_DATA_T expected_score = index->getDistanceFrom_Unsafe(id, normalized_query);
        // Verify that abs difference between the actual and expected score is at most 1/10^5.
        ASSERT_NEAR((TEST_DATA_T)score, expected_score, 1e-5f);
    };
    runTopKSearchTest(index, query, 10, verify_res);

    // Test with batch iterator.
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    size_t iteration_num = 0;

    // get the 10 vectors whose ids are the maximal among those that hasn't been returned yet,
    // in every iteration. The order should be from the largest to the lowest id.
    size_t n_res = 10;
    while (VecSimBatchIterator_HasNext(batchIterator)) {
        std::vector<size_t> expected_ids(n_res);
        auto verify_res_batch = [&](size_t id, double score, size_t result_rank) {
            // In case of quantization, the result is not guaranteed to be properly ordered
            if constexpr (TypeParam::get_quant_bits() == 0) {
                ASSERT_EQ(id, (n - n_res * iteration_num - result_rank));
            }
            TEST_DATA_T expected_score = index->getDistanceFrom_Unsafe(id, normalized_query);
            // Verify that abs difference between the actual and expected score is at most 1/10^5.
            ASSERT_NEAR((TEST_DATA_T)score, expected_score, 1e-5f);
        };
        runBatchIteratorSearchTest(batchIterator, n_res, verify_res_batch);
        iteration_num++;
    }
    ASSERT_EQ(iteration_num, n / n_res);
    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, testSizeEstimation) {
    size_t dim = 64;
    auto constexpr quantBits = TypeParam::get_quant_bits();
    size_t n = 0;
    size_t bs = DEFAULT_BLOCK_SIZE;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_Cosine,
        .blockSize = bs,
        /* SVS-Vamana specifics */
        .alpha = 0.9,
        .graph_max_degree = 63, // x^2-1 to round the graph block size
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);
    // EstimateInitialSize is called after CreateNewIndex because params struct is
    // changed in CreateNewIndex.
    size_t estimation = EstimateInitialSize(params);

    size_t actual = index->getAllocationSize();
    ASSERT_EQ(estimation, actual);

    estimation = EstimateBlockSize(params, bs, 1);

    GenerateAndAddVector<TEST_DATA_T>(index, dim, 0);
    actual = index->getAllocationSize() - actual; // get the delta
    ASSERT_GT(actual, 0);
    double estimation_accuracy = 0.01;
    ASSERT_GE(estimation * (1.0 + estimation_accuracy), actual);
    ASSERT_LE(estimation * (1.0 - estimation_accuracy), actual);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, testInitialSizeEstimation) {
    size_t dim = 128;
    size_t n = 100;
    size_t bs = DEFAULT_BLOCK_SIZE;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_Cosine,
        .blockSize = bs,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);
    // EstimateInitialSize is called after CreateNewIndex because params struct is
    // changed in CreateNewIndex.
    size_t estimation = EstimateInitialSize(params);

    size_t actual = index->getAllocationSize();
    ASSERT_EQ(estimation, actual);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, testTimeoutReturn_topK) {
    size_t dim = 4;
    VecSimQueryReply *rep;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        .blockSize = 5,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 1; }); // Always times out

    TEST_DATA_T vec[dim];
    GenerateVector<TEST_DATA_T>(vec, dim);

    VecSimIndex_AddVector(index, vec, 0);
    // Checks return code on timeout - knn
    rep = VecSimIndex_TopKQuery(index, vec, 1, NULL, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(rep), VecSim_QueryReply_TimedOut);
    ASSERT_EQ(VecSimQueryReply_Len(rep), 0);
    VecSimQueryReply_Free(rep);

    VecSimIndex_Free(index);
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 0; }); // cleanup
}

TYPED_TEST(SVSTest, testTimeoutReturn_range) {
    size_t dim = 4;
    VecSimQueryReply *rep;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        .blockSize = 5,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 1; }); // Always times out

    TEST_DATA_T vec[dim];
    GenerateVector<TEST_DATA_T>(vec, dim);

    VecSimIndex_AddVector(index, vec, 0);

    // Checks return code on timeout - range query
    rep = VecSimIndex_RangeQuery(index, vec, 1, NULL, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(rep), VecSim_QueryReply_TimedOut);
    ASSERT_EQ(VecSimQueryReply_Len(rep), 0);
    VecSimQueryReply_Free(rep);

    VecSimIndex_Free(index);
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 0; }); // cleanup
}

TYPED_TEST(SVSTest, testTimeoutReturn_batch_iterator) {
    size_t dim = 4;
    size_t n = 10;
    VecSimQueryReply *rep;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        .blockSize = 5,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);

    // Fail on second batch (after calculation already completed)
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);

    rep = VecSimBatchIterator_Next(batchIterator, 1, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(rep), VecSim_QueryReply_OK);
    ASSERT_NE(VecSimQueryReply_Len(rep), 0);
    VecSimQueryReply_Free(rep);

    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 1; }); // Always times out
    rep = VecSimBatchIterator_Next(batchIterator, 1, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(rep), VecSim_QueryReply_TimedOut);
    ASSERT_EQ(VecSimQueryReply_Len(rep), 0);
    VecSimQueryReply_Free(rep);

    VecSimBatchIterator_Free(batchIterator);

    // Fail on first batch (while calculating)
    // Timeout callback function already set to always time out
    batchIterator = VecSimBatchIterator_New(index, query, nullptr);

    rep = VecSimBatchIterator_Next(batchIterator, 1, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(rep), VecSim_QueryReply_TimedOut);
    ASSERT_EQ(VecSimQueryReply_Len(rep), 0);
    VecSimQueryReply_Free(rep);

    VecSimBatchIterator_Free(batchIterator);

    VecSimIndex_Free(index);
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 0; }); // cleanup
}

TYPED_TEST(SVSTest, rangeQuery) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t n = 2000;
    size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        .blockSize = n / 2,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    size_t pivot_id = n / 2; // The id to return vectors around it.
    TEST_DATA_T query[] = {(TEST_DATA_T)pivot_id, (TEST_DATA_T)pivot_id, (TEST_DATA_T)pivot_id,
                           (TEST_DATA_T)pivot_id};

    // Validate invalid params are caught with runtime exception.
    try {
        VecSimIndex_RangeQuery(index, query, -1, nullptr, BY_SCORE);
        FAIL();
    } catch (const std::runtime_error &err) {
        EXPECT_EQ(err.what(), std::string("radius must be non-negative"));
    }
    try {
        VecSimIndex_RangeQuery(index, query, 1, nullptr, VecSimQueryReply_Order(2));
        FAIL();
    } catch (const std::runtime_error &err) {
        EXPECT_EQ(err.what(), std::string("Possible order values are only 'BY_ID' or 'BY_SCORE'"));
    }

    auto verify_res_by_score = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(std::abs(int(id - pivot_id)), (index + 1) / 2);
        ASSERT_EQ(score, dim * pow((index + 1) / 2, 2));
    };
    uint expected_num_results = 11;
    // To get 11 results in the range [pivot_id - 5, pivot_id + 5], set the radius as the L2 score
    // in the boundaries.
    double radius = dim * pow(expected_num_results / 2, 2);
    runRangeQueryTest(index, query, radius, verify_res_by_score, expected_num_results, BY_SCORE);

    // Rerun with a given query params.
    SVSRuntimeParams svsRuntimeParams = {.epsilon = 1.0};
    auto query_params = CreateQueryParams(svsRuntimeParams);
    query_params.batchSize = 100;
    runRangeQueryTest(index, query, radius, verify_res_by_score, expected_num_results, BY_SCORE,
                      &query_params);

    // Get results by id.
    auto verify_res_by_id = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, pivot_id - expected_num_results / 2 + index);
        ASSERT_EQ(score, dim * pow(std::abs(int(id - pivot_id)), 2));
    };
    runRangeQueryTest(index, query, radius, verify_res_by_id, expected_num_results);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, joinSearchParams) {

    auto qbits = TypeParam::get_quant_bits();
    bool is_two_level_lvq = [=]() {
        switch (qbits) {
        case VecSimSvsQuant_4x4:
        case VecSimSvsQuant_4x8:
        case VecSimSvsQuant_4x8_LeanVec:
        case VecSimSvsQuant_8x8_LeanVec:
            return true;
        default:
            return false;
        }
    }();

    size_t default_window_size = 10;
    size_t default_buffer_capacity = 10;
    svs::index::vamana::SearchBufferConfig default_buffer_config{default_window_size,
                                                                 default_buffer_capacity};
    // only change window size = 100
    // sp should have window size = 100, buffer capacity = 100 or
    // 150 if Two-level LVQ is enabled
    SVSRuntimeParams svsRuntimeParams = {.windowSize = 100};
    auto query_params = CreateQueryParams(svsRuntimeParams);
    auto sp = svs_details::joinSearchParams({default_buffer_config, true, 0, 0}, &query_params,
                                            is_two_level_lvq);
    ASSERT_EQ(sp.buffer_config_.get_search_window_size(), svsRuntimeParams.windowSize);
    ASSERT_EQ(sp.buffer_config_.get_total_capacity(),
              (is_two_level_lvq) ? static_cast<size_t>(1.5 * svsRuntimeParams.windowSize)
                                 : svsRuntimeParams.windowSize);

    // change both window size and buffer capacity
    // sp should change based on runtime parameters
    svsRuntimeParams.windowSize = 200;
    svsRuntimeParams.bufferCapacity = 300;
    query_params = CreateQueryParams(svsRuntimeParams);
    sp = svs_details::joinSearchParams({default_buffer_config, true, 0, 0}, &query_params,
                                       is_two_level_lvq);
    ASSERT_EQ(sp.buffer_config_.get_search_window_size(), svsRuntimeParams.windowSize);
    ASSERT_EQ(sp.buffer_config_.get_total_capacity(), svsRuntimeParams.bufferCapacity);

    // only change buffer capacity = 100
    // buffer capacity is changed only if window size is changed
    // sp should be the same as default
    svsRuntimeParams.windowSize = 0;
    svsRuntimeParams.bufferCapacity = 100;
    query_params = CreateQueryParams(svsRuntimeParams);
    sp = svs_details::joinSearchParams({default_buffer_config, true, 0, 0}, &query_params,
                                       is_two_level_lvq);
    ASSERT_EQ(sp.buffer_config_.get_search_window_size(), default_window_size);
    ASSERT_EQ(sp.buffer_config_.get_total_capacity(), default_buffer_capacity);
}

TYPED_TEST(SVSTest, rangeQueryCosine) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    const size_t n = 100;
    const size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_Cosine,
        .blockSize = n / 2,
        /* SVS-Vamana specifics */
        .alpha = 0.9,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // To meet accurary in LVQ case we have to add bulk of vectors at once.
    std::vector<std::array<TEST_DATA_T, dim>> v(n);
    std::vector<size_t> ids(n);

    for (size_t i = 0; i < n; i++) {
        auto &f = v[i];
        f[0] = TEST_DATA_T(i + 1) / n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        // Use as label := n - (internal id)
        ids[i] = n - i;
    }

    auto svs_index = this->CastToSVS(index);
    svs_index->addVectors(v.data(), ids.data(), n);

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    TEST_DATA_T query[dim];
    query[0] = 1.1;
    for (size_t i = 1; i < dim; i++) {
        query[i] = 1.0;
    }
    auto verify_res = [&](size_t id, double score, size_t result_rank) {
        ASSERT_EQ(id, result_rank + 1);
        TEST_DATA_T expected_score = index->getDistanceFrom_Unsafe(id, query);
        // Verify that abs difference between the actual and expected score is at most 1/10^5.
        ASSERT_NEAR((TEST_DATA_T)score, expected_score, 1e-5f);
    };

    uint expected_num_results = 31;
    // Calculate the score of the 31st distant vector from the query vector (whose id should be 30)
    // to get the radius.
    VecSim_Normalize(query, dim, params.type);
    double radius = index->getDistanceFrom_Unsafe(31, query);
    runRangeQueryTest(index, query, radius, verify_res, expected_num_results, BY_SCORE);
    // Return results BY_ID should give the same results.
    runRangeQueryTest(index, query, radius, verify_res, expected_num_results, BY_ID);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, FitMemoryTest) {
    size_t dim = 4;
    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        .blockSize = DEFAULT_BLOCK_SIZE,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    size_t initial_memory = index->getAllocationSize();
    index->fitMemory();
    ASSERT_GE(index->getAllocationSize(), initial_memory);

    // Add vector
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 0);
    initial_memory = index->getAllocationSize();
    index->fitMemory();
    size_t final_size = index->getAllocationSize();
    // Due to the initial capacity, the memory for the vector was already allocated
    ASSERT_EQ(final_size, initial_memory);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, resolve_ws_search_runtime_params) {
    SVSParams params = {.dim = 4, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    VecSimQueryParams qparams, zero;
    bzero(&zero, sizeof(VecSimQueryParams));
    const char *err_msg = nullptr;

    std::vector<VecSimRawParam> rparams;

    auto mkRawParams = [](const std::string &name, const std::string &val) {
        return VecSimRawParam{name.c_str(), name.length(), val.c_str(), val.length()};
    };

    // Test with empty runtime params.
    for (VecsimQueryType query_type : test_utils::query_types) {
        ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                            query_type, nullptr),
                  VecSim_OK);
    }
    ASSERT_EQ(memcmp(&qparams, &zero, sizeof(VecSimQueryParams)), 0);

    std::string param_name = "search_window_size";
    std::string param_val = "100";
    rparams.push_back(mkRawParams(param_name, param_val));

    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSim_OK);
    ASSERT_EQ(qparams.svsRuntimeParams.windowSize, 100);

    param_name = "wrong_name";
    param_val = "100";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_NONE, &err_msg),
              VecSimParamResolverErr_UnknownParam);
    ASSERT_STREQ(err_msg, "Unknown query parameter: 'wrong_name'");

    // Testing for legal prefix but only partial parameter name.
    param_name = "search_window_si";
    param_val = "100";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_NONE, nullptr),
              VecSimParamResolverErr_UnknownParam);

    param_name = "search_window_size";
    param_val = "wrong_val";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, &err_msg),
              VecSimParamResolverErr_BadValue);
    ASSERT_STREQ(err_msg, "SEARCH_WINDOW_SIZE must be a positive integer");

    param_name = "search_window_size";
    param_val = "-30";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSimParamResolverErr_BadValue);

    param_name = "search_window_size";
    param_val = "1.618";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSimParamResolverErr_BadValue);

    param_name = "search_window_size";
    param_val = "100";
    rparams[0] = mkRawParams(param_name, param_val);
    rparams.push_back(mkRawParams(param_name, param_val));
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, &err_msg),
              VecSimParamResolverErr_AlreadySet);
    ASSERT_STREQ(err_msg, "SEARCH_WINDOW_SIZE was specified more than once");

    rparams[1] = (VecSimRawParam){.name = "HYBRID_POLICY",
                                  .nameLen = strlen("HYBRID_POLICY"),
                                  .value = "BATCHES",
                                  .valLen = strlen("BATCHES")};
    rparams.push_back((VecSimRawParam){.name = "batch_size",
                                       .nameLen = strlen("batch_size"),
                                       .value = "50",
                                       .valLen = strlen("50")});
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_HYBRID, nullptr),
              VecSim_OK);
    ASSERT_EQ(qparams.searchMode, HYBRID_BATCHES);
    ASSERT_EQ(qparams.batchSize, 50);
    ASSERT_EQ(qparams.svsRuntimeParams.windowSize, 100);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, resolve_bc_search_runtime_params) {
    SVSParams params = {.dim = 4, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    VecSimQueryParams qparams, zero;
    bzero(&zero, sizeof(VecSimQueryParams));
    const char *err_msg = nullptr;

    std::vector<VecSimRawParam> rparams;

    auto mkRawParams = [](const std::string &name, const std::string &val) {
        return VecSimRawParam{name.c_str(), name.length(), val.c_str(), val.length()};
    };

    // Test with empty runtime params.
    for (VecsimQueryType query_type : test_utils::query_types) {
        ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                            query_type, nullptr),
                  VecSim_OK);
    }
    ASSERT_EQ(memcmp(&qparams, &zero, sizeof(VecSimQueryParams)), 0);

    std::string param_name = "search_buffer_capacity";
    std::string param_val = "100";
    rparams.push_back(mkRawParams(param_name, param_val));

    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSim_OK);
    ASSERT_EQ(qparams.svsRuntimeParams.bufferCapacity, 100);

    param_name = "wrong_name";
    param_val = "100";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_NONE, nullptr),
              VecSimParamResolverErr_UnknownParam);

    // Testing for legal prefix but only partial parameter name.
    param_name = "search_buffer_cap";
    param_val = "100";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_NONE, nullptr),
              VecSimParamResolverErr_UnknownParam);

    param_name = "search_buffer_capacity";
    param_val = "wrong_val";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSimParamResolverErr_BadValue);

    param_name = "search_buffer_capacity";
    param_val = "-30";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSimParamResolverErr_BadValue);

    param_name = "search_buffer_capacity";
    param_val = "1.618";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSimParamResolverErr_BadValue);

    param_name = "search_buffer_capacity";
    param_val = "100";
    rparams[0] = mkRawParams(param_name, param_val);
    rparams.push_back(mkRawParams(param_name, param_val));
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, &err_msg),
              VecSimParamResolverErr_AlreadySet);
    ASSERT_STREQ(err_msg, "SEARCH_BUFFER_CAPACITY was specified more than once");

    rparams[1] = (VecSimRawParam){.name = "HYBRID_POLICY",
                                  .nameLen = strlen("HYBRID_POLICY"),
                                  .value = "BATCHES",
                                  .valLen = strlen("BATCHES")};
    rparams.push_back((VecSimRawParam){.name = "batch_size",
                                       .nameLen = strlen("batch_size"),
                                       .value = "50",
                                       .valLen = strlen("50")});
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_HYBRID, nullptr),
              VecSim_OK);
    ASSERT_EQ(qparams.searchMode, HYBRID_BATCHES);
    ASSERT_EQ(qparams.batchSize, 50);
    ASSERT_EQ(qparams.svsRuntimeParams.bufferCapacity, 100);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, resolve_use_search_history_runtime_params) {
    SVSParams params = {.dim = 4, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    VecSimQueryParams qparams, zero;
    bzero(&zero, sizeof(VecSimQueryParams));
    const char *err_msg = nullptr;

    std::vector<VecSimRawParam> rparams;

    auto mkRawParams = [](const std::string &name, const std::string &val) {
        return VecSimRawParam{name.c_str(), name.length(), val.c_str(), val.length()};
    };

    // Test with empty runtime params.
    for (VecsimQueryType query_type : test_utils::query_types) {
        ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                            query_type, nullptr),
                  VecSim_OK);
    }
    ASSERT_EQ(memcmp(&qparams, &zero, sizeof(VecSimQueryParams)), 0);

    std::string param_name = "use_search_history";
    std::string param_val = "on";
    rparams.push_back(mkRawParams(param_name, param_val));
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSim_OK);
    ASSERT_EQ(qparams.svsRuntimeParams.searchHistory, VecSimOption_ENABLE);

    param_name = "use_search_history";
    param_val = "off";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSim_OK);
    ASSERT_EQ(qparams.svsRuntimeParams.searchHistory, VecSimOption_DISABLE);

    param_name = "use_search_history";
    param_val = "auto";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSim_OK);
    ASSERT_EQ(qparams.svsRuntimeParams.searchHistory, VecSimOption_AUTO);

    param_name = "wrong_name";
    param_val = "on";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_NONE, nullptr),
              VecSimParamResolverErr_UnknownParam);

    // Testing for legal prefix but only partial parameter name.
    param_name = "use_search";
    param_val = "on";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_NONE, nullptr),
              VecSimParamResolverErr_UnknownParam);

    param_name = "use_search_history";
    param_val = "wrong_val";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, &err_msg),
              VecSimParamResolverErr_BadValue);
    ASSERT_STREQ(err_msg, "USE_SEARCH_HISTORY must be one of: ON, OFF, AUTO");

    param_name = "use_search_history";
    param_val = "1";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSimParamResolverErr_BadValue);

    param_name = "use_search_history";
    param_val = "disable";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSimParamResolverErr_BadValue);

    param_name = "use_search_history";
    param_val = "on";
    rparams[0] = mkRawParams(param_name, param_val);
    rparams.push_back(mkRawParams(param_name, param_val));
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_KNN, nullptr),
              VecSimParamResolverErr_AlreadySet);

    rparams[1] = (VecSimRawParam){.name = "HYBRID_POLICY",
                                  .nameLen = strlen("HYBRID_POLICY"),
                                  .value = "BATCHES",
                                  .valLen = strlen("BATCHES")};
    rparams.push_back((VecSimRawParam){.name = "batch_size",
                                       .nameLen = strlen("batch_size"),
                                       .value = "50",
                                       .valLen = strlen("50")});
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_HYBRID, nullptr),
              VecSim_OK);
    ASSERT_EQ(qparams.searchMode, HYBRID_BATCHES);
    ASSERT_EQ(qparams.batchSize, 50);
    ASSERT_EQ(qparams.svsRuntimeParams.searchHistory, VecSimOption_ENABLE);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, resolve_epsilon_runtime_params) {
    SVSParams params = {.dim = 4, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    VecSimQueryParams qparams, zero;
    bzero(&zero, sizeof(VecSimQueryParams));
    const char *err_msg = nullptr;

    std::vector<VecSimRawParam> rparams;

    auto mkRawParams = [](const std::string &name, const std::string &val) {
        return VecSimRawParam{name.c_str(), name.length(), val.c_str(), val.length()};
    };

    // Test with empty runtime params.
    for (VecsimQueryType query_type : test_utils::query_types) {
        ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                            query_type, nullptr),
                  VecSim_OK);
    }
    ASSERT_EQ(memcmp(&qparams, &zero, sizeof(VecSimQueryParams)), 0);

    std::string param_name = "epsilon";
    std::string param_val = "0.001";
    rparams.push_back(mkRawParams(param_name, param_val));
    for (VecsimQueryType query_type : {QUERY_TYPE_NONE, QUERY_TYPE_KNN, QUERY_TYPE_HYBRID}) {
        ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                            query_type, &err_msg),
                  VecSimParamResolverErr_InvalidPolicy_NRange);
        ASSERT_STREQ(err_msg, "EPSILON is only valid for range queries");
    }

    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_RANGE, nullptr),
              VecSim_OK);
    ASSERT_FLOAT_EQ(qparams.svsRuntimeParams.epsilon, 0.001);

    param_name = "wrong_name";
    param_val = "0.001";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_RANGE, nullptr),
              VecSimParamResolverErr_UnknownParam);

    // Testing for legal prefix but only partial parameter name.
    param_name = "epsi";
    param_val = "0.001";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_NONE, nullptr),
              VecSimParamResolverErr_UnknownParam);

    param_name = "epsilon";
    param_val = "wrong_val";
    rparams[0] = mkRawParams(param_name, param_val);
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_RANGE, nullptr),
              VecSimParamResolverErr_BadValue);

    rparams[0] = (VecSimRawParam){
        .name = "epsilon", .nameLen = strlen("epsilon"), .value = "-30", .valLen = 3};
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_RANGE, nullptr),
              VecSimParamResolverErr_BadValue);

    param_name = "epsilon";
    param_val = "0.001";
    rparams[0] = mkRawParams(param_name, param_val);
    rparams.push_back(mkRawParams(param_name, param_val));
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                        QUERY_TYPE_RANGE, nullptr),
              VecSimParamResolverErr_AlreadySet);

    VecSimIndex_Free(index);
}

// An uncompressed SVS index can hand back exactly what it stored, in insertion order, per the
// contract on `VecSimIndexAbstract::getDataByLabel`. A compressed or LeanVec-reduced one keeps
// vectors in the SVS library's own form and does not dequantize, so it must append nothing --
// an empty output reads as "cannot tell", which is the answer a caller comparing against stored
// data needs; anything else risks a dequantized reconstruction reading as a match or a
// difference that isn't real. Checked across both the single- and multi-value index variants,
// since the multi case reads through a different lookup (`get_label_to_external_lookup` +
// `get_parent_index`) than the single case's direct `get_datum`.
//
// Also covers a label the index does not hold at all, which must report nothing regardless of
// compression.
TEST(SVSTest, getDataByLabel) {
    // Limit VecSim log level to avoid printing too much information
    VecSimIndexInterface::setLogCallbackFunction(svsTestLogCallBackNoDebug);
    const size_t dim = 4;
    const size_t present_label = 1;
    const size_t absent_label = 999;

    for (bool is_multi : {false, true}) {
        for (auto quant_bits : {VecSimSvsQuant_NONE, VecSimSvsQuant_Scalar, VecSimSvsQuant_8,
                                VecSimSvsQuant_4, VecSimSvsQuant_4x4, VecSimSvsQuant_4x8,
                                VecSimSvsQuant_4x8_LeanVec, VecSimSvsQuant_8x8_LeanVec}) {
            SVSParams params = {
                .type = VecSimType_FLOAT32,
                .dim = dim,
                .metric = VecSimMetric_L2,
                .multi = is_multi,
                .quantBits = quant_bits,
            };
            VecSimParams index_params = CreateParams(params);
            VecSimIndex *index = VecSimIndex_New(&index_params);
            if (index == nullptr) {
                // Unsupported quant_bits on this build/CPU; `quant_modes` already covers that.
                continue;
            }
            const std::string case_msg = "is_multi: " + std::to_string(is_multi) +
                                         ", quant_bits: " + std::to_string(quant_bits);

            // `VecSimSvsQuant_NONE` is the only mode `isSVSQuantBitsSupported` ever falls back
            // to (an unsupported non-NONE mode falls back to Scalar, never to NONE), so the
            // requested mode alone tells us whether storage ended up compressed.
            const bool is_compressed = quant_bits != VecSimSvsQuant_NONE;

            std::vector<float> v1(dim), v2(dim);
            GenerateVector<float>(v1.data(), dim, 1.0f);
            GenerateVector<float>(v2.data(), dim, 2.0f);
            ASSERT_EQ(VecSimIndex_AddVector(index, v1.data(), present_label), 1) << case_msg;
            if (is_multi) {
                // A second vector under the same label, to exercise the multi-value lookup path.
                ASSERT_EQ(VecSimIndex_AddVector(index, v2.data(), present_label), 1) << case_msg;
            }

            auto *typed = dynamic_cast<VecSimIndexAbstract<float, float> *>(index);
            ASSERT_NE(typed, nullptr) << case_msg;

            std::vector<std::vector<float>> stored;
            typed->getDataByLabel(present_label, stored);
            if (is_compressed) {
                EXPECT_TRUE(stored.empty())
                    << "a compressed index must not report stored vectors: " << case_msg;
            } else if (is_multi) {
                ASSERT_EQ(stored.size(), 2) << case_msg;
                EXPECT_EQ(stored[0], v1) << case_msg;
                EXPECT_EQ(stored[1], v2) << case_msg;
            } else {
                ASSERT_EQ(stored.size(), 1) << case_msg;
                EXPECT_EQ(stored[0], v1) << case_msg;
            }

            std::vector<std::vector<float>> stored_absent;
            typed->getDataByLabel(absent_label, stored_absent);
            EXPECT_TRUE(stored_absent.empty())
                << "a label the index does not hold must report nothing: " << case_msg;

            VecSimIndex_Free(index);
        }
    }
}

TEST(SVSTest, quant_modes) {
    // Limit VecSim log level to avoid printing too much information
    VecSimIndexInterface::setLogCallbackFunction(svsTestLogCallBackNoDebug);

    const size_t dim = 4;
    const size_t n = 100;
    // Include both boundary labels, 45 and 55, which tie for query 50.
    const size_t k = 11;

    for (auto quant_bits : {VecSimSvsQuant_NONE, VecSimSvsQuant_Scalar, VecSimSvsQuant_8,
                            VecSimSvsQuant_4, VecSimSvsQuant_4x4, VecSimSvsQuant_4x8,
                            VecSimSvsQuant_4x8_LeanVec, VecSimSvsQuant_8x8_LeanVec}) {
        SVSParams params = {
            .type = VecSimType_FLOAT32,
            .dim = dim,
            .metric = VecSimMetric_L2,
            .blockSize = 1024,
            /* SVS-Vamana specifics */
            .quantBits = quant_bits,
            .graph_max_degree = 63, // x^2-1 to round the graph block size
            .construction_window_size = 20,
            .max_candidate_pool_size = 1024,
            .prune_to = 60,
            .use_search_history = VecSimOption_ENABLE,
        };

        VecSimParams index_params = CreateParams(params);
        VecSimIndex *index = VecSimIndex_New(&index_params);
        if (index == nullptr) {
            if (std::get<1>(svs_details::isSVSQuantBitsSupported(quant_bits))) {
                GTEST_FAIL() << "Failed to create SVS index";
            } else {
                GTEST_SKIP() << "SVS LVQ is not supported.";
            }
        }

        // Test initial size estimation
        // EstimateInitialSize is called after CreateNewIndex because params struct is
        // changed in CreateNewIndex.
        size_t estimation = EstimateInitialSize(params);
        size_t actual = index->getAllocationSize();
        EXPECT_EQ(estimation, actual);

        EXPECT_EQ(VecSimIndex_IndexSize(index), 0);
        EXPECT_EQ(index->debugInfo().svsInfo.quantBits,
                  std::get<0>(svs_details::isSVSQuantBitsSupported(quant_bits)));

        std::vector<std::array<float, dim>> v(n);
        for (size_t i = 0; i < n; i++) {
            GenerateVector<float>(v[i].data(), dim, i);
        }

        std::vector<size_t> ids(n);
        std::iota(ids.begin(), ids.end(), 0);

        auto svs_index = dynamic_cast<SVSIndexBase *>(index);
        ASSERT_NE(svs_index, nullptr);
        svs_index->addVectors(v.data(), ids.data(), n);

        ASSERT_EQ(VecSimIndex_IndexSize(index), n);

        estimation = EstimateBlockSize(params, params.blockSize, n);
        actual = index->getAllocationSize() - actual; // get the delta
        ASSERT_GT(actual, 0);
        // LVQ element size estimation accuracy is low
        auto quant_bits_fallback = std::get<0>(svs_details::isSVSQuantBitsSupported(quant_bits));
        double estimation_accuracy = (quant_bits_fallback != VecSimSvsQuant_NONE) ? 0.12 : 0.01;
        ASSERT_GE(estimation * (1.0 + estimation_accuracy), actual);
        ASSERT_LE(estimation * (1.0 - estimation_accuracy), actual);

        float query[] = {50, 50, 50, 50};
        auto verify_res = [&](size_t id, double score, size_t idx) {
            EXPECT_DOUBLE_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, id, query), score);
            EXPECT_EQ(id, (idx + 45));
        };
        runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

        VecSimIndex_Free(index);
    }
}

TEST(SVSTest, save_load) {
    namespace fs = std::filesystem;
    // Limit VecSim log level to avoid printing too much information
    VecSimIndexInterface::setLogCallbackFunction(svsTestLogCallBackNoDebug);

    const size_t dim = 4;
    const size_t n = 100;
    // Include both boundary labels, 45 and 55, in the single-value case.
    const size_t k = 11;

    // Helper function to convert quant_bits to string for error messages
    auto quant_bits_to_string = [](VecSimSvsQuantBits quant_bits) -> std::string {
        switch (quant_bits) {
        case VecSimSvsQuant_NONE:
            return "VecSimSvsQuant_NONE";
        case VecSimSvsQuant_Scalar:
            return "VecSimSvsQuant_Scalar";
        case VecSimSvsQuant_8:
            return "VecSimSvsQuant_8";
        case VecSimSvsQuant_4:
            return "VecSimSvsQuant_4";
        case VecSimSvsQuant_4x4:
            return "VecSimSvsQuant_4x4";
        case VecSimSvsQuant_4x8:
            return "VecSimSvsQuant_4x8";
        case VecSimSvsQuant_4x8_LeanVec:
            return "VecSimSvsQuant_4x8_LeanVec";
        case VecSimSvsQuant_8x8_LeanVec:
            return "VecSimSvsQuant_8x8_LeanVec";
        default:
            return "Unknown(" + std::to_string(static_cast<int>(quant_bits)) + ")";
        }
    };

    // Test both single and multi variations
    for (bool is_multi : {false, true}) {
        for (auto quant_bits : {VecSimSvsQuant_NONE, VecSimSvsQuant_Scalar, VecSimSvsQuant_8,
                                VecSimSvsQuant_4, VecSimSvsQuant_4x4, VecSimSvsQuant_4x8,
                                VecSimSvsQuant_4x8_LeanVec, VecSimSvsQuant_8x8_LeanVec}) {
            SVSParams params = {
                .type = VecSimType_FLOAT32,
                .dim = dim,
                .metric = VecSimMetric_L2,
                .multi = is_multi,
                .blockSize = 1024,
                /* SVS-Vamana specifics */
                .quantBits = quant_bits,
                .graph_max_degree = 63, // x^2-1 to round the graph block size
                .construction_window_size = 20,
                .max_candidate_pool_size = 1024,
                .prune_to = 60,
                .use_search_history = VecSimOption_ENABLE,
            };

            VecSimParams index_params = CreateParams(params);
            VecSimIndex *index = VecSimIndex_New(&index_params);
            if (index == nullptr) {
                if (std::get<1>(svs_details::isSVSQuantBitsSupported(quant_bits))) {
                    GTEST_FAIL() << "Failed to create SVS index with quant_bits: "
                                 << quant_bits_to_string(quant_bits)
                                 << ", multi: " << (is_multi ? "true" : "false");
                } else {
                    GTEST_SKIP() << "SVS LVQ is not supported for quant_bits: "
                                 << quant_bits_to_string(quant_bits)
                                 << ", multi: " << (is_multi ? "true" : "false");
                }
            }

            std::vector<std::array<float, dim>> v(n);
            std::vector<size_t> ids(n);

            if (is_multi) {
                const size_t per_label = 2;
                const size_t num_labels = n / per_label;

                for (size_t i = 0; i < n; i++) {
                    size_t label_id = (i / per_label);
                    GenerateVector<float>(v[i].data(), dim, i);
                    ids[i] = label_id;
                }
            } else {
                // For single-index, each vector has a unique label (same as its index)
                for (size_t i = 0; i < n; i++) {
                    GenerateVector<float>(v[i].data(), dim, i);
                    ids[i] = i;
                }
            }

            auto svs_index = dynamic_cast<SVSIndexBase *>(index);
            ASSERT_NE(svs_index, nullptr)
                << "Failed to cast to SVSIndexBase with quant_bits: "
                << quant_bits_to_string(quant_bits) << ", multi: " << (is_multi ? "true" : "false");
            svs_index->addVectors(v.data(), ids.data(), n);

            ASSERT_EQ(VecSimIndex_IndexSize(index), n)
                << "Index size mismatch after adding vectors with quant_bits: "
                << quant_bits_to_string(quant_bits) << ", multi: " << (is_multi ? "true" : "false");

            float query[] = {50, 50, 50, 50};
            auto verify_res = [&](size_t id, double score, size_t idx) {
                EXPECT_DOUBLE_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, id, query), score);
                // Single-value labels are 45..55; multi-value labels are 20..30.
                if (is_multi) {
                    // For multi, that label of {50,50,50,50} is 25
                    size_t expected_label = (20 + idx);
                    EXPECT_EQ(id, expected_label);
                } else {
                    EXPECT_EQ(id, (idx + 45));
                }
            };
            runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

            fs::path tmp{fs::temp_directory_path()};
            auto subdir = "vecsim_test_" + std::to_string(std::rand());
            auto index_path = tmp / subdir;
            while (fs::exists(index_path)) {
                subdir = "vecsim_test_" + std::to_string(std::rand());
                index_path = tmp / subdir;
            }
            fs::create_directories(index_path);

            try {
                svs_index->saveIndex(index_path.string());
            } catch (const std::exception &e) {
                GTEST_FAIL() << "Failed to save index with quant_bits: "
                             << quant_bits_to_string(quant_bits)
                             << ", multi: " << (is_multi ? "true" : "false")
                             << ", error: " << e.what();
            }
            VecSimIndex_Free(index);

            // Recreate the index from the saved path
            index = VecSimIndex_New(&index_params);
            svs_index = dynamic_cast<SVSIndexBase *>(index);
            ASSERT_NE(svs_index, nullptr)
                << "Failed to recreate index for loading with quant_bits: "
                << quant_bits_to_string(quant_bits) << ", multi: " << (is_multi ? "true" : "false");

            try {
                svs_index->loadIndex(index_path.string());
                svs_index->checkIntegrity();
            } catch (const std::exception &e) {
                GTEST_FAIL() << "Failed to load index with quant_bits: "
                             << quant_bits_to_string(quant_bits)
                             << ", multi: " << (is_multi ? "true" : "false")
                             << ", error: " << e.what();
            }

            // Verify the index was loaded correctly
            ASSERT_EQ(VecSimIndex_IndexSize(index), n)
                << "Index size mismatch after loading with quant_bits: "
                << quant_bits_to_string(quant_bits) << ", multi: " << (is_multi ? "true" : "false");
            runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

            // Test load from file with constructor
            VecSimIndex *svs_index_load = nullptr;
            try {
                svs_index_load = SVSFactory::NewIndex(index_path.string(), &index_params);
            } catch (const std::exception &e) {
                GTEST_FAIL() << "Failed to create index from file with quant_bits: "
                             << quant_bits_to_string(quant_bits)
                             << ", multi: " << (is_multi ? "true" : "false")
                             << ", error: " << e.what();
            }
            ASSERT_NE(svs_index_load, nullptr)
                << "Failed to create index from file with quant_bits: "
                << quant_bits_to_string(quant_bits) << ", multi: " << (is_multi ? "true" : "false");

            // Verify the index was loaded correctly
            ASSERT_EQ(VecSimIndex_IndexSize(svs_index_load), n)
                << "Index size mismatch for constructor-loaded index with quant_bits: "
                << quant_bits_to_string(quant_bits) << ", multi: " << (is_multi ? "true" : "false");
            runTopKSearchTest(svs_index_load, query, k, verify_res, nullptr, BY_ID);

            VecSimIndex_Free(svs_index_load);
            VecSimIndex_Free(index);

            // Cleanup
            fs::remove_all(index_path); // Cleanup the saved index directory
        }
    }
}

TYPED_TEST(SVSTest, logging_runtime_params) {
    const size_t dim = 4;
    const size_t n = 100;
    const size_t k = 11;

    std::ostringstream os_index;
    std::ostringstream os_global;

    VecSim_SetLogCallbackFunction([](void *ctx, const char *level, const char *message) {
        if (ctx == nullptr) {
            return;
        }
        assert(level != nullptr);
        assert(message != nullptr);
        // Cast the context to the correct type
        // and write the log message to the ostringstream
        std::ostringstream *os = static_cast<std::ostringstream *>(ctx);
        *os << level << ": " << message;
    });

    // Set the SVS global log context to the ostringstream
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(os_global);
    auto logger = std::make_shared<spdlog::logger>("GlobalLogger", sink);
    // Trace all messages
    logger->set_level(spdlog::level::trace);
    logger->set_pattern("%@\n\t%+");
    svs::logging::set(logger);

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
    };
    this->SetTypeParams(params);
    VecSimParams index_params = CreateParams(params);
    index_params.logCtx =
        static_cast<void *>(&os_index); // Set the index log context to the ostringstream
    VecSimIndex *index = this->CreateNewIndex(index_params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    std::vector<std::array<TEST_DATA_T, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        GenerateVector<TEST_DATA_T>(v[i].data(), dim, i);
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    svs_index->addVectors(v.data(), ids.data(), n);

    // Overrite vectors one-by-one
    for (size_t i = 0; i < 10; i++) {
        index->addVector(v[i].data(), ids[i]);
    }
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), 10);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n + 10);
    ASSERT_EQ(index->indexLabelCount(), n);

    float query[] = {50, 50, 50, 50};
    auto verify_res = [&](size_t id, double score, size_t index) { EXPECT_EQ(id, (index + 45)); };
    runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

    // Write custom logging info
    auto index_logger = svs_index->getLogger();
    ASSERT_NE(index_logger, nullptr);
    index_logger->trace("Custom log trace");
    index_logger->debug("Custom log debug");
    index_logger->info("Custom log info");
    index_logger->warn("Custom log warn");
    index_logger->error("Custom log error");
    index_logger->critical("Custom log critical");
    index_logger->flush();
    // Check that the log messages are written to the ostringstream
    auto index_log = os_index.str();
    EXPECT_NE(index_log.find("Custom log trace"), std::string::npos);
    EXPECT_NE(index_log.find("Custom log debug"), std::string::npos);
    EXPECT_NE(index_log.find("Custom log info"), std::string::npos);
    EXPECT_NE(index_log.find("Custom log warn"), std::string::npos);
    EXPECT_NE(index_log.find("Custom log critical"), std::string::npos);
    EXPECT_NE(index_log.find("Custom log error"), std::string::npos);

    VecSimIndex_Free(index);

    auto global_log = os_global.str();
    EXPECT_TRUE(global_log.empty()) << "Global log should be empty, but got: " << global_log;
}

TEST(SVSTest, scalar_quantization_query) {
    // Limit VecSim log level to avoid printing too much information
    VecSimIndexInterface::setLogCallbackFunction(svsTestLogCallBackNoDebug);

    const size_t dim = 32;
    const size_t bs = 1024;
    const size_t n = 100;
    const size_t k = 10;
    const double quant_precision = 1.0 / (1 << 7); // int8 quantization precision

    std::default_random_engine gen;
    std::uniform_real_distribution<float> dist(-1.0, 1.0);
    std::vector<float> dataset(n * dim);
    for (size_t i = 0; i < n * dim; i++) {
        dataset[i] = dist(gen);
    }
    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    float query[dim];
    GenerateVector<float>(query, dim, 0.1f);

    VecSimQueryReply *fp_results = nullptr;
    auto verify_res = [&](size_t id, double score, size_t result_rank) {
        const auto &fp_result = fp_results->results[result_rank];
        ASSERT_EQ(id, fp_result.id);
        // Verify that relative difference between the actual and expected score is within 8-bit
        // quantization precision.
        auto expected_diff = std::abs(score * quant_precision);
        ASSERT_NEAR(score, fp_result.score, expected_diff);
    };

    const std::pair<VecSimMetric, double> metrics[] = {
        {VecSimMetric_L2, 30.},
        {VecSimMetric_Cosine, 1.0},
    };

    for (auto [metric, radius] : metrics) {
        SVSParams params = {
            .dim = dim,
            .metric = metric,
            .blockSize = bs,
            /* SVS-Vamana specifics */
            .graph_max_degree = 63, // x^2-1 to round the graph block size
            .construction_window_size = 20,
            .max_candidate_pool_size = 1024,
            .prune_to = 60,
            .use_search_history = VecSimOption_ENABLE,
        };
        params.quantBits = VecSimSvsQuant_NONE;

        auto index_params = CreateParams(params);
        auto index_fp = VecSimIndex_New(&index_params);
        ASSERT_NE(index_fp, nullptr);

        dynamic_cast<SVSIndexBase *>(index_fp)->addVectors(dataset.data(), ids.data(), n);
        ASSERT_EQ(VecSimIndex_IndexSize(index_fp), n);

        params.quantBits = VecSimSvsQuant_Scalar;
        index_params = CreateParams(params);
        auto index_sq = VecSimIndex_New(&index_params);
        ASSERT_NE(index_sq, nullptr);

        auto estimation = EstimateInitialSize(params);
        auto actual = index_sq->getAllocationSize();
        ASSERT_EQ(estimation, actual);

        dynamic_cast<SVSIndexBase *>(index_sq)->addVectors(dataset.data(), ids.data(), n);
        ASSERT_EQ(VecSimIndex_IndexSize(index_sq), n);
        ASSERT_EQ(index_sq->indexCapacity(), n);

        estimation = EstimateBlockSize(params, params.blockSize, n);
        actual = index_sq->getAllocationSize() - actual; // get the delta
        ASSERT_GT(actual, 0);
        ASSERT_GE(estimation * 1.01, actual);
        ASSERT_LE(estimation * 0.99, actual);

        // test topK search
        fp_results = VecSimIndex_TopKQuery(index_fp, query, k, nullptr, BY_ID);
        runTopKSearchTest(index_sq, query, k, verify_res, nullptr, BY_ID);
        VecSimQueryReply_Free(fp_results);

        // test range search
        fp_results = VecSimIndex_RangeQuery(index_fp, query, radius, nullptr, BY_ID);
        ASSERT_GT(fp_results->results.size(), 0);
        runRangeQueryTest(index_sq, query, radius, verify_res, fp_results->results.size(), BY_ID);
        VecSimQueryReply_Free(fp_results);

        VecSimIndex_Free(index_sq);
        VecSimIndex_Free(index_fp);
    }
}

#if defined(__linux__) && defined(__x86_64__)
TEST(SVSTest, compute_distance) {
    // Test svs::distance computation for custom data allocations and alignments
    constexpr size_t dim = 4;

    // get system pagesize
    size_t page_size = sysconf(_SC_PAGESIZE);
    ASSERT_GT(page_size, 16);

    // Allocate two consecutive pages: one for data, one as a guard (inaccessible)
    uint8_t *raw_a = (uint8_t *)mmap(nullptr, 2 * page_size, PROT_READ | PROT_WRITE,
                                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(raw_a, MAP_FAILED);
    // Protect the second page to prevent access
    ASSERT_EQ(mprotect(raw_a + page_size, page_size, PROT_NONE), 0);

    // Allocate the second buffer
    uint8_t *raw_b = (uint8_t *)mmap(nullptr, 2 * page_size, PROT_READ | PROT_WRITE,
                                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(raw_b, MAP_FAILED);
    // Protect the second page to prevent access
    ASSERT_EQ(mprotect(raw_b + page_size, page_size, PROT_NONE), 0);

    // use last bytes of page for data
    // Note: Accessing above 'dim' should trigger Memory Access Error.
    constexpr size_t data_size = dim * sizeof(float);
    float *a = reinterpret_cast<float *>(raw_a + page_size - data_size);
    float *b = reinterpret_cast<float *>(raw_b + page_size - data_size);

    std::iota(a, a + dim, 1.f);
    std::iota(b, b + dim, 2.f);

    // Verify default implementation
    auto dist_l2 = svs::distance::compute(svs::DistanceL2{}, std::span(a, dim), std::span(b, dim));
    EXPECT_GT(dist_l2, 0.0);
    auto dist_ip = svs::distance::compute(svs::DistanceIP{}, std::span(a, dim), std::span(b, dim));
    EXPECT_GT(dist_ip, 0.0);

    // Verify AVX2 and AVX512 implementations
    if (svs::detail::avx_runtime_flags.is_avx2_supported()) {
        // AVX2 implementations
        auto dist_l2_avx2 = svs::distance::
            L2Impl<svs::Dynamic, float, float, svs::distance::AVX_AVAILABILITY::AVX2>::compute(
                a, b, svs::lib::MaybeStatic(dim));
        auto dist_ip_avx2 = svs::distance::
            IPImpl<svs::Dynamic, float, float, svs::distance::AVX_AVAILABILITY::AVX2>::compute(
                a, b, svs::lib::MaybeStatic(dim));
        EXPECT_DOUBLE_EQ(dist_l2, dist_l2_avx2);
        EXPECT_DOUBLE_EQ(dist_ip, dist_ip_avx2);
    }

    if (svs::detail::avx_runtime_flags.is_avx512f_supported()) {
        // AVX512 implementations
        auto dist_l2_avx512 = svs::distance::
            L2Impl<svs::Dynamic, float, float, svs::distance::AVX_AVAILABILITY::AVX512>::compute(
                a, b, svs::lib::MaybeStatic(dim));
        auto dist_ip_avx512 = svs::distance::
            IPImpl<svs::Dynamic, float, float, svs::distance::AVX_AVAILABILITY::AVX512>::compute(
                a, b, svs::lib::MaybeStatic(dim));
        EXPECT_DOUBLE_EQ(dist_l2, dist_l2_avx512);
        EXPECT_DOUBLE_EQ(dist_ip, dist_ip_avx512);
    }

    // unmap pages
    munmap(raw_a, 2 * page_size);
    munmap(raw_b, 2 * page_size);
}
#endif // defined(__linux__) && defined(__x86_64__)

// ---------------------------------------------------------------------------
// SVSParams::num_threads is deprecated and ignored — pool size comes from
// the shared singleton. Setting it should log a warning but not affect the pool.
// ---------------------------------------------------------------------------
TEST(SVSTest, NumThreadsParamIgnored) {
    // Mark the pool as attached so resize() applies eagerly (this test resizes
    // before constructing the SVS index, so the lazy code path would otherwise
    // just record the size without spawning threads).
    VecSimSVSThreadPoolImpl::instance()->onIndexAttached();
    // Resize the shared singleton pool to a known size.
    VecSimSVSThreadPool::resize(2);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 2);

    // Capture warning logs emitted by VecSim.
    std::string captured_log;
    VecSimIndexInterface::logCallback = [](void *ctx, const char *level, const char *msg) {
        auto *out = static_cast<std::string *>(ctx);
        *out += std::string(level) + ": " + msg + "\n";
    };

    // Create an SVS index with an explicit (deprecated) num_threads value.
    SVSParams svs_params = {
        .type = VecSimType_FLOAT32,
        .dim = 4,
        .metric = VecSimMetric_L2,
        .num_threads = 16, // should be ignored
    };
    VecSimParams params{.algo = VecSimAlgo_SVS,
                        .algoParams = {.svsParams = svs_params},
                        .logCtx = static_cast<void *>(&captured_log)};
    VecSimIndex *index = VecSimIndex_New(&params);
    ASSERT_NE(index, nullptr);

    // The shared pool size must remain at 2 — num_threads was ignored.
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 2);

    // The index reports the shared pool size, not the deprecated param value.
    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.svsInfo.numThreads, 2);

    // A deprecation warning should have been logged.
    EXPECT_NE(captured_log.find("deprecated"), std::string::npos)
        << "Expected deprecation warning in log, got: " << captured_log;

    VecSimIndex_Free(index);

    // Verify: creating an index without setting num_threads produces no warning.
    captured_log.clear();
    SVSParams svs_params_default = {
        .type = VecSimType_FLOAT32, .dim = 4, .metric = VecSimMetric_L2,
        // num_threads left as 0 (default / unset)
    };
    VecSimParams params_default{.algo = VecSimAlgo_SVS,
                                .algoParams = {.svsParams = svs_params_default},
                                .logCtx = static_cast<void *>(&captured_log)};
    VecSimIndex *index2 = VecSimIndex_New(&params_default);
    ASSERT_NE(index2, nullptr);
    EXPECT_EQ(captured_log.find("deprecated"), std::string::npos)
        << "No deprecation warning expected when num_threads is not set, got: " << captured_log;

    VecSimIndex_Free(index2);

    // Restore singleton to baseline (clears has_attached_index_ and slots) and
    // clear log callback so subsequent tests see a clean state.
    VecSimSVSThreadPoolImpl::instance()->resetForTest();
    VecSimIndexInterface::logCallback = nullptr;
}

// The SHARED_MEMORY field is a top-level field appended by
// VecSimIndex_DebugInfoIterator (mirrors VecSim_GetSharedMemory()); it is
// present on every algorithm. Verify it appears exactly once in an SVS
// response and reports the same bytes as the public API.
TYPED_TEST(SVSTest, debugInfoSharedMemoryMatchesApi) {
    // Request a non-trivial pool size; with lazy init the actual allocation only
    // happens on first SVS index creation below.
    VecSim_UpdateThreadPoolSize(2);

    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);
    ASSERT_GT(VecSim_GetSharedMemory(), 0u);

    VecSimDebugInfoIterator *infoIterator = VecSimIndex_DebugInfoIterator(index);

    bool seen_shared_memory = false;
    uint64_t shared_memory_value = 0;
    while (VecSimDebugInfoIterator_HasNextField(infoIterator)) {
        VecSim_InfoField *f = VecSimDebugInfoIterator_NextField(infoIterator);
        if (!strcmp(f->fieldName, VecSimCommonStrings::SHARED_MEMORY_STRING)) {
            ASSERT_FALSE(seen_shared_memory) << "SHARED_MEMORY appears more than once";
            ASSERT_EQ(f->fieldType, INFOFIELD_UINT64);
            shared_memory_value = f->fieldValue.uintegerValue;
            seen_shared_memory = true;
        }
    }
    EXPECT_TRUE(seen_shared_memory) << "SHARED_MEMORY field missing from SVS debug info";
    EXPECT_EQ(shared_memory_value, VecSim_GetSharedMemory());

    VecSimDebugInfoIterator_Free(infoIterator);
    VecSimIndex_Free(index);

    // Reset the shared singleton pool to size 1 so the next test is not affected.
    // Use VecSimSVSThreadPool::resize(1) directly (matching other thread-pool tests)
    // to avoid the write-mode side effect that VecSim_UpdateThreadPoolSize(0) carries.
    VecSimSVSThreadPool::resize(1);
}

// VecSim shared memory must actually track the SVS thread-pool allocation:
// it grows when the pool grows and shrinks when the pool shrinks. Without this,
// SHARED_MEMORY could be a constant and debugInfoSharedMemoryMatchesApi would
// still pass (both readouts share the same getSharedAllocationSize() source).
TYPED_TEST(SVSTest, sharedMemoryTracksThreadPoolResize) {
    // With lazy init, resize() only records the requested size until an SVS index
    // has attached. Mark the pool attached up front so the resizes below apply
    // eagerly and their allocation effect is observable (idempotent, matches the
    // pattern used by NumThreadsParamIgnored and the SVSThreadPoolTest fixture).
    VecSimSVSThreadPoolImpl::instance()->onIndexAttached();

    // Use the C API to ensure it is covered. VecSim_UpdateThreadPoolSize(0)
    // sets WriteInPlace and pool size 1.
    VecSim_UpdateThreadPoolSize(0);
    size_t mem_baseline = VecSim_GetSharedMemory();

    // Grow the pool (sets WriteAsync).
    VecSim_UpdateThreadPoolSize(8);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 8u);
    size_t mem_8 = VecSim_GetSharedMemory();
    EXPECT_GT(mem_8, mem_baseline) << "shared memory must grow when the pool grows";

    // Shrink back to size 1 (still WriteAsync).
    VecSim_UpdateThreadPoolSize(1);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 1u);
    size_t mem_after = VecSim_GetSharedMemory();
    EXPECT_LT(mem_after, mem_8) << "shared memory must shrink when the pool shrinks";

    // Restore to default baseline.
    VecSim_UpdateThreadPoolSize(0);
}

// ---------------------------------------------------------------------------
// Lazy thread-pool init: VecSim_UpdateThreadPoolSize must not allocate worker
// threads until the first SVS index attaches.
// ---------------------------------------------------------------------------
TEST(SVSTest, ThreadPoolLazyInit) {
    // Reset the shared singleton to a clean state — earlier tests may have
    // attached indexes and resized the pool. resetForTest() clears
    // has_attached_index_, so VecSim_GetSharedMemory() reports 0 even though the
    // singleton object (and its self-accounting allocator) still exist: shared
    // memory is only attributed once an SVS index attaches.
    VecSimSVSThreadPoolImpl::instance()->resetForTest();
    const size_t baseline_mem = VecSim_GetSharedMemory();
    EXPECT_EQ(baseline_mem, 0u) << "shared memory must be 0 before any SVS index attaches";

    // Recording a non-trivial requested size before any SVS index exists must
    // not allocate any worker slots.
    VecSim_UpdateThreadPoolSize(8);
    EXPECT_EQ(VecSim_GetSharedMemory(), baseline_mem)
        << "VecSim_UpdateThreadPoolSize must not allocate threads before any SVS index exists";
    EXPECT_EQ(VecSimSVSThreadPool::poolSize(), 1u)
        << "Pool size must stay at 1 (no worker slots) until first index attaches";

    // First SVS index creation triggers onIndexAttached(), which applies the
    // recorded size and spawns 7 worker threads.
    SVSParams params = {.type = VecSimType_FLOAT32, .dim = 4, .metric = VecSimMetric_L2};
    VecSimParams vp{.algo = VecSimAlgo_SVS, .algoParams = {.svsParams = params}};
    VecSimIndex *index = VecSimIndex_New(&vp);
    ASSERT_NE(index, nullptr);
    ASSERT_GT(VecSim_GetSharedMemory(), baseline_mem)
        << "First SVS index creation must trigger lazy thread spawn";
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 8u)
        << "Pool size must reflect the most recent requested size after first attach";

    VecSimIndex_Free(index);
    VecSimSVSThreadPoolImpl::instance()->resetForTest();
}

TYPED_TEST(SVSTest, updateVectorsReportsAFailedWrite) {
    // SVS signals a failure by throwing, and this is reached across an `extern "C"` boundary where
    // an escaping exception is undefined behaviour. The throw is injected, because SVS only throws
    // for states the checks in this method rule out first - so this is the only way to reach the
    // reporting at all.
    size_t dim = 4;
    size_t n = 5;
    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    auto *svs_index = dynamic_cast<SVSIndexBase *>(index);
    ASSERT_NE(svs_index, nullptr);

    TEST_DATA_T replacement[dim];
    GenerateVector<TEST_DATA_T>(replacement, dim, n + 5);

    // A failed update is reported as such - not as a refusal, which would tell the caller the
    // index left itself alone, and not by letting the exception out.
    svs_index->throwOnNextWriteForTest();
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 1, replacement, 1), VecSimUpdate_Failed);

    // The index is still usable afterwards, and the injection was one-shot: the same update now
    // goes through.
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 1, replacement, 1), VecSimUpdate_OK);
    ASSERT_EQ(index->indexLabelCount(), n);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, updateVectors) {
    size_t dim = 4;
    size_t n = 5;
    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    ASSERT_EQ(index->indexLabelCount(), n);

    auto *svs_index = dynamic_cast<SVSIndexBase *>(index);
    ASSERT_NE(svs_index, nullptr);

    const labelType label = 3;
    TEST_DATA_T old_vector[dim], replacement[dim];
    GenerateVector<TEST_DATA_T>(old_vector, dim, label);
    GenerateVector<TEST_DATA_T>(replacement, dim, n + 5);

    ASSERT_EQ(VecSimIndex_UpdateVectors(index, label, replacement, 1), VecSimUpdate_OK);

    // One vector took another's place, so no label was gained or lost. The entry that left is
    // marked, not gone - SVS deletes softly and drops the entry at a later consolidation - which
    // is what tells us the update went through the delete path rather than writing over the data
    // SVS placed the vector by.
    ASSERT_EQ(index->indexLabelCount(), n);
    ASSERT_TRUE(svs_index->isLabelExists(label));
    ASSERT_GT(svs_index->getNumMarkedDeleted(), 0);

    // Counted without the marked entries, the index holds as many vectors as it did.
    ASSERT_EQ(VecSimIndex_IndexSize(index) - svs_index->getNumMarkedDeleted(), n);

    if (!svs_index->isCompressed()) {
        // The label is found at its new value, and a query at the old one finds its neighbours by
        // value instead. Asserted only for an uncompressed index: a compressed one trains its
        // stored form on the vectors it was given, so a replacement outside that range - which
        // this one is - is clipped, and neither the nearest neighbour nor the distance to it says
        // anything reliable afterwards.
        auto verify_new = [&](size_t id, double score, size_t rank) { ASSERT_EQ(id, label); };
        runTopKSearchTest(index, replacement, 1, verify_new);
        auto verify_old = [&](size_t id, double score, size_t rank) { ASSERT_NE(id, label); };
        runTopKSearchTest(index, old_vector, 1, verify_old);
    }

    // A label the index does not hold yet is simply stored: with nothing to remove, the end state
    // the caller asked for is reached anyway.
    TEST_DATA_T fresh[dim];
    GenerateVector<TEST_DATA_T>(fresh, dim, n + 6);
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 100, fresh, 1), VecSimUpdate_OK);
    ASSERT_EQ(index->indexLabelCount(), n + 1);
    ASSERT_TRUE(svs_index->isLabelExists(100));

    // Two vectors under one label is not a state this index can hold. Refused before anything is
    // removed, rather than served by storing one of them.
    TEST_DATA_T two_vectors[2 * dim];
    GenerateVector<TEST_DATA_T>(two_vectors, dim, n + 7);
    GenerateVector<TEST_DATA_T>(two_vectors + dim, dim, n + 8);
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, label, two_vectors, 2),
              VecSimUpdate_MultiNotSupported);
    ASSERT_EQ(index->indexLabelCount(), n + 1);
    ASSERT_TRUE(svs_index->isLabelExists(label));

    // An empty update leaves the label holding nothing, which is a delete.
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 100, nullptr, 0), VecSimUpdate_OK);
    ASSERT_EQ(index->indexLabelCount(), n);
    ASSERT_FALSE(svs_index->isLabelExists(100));

    VecSimIndex_Free(index);
}

#if HAVE_SVS_REPLACE_EXTERNAL_ID

TYPED_TEST(SVSTest, relabelVector) {
    size_t dim = 4;
    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 1);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 2, 2);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 2);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 1);
    // The distance to the label's own vector, captured before the move. Asserting it is unchanged
    // afterwards says the move did not disturb the stored vector without assuming what that
    // distance is -- a quantized index does not answer 0 for a vector's own query.
    const double before = VecSimIndex_GetDistanceFrom_Unsafe(index, 1, query);
    ASSERT_FALSE(std::isnan(before));

    ASSERT_EQ(VecSimIndex_RelabelVector(index, 1, 100), VecSimRelabel_OK);

    // Nothing was added or removed, and the vector answers to the new label only.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 2);
    ASSERT_EQ(index->indexLabelCount(), 2);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, 100, query), before);
    ASSERT_TRUE(std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(index, 1, query)));

    auto verify_res = [&](size_t id, double score, size_t rank) {
        ASSERT_EQ(id, 100);
        ASSERT_EQ(score, before);
    };
    runTopKSearchTest(index, query, 1, verify_res);

    VecSimIndex_Free(index);
}

// SVS deletes softly -- the entry is marked and only dropped by a later consolidation, so it is
// still occupying an id when this runs. `has_id` excludes it, which is what makes the move report
// the label absent rather than renaming a tombstone: the same contract HNSW states for its own
// marked-deleted elements, and worth pinning here because the two arrive at it by different
// means.
TYPED_TEST(SVSTest, relabelVectorMarkedDeleted) {
    size_t dim = 4;
    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    GenerateAndAddVector<TEST_DATA_T>(index, dim, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 1);
    ASSERT_EQ(VecSimIndex_DeleteVector(index, 0), 1);

    auto *svs_index = dynamic_cast<SVSIndexBase *>(index);
    ASSERT_NE(svs_index, nullptr);
    // Soft, not gone: the assertions below are about a marked entry, not an absent one.
    ASSERT_GT(svs_index->getNumMarkedDeleted(), 0);

    ASSERT_EQ(VecSimIndex_RelabelVector(index, 0, 100), VecSimRelabel_OldLabelMissing);
    ASSERT_FALSE(svs_index->isLabelExists(100)) << "a tombstone was renamed";
    ASSERT_EQ(index->indexLabelCount(), 1);

    // A live label in the same index still relabels fine.
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 1, 101), VecSimRelabel_OK);
    ASSERT_TRUE(svs_index->isLabelExists(101));
    ASSERT_EQ(index->indexLabelCount(), 1);

    // And the deleted label stays free for reuse rather than being half-claimed by the refusal.
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 0, 7);
    ASSERT_TRUE(svs_index->isLabelExists(0));
    ASSERT_EQ(index->indexLabelCount(), 2);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSTest, relabelVectorRejects) {
    size_t dim = 4;
    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // An index that never held a vector has no SVS impl yet - still a clean rejection, not a crash.
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 1, 2), VecSimRelabel_OldLabelMissing);

    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 1);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 2, 2);

    ASSERT_EQ(VecSimIndex_RelabelVector(index, 42, 100), VecSimRelabel_OldLabelMissing);
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 1, 2), VecSimRelabel_NewLabelTaken);
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 1, 1), VecSimRelabel_SameLabel);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 2);
    ASSERT_EQ(index->indexLabelCount(), 2);
    for (labelType label : {1, 2}) {
        TEST_DATA_T v[dim];
        GenerateVector<TEST_DATA_T>(v, dim, label);
        // Present and answering for its own vector; the value itself depends on the encoding.
        ASSERT_FALSE(std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(index, label, v)))
            << "label " << label << " was modified";
    }

    VecSimIndex_Free(index);
}

#else // HAVE_SVS_REPLACE_EXTERNAL_ID

// Built against an SVS without `replace_external_id`, so SVSIndex leaves relabelVector to the
// interface default. Asserting the code here rather than skipping keeps the contract covered in
// this configuration too: a caller has to be able to tell "this index never relabels" from a
// rejection it could resolve itself.
TEST(SVSTest, relabelVectorUnsupported) {
    size_t dim = 4;
    SVSParams params = {.type = VecSimType_FLOAT32, .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams index_params = CreateParams(params);
    VecSimIndex *index = VecSimIndex_New(&index_params);
    ASSERT_NE(index, nullptr);

    GenerateAndAddVector<float>(index, dim, 1, 1);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 1);

    // The label exists and the target is free, so only the unsupported default can produce this.
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 1, 2), VecSimRelabel_Unsupported);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 1);

    VecSimIndex_Free(index);
}

#endif // HAVE_SVS_REPLACE_EXTERNAL_ID

#else // HAVE_SVS

TEST(SVSTest, svs_not_supported) {
    SVSParams params = {
        .type = VecSimType_FLOAT32,
        .dim = 16,
        .metric = VecSimMetric_IP,
    };
    auto index_params = CreateParams(params);
    auto index = VecSimIndex_New(&index_params);
    ASSERT_EQ(index, nullptr);

    auto size = VecSimIndex_EstimateInitialSize(&index_params);
    ASSERT_EQ(size, -1);

    auto size2 = VecSimIndex_EstimateElementSize(&index_params);
    ASSERT_EQ(size2, -1);
}

#endif
