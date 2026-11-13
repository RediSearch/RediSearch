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
#include "unit_test_utils.h"
#include "tests_utils.h"
#include "VecSim/index_factories/tiered_factory.h"
#include "VecSim/spaces/IP/IP.h"
#include <array>
#include <cmath>
#include <random>
#include <vector>

using float16 = vecsim_types::float16;

#if HAVE_SVS
#include <sstream>
#include "spdlog/sinks/ostream_sink.h"
#include "VecSim/algorithms/svs/svs.h"
#include "VecSim/spaces/L2/L2.h"
#include "VecSim/algorithms/svs/svs_tiered.h"

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
class FP16SVSTest : public ::testing::Test {
public:
    using data_t = typename index_type_t::data_t;

protected:
    void SetTypeParams(SVSParams &params) {
        params.quantBits = params.quantBits == VecSimSvsQuant_NONE ? index_type_t::get_quant_bits()
                                                                   : params.quantBits;
        params.type = index_type_t::get_index_type();
        params.multi = params.multi == 0 ? false : params.multi;
    }

    void SetUp() override {
        // Limit VecSim log level to avoid printing too much information
        VecSimIndexInterface::setLogCallbackFunction(svsTestLogCallBackNoDebug);
    }

    virtual VecSimIndex *CreateNewIndex(SVSParams &params) {
        SetTypeParams(params);
        VecSimParams index_params = CreateParams(params);
        return VecSimIndex_New(&index_params);
    }

    SVSIndexBase *CastToSVS(VecSimIndex *index) {
        auto indexBase = dynamic_cast<SVSIndexBase *>(index);
        assert(indexBase != nullptr);
        return indexBase;
    }

    // Check if the test is running in fallback mode to scalar quantization.
    bool isFallbackToSQ() const {
        // Get the fallback quantization mode and compare it to the scalar quantization mode.
        return VecSimSvsQuant_Scalar ==
               std::get<0>(svs_details::isSVSQuantBitsSupported(index_type_t::get_quant_bits()));
    }

    void GenerateVector(float16 *out_vec, size_t dim, float initial_value = 0.25f,
                        float step = 0.0f) {
        for (size_t i = 0; i < dim; i++) {
            out_vec[i] = vecsim_types::FP32_to_FP16(initial_value + step * i);
        }
    }

    int GenerateAndAddVector(VecSimIndex *index, size_t dim, size_t id, float initial_value = 0.25f,
                             float step = 0.0f) {
        float16 v[dim];
        this->GenerateVector(v, dim, initial_value, step);
        return VecSimIndex_AddVector(index, v, id);
    }

    std::vector<std::vector<char>> getStoredVectorDataByLabel(VecSimIndex *index, labelType label) {
        return (dynamic_cast<VecSimIndexAbstract<data_t, float> *>(index))
            ->getStoredVectorDataByLabel(label);
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


using SVSDataTypeSet = ::testing::Types<SVSIndexType<VecSimType_FLOAT16, float16, VecSimSvsQuant_NONE>
                                       ,SVSIndexType<VecSimType_FLOAT16, float16, VecSimSvsQuant_8>
                                       ,SVSIndexType<VecSimType_FLOAT16, float16, VecSimSvsQuant_8x8_LeanVec>
                                        >;
// clang-format on

TYPED_TEST_SUITE(FP16SVSTest, SVSDataTypeSet);

TYPED_TEST(FP16SVSTest, svs_vector_add_test) {

    constexpr size_t dim = 4;
    float initial_value = 0.5f;
    float step = 1.0f;
    size_t label = 0;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    EXPECT_EQ(VecSimIndex_IndexSize(index), 0);

    float16 vector[dim];
    this->GenerateVector(vector, dim, initial_value, step);
    VecSimIndex_AddVector(index, vector, label);

    EXPECT_EQ(VecSimIndex_IndexSize(index), 1);
    ASSERT_EQ(index->getDistanceFrom_Unsafe(label, vector), 0);
    if (!(TypeParam::get_quant_bits() == VecSimSvsQuant_8 ||
          TypeParam::get_quant_bits() == VecSimSvsQuant_8x8_LeanVec)) {
        // Get vector is only implemented for non-compressed indices.
        auto vector_data = this->getStoredVectorDataByLabel(index, label);
        const char *data_ptr = vector_data.at(0).data();
        for (size_t i = 0; i < dim; i++) {
            ASSERT_EQ(vecsim_types::FP16_to_FP32(((float16 *)data_ptr)[i]),
                      initial_value + step * float(i));
        }
    }

    size_t k = 1;
    auto verify_res = [&](size_t id, double score, size_t index) { EXPECT_EQ(score, 0.0); };
    runTopKSearchTest(index, vector, k, verify_res, nullptr, BY_ID);

    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSTest, svs_vector_update_test) {
    size_t dim = 4;
    size_t n = 1;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_IP,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto *svs_index = this->CastToSVS(index);

    EXPECT_EQ(VecSimIndex_IndexSize(index), 0);

    this->GenerateAndAddVector(index, dim, 1);

    EXPECT_EQ(VecSimIndex_IndexSize(index), 1);

    // Prepare new vector data and call addVector with the same id, different data.
    this->GenerateAndAddVector(index, dim, 1, 2.0);

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

/**
 * This test crashes during search in SVS < 0.09 due to the small window size, causing the
 * graph to be very sparse, and a bug in SVS that doesn't handle well such case.
 * See mod-10771
 */
TYPED_TEST(FP16SVSTest, svs_bulk_vectors_add_delete_test) {
    constexpr size_t n = 256;
    size_t k = 11;
    const size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        .construction_window_size = 20,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    std::vector<std::array<float16, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        this->GenerateVector(v[i].data(), dim, i);
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    auto svs_index = this->CastToSVS(index);
    svs_index->addVectors(v.data(), ids.data(), n);

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    float16 query[dim];
    this->GenerateVector(query, dim, 50);
    auto verify_res = [&](size_t id, double score, size_t index) { EXPECT_EQ(id, (index + 45)); };
    runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

    // Delete almost all vectors
    const size_t keep_num = 10;
    ASSERT_EQ(svs_index->deleteVectors(ids.data(), n - keep_num), n - keep_num);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), keep_num);
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), n - keep_num);

    auto verify_res_after_delete = [&](size_t id, double score, size_t index) {
        EXPECT_EQ(id, n - keep_num + index);
    };

    // Before the bug fix, here an exception is thrown from robin_hash.h: 917:
    // Thread 0: Couldn't find key.
    runTopKSearchTest(index, query, keep_num, verify_res_after_delete, nullptr, BY_ID);

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

TYPED_TEST(FP16SVSTest, svs_indexing_same_vector) {
    const size_t n = 100;
    const size_t k = 10;
    const size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    std::vector<std::array<float16, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        this->GenerateVector(v[i].data(), dim,
                             i / 10); // i / 10 is in integer (take the "floor" value).
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Run a query where all the results are supposed to be {5,5,5,5} (different ids).
    float16 query[dim] = {vecsim_types::FP32_to_FP16(4.9), vecsim_types::FP32_to_FP16(4.95),
                          vecsim_types::FP32_to_FP16(5.05), vecsim_types::FP32_to_FP16(5.1)};
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_TRUE(id >= 50 && id < 60 && score <= 1);
    };
    runTopKSearchTest(index, query, k, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSTest, svs_reindexing_same_vector) {
    const size_t n = 100;
    const size_t k = 10;
    const size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    std::vector<std::array<TEST_DATA_T, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        // i / 10 is in integer (take the "floor" value).
        this->GenerateVector(v[i].data(), dim, i / 10);
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Run a query where all the results are supposed to be {5,5,5,5} (different ids).
    float16 query[dim] = {vecsim_types::FP32_to_FP16(4.9), vecsim_types::FP32_to_FP16(4.95),
                          vecsim_types::FP32_to_FP16(5.05), vecsim_types::FP32_to_FP16(5.1)};
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
        this->GenerateAndAddVector(index, dim, i, i / 10);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), 2 * n);
    ASSERT_EQ(index->indexLabelCount(), n);
    ASSERT_EQ(svs_index->getNumMarkedDeleted(), n);

    // Run the same query again.
    runTopKSearchTest(index, query, k, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSTest, svs_reindexing_same_vector_different_id) {
    const size_t n = 100;
    const size_t k = 10;
    const size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    std::vector<std::array<float16, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        this->GenerateVector(v[i].data(), dim,
                             i / 10); // i / 10 is in integer (take the "floor" value).
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Run a query where all the results are supposed to be {5,5,5,5} (different ids).
    float16 query[dim] = {vecsim_types::FP32_to_FP16(4.9), vecsim_types::FP32_to_FP16(4.95),
                          vecsim_types::FP32_to_FP16(5.05), vecsim_types::FP32_to_FP16(5.1)};
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
        this->GenerateAndAddVector(index, dim, i + 10,
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

TYPED_TEST(FP16SVSTest, svs_batch_iterator) {
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
        };

        VecSimIndex *index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        for (size_t i = 0; i < n; i++) {
            this->GenerateAndAddVector(index, dim, i, i);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(index), n) << "Running for n = " << n;

        // Query for (n,n,...,n) vector (recall that n is the largest id in te index).
        float16 query[dim];
        this->GenerateVector(query, dim, n);

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
                ASSERT_EQ(id, expected_ids[index])
                    << "Running for n = " << n << " iteration_num: " << iteration_num
                    << " index: " << index << " score: " << score;
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
            iteration_num++;
        }
        ASSERT_EQ(iteration_num, n / n_res) << "Running for n = " << n;
        VecSimBatchIterator_Free(batchIterator);

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(FP16SVSTest, svs_batch_iterator_non_unique_scores) {
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
        };

        VecSimIndex *index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        for (size_t i = 0; i < n; i++) {
            this->GenerateAndAddVector(index, dim, i, i / 10);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(index), n) << "Running for n = " << n;

        // Query for (n,n,...,n) vector (recall that n is the largest id in te index).
        float16 query[dim];
        this->GenerateVector(query, dim, n);

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
                ASSERT_TRUE(expected_ids.find(id) != expected_ids.end())
                    << "Running for n = " << n << " iteration_num: " << iteration_num
                    << " index: " << index << " score: " << score;
                ;
                expected_ids.erase(id);
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
            // Make sure that the expected ids set is empty after two iterations.
            if (even_iteration) {
                ASSERT_TRUE(expected_ids.empty())
                    << "Running for n = " << n << " iteration_num: " << iteration_num;
            }
            iteration_num++;
            even_iteration = !even_iteration;
        }
        ASSERT_EQ(iteration_num, n / n_res);
        VecSimBatchIterator_Free(batchIterator);

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(FP16SVSTest, svs_batch_iterator_reset) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    constexpr size_t dim = 4;
    size_t n = 10000;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
        .construction_window_size = 20,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    for (size_t i = 0; i < n; i++) {
        this->GenerateAndAddVector(index, dim, i, i / 10);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Query for (n,n,...,n) vector (recall that n is the largest id in te index).
    float16 query[dim];
    this->GenerateVector(query, dim, n);
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
                ASSERT_TRUE(expected_ids.find(id) != expected_ids.end())
                    << "take: " << take << " iteration_num: " << iteration_num
                    << " index: " << index << " score: " << score;
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

TYPED_TEST(FP16SVSTest, svs_batch_iterator_corner_cases) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t n = 1000;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // Query for (n,n,...,n) vector (recall that n is the largest id in te index).
    float16 query[dim];
    this->GenerateVector(query, dim, n);

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
        this->GenerateAndAddVector(index, dim, i, i);
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
        ASSERT_TRUE(id == n - 1 - index) << "index: " << index << " score: " << score;
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
TYPED_TEST(FP16SVSTest, resizeIndex) {
    size_t dim = 4;
    size_t n = 10;
    size_t bs = 4;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = bs};

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // Add up to n.
    for (size_t i = 0; i < n; i++) {
        this->GenerateAndAddVector(index, dim, i, i);
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
    // The capacity shouldn't be changed.
    ASSERT_EQ(index->indexCapacity(), n + extra_cap);

    VecSimIndex_Free(index);
}

// Test empty index edge cases.
TYPED_TEST(FP16SVSTest, svs_empty_index) {
    size_t dim = 4;
    size_t n = 20;
    size_t k = 10;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Try to remove from an empty index - should fail because label doesn't exist.
    VecSimIndex_DeleteVector(index, 0);

    float16 query[dim];
    this->GenerateVector(query, dim);

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
        this->GenerateAndAddVector(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    for (size_t i = 0; i < n; i++) {
        VecSimIndex_DeleteVector(index, i);
    }
    index->runGC();
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Even an empty index holds the first block for nonquantized data
    // ASSERT_EQ(index->indexCapacity(), 0);

    // Try to remove it again.
    VecSimIndex_DeleteVector(index, 1);
    // Nor the size.
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

TYPED_TEST(FP16SVSTest, test_delete_vector) {
    size_t k = 5;
    constexpr size_t dim = 10;
    size_t block_size = 3;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // Delete from empty index
    ASSERT_EQ(VecSimIndex_DeleteVector(index, 111), 0);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    size_t n = 6;
    for (size_t i = 0; i < n; i++) {
        this->GenerateAndAddVector(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Here the shift should happen.
    VecSimIndex_DeleteVector(index, 1);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n - 1);

    float16 query[dim];
    this->GenerateVector(query, dim, 0.0);
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

TYPED_TEST(FP16SVSTest, sanity_reinsert_1280) {
    constexpr size_t n = 5;
    constexpr size_t d = 1280;
    size_t k = 5;

    SVSParams params = {
        .dim = d,
        .metric = VecSimMetric_Cosine,
        /* SVS-Vamana specifics */
        .alpha = 1.2,
        .graph_max_degree = 64,
        // .construction_window_size = 20,
        .max_candidate_pool_size = 1024,
        .prune_to = 60,
        .use_search_history = VecSimOption_ENABLE,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto *vectors = new float16[n * d];

    // Generate random vectors in every iteration and inert them under different ids.
    for (size_t iter = 1; iter <= 1; iter++) {
        for (size_t i = 0; i < n; i++) {
            size_t internal_iter = n * (iter - 1) + i;
            test_utils::populate_float16_vec(vectors + i * d, d, internal_iter, 0.0, 1.0f);
        }
        auto expected_ids = std::set<size_t>();
        for (size_t i = 0; i < n; i++) {
            ASSERT_EQ(VecSimIndex_AddVector(index, (vectors + i * d), i * iter), 1);
            expected_ids.insert(i * iter);
        }

        ASSERT_EQ(VecSimIndex_IndexSize(index), n);
        auto verify_res = [&](size_t id, double score, size_t index) {
            ASSERT_TRUE(expected_ids.find(id) != expected_ids.end())
                << "iter: " << iter << " index: " << index << " score: " << score << " id: " << id;
            expected_ids.erase(id);
        };

        // Send arbitrary vector (the first) and search for top k. This should return all the
        // vectors that were inserted in this iteration - verify their ids.
        ASSERT_NO_FATAL_FAILURE(runTopKSearchTest(index, vectors, k, verify_res))
            << "iter: " << iter;

        // Remove vectors form current iteration.
        for (size_t i = 0; i < n; i++) {
            VecSimIndex_DeleteVector(index, i * iter);
        }
    }
    delete[] vectors;
    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSTest, test_svs_info) {
    size_t n = 100;
    size_t d = 128;

    // Build with default args.

    for (auto metric : {VecSimMetric_L2, VecSimMetric_IP, VecSimMetric_Cosine}) {
        SCOPED_TRACE("Testing metric: " + std::string(VecSimMetric_ToString(metric)));
        SVSParams params = {
            .dim = d,
            .metric = metric,
        };

        VecSimIndex *index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
        ASSERT_EQ(info.commonInfo.basicInfo.dim, d);
        ASSERT_FALSE(info.commonInfo.basicInfo.isMulti);
        ASSERT_EQ(info.commonInfo.basicInfo.type, VecSimType_FLOAT16);
        // Default args.
        ASSERT_EQ(info.commonInfo.indexSize, 0);
        ASSERT_EQ(info.commonInfo.indexLabelCount, 0);
        EXPECT_EQ(info.commonInfo.memory, index->getAllocationSize());
        validateSVSIndexAttributesInfo(info.svsInfo, params);
        VecSimIndex_Free(index);

        d = 1280;
        params.dim = d;
        params.blockSize = DEFAULT_BLOCK_SIZE - 10;
        params.alpha = 52;
        params.graph_max_degree = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE + 2;
        params.construction_window_size = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE + 4;
        params.max_candidate_pool_size = params.construction_window_size * 8;
        params.prune_to = params.graph_max_degree + 56;
        params.use_search_history = VecSimOption_DISABLE;

        index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        info = VecSimIndex_DebugInfo(index);
        ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
        ASSERT_EQ(info.commonInfo.basicInfo.dim, d);
        ASSERT_FALSE(info.commonInfo.basicInfo.isMulti);
        ASSERT_FALSE(info.commonInfo.basicInfo.isTiered);

        // User args.
        ASSERT_EQ(info.commonInfo.basicInfo.blockSize, DEFAULT_BLOCK_SIZE - 10);
        ASSERT_EQ(info.commonInfo.indexSize, 0);
        validateSVSIndexAttributesInfo(info.svsInfo, params);

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
}

TYPED_TEST(FP16SVSTest, test_basic_svs_info_iterator) {
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

TYPED_TEST(FP16SVSTest, test_dynamic_svs_info_iterator) {
    if (TypeParam::get_quant_bits() == VecSimSvsQuant_8 ||
        TypeParam::get_quant_bits() == VecSimSvsQuant_8x8_LeanVec) {
        GTEST_SKIP() << "Already included in the test loop.";
    }
    constexpr size_t d = 128;
    for (auto quant_bits : {VecSimSvsQuant_NONE, VecSimSvsQuant_Scalar, VecSimSvsQuant_8,
                            VecSimSvsQuant_4, VecSimSvsQuant_4x4, VecSimSvsQuant_4x8,
                            VecSimSvsQuant_4x8_LeanVec, VecSimSvsQuant_8x8_LeanVec}) {

        SVSParams params = {
            .dim = d,
            .metric = VecSimMetric_Cosine,
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
        validateSVSIndexAttributesInfo(info.svsInfo, params);

        VecSimDebugInfoIterator *infoIter = VecSimIndex_DebugInfoIterator(index);
        ASSERT_EQ(1, info.commonInfo.basicInfo.blockSize);
        ASSERT_EQ(0, info.commonInfo.indexSize);
        compareSVSIndexInfoToIterator(info, infoIter);
        VecSimDebugInfoIterator_Free(infoIter);

        float16 v[d];
        this->GenerateVector(v, d);
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

TYPED_TEST(FP16SVSTest, svs_get_distance) {
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    const size_t dim = 4;
    static double constexpr expected_dists[2] = {0.25, -1.5}; // L2, IP

    for (VecSimMetric metric : {VecSimMetric_L2, VecSimMetric_IP}) {

        SVSParams params = {
            .dim = dim,
            .metric = metric,
        };

        VecSimIndex *index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        float16 vec[dim];
        this->GenerateVector(vec, dim, 0.25, 0.25); // {0.25, 0.5, 0.75, 1}
        VecSimIndex_AddVector(index, vec, 0);
        ASSERT_EQ(VecSimIndex_IndexSize(index), 1);

        float16 query[dim];
        this->GenerateVector(query, dim, 0.5, 0.25); // {0.5, 0.75, 1, 1.25}

        double dist = VecSimIndex_GetDistanceFrom_Unsafe(index, 0, query);

        // manually calculated. Values were chosen as such that don't cause any accuracy loss in
        // conversion from bfloat16 to float.
        ASSERT_EQ(dist, expected_dists[metric]) << "metric: " << metric;

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(FP16SVSTest, svs_vector_search_test_ip) {
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

        std::vector<std::array<float16, dim>> v(n);
        for (size_t i = 0; i < n; i++) {
            this->GenerateVector(v[i].data(), dim, i);
        }

        std::vector<size_t> ids(n);
        std::iota(ids.begin(), ids.end(), 0);

        svs_index->addVectors(v.data(), ids.data(), n);
        ASSERT_EQ(VecSimIndex_IndexSize(index), n);

        float16 query[dim];
        this->GenerateVector(query, dim, 0, 1.0);
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

TYPED_TEST(FP16SVSTest, svs_vector_search_test_l2) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    constexpr size_t dim = 4;
    size_t n = 100;
    size_t k = 11;

    for (size_t blocksize : {1, 12, DEFAULT_BLOCK_SIZE}) {

        SVSParams params = {.dim = dim,
                            .metric = VecSimMetric_L2,
                            .blockSize = blocksize,
                            /* SVS-Vamana specifics */
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
            this->GenerateAndAddVector(index, dim, i, i);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(index), n);

        auto verify_res = [&](size_t id, double score, size_t index) {
            size_t diff_id = (id > 50) ? (id - 50) : (50 - id);
            ASSERT_EQ(diff_id, (index + 1) / 2)
                << "index: " << index << " id: " << id << " score: " << score;
            ASSERT_EQ(score, (4 * ((index + 1) / 2) * ((index + 1) / 2)))
                << "index: " << index << " id: " << id << " score: " << score;
        };
        float16 query[dim];
        this->GenerateVector(query, dim, 50);
        runTopKSearchTest(index, query, k, verify_res);
        runTopKSearchTest(index, query, 0, verify_res); // For sanity, search for nothing

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(FP16SVSTest, batchIteratorSwapIndices) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    constexpr size_t dim = 4;
    size_t n = 1000;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,

    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    float16 close_vec[dim];
    this->GenerateVector(close_vec, dim, 1.0);
    float16 further_vec[dim];
    this->GenerateVector(further_vec, dim, 2.0);

    VecSimIndex_AddVector(index, further_vec, 0);
    VecSimIndex_AddVector(index, close_vec, 1);
    VecSimIndex_AddVector(index, further_vec, 2);
    VecSimIndex_AddVector(index, close_vec, 3);
    VecSimIndex_AddVector(index, close_vec, 4);
    VecSimIndex_AddVector(index, close_vec, 5);
    for (size_t i = 6; i < n; i++) {
        this->GenerateAndAddVector(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Query for (1,1,1,1) vector.
    TEST_DATA_T query[dim];
    this->GenerateVector(query, dim, 1.0);
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

TYPED_TEST(FP16SVSTest, svs_vector_search_test_cosine) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    const size_t dim = 128;
    const size_t n = 20;
    constexpr size_t query_id = n / 2;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_Cosine,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    std::vector<std::array<float16, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        test_utils::populate_float16_vec(v[i].data(), dim, i, -1.0f, 1.0f);
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    auto svs_index = this->CastToSVS(index);
    svs_index->addVectors(v.data(), ids.data(), n);

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    auto *query = v[query_id].data();
    VecSim_Normalize(query, dim, params.type);

    // Get expected results.
    std::vector<std::pair<size_t, double>> results;
    for (size_t i = 0; i < n; i++) {
        // normalize vector
        auto &f = v[i];
        VecSim_Normalize(f.data(), dim, params.type);
        double score = FP16_InnerProduct(query, f.data(), dim);

        results.push_back({i, score});
    }
    // Sort by score (best first)
    std::sort(results.begin(), results.end(),
              [](const auto &a, const auto &b) { return a.second < b.second; });

    auto verify_res = [&](size_t id, double score, size_t result_rank) {
        if (result_rank == 0) {
            ASSERT_EQ(id, query_id)
                << "result_rank: " << result_rank << " id: " << id << " score: " << score;
            ASSERT_NEAR(score, 0.0, 1e-3f)
                << "result_rank: " << result_rank << " id: " << id << " score: " << score;
        } else {
            ASSERT_EQ(id, results[result_rank].first)
                << "result_rank: " << result_rank << " id: " << id << " score: " << score;
            ASSERT_NEAR(score, results[result_rank].second, 1e-3f)
                << "result_rank: " << result_rank << " id: " << id << " score: " << score;
        }
    };
    runTopKSearchTest(index, query, 10, verify_res, nullptr, BY_SCORE);

    // Test with batch iterator.
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    size_t iteration_num = 0;

    size_t n_res = 5;
    while (VecSimBatchIterator_HasNext(batchIterator)) {
        auto verify_res_batch = [&](size_t id, double score, size_t result_rank) {
            size_t global_result_rank = iteration_num * n_res + result_rank;
            ASSERT_NO_FATAL_FAILURE(verify_res(id, score, global_result_rank))
                << "iteration_num: " << iteration_num
                << " global_result_rank: " << global_result_rank;
        };
        runBatchIteratorSearchTest(batchIterator, n_res, verify_res_batch);
        iteration_num++;
    }
    ASSERT_EQ(iteration_num, n / n_res);
    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSTest, testSizeEstimation) {
    size_t dim = 64;
    auto quantBits = TypeParam::get_quant_bits();
#if HAVE_SVS_LVQ
    // SVS block sizes always rounded to a power of 2
    // This why, in case of quantization, actual block size can be differ than requested
    // In addition, block size to be passed to graph and dataset counted in bytes,
    // converted then to a number of elements.
    // IMHO, would be better to always interpret block size to a number of elements
    // rather than conversion to-from number of bytes
    if (quantBits != VecSimSvsQuant_NONE && !this->isFallbackToSQ()) {
        // Extra data in LVQ vector
        const auto lvq_vector_extra = sizeof(svs::quantization::lvq::ScalarBundle);
        dim -= (lvq_vector_extra * 8) / TypeParam::get_quant_bits();
    }
#endif
    size_t n = 0;
    size_t bs = DEFAULT_BLOCK_SIZE;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_Cosine,
        /* SVS-Vamana specifics */
        .graph_max_degree = 63, // x^2-1 to round the graph block size
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);
    // EstimateInitialSize is called after CreateNewIndex because params struct is
    // changed in CreateNewIndex.
    size_t estimation = EstimateInitialSize(params);

    size_t actual = index->getAllocationSize();
    ASSERT_EQ(estimation, actual);

    estimation = EstimateBlockSize(params, bs, 1);

    this->GenerateAndAddVector(index, dim, 0);
    actual = index->getAllocationSize() - actual; // get the delta
    ASSERT_GT(actual, 0);
    // LVQ element estimation accuracy is low
    double estimation_accuracy = (quantBits != VecSimSvsQuant_NONE) ? 0.1 : 0.01;
    ASSERT_GE(estimation * (1.0 + estimation_accuracy), actual);
    ASSERT_LE(estimation * (1.0 - estimation_accuracy), actual);

    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSTest, rangeQuery) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t n = 100;
    size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    float pivot_value = 1.0f;
    float16 pivot_vec[dim];
    this->GenerateVector(pivot_vec, dim, pivot_value);

    float radius = 1.5f;
    std::mt19937 gen(42);
    std::uniform_real_distribution<> dis(pivot_value - radius, pivot_value + radius);

    // insert 20 vectors near a pivot vector.
    size_t n_close = 20;
    for (size_t i = 0; i < n_close; i++) {
        float random_number = dis(gen);
        this->GenerateAndAddVector(index, dim, i, random_number);
    }

    float16 max_vec[dim];
    this->GenerateVector(max_vec, dim, pivot_value + radius);
    double max_dist = FP16_L2Sqr(pivot_vec, max_vec, dim);

    // Add more vectors far from the pivot vector, under the same labels
    for (size_t i = n_close; i < n; i++) {
        float random_number = dis(gen);
        this->GenerateAndAddVector(index, dim, i, 5.0 + random_number);
    }

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    auto verify_res_by_score = [&](size_t id, double score, size_t index) {
        ASSERT_LE(id, n_close - 1) << "score: " << score;
        ASSERT_LE(score, max_dist);
    };
    uint expected_num_results = n_close;

    runRangeQueryTest(index, pivot_vec, max_dist, verify_res_by_score, expected_num_results,
                      BY_SCORE);

    // Rerun with a given query params.
    SVSRuntimeParams svsRuntimeParams = {.epsilon = 100.0};
    auto query_params = CreateQueryParams(svsRuntimeParams);
    query_params.batchSize = 100;
    runRangeQueryTest(index, pivot_vec, max_dist, verify_res_by_score, expected_num_results,
                      BY_SCORE, &query_params);

    // Get results by id.
    auto verify_res_by_id = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, index);
        ASSERT_LE(score, max_dist);
    };
    runRangeQueryTest(index, pivot_vec, max_dist, verify_res_by_id, expected_num_results);

    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSTest, rangeQueryCosine) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    const size_t n_close = 20;
    const size_t n_far = 20;
    const size_t n = n_close + n_far;
    const size_t dim = 4;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_Cosine,
    };

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // To meet accurary in LVQ case we have to add bulk of vectors at once.
    std::vector<std::array<float16, dim>> v(n);
    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    for (size_t i = 0; i < n_close; i++) {
        test_utils::populate_float16_vec(v[i].data(), dim, i, 0.0f, 2.0f);
    }

    for (size_t i = n_close; i < n; i++) {
        test_utils::populate_float16_vec(v[i].data(), dim, i, -5.0f, -3.0f);
    }

    auto svs_index = this->CastToSVS(index);
    svs_index->addVectors(v.data(), ids.data(), n);

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // create a query with values in the "close" range.
    float16 query[dim];
    test_utils::populate_float16_vec(query, dim, n, 0.0f, 2.0f);

    float16 normalized_query[dim];
    memcpy(normalized_query, query, dim * sizeof(float16));
    VecSim_Normalize(normalized_query, dim, params.type);

    auto verify_res = [&](size_t id, double score, size_t result_rank) {
        ASSERT_LE(id, n_close - 1)
            << "result_rank: " << result_rank << " id: " << id << " score: " << score;
        double expected_score = index->getDistanceFrom_Unsafe(id, normalized_query);
        // Verify that abs difference between the actual and expected score is at most 1/10^5.
        ASSERT_NEAR(score, expected_score, 1e-5f)
            << "result_rank: " << result_rank << " id: " << id << " score: " << score
            << " expected_score: " << expected_score;
    };

    double radius = 1.0;
    constexpr size_t expected_num_results = n_close;
    ASSERT_NO_FATAL_FAILURE(
        runRangeQueryTest(index, query, radius, verify_res, expected_num_results, BY_SCORE));
    // Return results BY_ID should give the same results.
    ASSERT_NO_FATAL_FAILURE(
        runRangeQueryTest(index, query, radius, verify_res, expected_num_results, BY_ID));

    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSTest, quant_modes) {
    if (TypeParam::get_quant_bits() == VecSimSvsQuant_8 ||
        TypeParam::get_quant_bits() == VecSimSvsQuant_8x8_LeanVec) {
        GTEST_SKIP() << "Already included in the test loop.";
    }

    // Limit VecSim log level to avoid printing too much information
    VecSimIndexInterface::setLogCallbackFunction(svsTestLogCallBackNoDebug);

    const size_t dim = 4;
    const size_t n = 100;
    const size_t k = 11;

    for (auto quant_bits : {VecSimSvsQuant_NONE, VecSimSvsQuant_Scalar, VecSimSvsQuant_8,
                            VecSimSvsQuant_4, VecSimSvsQuant_4x4, VecSimSvsQuant_4x8,
                            VecSimSvsQuant_4x8_LeanVec, VecSimSvsQuant_8x8_LeanVec}) {

        SCOPED_TRACE("quant_bits = " + std::string(VecSimQuantBits_ToString(quant_bits)));
        SVSParams params = {
            .dim = dim,
            .metric = VecSimMetric_L2,
            /* SVS-Vamana specifics */
            .quantBits = quant_bits,
            .graph_max_degree = 63,
        };

        VecSimIndex *index = this->CreateNewIndex(params);
        ASSERT_INDEX(index);

        // Test initial size estimation
        // EstimateInitialSize is called after CreateNewIndex because params struct is
        // changed in CreateNewIndex.
        size_t estimation = EstimateInitialSize(params);
        size_t actual = index->getAllocationSize();
        EXPECT_EQ(estimation, actual);

        EXPECT_EQ(VecSimIndex_IndexSize(index), 0);
        EXPECT_EQ(index->debugInfo().svsInfo.quantBits,
                  std::get<0>(svs_details::isSVSQuantBitsSupported(quant_bits)));

        std::vector<std::array<float16, dim>> v(n);
        for (size_t i = 0; i < n; i++) {
            this->GenerateVector(v[i].data(), dim, i);
        }

        std::vector<size_t> ids(n);
        std::iota(ids.begin(), ids.end(), 0);

        auto svs_index = dynamic_cast<SVSIndexBase *>(index);
        ASSERT_NE(svs_index, nullptr);
        svs_index->addVectors(v.data(), ids.data(), n);

        ASSERT_EQ(VecSimIndex_IndexSize(index), n);

        estimation = EstimateBlockSize(params, DEFAULT_BLOCK_SIZE, n);
        actual = index->getAllocationSize() - actual; // get the delta
        ASSERT_GT(actual, 0);
        // LVQ element size estimation accuracy is low
        auto quant_bits_fallback = std::get<0>(svs_details::isSVSQuantBitsSupported(quant_bits));
        double estimation_accuracy = (quant_bits_fallback != VecSimSvsQuant_NONE) ? 0.12 : 0.01;
        ASSERT_GE(estimation * (1.0 + estimation_accuracy), actual);
        ASSERT_LE(estimation * (1.0 - estimation_accuracy), actual);

        float16 query[dim];
        this->GenerateVector(query, dim, 50);
        auto verify_res = [&](size_t id, double score, size_t idx) {
            EXPECT_DOUBLE_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, id, query), score)
                << "idx: " << idx << " id: " << id;
            EXPECT_EQ(id, (idx + 45)) << "idx: " << idx << " id: " << id << " score: " << score;
        };
        runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(FP16SVSTest, test_override_all) {
    const size_t dim = 4;
    const size_t n = 100;
    const size_t k = 11;

    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
    };
    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    std::vector<std::array<float16, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        this->GenerateVector(v[i].data(), dim, i);
    }

    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    svs_index->addVectors(v.data(), ids.data(), n);

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Override all vectors.
    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ(this->GenerateAndAddVector(index, dim, i, i), 0);
    }

    float16 query[dim];
    this->GenerateVector(query, dim, 50);
    auto verify_res = [&](size_t id, double score, size_t index) {
        EXPECT_EQ(id, (index + 45)) << "index: " << index << " id: " << id << " score: " << score;
    };
    runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);
    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSTest, scalar_quantization_query) {
    if (TypeParam::get_quant_bits() == VecSimSvsQuant_8 ||
        TypeParam::get_quant_bits() == VecSimSvsQuant_8x8_LeanVec) {
        GTEST_SKIP() << "Test only VecSimSvsQuant_NONE and VecSimSvsQuant_Scalar.";
    }
    // Limit VecSim log level to avoid printing too much information
    VecSimIndexInterface::setLogCallbackFunction(svsTestLogCallBackNoDebug);

    const size_t dim = 32;
    const size_t bs = 1024;
    const size_t n = 100;
    const size_t k = 10;
    const double quant_precision = 1.0 / (1 << 7); // int8 quantization precision

    std::default_random_engine gen;
    std::uniform_real_distribution<float> dist(-1.0, 1.0);
    std::vector<std::array<float16, dim>> dataset(n);
    for (size_t i = 0; i < n; i++) {
        for (size_t j = 0; j < dim; j++) {
            dataset[i][j] = vecsim_types::FP32_to_FP16(dist(gen));
        }
    }
    std::vector<size_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0);

    float16 query[dim];
    this->GenerateVector(query, dim, 0.1f);

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
        SCOPED_TRACE(VecSimMetric_ToString(metric));
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

        VecSimIndex *index_fp = this->CreateNewIndex(params);
        ASSERT_INDEX(index_fp);

        dynamic_cast<SVSIndexBase *>(index_fp)->addVectors(dataset.data(), ids.data(), n);
        ASSERT_EQ(VecSimIndex_IndexSize(index_fp), n);

        params.quantBits = VecSimSvsQuant_Scalar;
        VecSimIndex *index_sq = this->CreateNewIndex(params);
        ASSERT_INDEX(index_sq);

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

/**
 * * * * * * * * * * * * * * * * * *
 *     Multi Vector index Tests    *
 * * * * * * * * * * * * * * * * * *
 */

template <typename index_type_t>
class FP16SVSMultiTest : public FP16SVSTest<index_type_t> {
public:
    VecSimIndex *CreateNewIndex(SVSParams &params) override {
        VecSimIndexInterface::setLogCallbackFunction(svsTestLogCallBackNoDebug);
        params.multi = true;
        this->SetTypeParams(params);
        VecSimParams index_params = CreateParams(params);
        return VecSimIndex_New(&index_params);
    }
};

TYPED_TEST_SUITE(FP16SVSMultiTest, SVSDataTypeSet);

TYPED_TEST(FP16SVSMultiTest, vector_add_multiple_test) {
    const size_t dim = 4;
    float initial_value = 0.5f;
    float step = 1.0f;
    const size_t n = 5;
    constexpr size_t labels_count = 1;
    const size_t label = 46;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_IP, .multi = true};
    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    std::vector<float16> v(dim * n);
    for (size_t i = 0; i < n; i++) {
        this->GenerateVector(v.data() + i * dim, dim, i * initial_value, step);
    }
    std::vector<size_t> ids(n, label);

    // Adding same vector multiple times under the same label
    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ(VecSimIndex_AddVector(index, v.data() + i * dim, label), 1);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), labels_count);

    // Deleting the label. All the vectors should be deleted.
    ASSERT_EQ(VecSimIndex_DeleteVector(index, label), n);
    index->runGC();
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);
    ASSERT_EQ(index->indexLabelCount(), 0);

    // Adding multiple vectors at once under the same label
    auto svs_index = this->CastToSVS(index);
    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), labels_count);

    if (!(TypeParam::get_quant_bits() == VecSimSvsQuant_8 ||
          TypeParam::get_quant_bits() == VecSimSvsQuant_8x8_LeanVec)) {
        // Get vector is only implemented for non-compressed indices.
        auto vectors_data = this->getStoredVectorDataByLabel(index, label);
        for (size_t i = 0; i < n; i++) {
            const char *data_ptr = vectors_data.at(i).data();
            for (size_t j = 0; j < dim; j++) {
                ASSERT_EQ(vecsim_types::FP16_to_FP32(((float16 *)data_ptr)[j]),
                          i * initial_value + step * float(j));
            }
        }
    }

    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSMultiTest, vector_search_test) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t n = 1000;
    size_t n_labels = 100;
    size_t k = 11;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        this->GenerateAndAddVector(index, dim, i % n_labels, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    TEST_DATA_T query[dim];
    this->GenerateVector(query, dim, 50);

    auto verify_res = [&](size_t id, double score, size_t index) {
        size_t diff_id = (id > 50) ? (id - 50) : (50 - id);
        ASSERT_EQ(diff_id, (index + 1) / 2)
            << "index: " << index << " id: " << id << " score: " << score;
        ASSERT_EQ(score, (4 * ((index + 1) / 2) * ((index + 1) / 2)))
            << "index: " << index << " id: " << id << " score: " << score;
    };
    runTopKSearchTest(index, query, k, verify_res);
    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSMultiTest, search_more_than_there_is) {
    const size_t dim = 4;
    const size_t n = 5;
    const size_t perLabel = 3;
    const size_t n_labels = ceil((float)n / perLabel);
    const size_t k = 3;
    // This test add 5 vectors under 2 labels, and then query for 3 results.
    // We want to make sure we get only 2 results back (because the results should have unique
    // labels), although the index contains 5 vectors.

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    std::vector<std::array<float16, dim>> v(n);
    std::vector<size_t> ids(n);

    for (size_t i = 0; i < n; i++) {
        ids[i] = i / perLabel;
        this->GenerateVector(v[i].data(), dim, i);
    }

    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    float16 query[dim];
    this->GenerateVector(query, dim, 0);

    VecSimQueryReply *res = VecSimIndex_TopKQuery(index, query, k, nullptr, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), n_labels);
    auto it = VecSimQueryReply_GetIterator(res);
    for (size_t i = 0; i < n_labels; i++) {
        auto el = VecSimQueryReply_IteratorNext(it);
        // SVS Scalar quantization is not enough precise
        if (!this->isFallbackToSQ()) {
            ASSERT_EQ(VecSimQueryResult_GetScore(el), i * perLabel * i * perLabel * dim);
        }
        labelType element_label = VecSimQueryResult_GetId(el);
        ASSERT_EQ(element_label, i);
    }
    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(res);
    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSMultiTest, find_better_score) {
    const size_t n = 100;
    const size_t k = 10;
    const size_t n_labels = 10;
    const size_t dim = 4;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    // Building the index. Each label gets 10 vectors with decreasing (by insertion order) element
    // value.
    std::vector<size_t> ids(n);
    std::vector<std::array<float16, dim>> v(n);
    std::map<size_t, double> scores;
    for (size_t i = 0; i < n; i++) {
        // For example, with n_labels == 10 and n == 100,
        // label 0 will get vector elements 18 -> 9 (aka (9 -> 0) + 9),
        // label 1 will get vector elements 17 -> 8 (aka (9 -> 0) + 8),
        // label 2 will get vector elements 16 -> 7 (aka (9 -> 0) + 7),
        // . . . . .
        // label 9 will get vector elements 9 -> 0 (aka (9 -> 0) + 0),
        // and so on, so each label has some common vectors with all the previous labels.
        size_t el = ((n - i - 1) % n_labels) + ((n - i - 1) / n_labels);
        ids[i] = i / n_labels;
        this->GenerateVector(v[i].data(), dim, el);
        // This should be the best score for each label.
        if (i % n_labels == n_labels - 1) {
            // `el * el * dim` is the L2-squared value with the 0 vector.
            scores.emplace(i / n_labels, el * el * dim);
        }
    }
    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, k - index - 1);
        // SVS scalar quantization is not enough precise
        if (!this->isFallbackToSQ()) {
            ASSERT_FLOAT_EQ(score, scores[id]);
        }
    };

    float16 query[dim];
    this->GenerateVector(query, dim, 0.0);
    runTopKSearchTest(index, query, k, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSMultiTest, find_better_score_after_pop) {
    const size_t n = 12;
    const size_t n_labels = 3;
    const size_t dim = 4;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    // Building the index. Each is better than the previous one.
    std::vector<size_t> ids(n);
    std::vector<std::array<float16, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        size_t el = n - i;
        ids[i] = i % n_labels;
        this->GenerateVector(v[i].data(), dim, el);
    }
    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    float16 query[dim];
    this->GenerateVector(query, dim, 0);
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, n_labels - index - 1);
    };

    runTopKSearchTest(index, query, n_labels, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSMultiTest, reindexing_same_vector_different_id) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t n = 100;
    size_t k = 10;
    constexpr size_t dim = 4;
    size_t perLabel = 3;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        // i / 10 is in integer (take the "floor" value)
        this->GenerateAndAddVector(index, dim, i, i / 10);
    }
    // Add more vectors under the same labels. their scores should be worst.
    for (size_t i = 0; i < n; i++) {
        for (size_t j = 0; j < perLabel - 1; j++) {
            this->GenerateAndAddVector(index, dim, i, float(i / 10) + n);
        }
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n * perLabel);

    // Run a query where all the results are supposed to be {5,5,5,5} (different ids).
    float16 query[dim];
    this->GenerateVector(query, dim, 4.9, 0.05);
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_TRUE(id >= 50 && id < 60 && score <= 1);
    };
    runTopKSearchTest(index, query, k, verify_res);

    for (size_t i = 0; i < n; i++) {
        VecSimIndex_DeleteVector(index, i);
    }
    index->runGC();
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Reinsert the same vectors under different ids than before.
    for (size_t i = 0; i < n; i++) {
        this->GenerateAndAddVector(index, dim, i + 10, i / 10);
    }
    // Add more vectors under the same labels. their scores should be worst.
    for (size_t i = 0; i < n; i++) {
        for (size_t j = 0; j < perLabel - 1; j++) {
            this->GenerateAndAddVector(index, dim, i + 10, float(i / 10) + n);
        }
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n * perLabel);

    // Run the same query again.
    auto verify_res_different_id = [&](int id, double score, size_t index) {
        ASSERT_TRUE(id >= 60 && id < 70 && score <= 1);
    };
    runTopKSearchTest(index, query, k, verify_res_different_id);

    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSMultiTest, test_svs_info) {
    size_t d = 128;
    // Build with default args
    SVSParams params = {.dim = d, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, d);
    ASSERT_TRUE(info.commonInfo.basicInfo.isMulti);
    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSMultiTest, test_basic_svs_info_iterator) {
    size_t n = 100;
    size_t d = 128;
    VecSimMetric metrics[3] = {VecSimMetric_Cosine, VecSimMetric_IP, VecSimMetric_L2};

    for (size_t i = 0; i < 3; i++) {
        SCOPED_TRACE("Testing metric: " + std::string(VecSimMetric_ToString(metrics[i])));

        // Build with default args.
        SVSParams params = {.dim = d, .metric = metrics[i]};
        VecSimIndex *index = this->CreateNewIndex(params);

        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        VecSimDebugInfoIterator *infoIter = VecSimIndex_DebugInfoIterator(index);
        compareSVSIndexInfoToIterator(info, infoIter);
        VecSimDebugInfoIterator_Free(infoIter);
        VecSimIndex_Free(index);
    }
}

TYPED_TEST(FP16SVSMultiTest, rangeQuery) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t n_labels = 100;
    size_t n_close = 20;
    size_t per_label = 5;
    size_t dim = 4;

    size_t n = n_labels * per_label;

    SVSParams params{.dim = dim, .metric = VecSimMetric_L2, .blockSize = n / 2};
    VecSimIndex *index = this->CreateNewIndex(params);

    float pivot_value = 1.0f;
    float16 pivot_vec[dim];
    this->GenerateVector(pivot_vec, dim, pivot_value);

    float radius = 1.5f;
    std::mt19937 gen(42);
    std::uniform_real_distribution<> dis(pivot_value - radius, pivot_value + radius);

    for (size_t i = 0; i < n_close; i++) {
        // First vector of each label among n_close labels is close to the pivot vector.
        float random_number = dis(gen);
        this->GenerateAndAddVector(index, dim, i, random_number);
    }

    float16 max_vec[dim];
    this->GenerateVector(max_vec, dim, pivot_value + radius);
    double max_dist = FP16_L2Sqr(pivot_vec, max_vec, dim);

    for (size_t i = 0; i < n_labels; i++) {
        size_t max_vec_for_label = per_label;
        if (i < n_close) {
            max_vec_for_label -= 1;
        }
        // Add more vectors, some under the same labels, worse than the previous vector (for the
        // given query)
        for (size_t j = 0; j < max_vec_for_label; j++) {
            float random_number = dis(gen);
            this->GenerateAndAddVector(index, dim, i, 5.0 + random_number);
        }
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    auto verify_res_by_score = [&](size_t id, double score, size_t index) {
        ASSERT_LE(id, n_close - 1) << "score: " << score;
        ASSERT_LE(score, max_dist);
    };
    uint expected_num_results = n_close;

    runRangeQueryTest(index, pivot_vec, max_dist, verify_res_by_score, expected_num_results,
                      BY_SCORE);

    // Rerun with a given query params.
    SVSRuntimeParams svsRuntimeParams = {.epsilon = 100.0};
    auto query_params = CreateQueryParams(svsRuntimeParams);
    query_params.batchSize = 100;
    runRangeQueryTest(index, pivot_vec, max_dist, verify_res_by_score, expected_num_results,
                      BY_SCORE, &query_params);

    // Get results by id.
    auto verify_res_by_id = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, index);
        ASSERT_LE(score, max_dist);
    };
    runRangeQueryTest(index, pivot_vec, max_dist, verify_res_by_id, expected_num_results);

    VecSimIndex_Free(index);
}

TYPED_TEST(FP16SVSMultiTest, svs_batch_iterator_basic) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t n_labels = 1000;
    size_t perLabel = 5;

    size_t n = n_labels * perLabel;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    // For every i, add the vector (i,i,i,i) under the label i.
    for (size_t i = 0; i < n; i++) {
        this->GenerateAndAddVector(index, dim, i / perLabel, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
    float16 query[dim];
    this->GenerateVector(query, dim, n);

    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    size_t iteration_num = 0;

    // Get the 5 vectors whose ids are the maximal among those that hasn't been returned yet
    // in every iteration. The results order should be sorted by their score (distance from the
    // query vector), which means sorted from the largest id to the lowest.
    size_t n_res = 5;
    while (VecSimBatchIterator_HasNext(batchIterator)) {
        std::vector<size_t> expected_ids(n_res);
        for (size_t i = 0; i < n_res; i++) {
            expected_ids[i] = (n_labels - iteration_num * n_res - i - 1);
        }
        auto verify_res = [&](size_t id, double score, size_t index) {
            ASSERT_EQ(expected_ids[index], id);
        };
        runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
        iteration_num++;
    }
    ASSERT_EQ(iteration_num, n_labels / n_res);
    VecSimBatchIterator_Free(batchIterator);

    VecSimIndex_Free(index);
}

/**
 * * * * * * * * * * * * * * * * * *
 *     SVS TIERED index Tests      *
 * * * * * * * * * * * * * * * * * *
 */

template <typename index_type_t>
class FP16SVSTieredIndexTest : public FP16SVSTest<index_type_t> {
protected:
    using data_t = typename index_type_t::data_t;

    TieredIndexParams
    CreateTieredSVSParams(VecSimParams &svs_params, tieredIndexMock &mock_thread_pool,
                          size_t training_threshold = SVS_VAMANA_DEFAULT_TRAINING_THRESHOLD,
                          size_t update_threshold = SVS_VAMANA_DEFAULT_UPDATE_THRESHOLD) {
        // trainingThreshold = training_threshold;
        // updateThreshold = update_threshold;
        return TieredIndexParams{
            .jobQueue = &mock_thread_pool.jobQ,
            .jobQueueCtx = mock_thread_pool.ctx,
            .submitCb = tieredIndexMock::submit_callback,
            .primaryIndexParams = &svs_params,
            .specificParams = {.tieredSVSParams =
                                   TieredSVSParams{.trainingTriggerThreshold = training_threshold,
                                                   .updateTriggerThreshold = update_threshold}}};
    }
    void verifyNumThreads(TieredSVSIndex<data_t> *tiered_index, size_t expected_capcity) {
        ASSERT_EQ(tiered_index->GetSVSIndex()->getParallelism(), 1);
        ASSERT_EQ(tiered_index->GetSVSIndex()->getPoolSize(), expected_capcity);
    }

    TieredSVSIndex<data_t> *CreateTieredSVSIndex(const TieredIndexParams &tiered_params,
                                                 tieredIndexMock &mock_thread_pool) {
        // Resize the shared SVS thread pool singleton to match the mock thread pool size,
        // simulating what RediSearch does via VecSim_UpdateThreadPoolSize during module init.
        VecSim_UpdateThreadPoolSize(mock_thread_pool.thread_pool_size);

        auto *tiered_index =
            reinterpret_cast<TieredSVSIndex<data_t> *>(TieredFactory::NewIndex(&tiered_params));

        // Set the created tiered index in the index external context (it will take ownership over
        // the index, and we'll need to release the ctx at the end of the test.
        mock_thread_pool.ctx->index_strong_ref.reset(tiered_index);

        verifyNumThreads(tiered_index, mock_thread_pool.thread_pool_size);
        return tiered_index;
    }

    TieredSVSIndex<data_t> *
    CreateTieredSVSIndex(VecSimParams &svs_params, tieredIndexMock &mock_thread_pool,
                         size_t training_threshold = SVS_VAMANA_DEFAULT_TRAINING_THRESHOLD,
                         size_t update_threshold = SVS_VAMANA_DEFAULT_UPDATE_THRESHOLD) {
        svs_params.algoParams.svsParams.quantBits = index_type_t::get_quant_bits();
        TieredIndexParams tiered_params = CreateTieredSVSParams(
            svs_params, mock_thread_pool, training_threshold, update_threshold);
        return CreateTieredSVSIndex(tiered_params, mock_thread_pool);
    }

    void SetUp() override {
        // Restore the write mode to default.
        VecSim_SetWriteMode(VecSim_WriteAsync);
        // Limit VecSim log level to avoid printing too much information
        VecSimIndexInterface::setLogCallbackFunction(svsTestLogCallBackNoDebug);
    }

    void verify_index(VecSimIndex *index) {
        if (index == nullptr) {
            if (std::get<1>(svs_details::isSVSQuantBitsSupported(index_type_t::get_quant_bits()))) {
                GTEST_FAIL() << "Failed to create SVS index";
            } else {
                GTEST_SKIP() << "SVS LVQ is not supported.";
            }
        }
    }

    void test_basic(bool is_multi, size_t per_label, VecSimMetric metric) {
        constexpr size_t dim = 4;

        // Create TieredSVS index instance with a mock queue.
        SVSParams params = {.dim = dim, .metric = metric, .multi = is_multi};
        this->SetTypeParams(params);
        VecSimParams svs_params = CreateParams(params);
        auto mock_thread_pool = tieredIndexMock();
        auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
        ASSERT_NO_FATAL_FAILURE(verify_index(tiered_index));

        // Get the allocator from the tiered index.
        auto allocator = tiered_index->getAllocator();

        // Add a vector to the flat index.
        float16 vector[dim];
        this->GenerateVector(vector, dim, 0.5f, 1.0f);
        labelType vector_label = 1;
        for (size_t i = 0; i < per_label; i++) {
            VecSimIndex_AddVector(tiered_index, vector, vector_label);
        }
        ASSERT_EQ(tiered_index->indexSize(), per_label);
        ASSERT_EQ(tiered_index->indexLabelCount(), 1);
        ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), per_label);
        ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0);

        bool is_compressed = index_type_t::get_quant_bits() == VecSimSvsQuant_8 ||
                             index_type_t::get_quant_bits() == VecSimSvsQuant_8x8_LeanVec;
        auto vec_to_compare = vector;
        if (metric == VecSimMetric_Cosine) {
            VecSim_Normalize(vec_to_compare, dim, VecSimType_FLOAT16);
        }
        if (!is_compressed) {
            for (size_t i = 0; i < per_label; i++) {
                auto vec_stored_data =
                    tiered_index->GetFlatIndex()->getStoredVectorDataByLabel(vector_label);
                ASSERT_NO_FATAL_FAILURE(
                    CompareVectors(reinterpret_cast<const float16 *>(vec_stored_data[i].data()),
                                   vec_to_compare, dim));
            }
        }
        // Submit the index update job.
        tiered_index->scheduleSVSIndexInit();
        ASSERT_EQ(mock_thread_pool.jobQ.size(), mock_thread_pool.thread_pool_size);

        // Execute the job from the queue and validate that the index was updated properly.
        mock_thread_pool.thread_iteration();
        ASSERT_EQ(tiered_index->indexSize(), per_label);
        ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(vector_label, vec_to_compare), 0, 1e-3f);
        ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
        ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), per_label);
        ASSERT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), 1);

        if (!is_compressed) {
            for (size_t i = 0; i < per_label; i++) {
                auto vec_stored_data =
                    tiered_index->GetBackendIndex()->getStoredVectorDataByLabel(vector_label);
                ASSERT_NO_FATAL_FAILURE(
                    CompareVectors(reinterpret_cast<const float16 *>(vec_stored_data[i].data()),
                                   vec_to_compare, dim));
            }
        }
    }

    void range_test(bool is_multi) {
        // Scalar quantization accuracy is insufficient for this test.
        if (this->isFallbackToSQ()) {
            GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
        }
        size_t dim = 4;
        size_t k = 11;
        size_t per_label = is_multi ? 5 : 1;

        size_t n_labels = k * 3;
        size_t n = n_labels * per_label;

        auto edge_delta = (k - 0.8) * per_label;
        auto mid_delta = edge_delta / 2;
        // `range` for querying the "edges" of the index and get k results.
        double range = dim * edge_delta * edge_delta; // L2 distance.
        // `half_range` for querying a point in the "middle" of the index and get k results around
        // it.
        double half_range = dim * mid_delta * mid_delta; // L2 distance.

        // Create TieredSVS index instance with a mock queue.
        SVSParams params = {
            .dim = dim,
            .metric = VecSimMetric_L2,
            .multi = is_multi,
            .epsilon = 3.0 * per_label,
        };

        this->SetTypeParams(params);
        VecSimParams svs_params = CreateParams(params);
        auto mock_thread_pool = tieredIndexMock();
        auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
        ASSERT_NO_FATAL_FAILURE(this->verify_index(tiered_index));

        auto svs_index = tiered_index->GetBackendIndex();
        auto flat_index = tiered_index->GetFlatIndex();

        float16 query_0[dim];
        this->GenerateVector(query_0, dim, 0);
        float16 query_1mid[dim];
        this->GenerateVector(query_1mid, dim, n / 3);
        float16 query_2mid[dim];
        this->GenerateVector(query_2mid, dim, n * 2 / 3);
        float16 query_n[dim];
        this->GenerateVector(query_n, dim, n - 1);

        // Search for vectors when the index is empty.
        runRangeQueryTest(tiered_index, query_0, range, nullptr, 0);

        // Define the verification functions.
        auto ver_res_0 = [&](size_t id, double score, size_t index) {
            EXPECT_EQ(id, index);
            // The expected score is the distance to the first vector of `id` label.
            auto element = id * per_label;
            EXPECT_DOUBLE_EQ(score, dim * element * element);
        };

        auto ver_res_1mid_by_id = [&](size_t id, double score, size_t index) {
            size_t q_id = vecsim_types::FP16_to_FP32(query_1mid[0]) / per_label;
            size_t mod = vecsim_types::FP16_to_FP32(query_1mid[0]) - q_id * per_label;
            // In single value mode, `per_label` is always 1 and `mod` is always 0, so the following
            // branchings is simply `expected_score = abs(id - q_id)`.
            // In multi value mode, for ids higher than the query id, the score is the distance to
            // the first vector of `id` label, and for ids lower than the query id, the score is the
            // distance to the last vector of `id` label. `mod` is the distance to the first vector
            // of `q_id` label.
            double expected_score = 0;
            if (id > q_id) {
                expected_score = (id - q_id) * per_label - mod;
            } else if (id < q_id) {
                expected_score = (q_id - id) * per_label - (per_label - mod - 1);
            }
            expected_score = expected_score * expected_score * dim;
            EXPECT_DOUBLE_EQ(score, expected_score);
        };

        auto ver_res_2mid_by_id = [&](size_t id, double score, size_t index) {
            size_t q_id = vecsim_types::FP16_to_FP32(query_2mid[0]) / per_label;
            size_t mod = vecsim_types::FP16_to_FP32(query_2mid[0]) - q_id * per_label;
            // In single value mode, `per_label` is always 1 and `mod` is always 0, so the following
            // branchings is simply `expected_score = abs(id - q_id)`.
            // In multi value mode, for ids higher than the query id, the score is the distance to
            // the first vector of `id` label, and for ids lower than the query id, the score is the
            // distance to the last vector of `id` label. `mod` is the distance to the first vector
            // of `q_id` label.
            double expected_score = 0;
            if (id > q_id) {
                expected_score = (id - q_id) * per_label - mod;
            } else if (id < q_id) {
                expected_score = (q_id - id) * per_label - (per_label - mod - 1);
            }
            expected_score = expected_score * expected_score * dim;
            EXPECT_DOUBLE_EQ(score, expected_score);
        };

        auto ver_res_1mid_by_score = [&](size_t id, double score, size_t index) {
            size_t q_id = vecsim_types::FP16_to_FP32(query_1mid[0]) / per_label;
            EXPECT_EQ(std::abs(int(id - q_id)), (index + 1) / 2);
            ver_res_1mid_by_id(id, score, index);
        };

        auto ver_res_2mid_by_score = [&](size_t id, double score, size_t index) {
            size_t q_id = vecsim_types::FP16_to_FP32(query_2mid[0]) / per_label;
            EXPECT_EQ(std::abs(int(id - q_id)), (index + 1) / 2);
            ver_res_2mid_by_id(id, score, index);
        };

        auto ver_res_n = [&](size_t id, double score, size_t index) {
            EXPECT_EQ(id, n_labels - 1 - index);
            auto element = index * per_label;
            EXPECT_DOUBLE_EQ(score, dim * element * element);
        };

        // Insert n/2 vectors to the main index.
        for (size_t i = 0; i < (n + 1) / 2; i++) {
            this->GenerateAndAddVector(svs_index, dim, i / per_label, i);
        }
        ASSERT_EQ(tiered_index->indexSize(), (n + 1) / 2);
        ASSERT_EQ(tiered_index->indexSize(), svs_index->indexSize());

        auto allocator = tiered_index->getAllocator();

        // Search for `range` with the flat index empty.
        size_t cur_memory_usage = allocator->getAllocationSize();
        runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_ID);
        runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_SCORE);
        runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_score, k, BY_SCORE);
        runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_id, k, BY_ID);
        ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

        // Insert n/2 vectors to the flat index.
        for (size_t i = (n + 1) / 2; i < n; i++) {
            this->GenerateAndAddVector(flat_index, dim, i / per_label, i);
        }
        ASSERT_EQ(tiered_index->indexSize(), n);
        ASSERT_EQ(tiered_index->indexSize(), svs_index->indexSize() + flat_index->indexSize());

        cur_memory_usage = allocator->getAllocationSize();
        // Search for `range` so all the vectors will be from the SVS index.
        runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_ID);
        runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_SCORE);
        // Search for `range` so all the vectors will be from the flat index.
        runRangeQueryTest(tiered_index, query_n, range, ver_res_n, k, BY_SCORE);
        // Search for `range` so some of the results will be from the main and some from the flat
        // index.
        runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_score, k, BY_SCORE);
        runRangeQueryTest(tiered_index, query_2mid, half_range, ver_res_2mid_by_score, k, BY_SCORE);
        runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_id, k, BY_ID);
        runRangeQueryTest(tiered_index, query_2mid, half_range, ver_res_2mid_by_id, k, BY_ID);
        // Memory usage should not change.
        ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

        // Add some overlapping vectors to the main and flat index.
        // adding directly to the underlying indexes to avoid jobs logic.
        // The main index will have vectors 0 - 2n/3 and the flat index will have vectors n/3 - n
        for (size_t i = n / 3; i < n / 2; i++) {
            this->GenerateAndAddVector(flat_index, dim, i / per_label, i);
        }
        for (size_t i = n / 2; i < n * 2 / 3; i++) {
            this->GenerateAndAddVector(svs_index, dim, i / per_label, i);
        }

        cur_memory_usage = allocator->getAllocationSize();
        // Search for `range` so all the vectors will be from the main index.
        runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_ID);
        runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_SCORE);
        // Search for `range` so all the vectors will be from the flat index.
        runRangeQueryTest(tiered_index, query_n, range, ver_res_n, k, BY_SCORE);
        // Search for `range` so some of the results will be from the main and some from the flat
        // index.
        runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_score, k, BY_SCORE);
        runRangeQueryTest(tiered_index, query_2mid, half_range, ver_res_2mid_by_score, k, BY_SCORE);
        runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_id, k, BY_ID);
        runRangeQueryTest(tiered_index, query_2mid, half_range, ver_res_2mid_by_id, k, BY_ID);
        // Memory usage should not change.
        ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);
    }

    static constexpr std::array<std::pair<std::string_view, bool (*)(size_t, size_t)>, 7> lambdas =
        {{
            {"100% SVS,   0% FLAT ", [](size_t idx, size_t n) -> bool { return 1; }},
            {" 50% SVS,  50% FLAT ", [](size_t idx, size_t n) -> bool { return idx % 2; }},
            {"  0% SVS, 100% FLAT ", [](size_t idx, size_t n) -> bool { return 0; }},
            {" 10% SVS,  90% FLAT ", [](size_t idx, size_t n) -> bool { return !(idx % 10); }},
            {"  1% SVS,  99% FLAT ", [](size_t idx, size_t n) -> bool { return !(idx % 100); }},
            {"first 10% are in SVS", [](size_t idx, size_t n) -> bool { return idx < (n / 10); }},
            {" last 10% are in FLAT",
             [](size_t idx, size_t n) -> bool { return idx < (9 * n / 10); }},
        }};

    void test_batch_iterator(bool is_multi) {
        // Scalar quantization accuracy is insufficient for this test.
        if (this->isFallbackToSQ()) {
            GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
        }
        constexpr size_t d = 4;
        constexpr size_t n = 1000;
        // Scale factor to avoid FP16 overflow. FP16 max is ~65504, and L2 distance is
        // d * (query - vec)^2. With scale=0.1, max distance is 4 * (100)^2 = 40000 < 65504.
        constexpr float scale = 0.1f;

        size_t per_label = is_multi ? 10 : 1;
        size_t n_labels = n / per_label;

        // Create TieredSVS index instance with a mock queue.
        SVSParams params = {
            .dim = d,
            .metric = VecSimMetric_L2,
            .multi = is_multi,
        };
        this->SetTypeParams(params);
        VecSimParams svs_params = CreateParams(params);

        for (auto &lambda : lambdas) {
            auto &decider_name = lambda.first;
            auto &decider = lambda.second;

            auto mock_thread_pool = tieredIndexMock();

            auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
            this->verify_index(tiered_index);

            auto *svs = tiered_index->GetBackendIndex();
            auto *flat = tiered_index->GetFlatIndex();

            // For every i, add the vector (i*scale, i*scale, ...) under the label i.
            // We scale down to avoid FP16 overflow in distance calculations.
            for (size_t i = 0; i < n; i++) {
                auto cur = decider(i, n) ? svs : flat;
                this->GenerateAndAddVector(cur, d, i % n_labels, i * scale);
            }
            ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n) << decider_name;

            // Query for (n*scale, n*scale, ...) vector.
            float16 query[d];
            this->GenerateVector(query, d, n * scale);

            VecSimBatchIterator *batchIterator =
                VecSimBatchIterator_New(tiered_index, query, nullptr);
            size_t iteration_num = 0;

            // Get the 5 vectors whose ids are the maximal among those that hasn't been returned yet
            // in every iteration. The results order should be sorted by their score (distance from
            // the query vector), which means sorted from the largest id to the lowest.
            size_t n_res = 5;
            while (VecSimBatchIterator_HasNext(batchIterator)) {
                std::vector<size_t> expected_ids(n_res);
                for (size_t i = 0; i < n_res; i++) {
                    expected_ids[i] = (n - iteration_num * n_res - i - 1) % n_labels;
                }
                auto verify_res = [&](size_t id, double score, size_t index) {
                    ASSERT_EQ(expected_ids[index], id) << decider_name;
                };
                runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
                iteration_num++;
            }
            ASSERT_EQ(iteration_num, n_labels / n_res) << decider_name;
            VecSimBatchIterator_Free(batchIterator);
        }
    }
};

TYPED_TEST_SUITE(FP16SVSTieredIndexTest, SVSDataTypeSet);

TYPED_TEST(FP16SVSTieredIndexTest, CreateIndexInstanceSingle) {
    for (VecSimMetric metric : {VecSimMetric_L2, VecSimMetric_Cosine}) {
        SCOPED_TRACE("Testing metric: " + std::string(VecSimMetric_ToString(metric)));
        this->test_basic(false, 1, metric);
    }
}

TYPED_TEST(FP16SVSTieredIndexTest, CreateIndexInstanceMulti) {
    for (VecSimMetric metric : {VecSimMetric_L2, VecSimMetric_Cosine}) {
        SCOPED_TRACE("Testing metric: " + std::string(VecSimMetric_ToString(metric)));
        this->test_basic(true, 5, metric);
    }
}
TYPED_TEST(FP16SVSTieredIndexTest, RangeTestSingle) { this->range_test(false); }
TYPED_TEST(FP16SVSTieredIndexTest, RangeTestMulti) { this->range_test(true); }

TYPED_TEST(FP16SVSTieredIndexTest, KNNSearch) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }

    size_t dim = 4;
    size_t k = 10;
    size_t n = k * 3;

    // Create TieredSVS index instance with a mock queue.
    SVSParams params = {
        .dim = dim,
        .metric = VecSimMetric_L2,
    };
    this->SetTypeParams(params);
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
    ASSERT_NO_FATAL_FAILURE(this->verify_index(tiered_index));

    auto allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    auto svs_index = tiered_index->GetBackendIndex();
    auto flat_index = tiered_index->GetFlatIndex();

    float16 query_0[dim];
    this->GenerateVector(query_0, dim, 0);
    float16 query_1mid[dim];
    this->GenerateVector(query_1mid, dim, n / 3);
    float16 query_2mid[dim];
    this->GenerateVector(query_2mid, dim, n * 2 / 3);
    float16 query_n[dim];
    this->GenerateVector(query_n, dim, n - 1);

    // Search for vectors when the index is empty.
    runTopKSearchTest(tiered_index, query_0, k, nullptr);

    // Define the verification functions.
    auto ver_res_0 = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, index);
        ASSERT_DOUBLE_EQ(score, dim * id * id);
    };

    auto ver_res_1mid = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(std::abs(int(id - vecsim_types::FP16_to_FP32(query_1mid[0]))), (index + 1) / 2);
        ASSERT_DOUBLE_EQ(score, dim * pow((index + 1) / 2, 2));
    };

    auto ver_res_2mid = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(std::abs(int(id - vecsim_types::FP16_to_FP32(query_2mid[0]))), (index + 1) / 2);
        ASSERT_DOUBLE_EQ(score, dim * pow((index + 1) / 2, 2));
    };

    auto ver_res_n = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, n - 1 - index);
        ASSERT_DOUBLE_EQ(score, dim * index * index);
    };

    // Insert n/2 vectors to the main index.
    for (size_t i = 0; i < n / 2; i++) {
        this->GenerateAndAddVector(svs_index, dim, i, i);
    }
    ASSERT_EQ(tiered_index->indexSize(), n / 2);
    ASSERT_EQ(tiered_index->indexSize(), svs_index->indexSize());

    // Search for k vectors with the flat index empty.
    size_t cur_memory_usage = allocator->getAllocationSize();
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Insert n/2 vectors to the flat index.
    for (size_t i = n / 2; i < n; i++) {
        this->GenerateAndAddVector(flat_index, dim, i, i);
    }
    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->indexSize(), svs_index->indexSize() + flat_index->indexSize());

    cur_memory_usage = allocator->getAllocationSize();
    // Search for k vectors so all the vectors will be from the flat index.
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    // Search for k vectors so all the vectors will be from the main index.
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);
    // Search for k so some of the results will be from the main and some from the flat index.
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    runTopKSearchTest(tiered_index, query_2mid, k, ver_res_2mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Add some overlapping vectors to the main and flat index.
    // adding directly to the underlying indexes to avoid jobs logic.
    // The main index will have vectors 0 - 2n/3 and the flat index will have vectors n/3 - n
    for (size_t i = n / 3; i < n / 2; i++) {
        this->GenerateAndAddVector(flat_index, dim, i, i);
    }
    for (size_t i = n / 2; i < n * 2 / 3; i++) {
        this->GenerateAndAddVector(svs_index, dim, i, i);
    }

    cur_memory_usage = allocator->getAllocationSize();
    // Search for k vectors so all the vectors will be from the main index.
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    // Search for k vectors so all the vectors will be from the flat index.
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);
    // Search for k so some of the results will be from the main and some from the flat index.
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    runTopKSearchTest(tiered_index, query_2mid, k, ver_res_2mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // More edge cases:

    // Search for more vectors than the index size.
    k = n + 1;
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);

    // Search for less vectors than the index size, but more than the flat and main index sizes.
    k = n * 5 / 6;
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);

    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Search for more vectors than the main index size, but less than the flat index size.
    for (size_t i = n / 2; i < n * 2 / 3; i++) {
        VecSimIndex_DeleteVector(svs_index, i);
    }
    ASSERT_EQ(flat_index->indexSize(), n * 2 / 3);
    ASSERT_EQ(svs_index->indexLabelCount(), n / 2);
    k = n * 2 / 3;
    cur_memory_usage = allocator->getAllocationSize();
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    runTopKSearchTest(tiered_index, query_2mid, k, ver_res_2mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Search for more vectors than the flat index size, but less than the main index size.
    for (size_t i = n / 2; i < n; i++) {
        VecSimIndex_DeleteVector(flat_index, i);
    }
    ASSERT_EQ(flat_index->indexSize(), n / 6);
    ASSERT_EQ(svs_index->indexLabelCount(), n / 2);
    k = n / 4;
    cur_memory_usage = allocator->getAllocationSize();
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Search for vectors when the flat index is not empty but the main index is empty.
    for (size_t i = 0; i < n * 2 / 3; i++) {
        VecSimIndex_DeleteVector(svs_index, i);
        this->GenerateAndAddVector(flat_index, dim, i, i);
    }
    ASSERT_EQ(flat_index->indexSize(), n * 2 / 3);
    ASSERT_EQ(svs_index->indexLabelCount(), 0);
    k = n / 3;
    cur_memory_usage = allocator->getAllocationSize();
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);
}

TYPED_TEST(FP16SVSTieredIndexTest, deleteVector) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};
    this->SetTypeParams(params);
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(1);
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_NO_FATAL_FAILURE(this->verify_index(tiered_index));

    auto allocator = tiered_index->getAllocator();

    labelType vec_label = 0;
    // Delete from an empty index.
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 0);

    // Create a vector and add it to the tiered index (expect it to go into the flat buffer).
    float16 vector[dim];
    this->GenerateVector(vector, dim, vec_label);
    VecSimIndex_AddVector(tiered_index, vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);

    // Remove vector from flat buffer.
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 1);
    ASSERT_EQ(tiered_index->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);

    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0);

    // Create a vector and add it to SVS in the tiered index.
    VecSimIndex_AddVector(tiered_index->GetBackendIndex(), vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);

    // Remove from main index.
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), 0);
    VecSimTieredIndex_GC(tiered_index);
    while (mock_thread_pool.jobQ.size() > 0)
        mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->indexSize(), 0);

    // Re-insert a deleted label with a different vector.
    float new_vec_val = 2.0;
    this->GenerateVector(vector, dim, new_vec_val);
    VecSimIndex_AddVector(tiered_index, vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);

    // Move the vector to SVS by executing the pending jobs.
    while (mock_thread_pool.jobQ.size() > 0)
        mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);
    // Scalar quantization accuracy is insufficient for this check.
    if (!this->isFallbackToSQ()) {
        // Check that the distance from the deleted vector (of zeros) to the label is the distance
        // to the new vector (L2 distance).
        float16 deleted_vector[dim];
        this->GenerateVector(deleted_vector, dim, 0);
        ASSERT_EQ(
            tiered_index->GetBackendIndex()->getDistanceFrom_Unsafe(vec_label, deleted_vector),
            dim * pow(new_vec_val, 2));
    }
}

TYPED_TEST(FP16SVSTieredIndexTest, deleteVectorMulti) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2, .multi = true};
    this->SetTypeParams(params);
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(1);
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_NO_FATAL_FAILURE(this->verify_index(tiered_index));

    // Test some more scenarios that are relevant only for multi value index.
    labelType vec_label = 0;
    float other_vec_val = 2.0;
    idType invalidJobsCounter = 0;
    // Create a vector and add it to SVS in the tiered index.
    float16 vector[dim];
    this->GenerateVector(vector, dim, vec_label);
    VecSimIndex_AddVector(tiered_index->GetBackendIndex(), vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);

    // Test deleting a label for which one of its vector's is in the flat index while the
    // second one is in SVS.
    this->GenerateAndAddVector(tiered_index, dim, vec_label, other_vec_val);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->indexSize(), 2);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);

    ASSERT_EQ(tiered_index->deleteVector(vec_label), 2);

    VecSimTieredIndex_GC(tiered_index);
    while (mock_thread_pool.jobQ.size() > 0)
        mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 0);

    while (mock_thread_pool.jobQ.size() > 0)
        mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Test deleting a label for which both of its vector's is in the flat index.
    this->GenerateAndAddVector(tiered_index, dim, vec_label, vec_label);
    this->GenerateAndAddVector(tiered_index, dim, vec_label, other_vec_val);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 2);
    ASSERT_EQ(tiered_index->indexSize(), 2);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0);

    ASSERT_EQ(tiered_index->deleteVector(vec_label), 2);
    ASSERT_EQ(tiered_index->indexLabelCount(), 0);

    ASSERT_EQ(mock_thread_pool.jobQ.size(), 2);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Test deleting a label for which both of its vector's is in SVS index.
    this->GenerateAndAddVector(tiered_index, dim, vec_label, vec_label);
    this->GenerateAndAddVector(tiered_index, dim, vec_label, other_vec_val);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 2);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 2);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 2);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), 0);
}

TYPED_TEST(FP16SVSTieredIndexTest, BatchIteratorSingle) { this->test_batch_iterator(false); }

TYPED_TEST(FP16SVSTieredIndexTest, BatchIteratorMulti) { this->test_batch_iterator(true); }

#else // HAVE_SVS

TEST(FP16SVSTest, svs_not_supported) {
    SVSParams params = {
        .type = VecSimType_FLOAT16,
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
