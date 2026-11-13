#include "gtest/gtest.h"
#include "VecSim/vec_sim.h"
#include "unit_test_utils.h"
#include <array>
#include <cmath>
#include <random>
#include <vector>

#if HAVE_SVS
#include <sstream>
#include "VecSim/algorithms/svs/svs.h"

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
class SVSMultiTest : public ::testing::Test {
public:
    using data_t = typename index_type_t::data_t;

protected:
    void SetTypeParams(SVSParams &params) {
        params.quantBits = index_type_t::get_quant_bits();
        params.type = index_type_t::get_index_type();
        params.multi = true;
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

TYPED_TEST_SUITE(SVSMultiTest, SVSDataTypeSet);

TYPED_TEST(SVSMultiTest, vector_add_multiple_test) {
    const size_t dim = 4;
    const size_t rep = 5;
    const size_t id = 46;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_IP};
    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    std::vector<TEST_DATA_T> v(dim * rep);
    for (size_t i = 0; i < rep; i++) {
        for (size_t j = 0; j < dim; j++) {
            v[i * dim + j] = (TEST_DATA_T)j * i + j;
        }
    }
    std::vector<size_t> ids(rep, id);

    // Adding same vector multiple times under the same label
    for (size_t i = 0; i < rep; i++) {
        ASSERT_EQ(VecSimIndex_AddVector(index, v.data() + i * dim, id), 1);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), rep);
    ASSERT_EQ(index->indexLabelCount(), 1);

    // Deleting the label. All the vectors should be deleted.
    ASSERT_EQ(VecSimIndex_DeleteVector(index, id), rep);
    index->runGC();
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);
    ASSERT_EQ(index->indexLabelCount(), 0);

    // Adding multiple vectors at once under the same label
    auto svs_index = this->CastToSVS(index);
    svs_index->addVectors(v.data(), ids.data(), rep);
    ASSERT_EQ(VecSimIndex_IndexSize(index), rep);
    ASSERT_EQ(index->indexLabelCount(), 1);

    VecSimIndex_Free(index);
}

// Test empty index edge cases.
TYPED_TEST(SVSMultiTest, empty_index) {
    size_t dim = 4;
    size_t n = 20;
    size_t bs = 6;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Try to remove from an empty index - should fail because label doesn't exist.
    ASSERT_EQ(VecSimIndex_DeleteVector(index, 1), 0);

    // Add one vector multiple times.
    for (size_t i = 0; i < 3; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 1.7);
    }

    // Try to remove it.
    ASSERT_EQ(VecSimIndex_DeleteVector(index, 1), 3);

    // Size equals 0.
    index->runGC();
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Try to remove it again.
    ASSERT_EQ(VecSimIndex_DeleteVector(index, 1), 0);

    // Size should be still zero.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, vector_search_test) {
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
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i % n_labels, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 50);

    auto verify_res = [&](size_t id, double score, size_t index) {
        size_t diff_id = (id > 50) ? (id - 50) : (50 - id);
        ASSERT_EQ(diff_id, (index + 1) / 2);
        ASSERT_EQ(score, (4 * ((index + 1) / 2) * ((index + 1) / 2)));
    };
    runTopKSearchTest(index, query, k, verify_res);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, search_more_than_there_is) {
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

    std::vector<std::array<TEST_DATA_T, dim>> v(n);
    std::vector<size_t> ids(n);

    for (size_t i = 0; i < n; i++) {
        ids[i] = i / perLabel;
        GenerateVector<TEST_DATA_T>(v[i].data(), dim, i);
    }

    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0);

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

TYPED_TEST(SVSMultiTest, indexing_same_vector) {
    const size_t n = 100;
    const size_t k = 10;
    const size_t perLabel = 10;
    const size_t dim = 4;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    auto svs_index = this->CastToSVS(index);
    ASSERT_NE(svs_index, nullptr);

    std::vector<std::array<TEST_DATA_T, dim>> v(n);
    std::vector<size_t> ids(n);

    for (size_t i = 0; i < n / perLabel; i++) {
        for (size_t j = 0; j < perLabel; j++) {
            ids[i * perLabel + j] = i;
            GenerateVector<TEST_DATA_T>(v[i * perLabel + j].data(), dim, i);
        }
    }

    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0.0);
    auto verify_res = [&](size_t id, double score, size_t index) { ASSERT_EQ(id, index); };
    runTopKSearchTest(index, query, k, verify_res);
    auto res = VecSimIndex_TopKQuery(index, query, k, nullptr, BY_SCORE);
    auto it = VecSimQueryReply_GetIterator(res);
    for (size_t i = 0; i < k; i++) {
        auto el = VecSimQueryReply_IteratorNext(it);
        labelType element_label = VecSimQueryResult_GetId(el);
        // SVS Scalar quantization is not enough precise
        if (!this->isFallbackToSQ()) {
            ASSERT_EQ(VecSimQueryResult_GetScore(el), i * i * dim);
        }
        ASSERT_EQ(element_label, i);
    }
    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(res);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, find_better_score) {
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
    std::vector<std::array<TEST_DATA_T, dim>> v(n);
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
        GenerateVector<TEST_DATA_T>(v[i].data(), dim, el);
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

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0.0);
    runTopKSearchTest(index, query, k, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, find_better_score_after_pop) {
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
    std::vector<std::array<TEST_DATA_T, dim>> v(n);
    for (size_t i = 0; i < n; i++) {
        size_t el = n - i;
        ids[i] = i % n_labels;
        GenerateVector<TEST_DATA_T>(v[i].data(), dim, el);
    }
    svs_index->addVectors(v.data(), ids.data(), n);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0);
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, n_labels - index - 1);
    };

    runTopKSearchTest(index, query, n_labels, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, reindexing_same_vector_different_id) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t n = 100;
    size_t k = 10;
    size_t dim = 4;
    size_t perLabel = 3;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        // i / 10 is in integer (take the "floor" value)
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i / 10);
    }
    // Add more vectors under the same labels. their scores should be worst.
    for (size_t i = 0; i < n; i++) {
        for (size_t j = 0; j < perLabel - 1; j++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, TEST_DATA_T(i / 10) + n);
        }
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n * perLabel);

    // Run a query where all the results are supposed to be {5,5,5,5} (different ids).
    TEST_DATA_T query[] = {4.9, 4.95, 5.05, 5.1};
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
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i + 10, i / 10);
    }
    // Add more vectors under the same labels. their scores should be worst.
    for (size_t i = 0; i < n; i++) {
        for (size_t j = 0; j < perLabel - 1; j++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i + 10, TEST_DATA_T(i / 10) + n);
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

TYPED_TEST(SVSMultiTest, test_svs_info) {
    // Build with default args.
    size_t n = 100;
    size_t d = 128;
    // Build with default args

    SVSParams params = {.dim = d, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, d);
    ASSERT_TRUE(info.commonInfo.basicInfo.isMulti);
    // Default args.
    ASSERT_EQ(info.commonInfo.basicInfo.blockSize, DEFAULT_BLOCK_SIZE);
    VecSimIndex_Free(index);

    d = 1280;
    size_t bs = 42;
    double epsilon = 0.005;
    params.dim = d;
    params.blockSize = bs;
    params.epsilon = epsilon;

    index = this->CreateNewIndex(params);
    info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, d);
    ASSERT_TRUE(info.commonInfo.basicInfo.isMulti);
    // User args.
    ASSERT_EQ(info.commonInfo.basicInfo.blockSize, bs);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, test_basic_svs_info_iterator) {
    size_t n = 100;
    size_t d = 128;
    VecSimMetric metrics[3] = {VecSimMetric_Cosine, VecSimMetric_IP, VecSimMetric_L2};

    for (size_t i = 0; i < 3; i++) {

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

TYPED_TEST(SVSMultiTest, test_dynamic_svs_info_iterator) {
    size_t n = 100;
    size_t d = 128;

    SVSParams params = {.dim = d, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    VecSimDebugInfoIterator *infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(DEFAULT_BLOCK_SIZE, info.commonInfo.basicInfo.blockSize);
    ASSERT_EQ(0, info.commonInfo.indexSize);
    compareSVSIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    // Add vectors.
    TEST_DATA_T v[d];
    for (size_t i = 0; i < d; i++) {
        v[i] = (TEST_DATA_T)i;
    }
    VecSimIndex_AddVector(index, v, 0);
    VecSimIndex_AddVector(index, v, 0);
    VecSimIndex_AddVector(index, v, 1);
    VecSimIndex_AddVector(index, v, 1);
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(4, info.commonInfo.indexSize);
    ASSERT_EQ(2, info.commonInfo.indexLabelCount);
    compareSVSIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    // Delete vectors.
    VecSimIndex_DeleteVector(index, 0);
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(4, info.commonInfo.indexSize);
    ASSERT_EQ(1, info.commonInfo.indexLabelCount);
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
    ASSERT_TRUE(VecSimIndex_PreferAdHocSearch(index, 1, 10, false));
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(HYBRID_BATCHES_TO_ADHOC_BF, info.commonInfo.lastMode);
    compareSVSIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, search_empty_index) {
    size_t dim = 4;
    size_t n = 100;
    size_t k = 11;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 50);
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
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 46, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    VecSimIndex_DeleteVector(index, 46);
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

TYPED_TEST(SVSMultiTest, svs_get_distance) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t n_labels = 2;
    size_t dim = 2;
    size_t numIndex = 3;
    VecSimIndex *index[numIndex];
    std::vector<double> distances;

    TEST_DATA_T v1_0[] = {M_PI, M_PI};
    TEST_DATA_T v2_0[] = {M_E, M_E};
    TEST_DATA_T v3_1[] = {M_PI, M_E};
    TEST_DATA_T v4_1[] = {M_SQRT2, -M_SQRT2};

    SVSParams params = {.dim = dim};

    for (size_t i = 0; i < numIndex; i++) {
        params.metric = (VecSimMetric)i;
        index[i] = this->CreateNewIndex(params);
        VecSimIndex_AddVector(index[i], v1_0, 0);
        VecSimIndex_AddVector(index[i], v2_0, 0);
        VecSimIndex_AddVector(index[i], v3_1, 1);
        VecSimIndex_AddVector(index[i], v4_1, 1);
        ASSERT_EQ(VecSimIndex_IndexSize(index[i]), 4);
    }

    TEST_DATA_T *query = v1_0;
    TEST_DATA_T *norm = v2_0;                 // {e, e}
    VecSim_Normalize(norm, dim, params.type); // now {1/sqrt(2), 1/sqrt(2)}
    ASSERT_FLOAT_EQ(norm[0], 1.0f / sqrt(2.0));
    ASSERT_FLOAT_EQ(norm[1], 1.0f / sqrt(2.0));
    double dist;

    auto qbits = TypeParam::get_quant_bits();
    double relative_err = qbits ? 1e-2 : 1.e-5;

    // VecSimMetric_L2
    // distances are [[0.000, 0.358], [0.179, 23.739]]
    // minimum of each label are:
    distances = {0, 0.1791922003030777};
    for (size_t i = 0; i < n_labels; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_L2], i, query);
        EXPECT_NEAR(dist, distances[i], std::abs(distances[i] * relative_err));
    }

    // VecSimMetric_IP
    // distances are [[-18.739, -16.079], [-17.409, 1.000]]
    // minimum of each label are:
    distances = {-18.73921012878418, -17.409339904785156};
    for (size_t i = 0; i < n_labels; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_IP], i, query);
        EXPECT_NEAR(dist, distances[i], std::abs(distances[i] * relative_err));
    }

    // VecSimMetric_Cosine
    // distances are [[5.960e-08, 5.960e-08], [0.0026, 1.000]]
    // minimum of each label are:
    distances = {5.9604644775390625e-08, 0.0025991201400756836};
    for (size_t i = 0; i < n_labels; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_Cosine], i, norm);
        EXPECT_NEAR(dist, distances[i], std::abs(distances[i] * relative_err));
    }

    // Bad values
    dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_Cosine], -1, norm);
    EXPECT_TRUE(std::isnan(dist));
    dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_L2], 46, query);
    EXPECT_TRUE(std::isnan(dist));

    // Clean-up.
    for (size_t i = 0; i < numIndex; i++) {
        VecSimIndex_Free(index[i]);
    }
}

TYPED_TEST(SVSMultiTest, testSizeEstimation) {
    size_t dim = 128;
#if HAVE_SVS_LVQ
    // SVS block sizes always rounded to a power of 2
    // This why, in case of quantization, actual block size can be differ than requested
    // In addition, block size to be passed to graph and dataset counted in bytes,
    // converted then to a number of elements.
    // IMHO, would be better to always interpret block size to a number of elements
    // rather than conversion to-from number of bytes
    auto constexpr quantBits = TypeParam::get_quant_bits();
    // Get the fallback quantization mode
    if constexpr (quantBits != VecSimSvsQuant_NONE) { // constexpr eliminates div-by-zero warning;
                                                      // inner condition is runtime-only
        if (!this->isFallbackToSQ()) {
            // Extra data in LVQ vector
            const auto lvq_vector_extra = sizeof(svs::quantization::lvq::ScalarBundle);
            dim -= (lvq_vector_extra * 8) / (quantBits & 0xf);
        }
    }
#endif
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
    ASSERT_GE(estimation * 1.01, actual);
    ASSERT_LE(estimation * 0.99, actual);

    VecSimIndex_Free(index);
}

// Test empty index edge cases.
TYPED_TEST(SVSMultiTest, emptyIndex) {
    size_t dim = 4;
    size_t bs = 6;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = bs};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Try to remove from an empty index - should fail because label doesn't exist.
    ASSERT_EQ(VecSimIndex_DeleteVector(index, 0), 0);

    // Add one vector.
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 1.7);

    // Try to remove it.
    ASSERT_EQ(VecSimIndex_DeleteVector(index, 1), 1);

    // Even an empty index holds the first block for nonquantized data
    // ASSERT_EQ(index->indexCapacity(), 0);

    // Size equals 0.
    index->runGC();
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Try to remove it again.
    ASSERT_EQ(VecSimIndex_DeleteVector(index, 1), 0);
    // Nor the size.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, svs_vector_search_by_id_test) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t n = 100;
    size_t dim = 4;
    size_t k = 11;
    size_t per_label = 5;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i / per_label, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n / per_label);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 50);
    auto verify_res = [&](size_t id, double score, size_t index) { ASSERT_EQ(id, (index + 5)); };
    runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, svs_batch_iterator_basic) {
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
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i / perLabel, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);

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

TYPED_TEST(SVSMultiTest, svs_batch_iterator_reset) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t n_labels = 1000;
    size_t perLabel = 5;

    size_t n = n_labels * perLabel;

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
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i % n_labels, i / 10);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);

    // Get the 100 vectors whose ids are the maximal among those that hasn't been returned yet, in
    // every iteration. Run this flow for 3 times, and reset the iterator.
    size_t n_res = 100;
    size_t re_runs = 3;

    for (size_t take = 0; take < re_runs; take++) {
        size_t iteration_num = 0;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            std::set<size_t> expected_ids;
            for (size_t i = 0; i < n_res; i++) {
                expected_ids.insert(n_labels - iteration_num * n_res - i - 1);
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_TRUE(expected_ids.find(id) != expected_ids.end());
                expected_ids.erase(id);
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_SCORE);
            iteration_num++;
        }
        ASSERT_EQ(iteration_num, n_labels / n_res);
        VecSimBatchIterator_Reset(batchIterator);
    }
    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, svs_batch_iterator_batch_size_1) {
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

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);

    for (size_t i = 0; i < n; i++) {
        // Set labels to be different than the internal ids.
        GenerateAndAddVector<TEST_DATA_T>(index, dim, (n - i - 1) / perLabel, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    size_t iteration_num = 0;
    size_t n_res = 1, expected_n_res = 1;
    while (VecSimBatchIterator_HasNext(batchIterator)) {
        // Expect to get results in the reverse order of labels - which is the order of the distance
        // from the query vector. Get one result in every iteration.
        auto verify_res = [&](size_t id, double score, size_t index) {
            ASSERT_EQ(id, iteration_num);
        };
        runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_SCORE, expected_n_res);
        iteration_num++;
    }

    ASSERT_EQ(iteration_num, n_labels);
    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, svs_batch_iterator_advanced) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t n_labels = 500;
    size_t perLabel = 5;

    size_t n = n_labels * perLabel;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);

    // Try to get results even though there are no vectors in the index.
    VecSimQueryReply *res = VecSimBatchIterator_Next(batchIterator, 10, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);
    ASSERT_FALSE(VecSimBatchIterator_HasNext(batchIterator));
    VecSimBatchIterator_Free(batchIterator);

    // Insert one vector and query again. The internal id will be 0.
    VecSimIndex_AddVector(index, query, n_labels - 1);
    batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    res = VecSimBatchIterator_Next(batchIterator, 10, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 1);
    VecSimQueryReply_Free(res);
    ASSERT_FALSE(VecSimBatchIterator_HasNext(batchIterator));
    VecSimBatchIterator_Free(batchIterator);

    // Insert vectors to the index and re-create the batch iterator.
    for (size_t i = 0; i < n - 1; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i / perLabel, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);
    batchIterator = VecSimBatchIterator_New(index, query, nullptr);

    // Try to get 0 results.
    res = VecSimBatchIterator_Next(batchIterator, 0, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);

    // n_res does not divide into ef or vice versa - expect leftovers between the graph scans.
    size_t n_res = 7;
    size_t iteration_num = 0;

    while (VecSimBatchIterator_HasNext(batchIterator)) {
        iteration_num++;
        std::vector<size_t> expected_ids;
        // We ask to get the results sorted by ID in a specific batch (in ascending order), but
        // in every iteration the ids should be lower than the previous one, according to the
        // distance from the query.
        for (size_t i = 0; i < n_res; i++) {
            expected_ids.push_back(n_labels - iteration_num * n_res + i);
        }
        auto verify_res = [&](size_t id, double score, size_t index) {
            ASSERT_EQ(expected_ids[index], id);
        };
        if (iteration_num <= n_labels / n_res) {
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_ID);
        } else {
            // In the last iteration there are n%n_res results left to return.
            // remove the first ids that aren't going to be returned since we pass the index size.
            for (size_t i = 0; i < n_res - n_labels % n_res; i++) {
                expected_ids.erase(expected_ids.begin());
            }
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_ID, n_labels % n_res);
        }
    }
    ASSERT_EQ(iteration_num, n_labels / n_res + 1);
    // Try to get more results even though there are no.
    res = VecSimBatchIterator_Next(batchIterator, 1, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);

    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, testCosine) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    const size_t dim = 256;
    const size_t n = 50;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_Cosine};

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

    // Add more worst vector for each label
    for (size_t i = 1; i <= n; i++) {
        auto &f = v[i - 1];
        f[0] = (TEST_DATA_T)i + n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
    }
    svs_index->addVectors(v.data(), ids.data(), n);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 2 * n);
    ASSERT_EQ(index->indexLabelCount(), n);

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
        ASSERT_TYPE_EQ(TEST_DATA_T(score), expected_score);
    };
    runTopKSearchTest(index, query, 10, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, testCosineBatchIterator) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    const size_t dim = 256;
    const size_t n = 50;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_Cosine};

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

    // Add more worst vector for each label
    for (size_t i = 1; i <= n; i++) {
        auto &f = v[i - 1];
        f[0] = (TEST_DATA_T)i + n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
    }
    svs_index->addVectors(v.data(), ids.data(), n);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 2 * n);
    ASSERT_EQ(index->indexLabelCount(), n);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 1.0);

    // topK search will normalize the query so we keep the original data to
    // avoid normalizing twice.
    TEST_DATA_T normalized_query[dim];
    memcpy(normalized_query, query, dim * sizeof(TEST_DATA_T));
    VecSim_Normalize(normalized_query, dim, params.type);

    // Test with batch iterator.
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    size_t iteration_num = 0;

    // Get the 10 vectors whose ids are the maximal among those that hasn't been returned yet,
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
            ASSERT_TYPE_EQ(TEST_DATA_T(score), expected_score);
        };
        runBatchIteratorSearchTest(batchIterator, n_res, verify_res_batch);
        iteration_num++;
    }
    ASSERT_EQ(iteration_num, n / n_res);
    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(SVSMultiTest, rangeQuery) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t n_labels = 1000;
    size_t per_label = 5;
    size_t dim = 4;

    size_t n = n_labels * per_label;

    SVSParams params{.dim = dim, .metric = VecSimMetric_L2, .blockSize = n / 2};
    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n_labels; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
        // Add some vectors, worst than the previous vector (for the given query)
        for (size_t j = 0; j < per_label - 1; j++)
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i + n);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    size_t pivot_id = n_labels / 2; // the id to return vectors around it.
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, pivot_id);

    auto verify_res_by_score = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(std::abs(int(id - pivot_id)), (index + 1) / 2);
        ASSERT_EQ(score, dim * pow((index + 1) / 2, 2));
    };
    uint expected_num_results = 11;
    // To get 11 results in the range [pivot_id - 5, pivot_id + 5], set the radius as the L2 score
    // in the boundaries.
    double radius = dim * pow(expected_num_results / 2, 2);
    runRangeQueryTest(index, query, radius, verify_res_by_score, expected_num_results, BY_SCORE);

    // Rerun with a given query params. This high epsilon value will cause the range search main
    // loop to break since we insert a candidate whose distance is within the dynamic range
    // boundaries at the beginning of the search, but when this candidate is popped out from the
    // queue, it's no longer within the dynamic range boundaries.
    VecSimQueryParams query_params = CreateQueryParams(SVSRuntimeParams{.epsilon = 1.0});
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

#endif // HAVE_SVS

#if HAVE_SVS_REPLACE_EXTERNAL_ID

// A multi-value label is where a move that handles only the label's first id still looks correct
// in the single-value tests: every one of its vectors has to answer to the new label, and none to
// the old. Runs across the type set, so it also covers moving a label whose vectors are quantized.
TYPED_TEST(SVSMultiTest, relabelVectorMulti) {
    const size_t dim = 4;
    const size_t per_label = 5;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // Label 7 gets several vectors, and label 8 is an untouched neighbour with its own, so a move
    // that is too broad shows up as well.
    for (size_t i = 0; i < per_label; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 7, i);
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 8, i + 100);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label * 2);
    ASSERT_EQ(index->indexLabelCount(), 2);

    // Captured per vector before the move: asserting these are unchanged afterwards says every
    // copy still holds its own data, without assuming what the distances are under quantization.
    std::vector<double> before(per_label);
    for (size_t i = 0; i < per_label; i++) {
        TEST_DATA_T v[dim];
        GenerateVector<TEST_DATA_T>(v, dim, i);
        before[i] = VecSimIndex_GetDistanceFrom_Unsafe(index, 7, v);
        ASSERT_FALSE(std::isnan(before[i]));
    }

    ASSERT_EQ(VecSimIndex_RelabelVector(index, 7, 70), VecSimRelabel_OK);

    // Every copy moved, and only the label changed.
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label * 2);
    ASSERT_EQ(index->indexLabelCount(), 2);
    for (size_t i = 0; i < per_label; i++) {
        TEST_DATA_T v[dim];
        GenerateVector<TEST_DATA_T>(v, dim, i);
        ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, 70, v), before[i])
            << "copy " << i << " was disturbed by the move";
    }
    TEST_DATA_T probe[dim];
    GenerateVector<TEST_DATA_T>(probe, dim, 0);
    ASSERT_TRUE(std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(index, 7, probe)))
        << "the old label still answers";

    // The neighbour kept all of its own copies.
    TEST_DATA_T neighbour[dim];
    GenerateVector<TEST_DATA_T>(neighbour, dim, 100);
    ASSERT_FALSE(std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(index, 8, neighbour)));

    // A search for one of the moved vectors reports the new label.
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0);
    auto verify_res = [&](size_t id, double score, size_t rank) { ASSERT_EQ(id, 70); };
    runTopKSearchTest(index, query, 1, verify_res);

    VecSimIndex_Free(index);
}

// Rejections on a multi index: a target that already holds vectors of its own is taken, however
// many copies either label has.
TYPED_TEST(SVSMultiTest, relabelVectorMultiRejects) {
    const size_t dim = 4;
    const size_t per_label = 3;

    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    for (size_t i = 0; i < per_label; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, i);
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 2, i + 100);
    }

    ASSERT_EQ(VecSimIndex_RelabelVector(index, 1, 2), VecSimRelabel_NewLabelTaken);
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 1, 1), VecSimRelabel_SameLabel);
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 42, 43), VecSimRelabel_OldLabelMissing);

    // A rejection leaves every copy of both labels in place.
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label * 2);
    ASSERT_EQ(index->indexLabelCount(), 2);

    VecSimIndex_Free(index);
}

#endif // HAVE_SVS_REPLACE_EXTERNAL_ID

TYPED_TEST(SVSMultiTest, updateVectorsMulti) {
    size_t dim = 4;
    size_t per_label = 3;
    SVSParams params = {.dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimIndex *index = this->CreateNewIndex(params);
    ASSERT_INDEX(index);

    // Label 0 holds the values 0, 1, 2 and label 1 holds 10, 11, 12.
    for (size_t i = 0; i < per_label; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 0, i);
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, i + 10);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label * 2);
    ASSERT_EQ(index->indexLabelCount(), 2);

    auto *svs_index = dynamic_cast<SVSIndexBase *>(index);
    ASSERT_NE(svs_index, nullptr);
    // SVS deletes softly - an entry is marked and only dropped by a later consolidation - so the
    // index size below counts the replaced vectors until then, and the live count is what is left
    // after subtracting the marked ones.
    auto live_vectors = [&]() {
        return VecSimIndex_IndexSize(index) - svs_index->getNumMarkedDeleted();
    };

    // Replace label 0's three vectors with two different ones. How many the label ends up holding
    // is decided by the update, not by what was there - and `addVector` cannot express this at
    // all here, since in a multi-value index it appends.
    TEST_DATA_T two[2 * dim];
    GenerateVector<TEST_DATA_T>(two, dim, 100);
    GenerateVector<TEST_DATA_T>(two + dim, dim, 101);
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 0, two, 2), VecSimUpdate_OK);

    ASSERT_EQ(live_vectors(), per_label + 2);
    ASSERT_EQ(index->indexLabelCount(), 2);

    if (!svs_index->isCompressed()) {
        // Each of the label's new vectors finds it, and the untouched label kept all of its own.
        // Asserted only for an uncompressed index: a compressed one trains its stored form on the
        // vectors it was given, so the replacements - outside that range - are clipped.
        for (size_t i = 0; i < 2; i++) {
            auto verify_new = [&](size_t id, double score, size_t rank) { ASSERT_EQ(id, 0); };
            runTopKSearchTest(index, two + i * dim, 1, verify_new);
        }
        for (size_t value : {10, 11, 12}) {
            TEST_DATA_T other[dim];
            GenerateVector<TEST_DATA_T>(other, dim, value);
            auto verify_other = [&](size_t id, double score, size_t rank) { ASSERT_EQ(id, 1); };
            runTopKSearchTest(index, other, 1, verify_other);
        }
    }

    // Growing the label works the same way.
    TEST_DATA_T four[4 * dim];
    for (size_t i = 0; i < 4; i++) {
        GenerateVector<TEST_DATA_T>(four + i * dim, dim, 200 + i);
    }
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 0, four, 4), VecSimUpdate_OK);
    ASSERT_EQ(live_vectors(), per_label + 4);
    ASSERT_EQ(index->indexLabelCount(), 2);

    // And an empty update removes the label altogether.
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 0, nullptr, 0), VecSimUpdate_OK);
    ASSERT_EQ(live_vectors(), per_label);
    ASSERT_EQ(index->indexLabelCount(), 1);

    VecSimIndex_Free(index);
}
