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
#include "VecSim/algorithms/brute_force/brute_force_multi.h"
#include <cmath>

template <typename index_type_t>
class BruteForceMultiTest : public ::testing::Test {
public:
    using data_t = typename index_type_t::data_t;
    using dist_t = typename index_type_t::dist_t;

protected:
    VecSimIndex *CreateNewIndex(BFParams &params) {
        // is_multi = false by default.
        return test_utils::CreateNewIndex(params, index_type_t::get_index_type(), true);
    }

    BruteForceIndex_Multi<data_t, dist_t> *CastToBF_Multi(VecSimIndex *index) {
        return reinterpret_cast<BruteForceIndex_Multi<data_t, dist_t> *>(index);
    }
};

// DataTypeSet, TEST_DATA_T and TEST_DIST_T are defined in unit_test_utils.h

TYPED_TEST_SUITE(BruteForceMultiTest, DataTypeSet);

TYPED_TEST(BruteForceMultiTest, vector_add_multiple_test) {
    size_t dim = 4;
    size_t rep = 5;

    BFParams params = {.dim = dim, .metric = VecSimMetric_IP};

    VecSimIndex *index = this->CreateNewIndex(params);

    BruteForceIndex_Multi<TEST_DATA_T, TEST_DIST_T> *index_bf_multi = this->CastToBF_Multi(index);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Adding multiple vectors under the same label
    for (size_t j = 0; j < rep; j++) {
        TEST_DATA_T a[dim];
        for (size_t i = 0; i < dim; i++) {
            a[i] = (TEST_DATA_T)i * j + i;
        }
        VecSimIndex_AddVector(index, a, 46);
    }

    ASSERT_EQ(VecSimIndex_IndexSize(index), rep);
    ASSERT_EQ(index_bf_multi->indexLabelCount(), 1);

    // Deleting the label. All the vectors should be deleted.
    ASSERT_EQ(index_bf_multi->deleteVector(46), rep);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);
    ASSERT_EQ(index_bf_multi->indexLabelCount(), 0);

    VecSimIndex_Free(index);
}

/**** resizing cases ****/

TYPED_TEST(BruteForceMultiTest, resize_and_align_index) {
    constexpr size_t dim = 4;
    constexpr size_t blockSize = 10;
    constexpr size_t per_label = 3;
    constexpr size_t labels = 7;
    constexpr size_t n = labels * per_label;
    size_t added_vectors = 0;
    size_t curr_label = 0;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = blockSize};

    VecSimIndex *index = this->CreateNewIndex(params);

    auto bf_index = this->CastToBF_Multi(index);
    DataBlocksContainer *vectors_container = dynamic_cast<DataBlocksContainer *>(bf_index->vectors);
    auto verify_index_size = [&](size_t expected_num_vectors, size_t expected_capacity,
                                 std::string msg) {
        SCOPED_TRACE("verify_index_size: " + msg);
        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        ASSERT_EQ(info.commonInfo.indexSize, expected_num_vectors);
        ASSERT_EQ(info.commonInfo.indexLabelCount,
                  (expected_num_vectors + per_label - 1) / per_label);
        ASSERT_EQ(bf_index->idToLabelMapping.size(), expected_capacity);
        ASSERT_EQ(bf_index->getVectorsContainer()->size(), expected_num_vectors);
        ASSERT_EQ(bf_index->indexCapacity(), expected_capacity);
        ASSERT_EQ(vectors_container->numBlocks(),
                  expected_num_vectors / blockSize + (expected_num_vectors % blockSize != 0))
            << "expected_size: " << expected_num_vectors
            << " expected_capacity: " << expected_capacity;
    };

    // Empty index with no initial capacity
    verify_index_size(0, 0, "empty index");

    // Add one label (vectors per label < blockSize), index capacity should grow to blockSize.
    for (size_t i = 0; i < per_label; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 0);
        added_vectors++;
    }
    curr_label++;
    verify_index_size(per_label, blockSize,
                      "add one label with " + std::to_string(per_label) + " vectors");

    ASSERT_LE(added_vectors, blockSize);
    // Add vector up to blocksize, index capacity should remain the same.
    while (added_vectors < blockSize) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, curr_label);
        added_vectors++;
        curr_label += added_vectors % per_label == 0;
    }
    verify_index_size(blockSize, blockSize, "add up to blocksize");

    // remove invalid id
    VecSimIndex_DeleteVector(index, 3459);

    // This should do nothing
    verify_index_size(blockSize, blockSize, "remove invalid id");

    // Add another vector, since index size equals to block size, this should cause resizing
    // of the capacity by one blocksize
    GenerateAndAddVector<TEST_DATA_T>(index, dim, curr_label);
    added_vectors++;
    ASSERT_EQ(added_vectors, blockSize + 1);
    curr_label += added_vectors % per_label == 0;
    verify_index_size(blockSize + 1, 2 * blockSize,
                      "add one more vector after reaching block size");

    // Now size = blockSize + 1 = 11, capacity = 2 * bs = 20. Test capacity overflow again
    // to check that it stays aligned with blocksize.
    size_t add_vectors_count = blockSize + 2; // 12
    while (added_vectors < add_vectors_count + blockSize + 1) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, curr_label);
        added_vectors++;
        curr_label += added_vectors % per_label == 0;
    }

    // Size should be blocksize + 1 + add_vectors_count (= 23).
    verify_index_size(
        blockSize + 1 + add_vectors_count, 3 * blockSize,
        "add more vectors after reaching 2 * blocksize capacity to trigger another resize");

    size_t total_vectors = 0;
    for (auto label_ids : bf_index->labelToIdsLookup) {
        total_vectors += label_ids.second.size();
    }
    ASSERT_EQ(total_vectors, added_vectors);

    // Delete vectors until one block plus some vectors are removed.
    size_t current_vectors_block_count = (added_vectors + blockSize - 1) / blockSize;
    ASSERT_EQ(current_vectors_block_count, 3);
    size_t num_deleted = 0;
    size_t label_to_delete = 0;
    auto remove_to_one_below_blocksize = [&]() {
        while (vectors_container->numBlocks() == current_vectors_block_count) {
            int ret = VecSimIndex_DeleteVector(index, label_to_delete++);
            ASSERT_EQ(ret, per_label) << "tried to remove label: " << label_to_delete - 1;
            num_deleted += ret;
        }
        int ret = VecSimIndex_DeleteVector(index, label_to_delete++);
        ASSERT_EQ(ret, per_label) << "tried to remove label: " << label_to_delete - 1;
        num_deleted += ret;
    };

    // First trigger of remove_to_one_below_blocksize will result in one free block.
    // This should not trigger shrinking of metadata containers.
    remove_to_one_below_blocksize(); // remove first block labels.
    ASSERT_LT(added_vectors - num_deleted, 2 * blockSize);
    verify_index_size(
        added_vectors - num_deleted, 3 * blockSize,
        "delete vectors so that indexsize < 2 * blocksize, but there is only one free block");

    // Second trigger of remove_to_one_below_blocksize will result in two free blocks.
    // This should trigger shrinking of metadata containers by one block.
    current_vectors_block_count--;
    remove_to_one_below_blocksize();
    ASSERT_LT(added_vectors - num_deleted, blockSize);
    verify_index_size(
        added_vectors - num_deleted, 2 * blockSize,
        "delete vectors so that indexsize % blocksize == 0 and there are two free blocks");

    // Now there is one block in use and one free. adding vectors up to blocksize (and below 2 *
    // blocksize) should not trigger another resize.
    while (vectors_container->numBlocks() == current_vectors_block_count) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, curr_label);
        added_vectors++;
        curr_label += added_vectors % per_label == 0;
    }
    verify_index_size(added_vectors - num_deleted, 2 * blockSize,
                      "add vectors up to blocksize after deleting two blocks");

    // Delete all vectors.
    while (VecSimIndex_IndexSize(index) > 0) {
        int ret = VecSimIndex_DeleteVector(index, label_to_delete++);
        ASSERT_LE(ret, per_label) << "tried to remove label: " << label_to_delete - 1;
        num_deleted += ret;
    }
    ASSERT_EQ(num_deleted, added_vectors);
    verify_index_size(0, 0, "delete all vectors");
    VecSimIndex_Free(index);
}

// Test empty index edge cases.
TYPED_TEST(BruteForceMultiTest, empty_index) {
    size_t dim = 4;
    size_t n = 20;
    size_t bs = 6;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = bs};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);
    ASSERT_EQ(this->CastToBF_Multi(index)->idToLabelMapping.size(), 0);

    // Try to remove from an empty index - should fail because label doesn't exist.
    VecSimIndex_DeleteVector(index, 0);

    // Add one vector.
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 1.7);

    // Try to remove it.
    VecSimIndex_DeleteVector(index, 1);

    ASSERT_EQ(this->CastToBF_Multi(index)->idToLabelMapping.size(), 0);

    // Size equals 0.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Try to remove it again.
    // The idToLabelMapping_size should remain unchanged, as we are trying to delete a label that
    // doesn't exist.
    VecSimIndex_DeleteVector(index, 1);
    ASSERT_EQ(this->CastToBF_Multi(index)->idToLabelMapping.size(), 0);
    // Nor the size.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, search_more_than_there_is) {
    size_t dim = 4;
    size_t n = 5;
    size_t perLabel = 3;
    size_t n_labels = ceil((float)n / perLabel);
    size_t k = 3;
    // This test add 5 vectors under 2 labels, and then query for 3 results.
    // We want to make sure we get only 2 results back (because the results should have unique
    // labels), although the index contains 5 vectors.

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i / perLabel, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(VecSimIndex_DebugInfo(index).commonInfo.indexLabelCount, n_labels);

    TEST_DATA_T query[] = {0, 0, 0, 0};
    VecSimQueryReply *res = VecSimIndex_TopKQuery(index, query, k, nullptr, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), n_labels);
    auto it = VecSimQueryReply_GetIterator(res);
    for (size_t i = 0; i < n_labels; i++) {
        auto el = VecSimQueryReply_IteratorNext(it);
        labelType element_label = VecSimQueryResult_GetId(el);
        ASSERT_EQ(element_label, i);
        auto ids = this->CastToBF_Multi(index)->labelToIdsLookup.at(element_label);
        for (size_t j = 0; j < ids.size(); j++) {
            // Verifying that each vector is labeled correctly.
            // ID is calculated according to insertion order.
            ASSERT_EQ(ids[j], i * perLabel + j);
        }
    }
    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(res);
    VecSimIndex_Free(index);
}
TYPED_TEST(BruteForceMultiTest, indexing_same_vector) {
    size_t n = 100;
    size_t k = 10;
    size_t perLabel = 10;
    size_t dim = 4;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i / perLabel, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    TEST_DATA_T query[] = {0, 0, 0, 0};
    auto verify_res = [&](size_t id, double score, size_t index) { ASSERT_EQ(id, index); };
    runTopKSearchTest(index, query, k, verify_res);
    auto res = VecSimIndex_TopKQuery(index, query, k, nullptr, BY_SCORE);
    auto it = VecSimQueryReply_GetIterator(res);
    for (size_t i = 0; i < k; i++) {
        auto el = VecSimQueryReply_IteratorNext(it);
        labelType element_label = VecSimQueryResult_GetId(el);
        ASSERT_EQ(element_label, i);
        auto ids = this->CastToBF_Multi(index)->labelToIdsLookup.at(element_label);
        for (size_t j = 0; j < ids.size(); j++) {
            // Verifying that each vector is labeled correctly.
            // ID is calculated according to insertion order.
            ASSERT_EQ(ids[j], i * perLabel + j);
        }
    }
    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(res);
    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, find_better_score) {
    size_t n = 100;
    size_t k = 10;
    size_t n_labels = 10;
    size_t dim = 4;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    // Building the index. Each label gets 10 vectors with decreasing (by insertion order) element
    // value, so when we search, each vector is better than the previous one. Furthermore, each
    // label gets at least one better vector than the previous label and one with a score equals to
    // the best of the previous label, so the multimap holds at least two labels with the same
    // score.
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

        GenerateAndAddVector<TEST_DATA_T>(index, dim, i / n_labels, el);
        // This should be the best score for each label.
        if (i % n_labels == n_labels - 1) {
            // `el * el * dim` is the L2-squared value with the 0 vector.
            scores.emplace(i / n_labels, el * el * dim);
        }
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(VecSimIndex_DebugInfo(index).commonInfo.indexLabelCount, n_labels);

    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, k - index - 1);
        ASSERT_DOUBLE_EQ(score, scores[id]);
    };

    TEST_DATA_T query[] = {0, 0, 0, 0};
    runTopKSearchTest(index, query, k, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, find_better_score_after_pop) {
    size_t n = 12;
    size_t n_labels = 3;
    size_t dim = 4;
    size_t initial_capacity = 200;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    // Building the index. Each is better than the previous one.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i % n_labels, n - i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(VecSimIndex_DebugInfo(index).commonInfo.indexLabelCount, n_labels);

    TEST_DATA_T query[] = {0, 0, 0, 0};
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, n_labels - index - 1);
    };
    // Having k = n_labels - 1, the heap will continuously pop the worst label before finding the
    // next vector, which is of the same label and is the best vector yet.
    runTopKSearchTest(index, query, n_labels - 1, verify_res);

    // Having k = n_labels, the heap will never get to pop the worst label, but we will update the
    // scores each time.
    runTopKSearchTest(index, query, n_labels, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, reindexing_same_vector_different_id) {
    size_t n = 100;
    size_t k = 10;
    size_t dim = 4;
    size_t perLabel = 3;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        // i / 10 is in integer (take the "floor" value)
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, TEST_DATA_T(i / 10));
    }
    // Add more vectors under the same labels. their scores should be worst.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T v[dim];
        GenerateVector<TEST_DATA_T>(v, dim, TEST_DATA_T(i / 10) + n);
        for (size_t j = 0; j < perLabel - 1; j++) {
            VecSimIndex_AddVector(index, v, i);
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
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Reinsert the same vectors under different ids than before.
    for (size_t i = 0; i < n; i++) {
        // i / 10 is in integer (take the "floor" value)
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i + 10, TEST_DATA_T(i / 10));
    }
    // Add more vectors under the same labels. their scores should be worst.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T v[dim];
        GenerateVector<TEST_DATA_T>(v, dim, TEST_DATA_T(i / 10) + n);
        for (size_t j = 0; j < perLabel - 1; j++) {
            VecSimIndex_AddVector(index, v, i + 10);
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

TYPED_TEST(BruteForceMultiTest, test_delete_swap_block) {
    size_t n = 6;
    size_t dim = 2;
    size_t n_labels = 3;
    size_t bs = 3;

    // This test creates 2 vector blocks with size of 3
    // Insert 6 vectors with ascending ids; The vector blocks will look like
    // block 0 [0, 1, 2]
    // block 1 [3, 4, 5]
    // Delete the id 1 will delete it from the first vector block 0 [0 ,1, 2] and will move vector
    // data of id 5 to vector block 0 at index 1. id2label[1] should hold the label of the vector
    // that was in id 5.

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = bs};

    VecSimIndex *index = this->CreateNewIndex(params);

    BruteForceIndex_Multi<TEST_DATA_T, TEST_DIST_T> *bfm_index = this->CastToBF_Multi(index);

    // idToLabelMapping initial size equals n.
    ASSERT_EQ(bfm_index->idToLabelMapping.size(), 0);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i % n_labels, i);
    }

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(bfm_index->indexLabelCount(), n_labels);
    for (auto label_ids : bfm_index->labelToIdsLookup) {
        ASSERT_EQ(label_ids.second.size(), n / n_labels);
    }
    // id2label size should be equal to the index size.
    ASSERT_EQ(bfm_index->idToLabelMapping.size(), n);

    labelType id1_prev_label = bfm_index->getVectorLabel(1);
    labelType id5_prev_label = bfm_index->getVectorLabel(5);

    // Here the shift should happen.
    //
    //         initial ids     labels
    // block 0 [0, 1, 2] ~~~~~ [0, 1, 2]
    // block 1 [3, 4, 5] ~~~~~ [0, 1, 2]
    //
    // We labeled each vector as its relative index in the block, so we delete ids 1 and 4 now.
    // We should get the followed result:
    //
    //         initial ids     labels
    // block 0 [0, 5, 2] ~~~~~ [0, 2, 2]
    // block 1 [3]       ~~~~~ [0]

    VecSimIndex_DeleteVector(index, 1);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n - (n / n_labels));
    ASSERT_EQ(bfm_index->indexLabelCount(), n_labels - 1);
    ASSERT_EQ(bfm_index->getVectorLabel(0), 0);
    ASSERT_EQ(bfm_index->getVectorLabel(1), 2);
    ASSERT_EQ(bfm_index->getVectorLabel(2), 2);
    ASSERT_EQ(bfm_index->getVectorLabel(3), 0);
    // id2label size should remain unchanged..
    ASSERT_EQ(bfm_index->idToLabelMapping.size(), n);

    // id1 gets what was previously id5's label.
    ASSERT_EQ(bfm_index->getVectorLabel(1), id5_prev_label);

    // The label of what initially was in id1 should be removed.
    auto deleted_label_id_pair = bfm_index->labelToIdsLookup.find(id1_prev_label);
    ASSERT_EQ(deleted_label_id_pair, bfm_index->labelToIdsLookup.end());

    // The vector in index1 should hold id5 data.
    TEST_DATA_T *vector_data = (TEST_DATA_T *)bfm_index->getVectorsContainer()->getElement(1);
    for (size_t i = 0; i < dim; ++i) {
        ASSERT_EQ(*vector_data, 5);
        ++vector_data;
    }

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, test_bf_info) {
    size_t n = 100;
    size_t d = 128;

    // Build with default args.

    BFParams params = {.dim = d, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_BF);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, d);
    ASSERT_TRUE(info.commonInfo.basicInfo.isMulti);
    // Default args.
    ASSERT_EQ(info.commonInfo.basicInfo.blockSize, DEFAULT_BLOCK_SIZE);
    ASSERT_EQ(info.commonInfo.indexSize, 0);
    VecSimIndex_Free(index);

    d = 1280;
    params.dim = d;
    params.blockSize = 1;
    index = this->CreateNewIndex(params);

    info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_BF);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, d);
    ASSERT_TRUE(info.commonInfo.basicInfo.isMulti);
    // User args.
    ASSERT_EQ(info.commonInfo.basicInfo.blockSize, 1);
    ASSERT_EQ(info.commonInfo.indexSize, 0);
    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, test_basic_bf_info_iterator) {
    size_t n = 100;
    size_t d = 128;
    VecSimMetric metrics[3] = {VecSimMetric_Cosine, VecSimMetric_IP, VecSimMetric_L2};

    for (size_t i = 0; i < 3; i++) {
        // Build with default args.

        BFParams params = {.dim = d, .metric = metrics[i]};

        VecSimIndex *index = this->CreateNewIndex(params);

        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        VecSimDebugInfoIterator *infoIter = VecSimIndex_DebugInfoIterator(index);
        compareFlatIndexInfoToIterator(info, infoIter);
        VecSimDebugInfoIterator_Free(infoIter);
        VecSimIndex_Free(index);
    }
}

TYPED_TEST(BruteForceMultiTest, test_dynamic_bf_info_iterator) {
    size_t d = 128;

    BFParams params = {.dim = d, .metric = VecSimMetric_L2, .blockSize = 1};

    VecSimIndex *index = this->CreateNewIndex(params);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    VecSimDebugInfoIterator *infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(1, info.commonInfo.basicInfo.blockSize);
    ASSERT_EQ(0, info.commonInfo.indexSize);
    compareFlatIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    TEST_DATA_T v[d];
    for (size_t i = 0; i < d; i++) {
        v[i] = (TEST_DATA_T)i;
    }

    // Add vectors.
    VecSimIndex_AddVector(index, v, 0);
    VecSimIndex_AddVector(index, v, 0);
    VecSimIndex_AddVector(index, v, 1);
    VecSimIndex_AddVector(index, v, 1);
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(4, info.commonInfo.indexSize);
    ASSERT_EQ(2, info.commonInfo.indexLabelCount);
    compareFlatIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    // Delete vectors.
    VecSimIndex_DeleteVector(index, 0);
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(2, info.commonInfo.indexSize);
    ASSERT_EQ(1, info.commonInfo.indexLabelCount);
    compareFlatIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    // Perform (or simulate) Search in all modes.
    VecSimIndex_AddVector(index, v, 0);
    auto res = VecSimIndex_TopKQuery(index, v, 1, nullptr, BY_SCORE);
    VecSimQueryReply_Free(res);
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(STANDARD_KNN, info.commonInfo.lastMode);
    compareFlatIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    res = VecSimIndex_RangeQuery(index, v, 1, nullptr, BY_SCORE);
    VecSimQueryReply_Free(res);
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(RANGE_QUERY, info.commonInfo.lastMode);
    compareFlatIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    ASSERT_TRUE(VecSimIndex_PreferAdHocSearch(index, 1, 1, true));
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(HYBRID_ADHOC_BF, info.commonInfo.lastMode);
    compareFlatIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    // Set the index size artificially so that BATCHES mode will be selected by the heuristics.
    size_t perLabel = 3;
    for (size_t i = 0; i < 1e4; i++) {
        VecSimIndex_AddVector(index, v, i / perLabel);
    }
    ASSERT_FALSE(VecSimIndex_PreferAdHocSearch(index, 7e3, 1, true));
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(HYBRID_BATCHES, info.commonInfo.lastMode);
    compareFlatIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    // Simulate the case where another call to the heuristics is done after realizing that
    // the subset size is smaller, and change the policy as a result.
    ASSERT_TRUE(VecSimIndex_PreferAdHocSearch(index, 1, 1, false));
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(HYBRID_BATCHES_TO_ADHOC_BF, info.commonInfo.lastMode);
    compareFlatIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, vector_search_test_l2) {
    size_t dim = 4;
    size_t n = 100;
    size_t k = 11;
    size_t perLabel = 4;

    for (size_t blocksize : {1, 12, DEFAULT_BLOCK_SIZE}) {

        BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = blocksize};

        VecSimIndex *index = this->CreateNewIndex(params);

        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_BF);
        ASSERT_EQ(info.commonInfo.basicInfo.blockSize, blocksize);

        for (size_t i = 0; i < n; i++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
        }
        // Add more vectors under the same labels. their scores should be worst.
        for (size_t i = 0; i < n; i++) {
            for (size_t j = 0; j < perLabel - 1; j++) {
                GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i + n);
            }
        }
        ASSERT_EQ(VecSimIndex_IndexSize(index), n * perLabel);

        auto verify_res = [&](size_t id, double score, size_t index) {
            size_t diff_id = (id > 50) ? (id - 50) : (50 - id);
            ASSERT_EQ(diff_id, (index + 1) / 2);
            ASSERT_EQ(score, (4 * ((index + 1) / 2) * ((index + 1) / 2)));
        };
        TEST_DATA_T query[] = {50, 50, 50, 50};
        runTopKSearchTest(index, query, k, verify_res);

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(BruteForceMultiTest, search_empty_index) {
    size_t dim = 4;
    size_t n = 100;
    size_t k = 11;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

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
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    for (size_t i = 0; i < n; i++) {
        VecSimIndex_DeleteVector(index, i);
    }
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

TYPED_TEST(BruteForceMultiTest, removeVectorWithSwaps) {
    size_t dim = 4;
    size_t n = 6;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};
    auto *index = this->CastToBF_Multi(this->CreateNewIndex(params));

    // Insert 3 vectors under two different labels, so that we will have:
    // {first_label->[0,1,3], second_label->[2,4,5]}
    labelType first_label = 1;
    labelType second_label = 2;

    GenerateAndAddVector<TEST_DATA_T>(index, dim, first_label);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, first_label);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, second_label);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, first_label);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, second_label);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, second_label);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Artificially reorder the internal ids to test that we make the right changes
    // when we have an id that appears twice in the array upon deleting the ids one by one.
    ASSERT_EQ(index->labelToIdsLookup.at(second_label).size(), n / 2);
    index->labelToIdsLookup.at(second_label)[0] = 4;
    index->labelToIdsLookup.at(second_label)[1] = 2;
    index->labelToIdsLookup.at(second_label)[2] = 5;

    // Expect that the ids array of label 0 will behave as following:
    // [|4, 2, 5] -> [4, |2, 4] -> [4, 2, |2] (where | marks the current position).
    ASSERT_EQ(index->deleteVector(second_label), n / 2);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n / 2);

    // Check that the internal ids of the first label are as expected.
    auto ids = index->labelToIdsLookup.at(first_label);
    ASSERT_EQ(ids.size(), n / 2);
    ASSERT_TRUE(std::find(ids.begin(), ids.end(), 0) != ids.end());
    ASSERT_TRUE(std::find(ids.begin(), ids.end(), 1) != ids.end());
    ASSERT_TRUE(std::find(ids.begin(), ids.end(), 2) != ids.end());
    ASSERT_EQ(index->deleteVector(first_label), n / 2);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, remove_vector_after_replacing_block) {
    size_t dim = 4;
    size_t bs = 2;
    size_t n = 6;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = bs};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Setting up vectors
    TEST_DATA_T f[n][dim];
    for (size_t i = 0; i < n; i++) {
        GenerateVector<TEST_DATA_T>(f[i], dim, i);
    }
    // Add 1 vector with label 1
    VecSimIndex_AddVector(index, f[0], 1);

    // Add 3 vectors with label 3
    VecSimIndex_AddVector(index, f[1], 3);
    VecSimIndex_AddVector(index, f[2], 3);
    VecSimIndex_AddVector(index, f[3], 3);

    // Add 2 vectors with label 2
    VecSimIndex_AddVector(index, f[4], 2);
    VecSimIndex_AddVector(index, f[5], 2);

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Delete label 3. the following drawing present the expected changes
    // [[1, 3], [3, 3], [2, 2]] -> [[1, 2], [3, 3], [2]] -> [[1, 2], [2, 3]] -> [[1, 2], [2]]
    // [[0, 1], [2, 3], [4, 5]] -> [[0, 5], [2, 3], [4]] -> [[0, 5], [4, 3]] -> [[0, 5], [4]]
    VecSimIndex_DeleteVector(index, 3);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 3);
    ASSERT_EQ(VecSimIndex_DebugInfo(index).commonInfo.indexLabelCount, 2);
    auto bf_index = this->CastToBF_Multi(index);
    ASSERT_EQ(bf_index->getVectorLabel(0), 1);
    ASSERT_EQ(bf_index->getVectorLabel(1), 2);
    ASSERT_EQ(bf_index->getVectorLabel(2), 2);
    // checking the blob swaps.
    ASSERT_EQ(bf_index->getDataByInternalId(0)[0], 0);
    ASSERT_EQ(bf_index->getDataByInternalId(1)[0], 5);
    ASSERT_EQ(bf_index->getDataByInternalId(2)[0], 4);

    VecSimIndex_DeleteVector(index, 1);
    VecSimIndex_DeleteVector(index, 2);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, batch_iterator) {
    size_t dim = 4;
    size_t perLabel = 5;

    // run the test twice - for index of size 100, every iteration will run select-based search,
    // as the number of results is 5, which is more than 0.1% of the index size. for index of size
    // 10000, we will run the heap-based search until we return 5000 results, and then switch to
    // select-based search.
    for (size_t m : {100, 10000}) {
        size_t n = m * perLabel;
        BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = 7};
        VecSimIndex *index = this->CreateNewIndex(params);
        for (size_t i = 0; i < n; i++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i / perLabel, i);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(index), n);
        ASSERT_EQ(VecSimIndex_DebugInfo(index).commonInfo.indexLabelCount, m);

        // Query for (n,n,...,n) vector (recall that n is the largest id in te index).
        TEST_DATA_T query[dim];
        GenerateVector<TEST_DATA_T>(query, dim, n);
        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
        size_t iteration_num = 0;

        // Get the 10 vectors whose ids are the maximal among those that hasn't been returned yet,
        // in every iteration. The order should be from the largest to the lowest id.
        size_t n_res = 5;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            std::vector<size_t> expected_ids(n_res);
            for (size_t i = 0; i < n_res; i++) {
                expected_ids[i] = (m - iteration_num * n_res - i - 1);
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_EQ(expected_ids[index], id);
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
            iteration_num++;
        }
        ASSERT_EQ(iteration_num, m / n_res);
        VecSimBatchIterator_Free(batchIterator);
        ASSERT_EQ(VecSimIndex_IndexSize(index), n);
        ASSERT_EQ(VecSimIndex_DebugInfo(index).commonInfo.indexLabelCount, m);

        // Cleanup before next round.
        VecSimIndex_Free(index);
    }
}

TYPED_TEST(BruteForceMultiTest, brute_force_batch_iterator_non_unique_scores) {
    size_t dim = 4;
    size_t perLabel = 5;

    // Run the test twice - for index of size 100, every iteration will run select-based search,
    // as the number of results is 5, which is more than 0.1% of the index size. For index of size
    // 10000, we will run the heap-based search until we return 5000 results, and then switch to
    // select-based search.
    for (size_t m : {100, 10000}) {
        size_t n = m * perLabel;
        BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = m / 2};

        VecSimIndex *index = this->CreateNewIndex(params);
        for (size_t i = 0; i < n; i++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i / perLabel, i / (10 * perLabel));
        }
        ASSERT_EQ(VecSimIndex_IndexSize(index), n);

        // Query for (n,n,...,n) vector (recall that n is the largest id in te index).
        TEST_DATA_T query[dim];
        GenerateVector<TEST_DATA_T>(query, dim, n);

        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
        size_t iteration_num = 0;

        // Get the 5 vectors whose ids are the maximal among those that hasn't been returned yet,
        // in every iteration. there are n/10 groups of 10 different vectors with the same score.
        size_t n_res = 5;
        bool even_iteration = false;
        std::set<size_t> expected_ids;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            // Insert the maximal 10 ids in every odd iteration.
            if (!even_iteration) {
                for (size_t i = 1; i <= 2 * n_res; i++) {
                    expected_ids.insert(m - iteration_num * n_res - i);
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
        ASSERT_EQ(iteration_num, m / n_res);
        VecSimBatchIterator_Free(batchIterator);
        ASSERT_EQ(VecSimIndex_IndexSize(index), n);
        ASSERT_EQ(VecSimIndex_DebugInfo(index).commonInfo.indexLabelCount, m);

        // Cleanup before next round.
        VecSimIndex_Free(index);
    }
}

TYPED_TEST(BruteForceMultiTest, batch_iterator_validate_scores) {
    size_t dim = 4;
    size_t perLabel = 10;
    size_t n_labels = 100;

    size_t init_n = n_labels * (perLabel - 1);

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = 5};

    VecSimIndex *index = this->CreateNewIndex(params);

    // Inserting some big vectors to the index
    for (size_t i = 0; i < init_n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i % n_labels, i + n_labels + 46);
    }
    // Lastly, inserting small vector for each label
    for (size_t label = 0; label < n_labels; label++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, label, label);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n_labels * perLabel);

    // Query for (0,0,0,...,0) vector.
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0);
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    size_t iteration_num = 0;

    size_t n_res = 5;
    // ids should be in ascending order
    // scores should match to the score of the last vector for each label.
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, index + iteration_num * n_res);
        ASSERT_DOUBLE_EQ(score, id * id * dim);
    };
    while (VecSimBatchIterator_HasNext(batchIterator)) {
        runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
        iteration_num++;
    }

    ASSERT_EQ(iteration_num, n_labels / n_res);
    VecSimBatchIterator_Free(batchIterator);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n_labels * perLabel);
    ASSERT_EQ(VecSimIndex_DebugInfo(index).commonInfo.indexLabelCount, n_labels);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, brute_get_distance) {
    size_t n_labels = 2;
    size_t dim = 2;
    size_t numIndex = 3;
    VecSimIndex *index[numIndex];
    std::vector<double> distances;

    TEST_DATA_T v1_0[] = {M_PI, M_PI};
    TEST_DATA_T v2_0[] = {M_E, M_E};
    TEST_DATA_T v3_1[] = {M_PI, M_E};
    TEST_DATA_T v4_1[] = {M_SQRT2, -M_SQRT2};

    BFParams params = {.dim = dim};

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
    ASSERT_TYPE_EQ(norm[0], TEST_DATA_T(1.0 / sqrt(2.0)));
    ASSERT_TYPE_EQ(norm[1], TEST_DATA_T(1.0 / sqrt(2.0)));
    double dist;

    // VecSimMetric_L2
    // distances are [[0.000, 0.358], [0.179, 23.739]]
    // minimum of each label are:
    distances = {0, 0.1791922003030777};
    for (size_t i = 0; i < n_labels; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_L2], i, query);
        ASSERT_NEAR(dist, distances[i], 1e-5);
    }

    // VecSimMetric_IP
    // distances are [[-18.739, -16.079], [-17.409, 1.000]]
    // minimum of each label are:
    distances = {-18.73921012878418, -17.409339904785156};
    for (size_t i = 0; i < n_labels; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_IP], i, query);
        ASSERT_NEAR(dist, distances[i], 1e-5);
    }

    // VecSimMetric_Cosine
    // distances are [[5.960e-08, 5.960e-08], [0.0026, 1.000]]
    // minimum of each label are:
    distances = {5.9604644775390625e-08, 0.0025991201400756836};
    for (size_t i = 0; i < n_labels; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_Cosine], i, norm);
        ASSERT_NEAR(dist, distances[i], 1e-5);
    }

    // Bad values
    dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_Cosine], -1, norm);
    ASSERT_TRUE(std::isnan(dist));
    dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_L2], 46, query);
    ASSERT_TRUE(std::isnan(dist));

    // Clean-up.
    for (size_t i = 0; i < numIndex; i++) {
        VecSimIndex_Free(index[i]);
    }
}

TYPED_TEST(BruteForceMultiTest, testCosine) {
    size_t dim = 128;
    size_t n = 100;

    BFParams params = {.dim = dim, .metric = VecSimMetric_Cosine};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 1; i <= n; i++) {
        TEST_DATA_T f[dim];
        f[0] = (TEST_DATA_T)i / n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        VecSimIndex_AddVector(index, f, i);
    }
    // Add more worst vector for each label
    for (TEST_DATA_T i = 1; i <= n; i++) {
        TEST_DATA_T f[dim];
        f[0] = (TEST_DATA_T)i + n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        VecSimIndex_AddVector(index, f, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), 2 * n);
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim);

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

    // Test with batch iterator.
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    size_t iteration_num = 0;

    // get the 10 vectors whose ids are the maximal among those that hasn't been returned yet,
    // in every iteration. The order should be from the largest to the lowest id.
    size_t n_res = 10;
    while (VecSimBatchIterator_HasNext(batchIterator)) {
        std::vector<size_t> expected_ids(n_res);
        auto verify_res_batch = [&](size_t id, double score, size_t result_rank) {
            ASSERT_EQ(id, (n - n_res * iteration_num - result_rank));
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

TYPED_TEST(BruteForceMultiTest, testSizeEstimation) {
    size_t dim = 128;
    size_t n = 0;
    size_t bs = DEFAULT_BLOCK_SIZE;

    BFParams params = {.dim = dim, .metric = VecSimMetric_Cosine, .blockSize = bs};

    VecSimIndex *index = this->CreateNewIndex(params);

    // EstimateInitialSize is called after CreateNewIndex because params struct is
    // changed in CreateNewIndex.
    size_t estimation = EstimateInitialSize(params);

    size_t actual = index->getAllocationSize();
    ASSERT_EQ(estimation, actual);

    estimation = EstimateElementSize(params) * bs;
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 0);
    actual = index->getAllocationSize() - actual; // get the delta

    ASSERT_GE(estimation * 1.01, actual);
    ASSERT_LE(estimation * 0.99, actual);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, testTimeoutReturn) {
    size_t dim = 4;
    VecSimQueryReply *rep;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = 5};

    VecSimIndex *index = this->CreateNewIndex(params);

    GenerateAndAddVector<TEST_DATA_T>(index, dim, 0, 1.0);

    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 1; }); // Always times out

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 1.0);
    // Checks return code on timeout - knn
    rep = VecSimIndex_TopKQuery(index, query, 1, NULL, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(rep), VecSim_QueryReply_TimedOut);
    ASSERT_EQ(VecSimQueryReply_Len(rep), 0);
    VecSimQueryReply_Free(rep);

    // Check timeout again - range query
    rep = VecSimIndex_RangeQuery(index, query, 1, NULL, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(rep), VecSim_QueryReply_TimedOut);
    ASSERT_EQ(VecSimQueryReply_Len(rep), 0);
    VecSimQueryReply_Free(rep);

    VecSimIndex_Free(index);
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 0; }); // cleanup
}

TYPED_TEST(BruteForceMultiTest, testTimeoutReturn_batch_iterator) {
    size_t dim = 4;
    size_t n = 10;
    VecSimQueryReply *rep;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = 5};

    VecSimIndex *index = this->CreateNewIndex(params);

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

TYPED_TEST(BruteForceMultiTest, rangeQuery) {
    size_t n_labels = 1000;
    size_t per_label = 5;
    size_t dim = 4;

    size_t n = n_labels * per_label;

    BFParams params{.dim = dim, .metric = VecSimMetric_L2, .blockSize = n / 2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n_labels; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
        // Add some vectors, worst than the second loop (for the given query)
        for (size_t j = 0; j < per_label - 1; j++)
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, (TEST_DATA_T)i + n);
    }

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    size_t pivot_id = n_labels / 2; // The id to return vectors around it.
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, pivot_id);

    // Validate invalid params are caught with runtime exception.
    try {
        VecSimIndex_RangeQuery(index, query, -1, nullptr, BY_SCORE);
        FAIL();
    } catch (std::runtime_error const &err) {
        EXPECT_EQ(err.what(), std::string("radius must be non-negative"));
    }
    try {
        VecSimIndex_RangeQuery(index, query, 1, nullptr, VecSimQueryReply_Order(2));
        FAIL();
    } catch (std::runtime_error const &err) {
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

    // Get results by id.
    auto verify_res_by_id = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, pivot_id - expected_num_results / 2 + index);
        ASSERT_EQ(score, dim * pow(std::abs(int(id - pivot_id)), 2));
    };
    runRangeQueryTest(index, query, radius, verify_res_by_id, expected_num_results);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, relabelVector) {
    size_t dim = 4;
    size_t per_label = 3;
    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    auto *bf_index = this->CastToBF_Multi(index);

    // Two labels, each holding several vectors.
    for (size_t i = 0; i < per_label; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 0, i);
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, i + 10);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label * 2);
    ASSERT_EQ(index->indexLabelCount(), 2);

    std::vector<std::vector<TEST_DATA_T>> before;
    bf_index->getDataByLabel(0, before);
    ASSERT_EQ(before.size(), per_label);

    ASSERT_EQ(VecSimIndex_RelabelVector(index, 0, 100), VecSimRelabel_OK);

    // Every vector under the label moves together, keeping its data and order, and the untouched
    // label is unaffected.
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label * 2);
    ASSERT_EQ(index->indexLabelCount(), 2);
    std::vector<std::vector<TEST_DATA_T>> after;
    bf_index->getDataByLabel(100, after);
    ASSERT_EQ(after.size(), per_label);
    for (size_t i = 0; i < per_label; i++) {
        CompareVectors(before[i].data(), after[i].data(), dim);
    }
    std::vector<std::vector<TEST_DATA_T>> other;
    bf_index->getDataByLabel(1, other);
    ASSERT_EQ(other.size(), per_label);

    // The old label is gone and the new one is searchable.
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0);
    ASSERT_TRUE(std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(index, 0, query)));
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, 100, query), 0);
    auto verify_res = [&](size_t id, double score, size_t rank) { ASSERT_EQ(id, 100); };
    runTopKSearchTest(index, query, 1, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, relabelVectorRejects) {
    size_t dim = 4;
    size_t per_label = 2;
    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    auto *bf_index = this->CastToBF_Multi(index);

    for (size_t i = 0; i < per_label; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 0, i);
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, i + 10);
    }

    // A missing source, an occupied target and a no-op move are all rejected. In a multi index an
    // accepted move onto an occupied label would silently merge two labels' vectors, so this is the
    // case that matters most here.
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 42, 100), VecSimRelabel_OldLabelMissing);
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 0, 1), VecSimRelabel_NewLabelTaken);
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 0, 0), VecSimRelabel_SameLabel);
    // An absent source outranks an occupied target: a caller that resolves the conflict by
    // freeing the target must not be sent down that path for a move with nothing to move.
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 42, 1), VecSimRelabel_OldLabelMissing);

    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label * 2);
    ASSERT_EQ(index->indexLabelCount(), 2);
    for (labelType label : {0, 1}) {
        std::vector<std::vector<TEST_DATA_T>> data;
        bf_index->getDataByLabel(label, data);
        ASSERT_EQ(data.size(), per_label) << "label " << label << " was modified";
    }

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceMultiTest, updateVectorsMulti) {
    size_t dim = 4;
    size_t per_label = 3;
    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimIndex *index = this->CreateNewIndex(params);
    // Held as the base type: `getElementIds` is public there, and the derived override is not.
    BruteForceIndex<TEST_DATA_T, TEST_DIST_T> *bf_index = this->CastToBF_Multi(index);

    // Label 0 holds the values 0, 1, 2 and label 1 holds 10, 11, 12.
    for (size_t i = 0; i < per_label; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 0, i);
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, i + 10);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label * 2);
    ASSERT_EQ(index->indexLabelCount(), 2);

    // Replace label 0's three vectors with two different ones. How many the label ends up holding
    // is decided by the update, not by what was there - which `addVector` cannot express here,
    // since in a multi-value index it appends.
    TEST_DATA_T two[2 * dim];
    GenerateVector<TEST_DATA_T>(two, dim, 100);
    GenerateVector<TEST_DATA_T>(two + dim, dim, 101);
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 0, two, 2), VecSimUpdate_OK);

    ASSERT_EQ(bf_index->getElementIds(0).size(), 2);
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label + 2);
    ASSERT_EQ(index->indexLabelCount(), 2);

    // The label holds the two new vectors and none of the three it held.
    // (`GetDistanceFrom_Unsafe` reports the closest vector of the label, so a zero distance means
    // the label holds that exact vector.)
    for (size_t value : {100, 101}) {
        TEST_DATA_T stored[dim];
        GenerateVector<TEST_DATA_T>(stored, dim, value);
        ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, 0, stored), 0) << "missing " << value;
    }
    for (size_t value : {0, 1, 2}) {
        TEST_DATA_T gone[dim];
        GenerateVector<TEST_DATA_T>(gone, dim, value);
        ASSERT_NE(VecSimIndex_GetDistanceFrom_Unsafe(index, 0, gone), 0) << "kept " << value;
    }

    // The other label is untouched throughout - the removals it takes to replace one label's
    // contents swap ids around, so this is the part worth pinning.
    ASSERT_EQ(bf_index->getElementIds(1).size(), per_label);
    for (size_t value : {10, 11, 12}) {
        TEST_DATA_T other[dim];
        GenerateVector<TEST_DATA_T>(other, dim, value);
        ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, 1, other), 0) << "lost " << value;
    }

    auto verify_res = [&](size_t id, double score, size_t rank) {
        ASSERT_EQ(id, 0);
        ASSERT_EQ(score, 0);
    };
    runTopKSearchTest(index, two, 1, verify_res);

    // Growing the label works the same way.
    TEST_DATA_T four[4 * dim];
    for (size_t i = 0; i < 4; i++) {
        GenerateVector<TEST_DATA_T>(four + i * dim, dim, 200 + i);
    }
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 0, four, 4), VecSimUpdate_OK);
    ASSERT_EQ(bf_index->getElementIds(0).size(), 4);
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label + 4);
    ASSERT_EQ(index->indexLabelCount(), 2);

    // And an empty update removes the label altogether.
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 0, nullptr, 0), VecSimUpdate_OK);
    ASSERT_TRUE(bf_index->getElementIds(0).empty());
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label);
    ASSERT_EQ(index->indexLabelCount(), 1);
    ASSERT_EQ(bf_index->getElementIds(1).size(), per_label);

    VecSimIndex_Free(index);
}
