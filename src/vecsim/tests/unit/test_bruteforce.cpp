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
#include "VecSim/algorithms/brute_force/brute_force.h"
#include "VecSim/algorithms/brute_force/brute_force_single.h"
#include "cpu_features_macros.h"
#include <cmath>

template <typename index_type_t>
class BruteForceTest : public ::testing::Test {
public:
    using data_t = typename index_type_t::data_t;
    using dist_t = typename index_type_t::dist_t;

protected:
    VecSimIndex *CreateNewIndex(BFParams &params) {
        // is_multi = false by default.
        return test_utils::CreateNewIndex(params, index_type_t::get_index_type());
    }

    BruteForceIndex_Single<data_t, dist_t> *CastToBF_Single(VecSimIndex *index) {
        return reinterpret_cast<BruteForceIndex_Single<data_t, dist_t> *>(index);
    }
    BruteForceIndex<data_t, dist_t> *CastToBF(VecSimIndex *index) {
        return reinterpret_cast<BruteForceIndex<data_t, dist_t> *>(index);
    }
};

// DataTypeSet, TEST_DATA_T and TEST_DIST_T are defined in unit_test_utils.h

TYPED_TEST_SUITE(BruteForceTest, DataTypeSet);

// An absent label appends nothing rather than throwing. `labelToIdLookup.at()` used to be
// used here, so asking about a label the index does not hold was an exception; the contract
// now lets the output size answer whether it is held, which is what the tiered lookup and
// RediSearch's vector comparison both rely on.
TYPED_TEST(BruteForceTest, getDataByLabelAbsentLabel) {
    size_t dim = 4;
    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    BruteForceIndex<TEST_DATA_T, TEST_DIST_T> *bf_index = this->CastToBF(index);

    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1);

    std::vector<std::vector<TEST_DATA_T>> stored;
    bf_index->getDataByLabel(2, stored);
    ASSERT_TRUE(stored.empty());

    bf_index->getDataByLabel(1, stored);
    ASSERT_EQ(stored.size(), 1);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, brute_force_vector_add_test) {

    size_t dim = 4;

    BFParams params = {.dim = dim, .metric = VecSimMetric_IP};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 1);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, brute_force_vector_update_test) {
    size_t dim = 4;
    size_t n = 1;

    BFParams params = {.dim = dim, .metric = VecSimMetric_IP};

    VecSimIndex *index = this->CreateNewIndex(params);

    BruteForceIndex<TEST_DATA_T, TEST_DIST_T> *bf_index = this->CastToBF(index);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 1);

    // Prepare new vector data and call addVector with the same id, different data.
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 2.0);

    // Index size shouldn't change.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 1);

    // The idTolabel mapping size should be aligned with the current index *capacity* (not its size)
    // hence, it is the default block size that was allocated at the first insertion.
    ASSERT_EQ(bf_index->idToLabelMapping.size(), DEFAULT_BLOCK_SIZE);

    // Check update.
    const TEST_DATA_T *vector_data = bf_index->getDataByInternalId(0);
    for (size_t i = 0; i < dim; ++i) {
        ASSERT_EQ(*vector_data, 2.0);
        ++vector_data;
    }

    // Delete the last vector.
    VecSimIndex_DeleteVector(index, 1);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // VectorBlocks vector is empty.
    ASSERT_EQ(bf_index->vectors->size(), 0);
    // idTolabel size should also decrease to zero.
    ASSERT_EQ(bf_index->idToLabelMapping.size(), 0);

    BruteForceIndex_Single<TEST_DATA_T, TEST_DIST_T> *bf_single_index =
        this->CastToBF_Single(index);

    // Label2id of the last vector doesn't exist.
    ASSERT_EQ(bf_single_index->labelToIdLookup.find(1), bf_single_index->labelToIdLookup.end());

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, brute_force_cosine_vector_overwrite_is_normalized) {
    size_t dim = 4;
    labelType label = 1;

    BFParams params = {.dim = dim, .metric = VecSimMetric_Cosine};
    VecSimIndex *index = this->CreateNewIndex(params);
    auto *bf_single_index = this->CastToBF_Single(index);

    TEST_DATA_T initial_vector[] = {1, 1, 1, 1};
    TEST_DATA_T replacement_vector[] = {2, 2, 2, 2};
    TEST_DATA_T normalized_replacement[dim];
    memcpy(normalized_replacement, replacement_vector, sizeof(replacement_vector));
    VecSim_Normalize(normalized_replacement, dim, TypeParam::get_index_type());

    VecSimIndex_AddVector(index, initial_vector, label);
    VecSimIndex_AddVector(index, replacement_vector, label);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 1);

    std::vector<std::vector<TEST_DATA_T>> stored_vectors;
    bf_single_index->getDataByLabel(label, stored_vectors);
    ASSERT_EQ(stored_vectors.size(), 1);
    ASSERT_NO_FATAL_FAILURE(CompareVectors(stored_vectors[0].data(), normalized_replacement, dim));

    VecSimIndex_Free(index);
}

/**** resizing cases ****/

TYPED_TEST(BruteForceTest, resize_and_align_index) {
    size_t dim = 4;
    size_t blockSize = 10;
    size_t curr_label = 0;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = blockSize};

    VecSimIndex *index = this->CreateNewIndex(params);

    BruteForceIndex<TEST_DATA_T, TEST_DIST_T> *bf_index = this->CastToBF(index);
    auto verify_index_size = [&](size_t expected_size, size_t expected_capacity, std::string msg) {
        SCOPED_TRACE("verify_index_size: " + msg);
        ASSERT_EQ(VecSimIndex_IndexSize(index), expected_size);
        ASSERT_EQ(bf_index->idToLabelMapping.size(), expected_capacity);
        ASSERT_EQ(bf_index->indexCapacity(), expected_capacity);
        ASSERT_EQ(dynamic_cast<DataBlocksContainer *>(bf_index->vectors)->numBlocks(),
                  expected_size / blockSize + (expected_size % blockSize != 0))
            << "expected_size: " << expected_size << " expected_capacity: " << expected_capacity;
    };
    // Empty index with no initial capacity
    verify_index_size(0, 0, "empty index");

    // Add one vector, index capacity should grow to blockSize.
    GenerateAndAddVector<TEST_DATA_T>(index, dim, curr_label++);
    verify_index_size(1, blockSize, "add one vector");

    // Add vector up to blocksize, index capacity should remain the same.
    while (curr_label < blockSize) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, curr_label++);
    }
    verify_index_size(blockSize, blockSize, "add up to blocksize");

    // remove invalid id
    VecSimIndex_DeleteVector(index, 3459);

    // This should do nothing
    verify_index_size(blockSize, blockSize, "remove invalid id");

    // Add another vector, since index size equals to block size, this should cause resizing
    // of the capacity by one blocksize
    GenerateAndAddVector<TEST_DATA_T>(index, dim, curr_label++);
    verify_index_size(blockSize + 1, 2 * blockSize,
                      "add one more vector after reaching block size");

    // Now size = blocksize + 1 (= 11), capacity = 2 * bs (= 20). Test capacity overflow again
    // to check that it stays aligned with block size.
    size_t add_vectors_count = blockSize + 2; // 12
    while (curr_label < add_vectors_count + blockSize + 1) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, curr_label++);
    }

    // Size should be blocksize + 1 + add_vectors_count (= 23).
    verify_index_size(
        blockSize + 1 + add_vectors_count, 3 * blockSize,
        "add more vectors after reaching 2 * blocksize capacity to trigger another resize");

    // Delete vectors so that indexsize % blocksize == 0 (and then delete one more)
    size_t num_deleted = 0;
    auto remove_to_one_below_blocksize = [&](size_t initial_label_to_remove) {
        while (VecSimIndex_IndexSize(index) % blockSize != 0) {
            ASSERT_EQ(VecSimIndex_DeleteVector(index, initial_label_to_remove++), 1)
                << "tried to remove label: " << initial_label_to_remove - 1;
            num_deleted++;
        }
        VecSimIndex_DeleteVector(index, initial_label_to_remove);
        num_deleted++;
    };

    // First trigger of remove_to_one_below_blocksize will result in one free block.
    // This should not trigger shrinking of metadata containers.
    remove_to_one_below_blocksize(0); // remove first block labels.
    verify_index_size(
        blockSize * 2 - 1, 3 * blockSize,
        "delete vectors so that indexsize % blocksize == 0, but there is only one free block");

    // Second trigger of remove_to_one_below_blocksize will result in two free blocks.
    // This should trigger shrinking of metadata containers by one block.
    remove_to_one_below_blocksize(num_deleted);
    verify_index_size(
        blockSize - 1, 2 * blockSize,
        "delete vectors so that indexsize % blocksize == 0 and there are two free blocks");

    // Now there is one block in use and one free. adding vectors up to blocksize should not trigger
    // another resize.
    GenerateAndAddVector<TEST_DATA_T>(index, dim, curr_label++);
    verify_index_size(blockSize, 2 * blockSize,
                      "add vectors up to blocksize after deleting two blocks");

    // Delete all vectors.
    while (VecSimIndex_IndexSize(index) > 0) {
        VecSimIndex_DeleteVector(index, num_deleted++);
    }
    verify_index_size(0, 0, "delete all vectors");
    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, brute_force_no_oscillation_test) {
    size_t dim = 4;
    size_t blockSize = 2;
    size_t cycles = 5; // Number of add/delete cycles to test

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = blockSize};
    VecSimIndex *index = this->CreateNewIndex(params);
    BruteForceIndex<TEST_DATA_T, TEST_DIST_T> *bf_index = this->CastToBF(index);

    auto verify_no_oscillation = [&](size_t expected_size, size_t expected_capacity,
                                     const std::string &msg) {
        SCOPED_TRACE("verify_no_oscillation: " + msg);
        ASSERT_EQ(VecSimIndex_IndexSize(index), expected_size);
        ASSERT_EQ(bf_index->indexCapacity(), expected_capacity);
    };

    // Initial state: empty index
    verify_no_oscillation(0, 0, "initial empty state");

    size_t current_label = 0;

    // Add initial 3 blocks
    size_t initial_num_blocks = 3;
    for (size_t i = 0; i < initial_num_blocks * blockSize; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, current_label++);
    }
    verify_no_oscillation(initial_num_blocks * blockSize, initial_num_blocks * blockSize,
                          "initial " + std::to_string(initial_num_blocks) +
                              " blocks vectors added");

    // Perform oscillation cycles: delete block, add block, delete block, add block...
    for (size_t cycle = 0; cycle < cycles; cycle++) {
        // Delete blockSize vectors (size becomes blockSize, but capacity should remain 2 *
        // blockSize due to buffer zone)
        for (size_t i = 0; i < blockSize; i++) {
            VecSimIndex_DeleteVector(index, cycle * blockSize + i);
        }
        verify_no_oscillation((initial_num_blocks - 1) * blockSize, initial_num_blocks * blockSize,
                              "cycle " + std::to_string(cycle) +
                                  " - after deleting block of vectors");

        // Add blockSize vectors back (size becomes 2 * blockSize, capacity should remain 2 *
        // blockSize)
        for (size_t i = 0; i < blockSize; i++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, current_label++);
        }
        verify_no_oscillation(initial_num_blocks * blockSize, initial_num_blocks * blockSize,
                              "cycle " + std::to_string(cycle) +
                                  " - after adding blockSize vectors back");
    }

    // Final verification: delete enough vectors to trigger actual shrinking
    // Delete blocksize vectors to have only one block of vectors (2 free blocks = shrinking
    // condition)
    size_t vectors_to_delete = 2 * blockSize;
    for (size_t i = 0; i < vectors_to_delete; i++) {
        VecSimIndex_DeleteVector(index, cycles * blockSize + i);
    }
    verify_no_oscillation(blockSize, 2 * blockSize,
                          "final shrinking to trigger shrinking by one block");

    // Verify we can still grow normally after the oscillation test
    for (size_t i = 0; i < blockSize; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, current_label++);
    }
    verify_no_oscillation(2 * blockSize, 2 * blockSize, "growth after oscillation test");

    VecSimIndex_Free(index);
}

// Test empty index edge cases.
TYPED_TEST(BruteForceTest, brute_force_empty_index) {
    size_t dim = 4;
    size_t n = 20;
    size_t bs = 6;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = bs};

    VecSimIndex *index = this->CreateNewIndex(params);

    BruteForceIndex<TEST_DATA_T, TEST_DIST_T> *bf_index = this->CastToBF(index);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);
    size_t expected_capacity = 0;
    ASSERT_EQ(expected_capacity, bf_index->idToLabelMapping.size());

    // Try to remove from an empty index - should fail because label doesn't exist.
    VecSimIndex_DeleteVector(index, 0);

    // Add one vector.
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 1.7);

    // Try to remove it.
    VecSimIndex_DeleteVector(index, 1);
    ASSERT_EQ(expected_capacity, bf_index->idToLabelMapping.size());

    // Size equals 0.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Try to remove it again.
    // The idToLabelMapping_size should remain unchanged, as we are trying to delete a label that
    // doesn't exist.
    VecSimIndex_DeleteVector(index, 1);
    ASSERT_EQ(expected_capacity, bf_index->idToLabelMapping.size());
    // Nor the size.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, brute_force_vector_search_by_id_test) {
    size_t n = 100;
    size_t k = 11;
    size_t dim = 4;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    TEST_DATA_T query[] = {50, 50, 50, 50};
    auto verify_res = [&](size_t id, double score, size_t index) { ASSERT_EQ(id, (index + 45)); };
    runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, brute_force_indexing_same_vector) {
    size_t n = 100;
    size_t k = 10;
    size_t dim = 4;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i,
                                          i / 10); // i / 10 is in integer (take the "floor" value).
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Run a query where all the results are supposed to be {5,5,5,5} (different ids).
    TEST_DATA_T query[] = {4.9, 4.95, 5.05, 5.1};
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_TRUE(id >= 50 && id < 60 && score <= 1);
    };
    runTopKSearchTest(index, query, k, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, brute_force_reindexing_same_vector) {
    size_t n = 100;
    size_t k = 10;
    size_t dim = 4;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    BruteForceIndex<TEST_DATA_T, TEST_DIST_T> *bf_index = this->CastToBF(index);

    for (size_t i = 0; i < n; i++) {
        // i / 10 is in integer (take the "floor" value).
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i / 10);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Run a query where all the results are supposed to be {5,5,5,5} (different ids).
    TEST_DATA_T query[] = {4.9, 4.95, 5.05, 5.1};
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_TRUE(id >= 50 && id < 60 && score <= 1);
    };
    runTopKSearchTest(index, query, k, verify_res);

    // Delete all vectors.
    for (size_t i = 0; i < n; i++) {
        VecSimIndex_DeleteVector(index, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // The vector block should be removed.
    ASSERT_EQ(bf_index->getVectorsContainer()->size(), 0);

    // id2label size and capacity should turn to zero.
    ASSERT_EQ(bf_index->idToLabelMapping.size(), 0);

    // Reinsert the same vectors under the same ids.
    for (size_t i = 0; i < n; i++) {
        // i / 10 is in integer (take the "floor value).
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i / 10);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Run the same query again.
    runTopKSearchTest(index, query, k, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, brute_force_reindexing_same_vector_different_id) {
    size_t n = 100;
    size_t k = 10;
    size_t dim = 4;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i,
                                          i / 10); // i / 10 is in integer (take the "floor" value).
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

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
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i + 10,
                                          i / 10); // i / 10 is in integer (take the "floor" value).
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Run the same query again.
    auto verify_res_different_id = [&](size_t id, double score, size_t index) {
        ASSERT_TRUE(id >= 60 && id < 70 && score <= 1);
    };
    runTopKSearchTest(index, query, k, verify_res_different_id);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, test_delete_swap_block) {
    size_t k = 5;
    size_t dim = 2;
    size_t block_size = 3;

    // This test creates 2 vector blocks with size of 3
    // Insert 6 vectors with ascending ids; The vector blocks will look like
    // 0 [0, 1, 2]
    // 1 [3, 4, 5]
    // Delete the id 1 will delete it from the first vector block 0 [0 ,1, 2] and will move vector
    // data of id 5 to vector block 0 at index 1. id2label[1] should hold the label of the vector
    // that was in id 5.

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = block_size};

    VecSimIndex *index = this->CreateNewIndex(params);

    BruteForceIndex<TEST_DATA_T, TEST_DIST_T> *bf_index = this->CastToBF(index);

    // idToLabelMapping initial size is aligned with block size.
    ASSERT_EQ(bf_index->idToLabelMapping.size(), 0);

    size_t n = 6;
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    labelType id1_prev_label = bf_index->getVectorLabel(1);
    labelType id5_prev_label = bf_index->getVectorLabel(5);

    // Here the shift should happen.
    VecSimIndex_DeleteVector(index, 1);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n - 1);
    // id2label size should remain unchanged.
    ASSERT_EQ(bf_index->idToLabelMapping.size(), n);

    // id1 gets what was previously id5's label.
    ASSERT_EQ(bf_index->getVectorLabel(1), id5_prev_label);

    BruteForceIndex_Single<TEST_DATA_T, TEST_DIST_T> *bf_single_index =
        this->CastToBF_Single(index);

    // label2id value at label5 should be 1
    auto last_vector_new_id = bf_single_index->labelToIdLookup[id5_prev_label];
    ASSERT_EQ(last_vector_new_id, 1);

    // The label of what initially was in id1 should be removed.
    auto deleted_label_id_pair = bf_single_index->labelToIdLookup.find(id1_prev_label);
    ASSERT_EQ(deleted_label_id_pair, bf_single_index->labelToIdLookup.end());

    // The vector in index1 should hold id5 data.
    const TEST_DATA_T *vector_data = bf_index->getDataByInternalId(1);
    for (size_t i = 0; i < dim; ++i) {
        ASSERT_EQ(*vector_data, 5);
        ++vector_data;
    }

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

#ifdef CPU_FEATURES_ARCH_X86_64
TYPED_TEST(BruteForceTest, AlignmentSanity) {
    BFParams params = {.dim = 512};
    VecSimIndex *index = this->CreateNewIndex(params);
    auto *bf = this->CastToBF(index);
    // Assuming we have some optimizations (at least SSE), the alignment should be non-zero
    // (Register byte size)
    ASSERT_NE(bf->getStorageAlignment(), 0);
    VecSimIndex_Free(index);
}
#endif

TYPED_TEST(BruteForceTest, sanity_reinsert_1280) {
    size_t n = 5;
    size_t d = 1280;
    size_t k = 5;

    BFParams params = {.dim = d, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

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

TYPED_TEST(BruteForceTest, test_bf_info) {
    size_t n = 100;
    size_t d = 128;

    // Build with default args.

    BFParams params = {.dim = d, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_BF);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, d);
    ASSERT_FALSE(info.commonInfo.basicInfo.isMulti);
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

TYPED_TEST(BruteForceTest, test_basic_bf_info_iterator) {
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

TYPED_TEST(BruteForceTest, test_dynamic_bf_info_iterator) {
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
    // Add vector.
    VecSimIndex_AddVector(index, v, 0);
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(1, info.commonInfo.indexSize);
    compareFlatIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    // Delete vector.
    VecSimIndex_DeleteVector(index, 0);
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(0, info.commonInfo.indexSize);
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
    this->CastToBF(index)->count = 1e4;
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

TYPED_TEST(BruteForceTest, debugInfoIteratorFieldOrder) {
    namespace expected_output = test_utils::test_debug_info_iterator_order;

    size_t d = 4;
    BFParams params = {.dim = d, .metric = VecSimMetric_L2, .blockSize = 1};
    VecSimIndex *index = this->CreateNewIndex(params);

    // Add a vector to ensure the index is not empty
    GenerateAndAddVector<TEST_DATA_T>(index, d, 1, 1);

    VecSimDebugInfoIterator *infoIterator = VecSimIndex_DebugInfoIterator(index);

    // Test the field order using the common function
    expected_output::testDebugInfoIteratorFieldOrder(infoIterator,
                                                     expected_output::getFlatFields());

    VecSimDebugInfoIterator_Free(infoIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, brute_force_vector_search_test_ip) {
    size_t dim = 4;
    size_t n = 100;
    size_t k = 11;

    for (size_t blocksize : {1, 12, DEFAULT_BLOCK_SIZE}) {

        BFParams params = {.dim = dim, .metric = VecSimMetric_IP, .blockSize = blocksize};

        VecSimIndex *index = this->CreateNewIndex(params);

        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_BF);
        ASSERT_EQ(info.commonInfo.basicInfo.blockSize, blocksize);

        for (size_t i = 0; i < n; i++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
        }
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

TYPED_TEST(BruteForceTest, brute_force_vector_search_test_l2) {
    size_t dim = 4;
    size_t n = 100;
    size_t k = 11;

    for (size_t blocksize : {1, 12, DEFAULT_BLOCK_SIZE}) {

        BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = blocksize};

        VecSimIndex *index = this->CreateNewIndex(params);

        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_BF);
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

TYPED_TEST(BruteForceTest, brute_force_search_empty_index) {
    size_t dim = 4;
    size_t n = 100;
    size_t k = 11;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

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

TYPED_TEST(BruteForceTest, brute_force_test_inf_score) {
    size_t n = 4;
    size_t k = 4;
    size_t dim = 2;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

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

TYPED_TEST(BruteForceTest, brute_force_remove_vector_after_replacing_block) {
    size_t dim = 4;
    size_t n = 2;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = 1};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Add 2 vectors, into 2 separated blocks.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // After deleting the first vector, the second one will be moved to the first block.
    for (size_t i = 0; i < n; i++) {
        VecSimIndex_DeleteVector(index, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, brute_force_zero_minimal_capacity) {
    size_t dim = 4;
    size_t n = 2;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = 1};

    VecSimIndex *index = this->CreateNewIndex(params);

    BruteForceIndex<TEST_DATA_T, TEST_DIST_T> *bf_index = this->CastToBF(index);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Add 2 vectors, into 2 separated blocks.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // id2label size should be the same as index size.
    ASSERT_EQ(bf_index->idToLabelMapping.size(), n);

    // After deleting the first vector, the second one will be moved to the first block.
    for (size_t i = 0; i < n; i++) {
        VecSimIndex_DeleteVector(index, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);
    // id2label size should be the same as index size
    ASSERT_EQ(bf_index->idToLabelMapping.size(), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, brute_force_batch_iterator) {
    size_t dim = 4;

    // run the test twice - for index of size 100, every iteration will run select-based search,
    // as the number of results is 5, which is more than 0.1% of the index size. for index of size
    // 10000, we will run the heap-based search until we return 5000 results, and then switch to
    // select-based search.
    for (size_t n : {100, 10000}) {
        BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = 5};

        VecSimIndex *index = this->CreateNewIndex(params);
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
        size_t n_res = 5;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            std::vector<size_t> expected_ids(n_res);
            for (size_t i = 0; i < n_res; i++) {
                expected_ids[i] = (n - iteration_num * n_res - i - 1);
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_TRUE(expected_ids[index] == id);
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
            iteration_num++;
        }
        ASSERT_EQ(iteration_num, n / n_res);
        VecSimBatchIterator_Free(batchIterator);

        VecSimIndex_Free(index);
    }
}

TYPED_TEST(BruteForceTest, brute_force_batch_iterator_non_unique_scores) {
    size_t dim = 4;

    // Run the test twice - for index of size 100, every iteration will run select-based search,
    // as the number of results is 5, which is more than 0.1% of the index size. for index of size
    // 10000, we will run the heap-based search until we return 5000 results, and then switch to
    // select-based search.
    for (size_t n : {100, 10000}) {
        BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = 5};
        VecSimIndex *index = this->CreateNewIndex(params);

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

TYPED_TEST(BruteForceTest, brute_force_batch_iterator_reset) {
    size_t dim = 4;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = 100000};

    VecSimIndex *index = this->CreateNewIndex(params);

    size_t n = 10000;
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
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
            for (size_t i = 1; i <= n_res; i++) {
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

TYPED_TEST(BruteForceTest, brute_force_batch_iterator_corner_cases) {
    size_t dim = 4;
    size_t n = 1000;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

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
    res = VecSimBatchIterator_Next(batchIterator, 1, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);
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

TYPED_TEST(BruteForceTest, brute_force_resolve_params) {
    size_t dim = 4;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = 5};

    VecSimIndex *index = this->CreateNewIndex(params);

    VecSimQueryParams qparams, zero;
    bzero(&zero, sizeof(VecSimQueryParams));
    const char *err_msg = nullptr;

    std::vector<VecSimRawParam> rparams;

    // EPSILON is not a valid parameter for BF index.
    rparams.push_back(VecSimRawParam{"epsilon", strlen("epsilon"), "0.1", strlen("0.1")});

    for (VecsimQueryType query_type : test_utils::query_types) {
        ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                            query_type, &err_msg),
                  VecSimParamResolverErr_UnknownParam);
        ASSERT_STREQ(err_msg, "EPSILON is only valid for HNSW or SVS indexes");
    }
    // SVS runtime params are not valid parameters for BF index.
    rparams[0] = {.name = "search_window_size",
                  .nameLen = strlen("search_window_size"),
                  .value = "100",
                  .valLen = strlen("100")};

    for (VecsimQueryType query_type : test_utils::query_types) {
        ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                            query_type, &err_msg),
                  VecSimParamResolverErr_UnknownParam);
        ASSERT_STREQ(err_msg, "SEARCH_WINDOW_SIZE is only valid for SVS indexes");
    }
    rparams[0] = {.name = "search_buffer_capacity",
                  .nameLen = strlen("search_buffer_capacity"),
                  .value = "100",
                  .valLen = strlen("100")};

    for (VecsimQueryType query_type : test_utils::query_types) {
        ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                            query_type, &err_msg),
                  VecSimParamResolverErr_UnknownParam);
        ASSERT_STREQ(err_msg, "SEARCH_BUFFER_CAPACITY is only valid for SVS indexes");
    }
    rparams[0] = {.name = "use_search_history",
                  .nameLen = strlen("use_search_history"),
                  .value = "on",
                  .valLen = strlen("on")};

    for (VecsimQueryType query_type : test_utils::query_types) {
        ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                            query_type, &err_msg),
                  VecSimParamResolverErr_UnknownParam);
        ASSERT_STREQ(err_msg, "USE_SEARCH_HISTORY is only valid for SVS indexes");
    }
    // EF_RUNTIME is not a valid parameter for BF index.
    rparams[0] = {.name = "ef_runtime",
                  .nameLen = strlen("ef_runtime"),
                  .value = "200",
                  .valLen = strlen("200")};

    for (VecsimQueryType query_type : test_utils::query_types) {
        ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                            query_type, &err_msg),
                  VecSimParamResolverErr_UnknownParam);
        ASSERT_STREQ(err_msg, "EF_RUNTIME is only valid for HNSW indexes");
    }
    /** Testing with hybrid query params - cases which are only relevant for BF flat index. **/
    // Sending only "batch_size" param is valid.
    rparams.push_back(VecSimRawParam{"batch_size", strlen("batch_size"), "100", strlen("100")});
    ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data() + 1, 1, &qparams, QUERY_TYPE_HYBRID,
                                        nullptr),
              VecSim_OK);
    ASSERT_EQ(qparams.batchSize, 100);

    // With EF_RUNTIME, its again invalid (for hybrid queries as well).
    for (VecsimQueryType query_type : test_utils::query_types) {
        ASSERT_EQ(VecSimIndex_ResolveParams(index, rparams.data(), rparams.size(), &qparams,
                                            query_type, nullptr),
                  VecSimParamResolverErr_UnknownParam);
    }
    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, brute_get_distance) {
    size_t n = 4;
    size_t dim = 2;
    size_t numIndex = 3;
    VecSimIndex *index[numIndex];
    std::vector<double> distances;

    TEST_DATA_T v1[] = {M_PI, M_PI};
    TEST_DATA_T v2[] = {M_E, M_E};
    TEST_DATA_T v3[] = {M_PI, M_E};
    TEST_DATA_T v4[] = {M_SQRT2, -M_SQRT2};

    BFParams params = {.dim = dim};

    for (size_t i = 0; i < numIndex; i++) {
        params.metric = (VecSimMetric)i;
        index[i] = this->CreateNewIndex(params);
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

    // VecSimMetric_L2
    distances = {0, 0.3583844006061554, 0.1791922003030777, 23.739208221435547};
    for (size_t i = 0; i < n; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_L2], i + 1, query);
        ASSERT_NEAR(dist, distances[i], 1e-5);
    }

    // VecSimMetric_IP
    distances = {-18.73921012878418, -16.0794677734375, -17.409339904785156, 1};
    for (size_t i = 0; i < n; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_IP], i + 1, query);
        ASSERT_NEAR(dist, distances[i], 1e-5);
    }

    // VecSimMetric_Cosine
    distances = {5.9604644775390625e-08, 5.9604644775390625e-08, 0.0025991201400756836, 1};
    for (size_t i = 0; i < n; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_Cosine], i + 1, norm);
        ASSERT_NEAR(dist, distances[i], 1e-5);
    }

    // Bad values
    dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_Cosine], 0, norm);
    ASSERT_TRUE(std::isnan(dist));
    dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_L2], 46, query);
    ASSERT_TRUE(std::isnan(dist));

    // Clean-up.
    for (size_t i = 0; i < numIndex; i++) {
        VecSimIndex_Free(index[i]);
    }
}

TYPED_TEST(BruteForceTest, preferAdHocOptimization) {
    // Save the expected ratio which is the threshold between ad-hoc and batches mode
    // for every combination of index size and dim.
    std::map<std::pair<size_t, size_t>, float> threshold;
    threshold[{1000, 4}] = threshold[{1000, 80}] = threshold[{1000, 350}] = threshold[{1000, 780}] =
        1.0;
    threshold[{6000, 4}] = 0.2;
    threshold[{6000, 80}] = 0.4;
    threshold[{6000, 350}] = 0.6;
    threshold[{6000, 780}] = 0.8;
    threshold[{600000, 4}] = threshold[{600000, 80}] = 0.2;
    threshold[{600000, 350}] = 0.6;
    threshold[{600000, 780}] = 0.8;

    for (size_t index_size : {1000, 6000, 600000}) {
        for (size_t dim : {4, 80, 350, 780}) {
            // Create index and check for the expected output of "prefer ad-hoc".

            BFParams params = {.dim = dim, .metric = VecSimMetric_IP};

            VecSimIndex *index = this->CreateNewIndex(params);

            // Set the index size artificially to be the required one.
            (this->CastToBF(index))->count = index_size;
            ASSERT_EQ(VecSimIndex_IndexSize(index), index_size);
            for (float r : {0.1f, 0.3f, 0.5f, 0.7f, 0.9f}) {
                bool res = VecSimIndex_PreferAdHocSearch(index, (size_t)(r * index_size), 50, true);
                // If r is below the threshold for this specific configuration of (index_size, dim),
                // expect that result will be ad-hoc (i.e., true), and otherwise, batches (i.e.,
                // false)
                bool expected_res = r < threshold[{index_size, dim}];
                ASSERT_EQ(res, expected_res);
            }
            VecSimIndex_Free(index);
        }
    }
    // Corner cases - empty index.

    BFParams params = {.dim = 4, .metric = VecSimMetric_IP};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_TRUE(VecSimIndex_PreferAdHocSearch(index, 0, 50, true));

    // Corner cases - subset size is greater than index size.
    ASSERT_EQ(VecSimIndex_PreferAdHocSearch(index, 42, 50, true),
              VecSimIndex_PreferAdHocSearch(index, 0, 50, true));

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, batchIteratorSwapIndices) {
    size_t dim = 4;
    size_t n = 10000;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .initialCapacity = n};

    VecSimIndex *index = this->CreateNewIndex(params);

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

TYPED_TEST(BruteForceTest, testCosine) {
    size_t dim = 128;
    size_t n = 100;

    BFParams params = {.dim = dim, .metric = VecSimMetric_Cosine, .initialCapacity = n};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 1; i <= n; i++) {
        TEST_DATA_T f[dim];
        f[0] = (TEST_DATA_T)i / n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        VecSimIndex_AddVector(index, f, i);
    }
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

TYPED_TEST(BruteForceTest, testSizeEstimation) {
    size_t dim = 128;
    size_t n = 0;
    size_t bs = DEFAULT_BLOCK_SIZE;

    BFParams params = {
        .dim = dim, .metric = VecSimMetric_Cosine, .initialCapacity = n, .blockSize = bs};

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

TYPED_TEST(BruteForceTest, testInitialSizeEstimationWithInitialCapacity) {
    size_t dim = 128;
    size_t n = 100;
    size_t bs = DEFAULT_BLOCK_SIZE;

    BFParams params = {
        .dim = dim, .metric = VecSimMetric_Cosine, .initialCapacity = n, .blockSize = bs};

    VecSimIndex *index = this->CreateNewIndex(params);
    // EstimateInitialSize is called after CreateNewIndex because params struct is
    // changed in CreateNewIndex.
    size_t estimation = EstimateInitialSize(params);

    size_t actual = index->getAllocationSize();
    ASSERT_EQ(estimation, actual);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, testTimeoutReturn) {
    size_t dim = 4;
    VecSimQueryReply *rep;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .initialCapacity = 1, .blockSize = 5};

    VecSimIndex *index = this->CreateNewIndex(params);

    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 1; }); // Always times out

    TEST_DATA_T vec[dim];
    GenerateVector<TEST_DATA_T>(vec, dim);

    VecSimIndex_AddVector(index, vec, 0);
    // Checks return code on timeout - knn
    rep = VecSimIndex_TopKQuery(index, vec, 1, NULL, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(rep), VecSim_QueryReply_TimedOut);
    ASSERT_EQ(VecSimQueryReply_Len(rep), 0);
    VecSimQueryReply_Free(rep);

    // Check timeout again - range query
    rep = VecSimIndex_RangeQuery(index, vec, 1, NULL, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(rep), VecSim_QueryReply_TimedOut);
    ASSERT_EQ(VecSimQueryReply_Len(rep), 0);
    VecSimQueryReply_Free(rep);

    VecSimIndex_Free(index);
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 0; }); // cleanup
}

TYPED_TEST(BruteForceTest, testTimeoutReturn_batch_iterator) {
    size_t dim = 4;
    size_t n = 10;
    VecSimQueryReply *rep;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .initialCapacity = n, .blockSize = 5};

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

TYPED_TEST(BruteForceTest, rangeQuery) {
    size_t n = 2000;
    size_t dim = 4;

    BFParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = n / 2};

    VecSimIndex *index = this->CreateNewIndex(params);

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

TYPED_TEST(BruteForceTest, rangeQueryCosine) {
    size_t n = 100;
    size_t dim = 4;

    BFParams params = {.dim = dim, .metric = VecSimMetric_Cosine, .blockSize = n / 2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T f[dim];
        f[0] = TEST_DATA_T(i + 1) / n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        // Use as label := n - (internal id)
        VecSimIndex_AddVector(index, (const void *)f, n - i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    TEST_DATA_T query[dim];
    TEST_DATA_T normalized_query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);
    GenerateVector<TEST_DATA_T>(normalized_query, dim, n);
    auto verify_res = [&](size_t id, double score, size_t result_rank) {
        ASSERT_EQ(id, result_rank + 1);
        double expected_score = index->getDistanceFrom_Unsafe(id, normalized_query);
        // Verify that abs difference between the actual and expected score is at most 1/10^5.
        ASSERT_EQ(score, expected_score);
    };

    uint expected_num_results = 31;
    // Calculate the score of the 31st distant vector from the query vector (whose id should be 30)
    // to get the radius.
    VecSim_Normalize(normalized_query, dim, params.type);
    double radius = index->getDistanceFrom_Unsafe(31, normalized_query);
    runRangeQueryTest(index, query, radius, verify_res, expected_num_results, BY_SCORE);
    // Return results BY_ID should give the same results.
    runRangeQueryTest(index, query, radius, verify_res, expected_num_results, BY_ID);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, FitMemoryTest) {
    size_t dim = 4;
    BFParams params = {.dim = dim, .blockSize = DEFAULT_BLOCK_SIZE};
    VecSimIndex *index = this->CreateNewIndex(params);

    size_t initial_memory = index->getAllocationSize();
    index->fitMemory();
    ASSERT_EQ(index->getAllocationSize(), initial_memory);

    // Add vector
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 0);
    initial_memory = index->getAllocationSize();
    index->fitMemory();
    size_t final_size = index->getAllocationSize();
    ASSERT_EQ(final_size, initial_memory);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, relabelVector) {
    size_t dim = 4;
    size_t n = 10;
    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }

    const labelType old_label = 3;
    const labelType new_label = 100;
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, old_label);
    // A relabel must not move the stored data, so the distance from the label's own vector stays 0.
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, old_label, query), 0);

    ASSERT_EQ(VecSimIndex_RelabelVector(index, old_label, new_label), VecSimRelabel_OK);

    // Nothing was added or removed, and the vector now answers to the new label only.
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, new_label, query), 0);
    ASSERT_TRUE(std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(index, old_label, query)));

    auto verify_res = [&](size_t id, double score, size_t rank) {
        ASSERT_EQ(id, new_label);
        ASSERT_EQ(score, 0);
    };
    runTopKSearchTest(index, query, 1, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, relabelVectorRejects) {
    size_t dim = 4;
    size_t n = 5;
    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }

    // A missing source, an occupied target and a no-op move are all rejected without modifying
    // the index.
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 42, 100), VecSimRelabel_OldLabelMissing);
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 1, 2), VecSimRelabel_NewLabelTaken);
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 1, 1), VecSimRelabel_SameLabel);
    // An absent source outranks an occupied target: a caller that resolves the conflict by
    // freeing the target must not be sent down that path for a move with nothing to move.
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 42, 1), VecSimRelabel_OldLabelMissing);

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n);
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T v[dim];
        GenerateVector<TEST_DATA_T>(v, dim, i);
        ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, i, v), 0) << "label " << i << " moved";
    }
    TEST_DATA_T probe[dim];
    GenerateVector<TEST_DATA_T>(probe, dim, 0);
    ASSERT_TRUE(std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(index, 100, probe)));

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, updateVectors) {
    size_t dim = 4;
    size_t n = 10;
    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    auto *bf_index = this->CastToBF_Single(index);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }

    const labelType label = 3;
    TEST_DATA_T old_vector[dim], new_vector[dim];
    GenerateVector<TEST_DATA_T>(old_vector, dim, label); // the value stored under `label`
    GenerateVector<TEST_DATA_T>(new_vector, dim, n + 5); // a value no other element holds
    const idType id_before = bf_index->getIdOfLabel(label);

    ASSERT_EQ(VecSimIndex_UpdateVectors(index, label, new_vector, 1), VecSimUpdate_OK);

    // A flat index derives nothing from the values, so replacing the one vector of a single-value
    // label is a write over its stored data: the id it sits on is kept, and nothing else moves.
    ASSERT_EQ(bf_index->getIdOfLabel(label), id_before);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, label, new_vector), 0);
    ASSERT_NE(VecSimIndex_GetDistanceFrom_Unsafe(index, label, old_vector), 0);

    // It is found by search at its new value, and a query at the old value finds its neighbours by
    // value instead.
    auto verify_new = [&](size_t id, double score, size_t rank) {
        ASSERT_EQ(id, label);
        ASSERT_EQ(score, 0);
    };
    runTopKSearchTest(index, new_vector, 1, verify_new);
    auto verify_old = [&](size_t id, double score, size_t rank) { ASSERT_NE(id, label); };
    runTopKSearchTest(index, old_vector, 1, verify_old);

    // A label the index does not hold yet is simply stored: with nothing to remove, the end state
    // the caller asked for is reached anyway.
    TEST_DATA_T fresh[dim];
    GenerateVector<TEST_DATA_T>(fresh, dim, n + 6);
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 100, fresh, 1), VecSimUpdate_OK);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n + 1);
    ASSERT_EQ(index->indexLabelCount(), n + 1);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, 100, fresh), 0);

    // An empty update leaves the label holding nothing, which is a delete.
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 100, nullptr, 0), VecSimUpdate_OK);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n);
    TEST_DATA_T probe[dim];
    GenerateVector<TEST_DATA_T>(probe, dim, n + 6);
    ASSERT_TRUE(std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(index, 100, probe)));

    VecSimIndex_Free(index);
}

TYPED_TEST(BruteForceTest, updateVectorsRejectsSeveralVectorsInSingle) {
    size_t dim = 4;
    size_t n = 5;
    BFParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }

    // Two vectors under one label is not a state this index can reach. Refused rather than served
    // by storing one of them, which would leave the caller believing both are there.
    TEST_DATA_T two_vectors[2 * dim];
    GenerateVector<TEST_DATA_T>(two_vectors, dim, 100);
    GenerateVector<TEST_DATA_T>(two_vectors + dim, dim, 101);
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 1, two_vectors, 2), VecSimUpdate_MultiNotSupported);

    // The refusal changed nothing - in particular it did not delete the label first.
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n);
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T expected[dim];
        GenerateVector<TEST_DATA_T>(expected, dim, i);
        ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index, i, expected), 0) << "label " << i;
    }

    VecSimIndex_Free(index);
}
