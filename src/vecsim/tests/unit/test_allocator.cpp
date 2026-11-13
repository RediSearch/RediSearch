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
#include "VecSim/memory/vecsim_malloc.h"
#include "VecSim/memory/vecsim_base.h"
#include "VecSim/algorithms/brute_force/brute_force_single.h"
#include "VecSim/algorithms/hnsw/hnsw_single.h"
#include "unit_test_utils.h"
#include "VecSim/utils/serializer.h"
#include "VecSim/index_factories/hnsw_factory.h"

const size_t vecsimAllocationOverhead = VecSimAllocator::getAllocationOverheadSize();

const size_t hashTableNodeSize = getLabelsLookupNodeSize();

class AllocatorTest : public ::testing::Test {};
struct SimpleObject : public VecsimBaseObject {
public:
    SimpleObject(std::shared_ptr<VecSimAllocator> allocator) : VecsimBaseObject(allocator) {}
    int x;
};

struct ObjectWithSTL : public VecsimBaseObject {
    std::vector<int, VecsimSTLAllocator<int>> test_vec;

public:
    ObjectWithSTL(std::shared_ptr<VecSimAllocator> allocator)
        : VecsimBaseObject(allocator), test_vec(allocator) {};
};

struct NestedObject : public VecsimBaseObject {
    ObjectWithSTL stl_object;
    SimpleObject simpleObject;

public:
    NestedObject(std::shared_ptr<VecSimAllocator> allocator)
        : VecsimBaseObject(allocator), stl_object(allocator), simpleObject(allocator) {};
};

TEST_F(AllocatorTest, test_simple_object) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    size_t expectedAllocationSize = sizeof(VecSimAllocator);
    ASSERT_EQ(allocator->getAllocationSize(), expectedAllocationSize);
    SimpleObject *obj = new (allocator) SimpleObject(allocator);
    expectedAllocationSize += sizeof(SimpleObject) + vecsimAllocationOverhead;
    ASSERT_EQ(allocator->getAllocationSize(), expectedAllocationSize);
    delete obj;
    expectedAllocationSize -= sizeof(SimpleObject) + vecsimAllocationOverhead;
    ASSERT_EQ(allocator->getAllocationSize(), sizeof(VecSimAllocator));
}

TEST_F(AllocatorTest, test_object_with_stl) {
    std::shared_ptr<VecSimAllocator> allocator(VecSimAllocator::newVecsimAllocator());
    size_t expectedAllocationSize = sizeof(VecSimAllocator);
    ASSERT_EQ(allocator->getAllocationSize(), expectedAllocationSize);
    ObjectWithSTL *obj = new (allocator) ObjectWithSTL(allocator);
    expectedAllocationSize += sizeof(ObjectWithSTL) + vecsimAllocationOverhead;
    ASSERT_EQ(allocator->getAllocationSize(), expectedAllocationSize);
    obj->test_vec.push_back(1);
    expectedAllocationSize += sizeof(int) + vecsimAllocationOverhead;
    ASSERT_EQ(allocator->getAllocationSize(), expectedAllocationSize);
    delete obj;
}

TEST_F(AllocatorTest, test_nested_object) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    size_t expectedAllocationSize = sizeof(VecSimAllocator);
    ASSERT_EQ(allocator->getAllocationSize(), expectedAllocationSize);
    NestedObject *obj = new (allocator) NestedObject(allocator);
    expectedAllocationSize += sizeof(NestedObject) + vecsimAllocationOverhead;
    ASSERT_EQ(allocator->getAllocationSize(), expectedAllocationSize);
    obj->stl_object.test_vec.push_back(1);
    expectedAllocationSize += sizeof(int) + vecsimAllocationOverhead;
    ASSERT_EQ(allocator->getAllocationSize(), expectedAllocationSize);
    delete obj;
}

template <typename index_type_t>
class IndexAllocatorTest : public ::testing::Test {};

// DataTypeSet, TEST_DATA_T and TEST_DIST_T are defined in unit_test_utils.h

TYPED_TEST_SUITE(IndexAllocatorTest, DataTypeSet);

TYPED_TEST(IndexAllocatorTest, test_bf_index_block_size_1) {
    // Create only the minimal struct.
    size_t dim = 128;
    size_t blockSize = 1;
    BFParams params = {.type = TypeParam::get_index_type(),
                       .dim = dim,
                       .metric = VecSimMetric_IP,
                       .blockSize = blockSize};
    auto *bfIndex = dynamic_cast<BruteForceIndex_Single<TEST_DATA_T, TEST_DIST_T> *>(
        BruteForceFactory::NewIndex(&params));
    auto allocator = bfIndex->getAllocator();
    TEST_DATA_T vec[128] = {};
    uint64_t expectedAllocationSize = sizeof(VecSimAllocator);
    expectedAllocationSize +=
        sizeof(BruteForceIndex_Single<TEST_DATA_T, TEST_DIST_T>) + vecsimAllocationOverhead;
    expectedAllocationSize += sizeof(DataBlocksContainer) + vecsimAllocationOverhead;
    expectedAllocationSize +=
        sizeof(DistanceCalculatorCommon<TEST_DIST_T>) + vecsimAllocationOverhead;
    expectedAllocationSize += sizeof(PreprocessorsContainerAbstract) + vecsimAllocationOverhead;
    ASSERT_EQ(allocator->getAllocationSize(), expectedAllocationSize);
    size_t memory = VecSimIndex_StatsInfo(bfIndex).memory;
    ASSERT_EQ(allocator->getAllocationSize(), memory);

    // @param expected_size - The expected number of elements in the index.
    // @param expected_data_container_blocks - The expected number of blocks in the data containers.
    // @param expected_map_containers_capacity - The expected capacity of the map containers in
    // number of elements.
    auto verify_containers_size = [&](size_t expected_size, size_t expected_data_container_blocks,
                                      size_t expected_map_containers_size) {
        ASSERT_EQ(bfIndex->indexSize(), expected_size);
        ASSERT_EQ(dynamic_cast<DataBlocksContainer *>(bfIndex->vectors)->numBlocks(),
                  expected_data_container_blocks);
        ASSERT_EQ(bfIndex->vectors->size(), expected_size);

        ASSERT_EQ(bfIndex->indexCapacity(), expected_map_containers_size);
        ASSERT_EQ(bfIndex->idToLabelMapping.capacity(), expected_map_containers_size);
        ASSERT_EQ(bfIndex->indexMetaDataCapacity(), expected_map_containers_size);
        ASSERT_EQ(bfIndex->idToLabelMapping.size(), expected_map_containers_size);
        ASSERT_GE(bfIndex->labelToIdLookup.bucket_count(), expected_map_containers_size);
    };
    // =========== Add label 1 ===========
    int before = allocator->getAllocationSize();
    size_t buckets_num_before = bfIndex->labelToIdLookup.bucket_count();
    auto vectors_blocks = dynamic_cast<DataBlocksContainer *>(bfIndex->vectors);
    size_t vectors_blocks_capacity = vectors_blocks->capacity();

    VecSimIndex_AddVector(bfIndex, vec, 1);
    int addCommandAllocationDelta = allocator->getAllocationSize() - before;
    int64_t expectedAllocationDelta =
        sizeof(labelType) + vecsimAllocationOverhead; // resize idToLabelMapping
    expectedAllocationDelta +=
        (vectors_blocks->capacity() - vectors_blocks_capacity) * sizeof(DataBlock) +
        vecsimAllocationOverhead; // New vectors blocks
    expectedAllocationDelta += blockSize * sizeof(TEST_DATA_T) * dim + vecsimAllocationOverhead +
                               bfIndex->getStorageAlignment(); // block vectors buffer
    expectedAllocationDelta += hashTableNodeSize;              // New node in the label lookup
    // Account for the allocation of a new buckets in the labels_lookup hash table.
    expectedAllocationDelta +=
        (bfIndex->labelToIdLookup.bucket_count() - buckets_num_before) * sizeof(size_t);
    // Assert that the additional allocated delta did occur, and it is limited, as some STL
    // collection allocate additional structures for their internal implementation.
    {
        SCOPED_TRACE("Verifying allocation delta for adding first vector");
        verify_containers_size(1, 1, 1);
        ASSERT_EQ(allocator->getAllocationSize(),
                  expectedAllocationSize + addCommandAllocationDelta);
        ASSERT_LE(expectedAllocationSize + expectedAllocationDelta, allocator->getAllocationSize());
        ASSERT_LE(expectedAllocationDelta, addCommandAllocationDelta);
        memory = VecSimIndex_StatsInfo(bfIndex).memory;
        ASSERT_EQ(allocator->getAllocationSize(), memory);
    }

    // =========== labels = [1], vector blocks = 1, maps capacity = 1. Add label 2 + 3 ===========

    // Prepare for next assertion test
    expectedAllocationSize = memory;
    expectedAllocationDelta = 0;

    before = allocator->getAllocationSize();
    vectors_blocks_capacity = vectors_blocks->capacity();
    buckets_num_before = bfIndex->labelToIdLookup.bucket_count();

    VecSimIndex_AddVector(bfIndex, vec, 2);
    VecSimIndex_AddVector(bfIndex, vec, 3);
    addCommandAllocationDelta = allocator->getAllocationSize() - before;
    expectedAllocationDelta += (vectors_blocks->capacity() - vectors_blocks_capacity) *
                               sizeof(DataBlock);     // New vector blocks
    expectedAllocationDelta += 2 * sizeof(labelType); // resize idToLabelMapping
    expectedAllocationDelta +=
        2 * (blockSize * sizeof(TEST_DATA_T) * dim + vecsimAllocationOverhead +
             bfIndex->getStorageAlignment());         // Two block vectors buffer
    expectedAllocationDelta += 2 * hashTableNodeSize; // New nodes in the label lookup
    expectedAllocationDelta +=
        (bfIndex->labelToIdLookup.bucket_count() - buckets_num_before) * sizeof(size_t);
    {
        SCOPED_TRACE("Index size = 1Verifying allocation delta for adding two more vectors");
        verify_containers_size(3, 3, 3);
        ASSERT_EQ(allocator->getAllocationSize(),
                  expectedAllocationSize + addCommandAllocationDelta);
        ASSERT_EQ(expectedAllocationSize + expectedAllocationDelta, allocator->getAllocationSize());
        ASSERT_EQ(expectedAllocationDelta, addCommandAllocationDelta);
        memory = VecSimIndex_StatsInfo(bfIndex).memory;
        ASSERT_EQ(allocator->getAllocationSize(), memory);
    }

    // =========== labels = [1, 2, 3], vector blocks = 3, maps capacity = 3. Delete label 1
    // ===========

    // Prepare for next assertion test
    expectedAllocationSize = memory;
    expectedAllocationDelta = 0;

    before = allocator->getAllocationSize();
    vectors_blocks_capacity = vectors_blocks->capacity();
    buckets_num_before = bfIndex->labelToIdLookup.bucket_count();
    {
        SCOPED_TRACE("Verifying allocation delta for deleting a vector from index size 3");
        ASSERT_EQ(VecSimIndex_DeleteVector(bfIndex, 1), 1);
        int deleteCommandAllocationDelta = allocator->getAllocationSize() - before;
        verify_containers_size(2, 2, 3);
        // Removing blocks doesn't change vectors_blocks->capacity(), but the block buffer is freed.
        ASSERT_EQ(vectors_blocks->capacity(), vectors_blocks_capacity);
        expectedAllocationDelta -=
            blockSize * sizeof(TEST_DATA_T) * dim + vecsimAllocationOverhead +
            bfIndex->getStorageAlignment();           // Free the vector buffer in the vector block
        expectedAllocationDelta -= hashTableNodeSize; // Remove node from the label lookup
        // idToLabelMapping and label:id should not change since count > capacity - 2 * blockSize
        ASSERT_EQ(bfIndex->labelToIdLookup.bucket_count(), buckets_num_before);

        ASSERT_EQ(allocator->getAllocationSize(),
                  expectedAllocationSize + deleteCommandAllocationDelta);
        ASSERT_EQ(expectedAllocationSize + expectedAllocationDelta, allocator->getAllocationSize());
        ASSERT_EQ(expectedAllocationDelta, deleteCommandAllocationDelta);

        memory = VecSimIndex_StatsInfo(bfIndex).memory;
        ASSERT_EQ(allocator->getAllocationSize(), memory);
    }

    // =========== labels = [2, 3], vector blocks = 2, maps capacity = 3. Add label 4 ===========

    // Prepare for next assertion test
    expectedAllocationSize = memory;
    expectedAllocationDelta = 0;

    before = allocator->getAllocationSize();
    vectors_blocks_capacity = vectors_blocks->capacity();
    buckets_num_before = bfIndex->labelToIdLookup.bucket_count();
    size_t idToLabel_size_before = bfIndex->idToLabelMapping.size();

    VecSimIndex_AddVector(bfIndex, vec, 4);
    addCommandAllocationDelta = allocator->getAllocationSize() - before;
    expectedAllocationDelta += (vectors_blocks->capacity() - vectors_blocks_capacity) *
                               sizeof(DataBlock); // New vector block
    expectedAllocationDelta += blockSize * sizeof(TEST_DATA_T) * dim + vecsimAllocationOverhead +
                               bfIndex->getStorageAlignment(); // block vectors buffer
    expectedAllocationDelta += hashTableNodeSize;              // New node in the label lookup
    {
        SCOPED_TRACE(
            "Verifying allocation delta for adding a vector to index size 2 with capacity 3");
        verify_containers_size(3, 3, 3);
        ASSERT_EQ(allocator->getAllocationSize(),
                  expectedAllocationSize + addCommandAllocationDelta);
        ASSERT_EQ(expectedAllocationSize + expectedAllocationDelta, allocator->getAllocationSize());
        ASSERT_EQ(expectedAllocationDelta, addCommandAllocationDelta);
        memory = VecSimIndex_StatsInfo(bfIndex).memory;
        ASSERT_EQ(allocator->getAllocationSize(), memory);

        // idToLabelMapping and label:id should not change since if we one free block
        ASSERT_EQ(bfIndex->labelToIdLookup.bucket_count(), buckets_num_before);
        ASSERT_EQ(bfIndex->idToLabelMapping.size(), idToLabel_size_before);
    }

    // =========== labels = [2, 3, 4], vector blocks = 3, maps capacity = 3. Delete label 2 + 3
    // ===========

    // Prepare for next assertion test
    expectedAllocationSize = memory;
    expectedAllocationDelta = 0;

    before = allocator->getAllocationSize();
    vectors_blocks_capacity = vectors_blocks->capacity();
    buckets_num_before = bfIndex->labelToIdLookup.bucket_count();
    {
        SCOPED_TRACE("Verifying allocation delta for deleting two vectors from index size 3");
        ASSERT_EQ(VecSimIndex_DeleteVector(bfIndex, 2), 1);
        ASSERT_EQ(VecSimIndex_DeleteVector(bfIndex, 3), 1);

        int deleteCommandAllocationDelta = allocator->getAllocationSize() - before;
        verify_containers_size(1, 1, 2);
        // Removing blocks doesn't change vectors_blocks->capacity(), but the block buffer is freed.
        ASSERT_EQ(vectors_blocks->capacity(), vectors_blocks_capacity);
        expectedAllocationDelta -=
            2 * (blockSize * sizeof(TEST_DATA_T) * dim + vecsimAllocationOverhead +
                 bfIndex->getStorageAlignment()); // Free the vector buffer in the vector block
        expectedAllocationDelta -= 2 * hashTableNodeSize; // Remove nodes from the label lookup
        // idToLabelMapping and label:id should shrink by block since count >= capacity - 2 *
        // blockSize
        expectedAllocationDelta -= sizeof(labelType); // remove one idToLabelMapping
        expectedAllocationDelta -=
            (buckets_num_before - bfIndex->labelToIdLookup.bucket_count()) * sizeof(size_t);
        ASSERT_EQ(allocator->getAllocationSize(),
                  expectedAllocationSize + deleteCommandAllocationDelta);
        ASSERT_EQ(expectedAllocationSize + expectedAllocationDelta, allocator->getAllocationSize());
        ASSERT_EQ(expectedAllocationDelta, deleteCommandAllocationDelta);

        memory = VecSimIndex_StatsInfo(bfIndex).memory;
        ASSERT_EQ(allocator->getAllocationSize(), memory);
    }

    // =========== labels = [4], vector blocks = 1, maps capacity = 2. Delete last label ===========

    // Prepare for next assertion test
    expectedAllocationSize = memory;
    expectedAllocationDelta = 0;

    before = allocator->getAllocationSize();
    vectors_blocks_capacity = vectors_blocks->capacity();
    buckets_num_before = bfIndex->labelToIdLookup.bucket_count();
    {
        SCOPED_TRACE("Verifying allocation delta for emptying the index");
        ASSERT_EQ(VecSimIndex_DeleteVector(bfIndex, 4), 1);

        int deleteCommandAllocationDelta = allocator->getAllocationSize() - before;
        verify_containers_size(0, 0, 0);
        // Removing blocks doesn't change vectors_blocks->capacity(), but the block buffer is freed.
        ASSERT_EQ(vectors_blocks->capacity(), vectors_blocks_capacity);
        expectedAllocationDelta -=
            (blockSize * sizeof(TEST_DATA_T) * dim + vecsimAllocationOverhead +
             bfIndex->getStorageAlignment());         // Free the vector buffer in the vector block
        expectedAllocationDelta -= hashTableNodeSize; // Remove nodes from the label lookup
        // idToLabelMapping and label:id should shrink by block since count >= capacity - 2 *
        // blockSize
        expectedAllocationDelta -=
            2 * sizeof(labelType) +
            vecsimAllocationOverhead; // remove two idToLabelMapping and free the container
        // resizing labelToIdLookup to 0
        size_t buckets_after = bfIndex->labelToIdLookup.bucket_count();
        ASSERT_EQ(bfIndex->labelToIdLookup.size(), 0);
        ASSERT_LE(buckets_after, buckets_num_before);
        expectedAllocationDelta -= (buckets_num_before - buckets_after) * sizeof(size_t);
        ASSERT_EQ(allocator->getAllocationSize(),
                  expectedAllocationSize + deleteCommandAllocationDelta);
        ASSERT_LE(abs(expectedAllocationDelta), abs(deleteCommandAllocationDelta));
        ASSERT_GE(expectedAllocationSize + expectedAllocationDelta, allocator->getAllocationSize());

        memory = VecSimIndex_StatsInfo(bfIndex).memory;
        ASSERT_EQ(allocator->getAllocationSize(), memory);
    }

    VecSimIndex_Free(bfIndex);
}

TYPED_TEST(IndexAllocatorTest, test_hnsw) {
    size_t d = 128;

    // Build with default args
    HNSWParams params = {.type = TypeParam::get_index_type(), .dim = d, .metric = VecSimMetric_L2};

    TEST_DATA_T vec[128] = {};
    auto *hnswIndex =
        dynamic_cast<HNSWIndex_Single<TEST_DATA_T, TEST_DIST_T> *>(HNSWFactory::NewIndex(&params));
    auto allocator = hnswIndex->getAllocator();
    uint64_t expectedAllocationSize = sizeof(VecSimAllocator);

    expectedAllocationSize +=
        sizeof(HNSWIndex_Single<TEST_DATA_T, TEST_DIST_T>) + vecsimAllocationOverhead;
    ASSERT_GE(allocator->getAllocationSize(), expectedAllocationSize);
    size_t memory = VecSimIndex_StatsInfo(hnswIndex).memory;
    ASSERT_EQ(allocator->getAllocationSize(), memory);
    expectedAllocationSize = memory;

    int before = allocator->getAllocationSize();
    VecSimIndex_AddVector(hnswIndex, vec, 1);
    int addCommandAllocationDelta = allocator->getAllocationSize() - before;
    ASSERT_EQ(allocator->getAllocationSize(), expectedAllocationSize + addCommandAllocationDelta);
    memory = VecSimIndex_StatsInfo(hnswIndex).memory;
    ASSERT_EQ(allocator->getAllocationSize(), memory);
    expectedAllocationSize = memory;

    before = allocator->getAllocationSize();
    VecSimIndex_AddVector(hnswIndex, vec, 2);
    addCommandAllocationDelta = allocator->getAllocationSize() - before;

    ASSERT_EQ(allocator->getAllocationSize(), expectedAllocationSize + addCommandAllocationDelta);
    memory = VecSimIndex_StatsInfo(hnswIndex).memory;
    ASSERT_EQ(allocator->getAllocationSize(), memory);

    expectedAllocationSize = memory;

    before = allocator->getAllocationSize();
    VecSimIndex_DeleteVector(hnswIndex, 2);
    int deleteCommandAllocationDelta = allocator->getAllocationSize() - before;

    ASSERT_EQ(expectedAllocationSize + deleteCommandAllocationDelta,
              allocator->getAllocationSize());
    memory = VecSimIndex_StatsInfo(hnswIndex).memory;
    ASSERT_EQ(allocator->getAllocationSize(), memory);
    expectedAllocationSize = memory;

    before = allocator->getAllocationSize();
    VecSimIndex_DeleteVector(hnswIndex, 1);
    deleteCommandAllocationDelta = allocator->getAllocationSize() - before;

    ASSERT_EQ(expectedAllocationSize + deleteCommandAllocationDelta,
              allocator->getAllocationSize());
    memory = VecSimIndex_StatsInfo(hnswIndex).memory;
    ASSERT_EQ(allocator->getAllocationSize(), memory);
    VecSimIndex_Free(hnswIndex);
}

TYPED_TEST(IndexAllocatorTest, testIncomingEdgesSet) {

    size_t d = 2;

    // Build index, use small M to simplify the scenario.
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = d, .metric = VecSimMetric_L2, .M = 2};
    auto *hnswIndex =
        dynamic_cast<HNSWIndex_Single<TEST_DATA_T, TEST_DIST_T> *>(HNSWFactory::NewIndex(&params));
    auto allocator = hnswIndex->getAllocator();

    // Add a "dummy" vector - labels_lookup hash table will allocate initial size of buckets here.
    GenerateAndAddVector<TEST_DATA_T>(hnswIndex, d, 0, 0.0);

    // Add another vector and validate it's exact memory allocation delta.
    TEST_DATA_T vec1[] = {1.0, 0.0};
    int before = allocator->getAllocationSize();
    VecSimIndex_AddVector(hnswIndex, vec1, 1);
    // Since the memory before did not account for the visited nodes handler pool (it was created
    // lazily), we need to clear it before calculating the delta.
    hnswIndex->visitedNodesHandlerPool.clearPool();
    int allocation_delta = allocator->getAllocationSize() - before;
    size_t vec_max_level = hnswIndex->getGraphDataByInternalId(1)->toplevel;

    // Expect the creation of an empty incoming edges set in every level (+ the allocator header
    // overhead), and a single node in the labels' lookup hash table.
    size_t expected_allocation_delta =
        (vec_max_level + 1) * (sizeof(vecsim_stl::vector<idType>) + vecsimAllocationOverhead);
    expected_allocation_delta += hashTableNodeSize;

    // Account for allocating link lists for levels higher than 0, if exists.
    if (vec_max_level > 0) {
        expected_allocation_delta +=
            hnswIndex->levelDataSize * vec_max_level + vecsimAllocationOverhead;
    }
    ASSERT_EQ(allocation_delta, expected_allocation_delta);

    // Add three more vectors, all should have a connections to vec1.
    TEST_DATA_T vec2[] = {2.0f, 0.0f};
    VecSimIndex_AddVector(hnswIndex, vec2, 2);
    TEST_DATA_T vec3[] = {1.0f, 1.0f};
    VecSimIndex_AddVector(hnswIndex, vec3, 3);
    TEST_DATA_T vec4[] = {1.0f, -1.0f};
    VecSimIndex_AddVector(hnswIndex, vec4, 4);

    // Layer 0 should look like this (all edges bidirectional):
    //    3                    3
    //    |                    |
    // 0--1--2      =>   0--5--1--2
    //    |              |----^|
    //    4                    4

    // Next, insertion of vec5 should make 0->1 unidirectional, thus adding 0 to 1's incoming edges
    // set.
    TEST_DATA_T vec5[] = {0.5f, 0.0f};
    size_t buckets_num_before = hnswIndex->labelLookup.bucket_count();
    before = allocator->getAllocationSize();
    VecSimIndex_AddVector(hnswIndex, vec5, 5);
    allocation_delta = allocator->getAllocationSize() - before;
    vec_max_level = hnswIndex->getGraphDataByInternalId(5)->toplevel;

    /* Compute the expected allocation delta:
     * 1. empty incoming edges set in every level (+ allocator's header).
     * 2. A node in the labels_lookup has table (+ allocator's header). If rehashing occurred, we
     * account also for the diff in the buckets size (each bucket has sizeof(size_t) overhead).
     * 3. Account for allocating link lists for levels higher than 0, if exists.
     * 4. Finally, expect an allocation of the data buffer in the incoming edges vector of vec1 due
     * to the insertion, and the fact that vec1 will re-select its neighbours.
     */
    expected_allocation_delta =
        (vec_max_level + 1) * (sizeof(vecsim_stl::vector<idType>) + vecsimAllocationOverhead) +
        hashTableNodeSize;
    size_t buckets_diff = hnswIndex->labelLookup.bucket_count() - buckets_num_before;
    expected_allocation_delta += buckets_diff * sizeof(size_t);
    if (vec_max_level > 0) {
        expected_allocation_delta +=
            hnswIndex->levelDataSize * vec_max_level + vecsimAllocationOverhead;
    }

    // Expect that the first element is pushed to the incoming edges vector of element 1 in level 0.
    // Then, we account for the capacity of the buffer that is allocated for the vector data.
    expected_allocation_delta +=
        hnswIndex->getElementLevelData(1, 0).incomingUnidirectionalEdges->capacity() *
            sizeof(idType) +
        vecsimAllocationOverhead;
    ASSERT_EQ(allocation_delta, expected_allocation_delta);

    VecSimIndex_Free(hnswIndex);
}

TYPED_TEST(IndexAllocatorTest, test_hnsw_reclaim_memory) {
    size_t d = 128;

    VecSimType type = TypeParam::get_index_type();

    // Build HNSW index with default args and initial capacity of zero.
    HNSWParams params = {.type = type, .dim = d, .metric = VecSimMetric_L2};
    auto *hnswIndex =
        dynamic_cast<HNSWIndex_Single<TEST_DATA_T, TEST_DIST_T> *>(HNSWFactory::NewIndex(&params));
    auto allocator = hnswIndex->getAllocator();
    ASSERT_EQ(hnswIndex->indexCapacity(), 0);
    size_t initial_memory_size = allocator->getAllocationSize();
    // labels_lookup and element_levels containers are not allocated at all in some platforms,
    // when initial capacity is zero, while in other platforms labels_lookup is allocated with a
    // single bucket. This, we get the following range in which we expect the initial memory to be
    // in.
    ASSERT_GE(initial_memory_size, HNSWFactory::EstimateInitialSize(&params));
    ASSERT_LE(initial_memory_size, HNSWFactory::EstimateInitialSize(&params) + sizeof(size_t) +
                                       2 * vecsimAllocationOverhead);

    // Add vectors up to the size of a whole block, and calculate the total memory delta.
    size_t block_size = hnswIndex->basicInfo().blockSize;

    size_t prev_bucket_count = hnswIndex->labelLookup.bucket_count();
    for (size_t i = 0; i < block_size; i++) {
        GenerateAndAddVector<TEST_DATA_T>(hnswIndex, d, i, i);
    }
    // Get the memory delta after adding the block.
    size_t one_block_mem_delta = allocator->getAllocationSize() - initial_memory_size;

    size_t one_block_buckets = hnswIndex->labelLookup.bucket_count();
    // @param expected_size - The expected number of elements in the index.
    // @param expected_data_container_blocks - The expected number of blocks in the data containers.
    // @param expected_map_containers_capacity - The expected capacity of the map containers in
    // number of elements.
    auto verify_containers_size = [&](size_t expected_size, size_t expected_data_container_blocks,
                                      size_t expected_map_containers_size) {
        SCOPED_TRACE("Verifying containers size for size " + std::to_string(expected_size));
        ASSERT_EQ(hnswIndex->indexSize(), expected_size);
        ASSERT_EQ(hnswIndex->indexCapacity(), expected_data_container_blocks * block_size);
        ASSERT_EQ(hnswIndex->indexCapacity(), hnswIndex->maxElements);
        ASSERT_EQ(hnswIndex->graphDataBlocks.size(), expected_data_container_blocks);
        ASSERT_EQ(dynamic_cast<DataBlocksContainer *>(hnswIndex->vectors)->numBlocks(),
                  expected_data_container_blocks);
        ASSERT_EQ(hnswIndex->vectors->size(), expected_size);

        ASSERT_EQ(hnswIndex->idToMetaData.capacity(), expected_map_containers_size);
        ASSERT_EQ(hnswIndex->indexMetaDataCapacity(), expected_map_containers_size);
        ASSERT_EQ(hnswIndex->idToMetaData.size(), expected_map_containers_size);
        ASSERT_GE(hnswIndex->labelLookup.bucket_count(), expected_map_containers_size);
        // Also validate that there are no unidirectional connections (these add memory to the
        // incoming edges sets).
        ASSERT_EQ(hnswIndex->checkIntegrity().unidirectional_connections, 0);
    };

    // Validate that a single block exists.
    verify_containers_size(block_size, 1, block_size);

    size_t one_block_mem = allocator->getAllocationSize();
    // Add another vector, expect resizing of the index to contain two blocks.
    GenerateAndAddVector<TEST_DATA_T>(hnswIndex, d, block_size, block_size);
    verify_containers_size(block_size + 1, 2, 2 * block_size);
    size_t mem_delta = allocator->getAllocationSize() - one_block_mem;

    // Compute the expected memory allocation due to the last vector insertion.
    size_t vec_max_level = hnswIndex->getGraphDataByInternalId(block_size)->toplevel;
    size_t last_vec_graph_data_mem =
        (sizeof(vecsim_stl::vector<idType>) + vecsimAllocationOverhead) + hashTableNodeSize;
    if (vec_max_level > 0) {
        last_vec_graph_data_mem +=
            hnswIndex->levelDataSize * vec_max_level + vecsimAllocationOverhead;
    }
    size_t expected_mem_delta = last_vec_graph_data_mem;
    // Also account for all the memory allocation caused by the resizing that this vector triggered
    // except for the bucket count of the labels_lookup hash table that is calculated separately.
    // Calculate the expected memory delta for adding a block.
    size_t data_containers_block_mem =
        2 * (sizeof(DataBlock) + vecsimAllocationOverhead) + hnswIndex->getStorageAlignment();
    size_t size_total_data_per_element =
        hnswIndex->elementGraphDataSize + hnswIndex->getStoredDataSize();
    data_containers_block_mem += size_total_data_per_element * block_size;
    // account for idToMetaData and visitedNodesHandlerPool entries.
    expected_mem_delta +=
        (sizeof(tag_t) + sizeof(ElementMetaData) + sizeof(vecsim_stl::one_byte_mutex)) *
            block_size +
        data_containers_block_mem;
    // Account for the allocation of a new bucket in the labels_lookup hash table.
    expected_mem_delta +=
        (hnswIndex->labelLookup.bucket_count() - one_block_buckets) * sizeof(size_t);
    // New blocks allocated - 1 aligned block for vectors and 1 unaligned block for graph data.

    ASSERT_EQ(expected_mem_delta, mem_delta);

    // Remove the last vector, expect datablocks containers (vectors buffer and graph data) resizing
    // back to a single block. Index-size container such as id to label mapping, are only freed when
    // there two empty blocks.
    size_t before_delete_mem = allocator->getAllocationSize();
    size_t graph_data_blocks_capacity = hnswIndex->graphDataBlocks.capacity();
    auto vectors_blocks = dynamic_cast<DataBlocksContainer *>(hnswIndex->vectors);
    size_t vectors_blocks_capacity = vectors_blocks->capacity();
    VecSimIndex_DeleteVector(hnswIndex, block_size);
    verify_containers_size(block_size, 1, 2 * block_size);

    size_t expected_allocation_size =
        before_delete_mem - last_vec_graph_data_mem - hnswIndex->getStorageAlignment();
    // Free the buffer of the last block in both data containers.
    expected_allocation_size -=
        size_total_data_per_element * block_size + 2 * vecsimAllocationOverhead;
    expected_allocation_size -=
        (graph_data_blocks_capacity - hnswIndex->graphDataBlocks.capacity()) *
        (sizeof(DataBlock) + vecsimAllocationOverhead);
    expected_allocation_size -= (vectors_blocks_capacity - vectors_blocks->capacity()) *
                                (sizeof(DataBlock) + vecsimAllocationOverhead);
    ASSERT_EQ(allocator->getAllocationSize(), expected_allocation_size);

    // Remove the rest of the vectors, and validate that the memory returns to its initial state.
    for (size_t i = 0; i < block_size; i++) {
        VecSimIndex_DeleteVector(hnswIndex, i);
    }

    ASSERT_EQ(hnswIndex->indexSize(), 0);
    ASSERT_EQ(hnswIndex->indexCapacity(), 0);
    // All data structures' memory returns to as it was, with the exceptional of the labels_lookup
    // (STL unordered_map with hash table implementation), that leaves some empty buckets.
    size_t hash_table_memory = hnswIndex->labelLookup.bucket_count() * sizeof(size_t);
    // Data block vectors do not shrink on resize so extra memory is expected.
    size_t block_vectors_memory =
        sizeof(DataBlock) * (hnswIndex->graphDataBlocks.capacity() +
                             dynamic_cast<DataBlocksContainer *>(hnswIndex->vectors)->capacity()) +
        2 * vecsimAllocationOverhead;
    // Current memory should be back as it was initially. The label_lookup hash table is an
    // exception, since in some platforms, empty buckets remain even when the capacity is set to
    // zero, while in others the entire capacity reduced to zero (including the header).
    // Also, visitedNodesHandlerPool that was created lazily is not freed, but it should not be
    // accounted when comparing to the initial memory size estimation of the index.
    hnswIndex->visitedNodesHandlerPool.clearPool();
    ASSERT_LE(allocator->getAllocationSize(), HNSWFactory::EstimateInitialSize(&params) +
                                                  block_vectors_memory + hash_table_memory +
                                                  2 * vecsimAllocationOverhead);
    ASSERT_GE(allocator->getAllocationSize(),
              HNSWFactory::EstimateInitialSize(&params) + block_vectors_memory + hash_table_memory);
    VecSimIndex_Free(hnswIndex);
}
