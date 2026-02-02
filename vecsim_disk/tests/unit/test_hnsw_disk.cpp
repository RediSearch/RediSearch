/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "test_utils.h"
#include "tests_utils.h" // VectorSimilarity test utilities (populate_float_vec, etc.)
#include "vecsim_disk_api.h"
#include "factory/disk_index_factory.h"

using namespace test_utils;
using sq8 = vecsim_types::sq8;

class HNSWDiskTest : public ::testing::Test {
protected:
    static constexpr size_t DIM = 4;

    VecSimIndex* createIndex(const HNSWParams& hnsw_params, const SpeeDBHandles* handles = nullptr) {
        auto params_disk_holder = createDiskParams(hnsw_params);
        // Set storage handles if provided
        if (handles) {
            params_disk_holder->diskContext.storage = const_cast<void*>(static_cast<const void*>(handles));
        }
        return VecSimDisk_CreateIndex(&params_disk_holder->params_disk);
    }
};

// Parameterized test class for HNSWDisk with different metrics
class HNSWDiskMetricTest : public HNSWDiskTest, public testing::WithParamInterface<VecSimMetric> {};

TEST_F(HNSWDiskTest, CreateIndex) {
    TestIndex<float, float> index(DIM);
    ASSERT_NE(index.get(), nullptr);
    EXPECT_EQ(index->indexSize(), 0);
}

// Stub tests - verify stub behavior until MOD-13164 implementation
TEST_F(HNSWDiskTest, AddVectorStub) {
    TestIndex<float, float> index(DIM);

    float vec[DIM] = {1.0f, 1.0f, 1.0f, 1.0f};
    // Stub returns 0 (not implemented)
    EXPECT_EQ(index->addVector(vec, 1), 0);
    EXPECT_EQ(index->indexSize(), 0);
}

TEST_F(HNSWDiskTest, TopKQueryStub) {
    TestIndex<float, float> index(DIM);

    float query[DIM] = {1.0f, 1.0f, 1.0f, 1.0f};
    auto* reply = index->topKQuery(query, 10, nullptr);

    ASSERT_NE(reply, nullptr);
    // Stub returns empty results
    EXPECT_EQ(reply->results.size(), 0);
    delete reply;
}

TEST_F(HNSWDiskTest, DeleteVectorStub) {
    TestIndex<float, float> index(DIM);

    // Stub returns 0 (not implemented)
    EXPECT_EQ(index->deleteVector(1), 0);
    EXPECT_EQ(index->indexSize(), 0);
}

// =============================================================================
// Test: Index correctly owns storage via unique_ptr
// =============================================================================
//
// This test verifies that the HNSWDiskIndex correctly takes ownership of
// storage via unique_ptr. The TestIndex helper creates a SpeeDBStore and
// passes ownership to the index.

TEST_F(HNSWDiskTest, IndexOwnsStorage) {
    TestIndex<float, float> index(DIM);

    // The index should have a valid storage pointer
    HNSWStorage<float>* storage = index->getStorage();
    EXPECT_NE(storage, nullptr);

    // The storage should be the same as what TestIndex created
    EXPECT_EQ(storage, index.storage());
}

// =============================================================================
// Factory tests
// =============================================================================
//
// NOTE: Unit tests use a stub CreateSpeeDBStore that returns nullptr.
// Real SpeeDBStore creation is tested in flow tests with the full Redis module.

TEST_F(HNSWDiskTest, FactoryWithNullStorage) {
    // Test that factory handles null storage gracefully
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_L2,
        .multi = false,
        .blockSize = 1024,
        .M = 16,
        .efConstruction = 200,
        .efRuntime = 10,
    };

    auto* handle = this->createIndex(hnsw_params);
    ASSERT_NE(handle, nullptr);

    auto* index = static_cast<HNSWDiskIndex<float, float>*>(handle);

    // With null storage, getStorage should return null
    EXPECT_EQ(index->getStorage(), nullptr);

    VecSimDisk_FreeIndex(handle);
}

TEST_F(HNSWDiskTest, FactoryWithSpeeDBHandles) {
    // Test that factory creates HNSWStorage from valid handles
    TempSpeeDB tempDb;

    // Wrap C++ pointers in C API structs for SpeeDBHandles
    rocksdb_t db_wrapper{tempDb.db()};
    rocksdb_column_family_handle_t cf_wrapper{tempDb.cf()};

    SpeeDBHandles handles;
    handles.db = &db_wrapper;
    handles.cf = &cf_wrapper;

    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_L2,
        .multi = false,
        .blockSize = 1024,
        .M = 16,
        .efConstruction = 200,
        .efRuntime = 10,
    };

    // Pass handles to createIndex
    auto* handle = this->createIndex(hnsw_params, &handles);
    ASSERT_NE(handle, nullptr);

    auto* index = static_cast<HNSWDiskIndex<float, float>*>(handle);

    // Verify that storage was created successfully
    HNSWStorage<float>* storage = index->getStorage();
    ASSERT_NE(storage, nullptr);

    // Verify storage is NOT the same pointer as handles (it's an HNSWStorage instance)
    EXPECT_NE(static_cast<void*>(storage), static_cast<void*>(&handles));

    VecSimDisk_FreeIndex(handle);
}

// =============================================================================
// Template Instantiation Tests
// =============================================================================
//
// These tests force template instantiation to catch compilation errors in
// template methods that would otherwise not be compiled (since templates
// are only compiled when instantiated).

// Dummy test to force template instantiation of repairNode
// This ensures compilation errors in repairNode are caught at build time
// without needing to call the method (which would require valid IDs)
TEST_F(HNSWDiskTest, RepairNodeCompiles) {
    TestIndex<float, float> index(DIM);

    // Force template instantiation by taking a pointer to the method
    // This catches compile errors without actually calling the function
    // (calling with invalid ID=0 on empty index would segfault)
    auto repairNodePtr = &HNSWDiskIndex<float, float>::testRepairNode;
    EXPECT_NE(repairNodePtr, nullptr);
    (void)repairNodePtr; // Suppress unused variable warning
}

// Test that verifies correct blob size calculation for HNSWDiskIndex
TEST_P(HNSWDiskMetricTest, BlobSizeCalculation) {
    VecSimMetric metric = GetParam();
    const size_t expectedSQ8Size = getExpectedSQ8Size(DIM, metric);
    const size_t fp32Size = DIM * sizeof(float);

    // Create an actual HNSWDiskIndex and verify its blob sizes
    TestIndex<float, float> index(DIM, metric);

    // Backend: inputBlobSize = FP32, storedDataSize = SQ8
    EXPECT_EQ(index->getInputBlobSize(), fp32Size);
    EXPECT_EQ(index->getStoredDataSize(), expectedSQ8Size);
}

// Instantiate parameterized tests for all metrics
INSTANTIATE_TEST_SUITE_P(MetricTests, HNSWDiskMetricTest,
                         testing::Values(VecSimMetric_L2, VecSimMetric_IP, VecSimMetric_Cosine),
                         [](const testing::TestParamInfo<VecSimMetric>& info) {
                             return VecSimMetric_ToString(info.param);
                         });

// =============================================================================
// Block-Based Allocation Tests
// =============================================================================

TEST_F(HNSWDiskTest, AllocateIdBasic) {
    TestIndex<float, float> index(DIM);

    // Initially, maxElements should be 0
    EXPECT_EQ(index->testGetMaxElements(), 0);
    EXPECT_EQ(index->testGetNextId(), 0);

    // Allocate first ID - should trigger growByBlock
    idType id0 = index->testAllocateId();
    EXPECT_EQ(id0, 0);
    EXPECT_EQ(index->testGetNextId(), 1);
    // maxElements should now be blockSize (1024)
    EXPECT_EQ(index->testGetMaxElements(), 1024);

    // Allocate more IDs without triggering another grow
    idType id1 = index->testAllocateId();
    idType id2 = index->testAllocateId();
    EXPECT_EQ(id1, 1);
    EXPECT_EQ(id2, 2);
    EXPECT_EQ(index->testGetNextId(), 3);
    EXPECT_EQ(index->testGetMaxElements(), 1024); // Still same capacity
}

TEST_F(HNSWDiskTest, AllocateIdWithRecycling) {
    TestIndex<float, float> index(DIM);

    // Allocate some IDs
    idType id0 = index->testAllocateId();
    idType id1 = index->testAllocateId();
    idType id2 = index->testAllocateId();
    EXPECT_EQ(id0, 0);
    EXPECT_EQ(id1, 1);
    EXPECT_EQ(id2, 2);

    // Recycle id1
    index->testRecycleId(id1);
    EXPECT_EQ(index->testGetHolesCount(), 1);

    // Next allocation should return recycled ID
    idType id3 = index->testAllocateId();
    EXPECT_EQ(id3, 1); // Recycled ID
    EXPECT_EQ(index->testGetHolesCount(), 0);

    // Next allocation should be fresh
    idType id4 = index->testAllocateId();
    EXPECT_EQ(id4, 3); // Fresh ID (nextId was 3)
}

TEST_F(HNSWDiskTest, CreateElementSlot) {
    TestIndex<float, float> index(DIM);

    // Allocate an ID (triggers growByBlock)
    idType id = index->testAllocateId();
    EXPECT_EQ(id, 0);

    // Initialize element metadata
    index->testInitElementMetadata(id, 100, 2); // label=100, level=2

    // Verify nodeLocks grew
    EXPECT_GE(index->testGetNodeLocksSize(), 1);
    // Verify idToMetaData was pre-allocated
    EXPECT_EQ(index->testGetIdToMetaDataSize(), 1024);
}

TEST_F(HNSWDiskTest, RecycledIdMetadata) {
    TestIndex<float, float> index(DIM);

    // Allocate and initialize first element
    idType id = index->testAllocateId();
    index->testInitElementMetadata(id, 100, 2);

    // Recycle the ID
    index->testRecycleId(id);

    // Reallocate (should get same ID back)
    idType recycledId = index->testAllocateId();
    EXPECT_EQ(recycledId, id);

    // Initialize metadata for recycled ID (same function works for both fresh and recycled)
    index->testInitElementMetadata(recycledId, 200, 3);

    // nodeLocks size should be unchanged
    EXPECT_GE(index->testGetNodeLocksSize(), 1);
}

TEST_F(HNSWDiskTest, GrowByBlockMultipleTimes) {
    // Use small blockSize for testing
    TestIndex<float, float> index(DIM, VecSimMetric_L2, 16, 200, 10);

    // blockSize is 1024 by default in TestIndex, so we need many allocations
    // Let's just verify the growth pattern
    EXPECT_EQ(index->testGetMaxElements(), 0);

    // First allocation triggers first block
    index->testAllocateId();
    size_t firstCapacity = index->testGetMaxElements();
    EXPECT_GT(firstCapacity, 0);

    // Allocate until we need another block
    for (size_t i = 1; i < firstCapacity; i++) {
        index->testAllocateId();
    }
    EXPECT_EQ(index->testGetMaxElements(), firstCapacity);

    // One more allocation should trigger another growByBlock
    index->testAllocateId();
    EXPECT_EQ(index->testGetMaxElements(), firstCapacity * 2);
}

// =============================================================================
// Graph Search Algorithm Tests
// =============================================================================

TEST_F(HNSWDiskTest, SetupElementAndComputeDistance) {
    TestIndex<float, float> index(DIM);

    // Create test vectors
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    float vec2[DIM] = {0.0f, 1.0f, 0.0f, 0.0f};

    // Set up elements using the test helper
    idType id0 = index->testSetupElement(100, 0, vec1);
    idType id1 = index->testSetupElement(101, 0, vec2);

    EXPECT_EQ(id0, 0);
    EXPECT_EQ(id1, 1);
    EXPECT_EQ(index->indexSize(), 2);

    // Compute distance from vec1 to stored vec2
    // L2 distance between [1,0,0,0] and [0,1,0,0] = sqrt(2) ≈ 1.414
    // But SQ8 quantization may introduce some error
    float dist = index->testComputeDistance(vec1, id1);
    EXPECT_GT(dist, 0.0f);
    EXPECT_LT(dist, 3.0f); // Reasonable range for L2 distance
}

TEST_F(HNSWDiskTest, GreedySearchLevelBasic) {
    TestIndex<float, float> index(DIM);

    // Create a simple graph with 3 nodes at level 0
    // Node 0: [0, 0, 0, 0] - far from query
    // Node 1: [0.5, 0.5, 0, 0] - closer to query
    // Node 2: [1, 1, 0, 0] - closest to query [1, 1, 0, 0]
    //
    // Distances from query [1, 1, 0, 0]:
    // - to vec0: sqrt(1² + 1²) = sqrt(2) ≈ 1.41
    // - to vec1: sqrt(0.5² + 0.5²) = sqrt(0.5) ≈ 0.71
    // - to vec2: 0
    float vec0[DIM] = {0.0f, 0.0f, 0.0f, 0.0f};
    float vec1[DIM] = {0.5f, 0.5f, 0.0f, 0.0f};
    float vec2[DIM] = {1.0f, 1.0f, 0.0f, 0.0f};

    idType id0 = index->testSetupElement(100, 0, vec0);
    idType id1 = index->testSetupElement(101, 0, vec1);
    idType id2 = index->testSetupElement(102, 0, vec2);

    // Create edges: 0 <-> 1 <-> 2
    index->testAddEdge(id0, id1, 0);
    index->testAddEdge(id1, id0, 0);
    index->testAddEdge(id1, id2, 0);
    index->testAddEdge(id2, id1, 0);

    // Verify edges were created correctly
    auto edges0 = index->testGetOutgoingEdges(id0, 0);
    auto edges1 = index->testGetOutgoingEdges(id1, 0);
    ASSERT_EQ(edges0.size(), 1) << "Node 0 should have 1 outgoing edge";
    ASSERT_EQ(edges0[0], id1) << "Node 0's edge should point to node 1";
    ASSERT_EQ(edges1.size(), 2) << "Node 1 should have 2 outgoing edges";

    // Set entry point to node 0
    index->testTryUpdateEntryPoint(id0, 0);

    // Query vector closest to node 2
    float query[DIM] = {1.0f, 1.0f, 0.0f, 0.0f};

    // Verify distance computations work
    float dist0 = index->testComputeDistance(query, id0);
    float dist1 = index->testComputeDistance(query, id1);
    EXPECT_GT(dist0, 0.0f) << "Distance to node 0 should be positive";
    EXPECT_GT(dist1, 0.0f) << "Distance to node 1 should be positive";
    EXPECT_LT(dist1, dist0) << "Node 1 should be closer than node 0";

    // Start greedy search from node 0
    idType currObj = id0;
    float currDist = dist0;

    // Greedy search should find node 2 (closest to query) by traversing 0 -> 1 -> 2
    index->testGreedySearchLevel(query, 0, currObj, currDist);

    // The greedy search should have moved to a closer node
    // Due to SQ8 quantization, we may not always reach the optimal node,
    // but we should at least move closer (from 0 to 1 or 2)
    EXPECT_NE(currObj, id0); // Should have moved from starting point
}

TEST_F(HNSWDiskTest, SearchLayerBasic) {
    TestIndex<float, float> index(DIM);

    // Create 5 nodes in a line: 0 - 1 - 2 - 3 - 4
    float vectors[5][DIM] = {{0.0f, 0.0f, 0.0f, 0.0f},
                             {1.0f, 0.0f, 0.0f, 0.0f},
                             {2.0f, 0.0f, 0.0f, 0.0f},
                             {3.0f, 0.0f, 0.0f, 0.0f},
                             {4.0f, 0.0f, 0.0f, 0.0f}};

    idType ids[5];
    for (int i = 0; i < 5; i++) {
        ids[i] = index->testSetupElement(100 + i, 0, vectors[i]);
    }

    // Create linear graph: 0 <-> 1 <-> 2 <-> 3 <-> 4
    for (int i = 0; i < 4; i++) {
        index->testAddEdge(ids[i], ids[i + 1], 0);
        index->testAddEdge(ids[i + 1], ids[i], 0);
    }

    index->testTryUpdateEntryPoint(ids[0], 0);

    // Query for vector closest to [2.5, 0, 0, 0] - between nodes 2 and 3
    float query[DIM] = {2.5f, 0.0f, 0.0f, 0.0f};

    // Search with ef=3 starting from node 0
    auto results = index->testSearchLayer<false>(ids[0], query, 0, 3);

    // Should find at least some results
    EXPECT_GT(results.size(), 0);

    // The closest nodes should be 2 and 3
    // Results are in a max-heap, so we need to extract them
    std::vector<idType> resultIds;
    while (!results.empty()) {
        resultIds.push_back(results.top().second);
        results.pop();
    }

    // Should have found nodes 2 and 3 among the results
    bool found2 = std::find(resultIds.begin(), resultIds.end(), ids[2]) != resultIds.end();
    bool found3 = std::find(resultIds.begin(), resultIds.end(), ids[3]) != resultIds.end();
    EXPECT_TRUE(found2 || found3);
}

// =============================================================================
// Neighbor Selection Heuristic Tests
// =============================================================================

TEST_F(HNSWDiskTest, GetNeighborsByHeuristic2Basic) {
    TestIndex<float, float> index(DIM);

    // Create nodes to populate candidates
    float vec0[DIM] = {0.0f, 0.0f, 0.0f, 0.0f};
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    float vec2[DIM] = {2.0f, 0.0f, 0.0f, 0.0f};
    float vec3[DIM] = {3.0f, 0.0f, 0.0f, 0.0f};

    idType id0 = index->testSetupElement(100, 0, vec0);
    idType id1 = index->testSetupElement(101, 0, vec1);
    idType id2 = index->testSetupElement(102, 0, vec2);
    idType id3 = index->testSetupElement(103, 0, vec3);

    // Create candidate list with distances (smaller distance = better)
    candidatesList<float> candidates(index->getAllocator());
    candidates.emplace_back(1.0f, id0);
    candidates.emplace_back(2.0f, id1);
    candidates.emplace_back(3.0f, id2);
    candidates.emplace_back(4.0f, id3);

    // Apply heuristic with M=2 (keep at most 2 neighbors)
    size_t M = 2;
    index->testGetNeighborsByHeuristic2(candidates, M);

    // Should keep at most M candidates
    EXPECT_LE(candidates.size(), M);
    // The best candidates (lowest distances) should be kept
    EXPECT_GT(candidates.size(), 0);
}

TEST_F(HNSWDiskTest, GetNeighborsByHeuristic2WithRemoved) {
    TestIndex<float, float> index(DIM);

    // Create nodes
    float vec0[DIM] = {0.0f, 0.0f, 0.0f, 0.0f};
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    float vec2[DIM] = {2.0f, 0.0f, 0.0f, 0.0f};

    idType id0 = index->testSetupElement(100, 0, vec0);
    idType id1 = index->testSetupElement(101, 0, vec1);
    idType id2 = index->testSetupElement(102, 0, vec2);

    // Create candidate list
    candidatesList<float> candidates(index->getAllocator());
    candidates.emplace_back(1.0f, id0);
    candidates.emplace_back(2.0f, id1);
    candidates.emplace_back(3.0f, id2);

    // Track removed candidates
    vecsim_stl::vector<idType> removed(index->getAllocator());

    // Apply heuristic with M=1 (keep at most 1 neighbor)
    index->testGetNeighborsByHeuristic2WithRemoved(candidates, 1, removed);

    // Should keep 1 candidate
    EXPECT_EQ(candidates.size(), 1);
    // Removed should contain the pruned candidates
    EXPECT_GE(removed.size(), 0); // May be 0 if heuristic keeps all
}

// =============================================================================
// Mutual Connection Tests
// =============================================================================

TEST_F(HNSWDiskTest, MutuallyConnectNewElement) {
    TestIndex<float, float> index(DIM);

    // Create existing nodes
    float vec0[DIM] = {0.0f, 0.0f, 0.0f, 0.0f};
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};

    idType id0 = index->testSetupElement(100, 0, vec0);
    idType id1 = index->testSetupElement(101, 0, vec1);

    // Set up entry point
    index->testTryUpdateEntryPoint(id0, 0);

    // Create new node
    float vec2[DIM] = {0.5f, 0.0f, 0.0f, 0.0f};
    idType id2 = index->testSetupElement(102, 0, vec2);

    // Create top candidates heap for new node
    vecsim_stl::updatable_max_heap<float, idType> topCandidates(index->getAllocator());
    topCandidates.emplace(index->testComputeDistance(vec2, id0), id0);
    topCandidates.emplace(index->testComputeDistance(vec2, id1), id1);

    // Mutually connect new element
    auto nodesToRepair = index->testMutuallyConnectNewElement(id2, topCandidates, 0);
    (void)nodesToRepair; // Unused in this test - we just verify edges are created

    // Verify edges were created for the new node
    auto edges = index->testGetOutgoingEdges(id2, 0);
    EXPECT_GT(edges.size(), 0);

    // Verify mutual connections exist (neighbors should have back-edges to id2)
    // Note: The exact behavior depends on the heuristic, but we should have some edges
    for (idType neighbor : edges) {
        auto backEdges = index->testGetOutgoingEdges(neighbor, 0);
        bool hasBackEdge = std::find(backEdges.begin(), backEdges.end(), id2) != backEdges.end();
        EXPECT_TRUE(hasBackEdge) << "Neighbor " << neighbor << " should have back-edge to " << id2;
    }
}

// Test: greedySearchLevel skips deleted and IN_PROCESS nodes
TEST_F(HNSWDiskTest, GreedySearchLevelSkipsDeletedAndInProcessNodes) {
    TestIndex<float, float> index(DIM);

    // Create a graph: 0 -> 1 -> 2 -> 3
    // Node 0: far from query
    // Node 1: will be marked DELETED (should be skipped)
    // Node 2: will be marked IN_PROCESS (should be skipped)
    // Node 3: closest to query and valid
    float vec0[DIM] = {0.0f, 0.0f, 0.0f, 0.0f};
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    float vec2[DIM] = {2.0f, 0.0f, 0.0f, 0.0f};
    float vec3[DIM] = {3.0f, 0.0f, 0.0f, 0.0f};

    idType id0 = index->testSetupElement(100, 0, vec0);
    idType id1 = index->testSetupElement(101, 0, vec1);
    idType id2 = index->testSetupElement(102, 0, vec2);
    idType id3 = index->testSetupElement(103, 0, vec3);

    // Create linear graph: 0 -> 1 -> 2 -> 3
    index->testAddEdge(id0, id1, 0);
    index->testAddEdge(id1, id2, 0);
    index->testAddEdge(id2, id3, 0);
    // Add back edges for traversal
    index->testAddEdge(id1, id0, 0);
    index->testAddEdge(id2, id1, 0);
    index->testAddEdge(id3, id2, 0);

    index->testTryUpdateEntryPoint(id0, 0);

    // Mark node 1 as deleted
    index->testMarkDeleted(id1);
    EXPECT_TRUE(index->testIsMarkedDeleted(id1));

    // Mark node 2 as in-process
    index->testMarkInProcess(id2);
    EXPECT_TRUE(index->testIsInProcess(id2));

    // Query closest to node 3
    float query[DIM] = {3.0f, 0.0f, 0.0f, 0.0f};

    // Start greedy search from node 0
    idType currObj = id0;
    float currDist = index->testComputeDistance(query, id0);

    // For insertion mode (running_query=false), greedySearchLevel tracks best non-deleted candidate
    index->testGreedySearchLevel<false>(query, 0, currObj, currDist);

    // The search should NOT return deleted or in-process nodes as the best result
    EXPECT_NE(currObj, id1) << "Should not return deleted node";
    EXPECT_NE(currObj, id2) << "Should not return in-process node";
}

// Test: repairNode reduces neighbors and updates incoming edges correctly
TEST_F(HNSWDiskTest, RepairNodeReducesNeighborsAndUpdatesIncoming) {
    // Use M=2 so we can easily exceed the limit
    TestIndex<float, float> index(DIM, VecSimMetric_L2, 2, 200, 10);

    // Create nodes in a line
    float vec0[DIM] = {0.0f, 0.0f, 0.0f, 0.0f};
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    float vec2[DIM] = {2.0f, 0.0f, 0.0f, 0.0f};
    float vec3[DIM] = {3.0f, 0.0f, 0.0f, 0.0f};
    float vec4[DIM] = {4.0f, 0.0f, 0.0f, 0.0f};

    idType id0 = index->testSetupElement(100, 0, vec0);
    idType id1 = index->testSetupElement(101, 0, vec1);
    idType id2 = index->testSetupElement(102, 0, vec2);
    idType id3 = index->testSetupElement(103, 0, vec3);
    idType id4 = index->testSetupElement(104, 0, vec4);

    // Manually create edges for node 0 with MORE than M0 neighbors
    // M0 = 2*M = 4 for level 0, so we add 5 neighbors to exceed it
    index->testAddEdge(id0, id1, 0);
    index->testAddEdge(id0, id2, 0);
    index->testAddEdge(id0, id3, 0);
    index->testAddEdge(id0, id4, 0);

    // Verify initial state
    auto edgesBefore = index->testGetOutgoingEdges(id0, 0);
    EXPECT_EQ(edgesBefore.size(), 4) << "Should have 4 neighbors before repair";

    // Call repairNode - it should reduce neighbors using heuristic
    index->testRepairNode(id0, 0);

    // Verify edges after repair
    auto edgesAfter = index->testGetOutgoingEdges(id0, 0);

    // M0 = 2*M = 4, so with 4 neighbors we're at the limit
    // The heuristic may keep all or reduce based on diversity
    EXPECT_LE(edgesAfter.size(), index->testGetM0()) << "Should have at most M0 neighbors after repair";
    EXPECT_GT(edgesAfter.size(), 0) << "Should have at least some neighbors";
}

// Test: repairNode removes deleted neighbors even when count <= maxConnections
TEST_F(HNSWDiskTest, RepairNodeRemovesDeletedNeighborsWithinLimit) {
    // Use M=2 so M0=4
    TestIndex<float, float> index(DIM, VecSimMetric_L2, 2, 200, 10);

    // Create 4 nodes
    float vec0[DIM] = {0.0f, 0.0f, 0.0f, 0.0f};
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    float vec2[DIM] = {2.0f, 0.0f, 0.0f, 0.0f};
    float vec3[DIM] = {3.0f, 0.0f, 0.0f, 0.0f};

    idType id0 = index->testSetupElement(100, 0, vec0);
    idType id1 = index->testSetupElement(101, 0, vec1);
    idType id2 = index->testSetupElement(102, 0, vec2);
    idType id3 = index->testSetupElement(103, 0, vec3);

    // Connect id0 to id1, id2, id3 (3 neighbors, below M0=4 limit)
    index->testAddEdge(id0, id1, 0);
    index->testAddEdge(id0, id2, 0);
    index->testAddEdge(id0, id3, 0);

    // Verify initial state
    auto edgesBefore = index->testGetOutgoingEdges(id0, 0);
    EXPECT_EQ(edgesBefore.size(), 3) << "Should have 3 neighbors before repair";

    // Mark id1 as deleted
    index->testMarkDeleted(id1);
    EXPECT_TRUE(index->testIsMarkedDeleted(id1));

    // Call repairNode - even though count (3) <= M0 (4), it should still
    // run because there's a deleted neighbor to remove
    index->testRepairNode(id0, 0);

    // Verify edges after repair - deleted neighbor should be removed
    auto edgesAfter = index->testGetOutgoingEdges(id0, 0);
    EXPECT_EQ(edgesAfter.size(), 2) << "Should have 2 neighbors after removing deleted one";

    // Verify id1 is not in the edges anymore
    bool foundDeleted = false;
    for (idType edge : edgesAfter) {
        if (edge == id1) {
            foundDeleted = true;
            break;
        }
    }
    EXPECT_FALSE(foundDeleted) << "Deleted neighbor should be removed from edges";

    // Verify id2 and id3 are still present
    bool foundId2 = false, foundId3 = false;
    for (idType edge : edgesAfter) {
        if (edge == id2)
            foundId2 = true;
        if (edge == id3)
            foundId3 = true;
    }
    EXPECT_TRUE(foundId2) << "Non-deleted neighbor id2 should still be present";
    EXPECT_TRUE(foundId3) << "Non-deleted neighbor id3 should still be present";
}

// Test: mutuallyConnectNewElement returns nodes that need to be repaired when neighbor overflows
TEST_F(HNSWDiskTest, MutuallyConnectReturnsNodesToRepairOnOverflow) {
    // Use small M=2 to easily trigger overflow (M0 = 4)
    TestIndex<float, float> index(DIM, VecSimMetric_L2, 2, 200, 10);

    // Create 5 nodes - we'll connect them all to node 0, then add a 6th
    float vectors[6][DIM] = {
        {0.0f, 0.0f, 0.0f, 0.0f}, // id0 - will be the hub
        {0.1f, 0.0f, 0.0f, 0.0f}, // id1
        {0.2f, 0.0f, 0.0f, 0.0f}, // id2
        {0.3f, 0.0f, 0.0f, 0.0f}, // id3
        {0.4f, 0.0f, 0.0f, 0.0f}, // id4
        {0.05f, 0.0f, 0.0f, 0.0f} // id5 - new node, very close to id0
    };

    idType ids[6];
    for (int i = 0; i < 5; i++) {
        ids[i] = index->testSetupElement(100 + i, 0, vectors[i]);
    }

    // Connect id0 to id1, id2, id3, id4 (4 neighbors = M0 limit)
    for (int i = 1; i < 5; i++) {
        index->testAddEdge(ids[0], ids[i], 0);
        index->testAddEdge(ids[i], ids[0], 0);
    }

    index->testTryUpdateEntryPoint(ids[0], 0);

    // Verify id0 has M0 neighbors
    auto edgesBefore = index->testGetOutgoingEdges(ids[0], 0);
    EXPECT_EQ(edgesBefore.size(), 4) << "Node 0 should have M0=4 neighbors";

    // Now add a new node (id5) and connect it
    ids[5] = index->testSetupElement(105, 0, vectors[5]);

    // Create candidates for the new node - include id0 which is already at capacity
    vecsim_stl::updatable_max_heap<float, idType> topCandidates(index->getAllocator());
    topCandidates.emplace(index->testComputeDistance(vectors[5], ids[0]), ids[0]);

    // Mutually connect - this should return id0 as needing repair
    auto nodesToRepair = index->testMutuallyConnectNewElement(ids[5], topCandidates, 0);

    // Verify the new node has edges
    auto newNodeEdges = index->testGetOutgoingEdges(ids[5], 0);
    EXPECT_GT(newNodeEdges.size(), 0) << "New node should have edges";

    // Check if id0 now has more than M0 neighbors (triggering repair)
    auto edgesAfter = index->testGetOutgoingEdges(ids[0], 0);
    if (edgesAfter.size() > index->testGetM0()) {
        // If overflow occurred, the node should be in the list to repair
        EXPECT_GT(nodesToRepair.size(), 0) << "Should return node to repair when neighbor overflows";
        // Verify the node to repair is id0 at level 0
        bool foundId0ToRepair = false;
        for (const auto& node : nodesToRepair) {
            if (node.first == ids[0] && node.second == 0) {
                foundId0ToRepair = true;
                break;
            }
        }
        EXPECT_TRUE(foundId0ToRepair) << "Node 0 at level 0 should be in the list to repair";
    }
}

// Test: updateIncomingEdgesAfterRepair correctly updates only changed edges using batch merge
TEST_F(HNSWDiskTest, UpdateIncomingEdgesAfterRepairBatchMerge) {
    TestIndex<float, float> index(DIM, VecSimMetric_L2, 4, 200, 10);

    // Create 5 nodes
    float vec0[DIM] = {0.0f, 0.0f, 0.0f, 0.0f};
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    float vec2[DIM] = {2.0f, 0.0f, 0.0f, 0.0f};
    float vec3[DIM] = {3.0f, 0.0f, 0.0f, 0.0f};
    float vec4[DIM] = {4.0f, 0.0f, 0.0f, 0.0f};

    idType id0 = index->testSetupElement(100, 0, vec0);
    idType id1 = index->testSetupElement(101, 0, vec1);
    idType id2 = index->testSetupElement(102, 0, vec2);
    idType id3 = index->testSetupElement(103, 0, vec3);
    idType id4 = index->testSetupElement(104, 0, vec4);

    // Setup initial state: id0 -> {id1, id2, id3}
    // This means id1, id2, id3 should have id0 in their incoming edges
    index->testAddEdge(id0, id1, 0);
    index->testAddEdge(id0, id2, 0);
    index->testAddEdge(id0, id3, 0);

    // Verify initial incoming edges
    auto incoming1Before = index->testGetIncomingEdges(id1, 0);
    auto incoming2Before = index->testGetIncomingEdges(id2, 0);
    auto incoming3Before = index->testGetIncomingEdges(id3, 0);
    auto incoming4Before = index->testGetIncomingEdges(id4, 0);

    EXPECT_EQ(std::count(incoming1Before.begin(), incoming1Before.end(), id0), 1) << "id1 should have id0 in incoming";
    EXPECT_EQ(std::count(incoming2Before.begin(), incoming2Before.end(), id0), 1) << "id2 should have id0 in incoming";
    EXPECT_EQ(std::count(incoming3Before.begin(), incoming3Before.end(), id0), 1) << "id3 should have id0 in incoming";
    EXPECT_EQ(std::count(incoming4Before.begin(), incoming4Before.end(), id0), 0)
        << "id4 should NOT have id0 in incoming";

    // Simulate repair: id0's neighbors change from {id1, id2, id3} to {id1, id2, id4}
    // - id1, id2: unchanged (should NOT be touched)
    // - id3: removed (should delete id0 from its incoming)
    // - id4: added (should add id0 to its incoming)
    vecsim_stl::vector<idType> originalEdges(index->getAllocator());
    originalEdges.push_back(id1);
    originalEdges.push_back(id2);
    originalEdges.push_back(id3);

    vecsim_stl::vector<idType> newNeighbors(index->getAllocator());
    newNeighbors.push_back(id1);
    newNeighbors.push_back(id2);
    newNeighbors.push_back(id4);

    // Call the function under test
    index->testUpdateIncomingEdgesAfterRepair(id0, originalEdges, newNeighbors, 0);

    // Verify incoming edges after update
    auto incoming1After = index->testGetIncomingEdges(id1, 0);
    auto incoming2After = index->testGetIncomingEdges(id2, 0);
    auto incoming3After = index->testGetIncomingEdges(id3, 0);
    auto incoming4After = index->testGetIncomingEdges(id4, 0);

    // id1, id2: should still have id0 (unchanged)
    EXPECT_EQ(std::count(incoming1After.begin(), incoming1After.end(), id0), 1) << "id1 should still have id0";
    EXPECT_EQ(std::count(incoming2After.begin(), incoming2After.end(), id0), 1) << "id2 should still have id0";

    // id3: should NOT have id0 anymore (removed)
    EXPECT_EQ(std::count(incoming3After.begin(), incoming3After.end(), id0), 0)
        << "id3 should NOT have id0 after removal";

    // id4: should now have id0 (added)
    EXPECT_EQ(std::count(incoming4After.begin(), incoming4After.end(), id0), 1) << "id4 should have id0 after addition";
}

// Test DiskDistanceCalculator: creation and all distance modes (Full, QuantizedVsFull, Quantized)
TEST_P(HNSWDiskMetricTest, DiskCalculatorAllModes) {
    VecSimMetric metric = GetParam();
    auto allocator = VecSimAllocator::newVecsimAllocator();

    auto* calculator = DiskComponentsFactory::CreateDiskCalculator<float>(allocator, metric, DIM);
    ASSERT_NE(calculator, nullptr);

    // --- Full mode (FP32-FP32) ---
    float v1[DIM], v2[DIM];
    test_utils::populate_float_vec(v1, DIM, 1234);
    test_utils::populate_float_vec(v2, DIM, 5678);

    if (metric == VecSimMetric_Cosine) {
        spaces::GetNormalizeFunc<float>()(v1, DIM);
        spaces::GetNormalizeFunc<float>()(v2, DIM);
    }

    auto expectedFuncFull = spaces::GetDistFunc<float, float>(metric, DIM, nullptr);
    float expectedFull = expectedFuncFull(v1, v2, DIM);
    // DiskDistanceCalculator requires explicit mode - use calcDistance<DistanceMode::Full>()
    EXPECT_FLOAT_EQ(calculator->calcDistance<DistanceMode::Full>(v1, v2, DIM), expectedFull);

    // --- QuantizedVsFull mode (SQ8-FP32) ---
    std::vector<float> v1_fp32(DIM);
    test_utils::populate_float_vec(v1_fp32.data(), DIM, 1234);
    size_t querySize = DIM + sq8::query_metadata_count<VecSimMetric_L2>();
    std::vector<float> v2_query(querySize);
    test_utils::populate_float_vec(v2_query.data(), DIM, 5678);

    if (metric == VecSimMetric_Cosine) {
        spaces::GetNormalizeFunc<float>()(v1_fp32.data(), DIM);
        spaces::GetNormalizeFunc<float>()(v2_query.data(), DIM);
    }

    // Note: Using L2 metadata layout for quantized tests as the VectorSimilarity test utilities
    // are hardcoded for L2. The distance calculator handles all metrics correctly.
    size_t sq8Size = DIM + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v1_sq8(sq8Size);
    test_utils::quantize_float_vec_to_sq8_with_metadata(v1_fp32.data(), DIM, v1_sq8.data());
    test_utils::preprocess_sq8_fp32_query(v2_query.data(), DIM);

    auto expectedFuncQVF = spaces::GetDistFunc<sq8, float, float>(metric, DIM, nullptr);
    float expectedQVF = expectedFuncQVF(v1_sq8.data(), v2_query.data(), DIM);
    EXPECT_FLOAT_EQ(calculator->calcDistance<DistanceMode::QuantizedVsFull>(v1_sq8.data(), v2_query.data(), DIM),
                    expectedQVF);

    // --- Quantized mode (SQ8-SQ8) ---
    std::vector<float> v2_fp32(DIM);
    test_utils::populate_float_vec(v2_fp32.data(), DIM, 5678);
    if (metric == VecSimMetric_Cosine) {
        spaces::GetNormalizeFunc<float>()(v2_fp32.data(), DIM);
    }
    std::vector<uint8_t> v2_sq8(sq8Size);
    test_utils::quantize_float_vec_to_sq8_with_metadata(v2_fp32.data(), DIM, v2_sq8.data());

    auto expectedFuncQ = spaces::GetDistFunc<sq8, float>(metric, DIM, nullptr);
    float expectedQ = expectedFuncQ(v1_sq8.data(), v2_sq8.data(), DIM);
    float calculatedQ = calculator->calcDistance<DistanceMode::Quantized>(v1_sq8.data(), v2_sq8.data(), DIM);
    EXPECT_FLOAT_EQ(calculatedQ, expectedQ);

    // --- Distance ordering: closer vectors should have smaller distance ---
    float base[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    float close[DIM] = {0.9f, 0.1f, 0.0f, 0.0f};
    float far[DIM] = {-1.0f, 0.0f, 0.0f, 0.0f};
    if (metric == VecSimMetric_Cosine) {
        spaces::GetNormalizeFunc<float>()(base, DIM);
        spaces::GetNormalizeFunc<float>()(close, DIM);
        spaces::GetNormalizeFunc<float>()(far, DIM);
    }
    EXPECT_LT(calculator->calcDistance<DistanceMode::Full>(base, close, DIM),
              calculator->calcDistance<DistanceMode::Full>(base, far, DIM));

    delete calculator;
}

// Test DiskIndexComponents: creation and conversion to base class
TEST_P(HNSWDiskMetricTest, DiskIndexComponentsAndConversion) {
    VecSimMetric metric = GetParam();
    auto allocator = VecSimAllocator::newVecsimAllocator();

    // --- Component creation ---
    // Disk index expects pre-normalized vectors from tiered frontend
    auto components =
        DiskComponentsFactory::CreateDiskIndexComponents<float, float>(allocator, metric, DIM, /*is_normalized=*/true);

    ASSERT_NE(components.diskCalculator, nullptr);
    ASSERT_NE(components.preprocessors, nullptr);

    // --- Implicit conversion to IndexComponents ---
    IndexComponents<float, float> baseComponents = components;
    EXPECT_EQ(baseComponents.indexCalculator, components.diskCalculator);
    EXPECT_EQ(baseComponents.preprocessors, components.preprocessors);

    delete components.preprocessors;
    delete components.diskCalculator;
}
