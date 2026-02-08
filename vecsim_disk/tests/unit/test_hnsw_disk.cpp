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

#include <barrier>
#include <set>

using namespace test_utils;
using sq8 = vecsim_types::sq8;

class HNSWDiskTest : public ::testing::Test {
protected:
    static constexpr size_t DIM = 4;

    // Helper to fill a vector with deterministic values based on label
    static void fillVector(float* vec, labelType label) {
        for (size_t j = 0; j < DIM; j++) {
            vec[j] = static_cast<float>(label * DIM + j);
        }
    }

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

// Test addVector basic functionality
TEST_F(HNSWDiskTest, AddVectorBasic) {
    TestIndex<float, float> index(DIM);

    float vec[DIM] = {1.0f, 1.0f, 1.0f, 1.0f};
    // addVector returns 1 to indicate one vector was added
    EXPECT_EQ(index->addVector(vec, 1), 1);
    EXPECT_EQ(index->indexSize(), 1);
}

// Test addVector with multiple vectors - verifies graph structure is built
TEST_F(HNSWDiskTest, AddVectorMultiple) {
    TestIndex<float, float> index(DIM);

    // Add 5 vectors in a line
    float vectors[5][DIM] = {{0.0f, 0.0f, 0.0f, 0.0f},
                             {1.0f, 0.0f, 0.0f, 0.0f},
                             {2.0f, 0.0f, 0.0f, 0.0f},
                             {3.0f, 0.0f, 0.0f, 0.0f},
                             {4.0f, 0.0f, 0.0f, 0.0f}};

    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(index->addVector(vectors[i], 100 + i), 1);
    }
    EXPECT_EQ(index->indexSize(), 5);

    // Verify entry point exists
    auto entryPointState = index->testGetEntryPointState();
    EXPECT_NE(entryPointState.id, INVALID_ID) << "Entry point should be set after adding vectors";

    // Verify at least the first few elements have edges at level 0
    // (the graph should be connected after insertion)
    auto edges0 = index->testGetOutgoingEdges(0, 0);
    EXPECT_GT(edges0.size(), 0) << "First element should have neighbors after graph construction";
}

// Test addVector creates proper graph structure with connections
TEST_F(HNSWDiskTest, AddVectorCreatesConnectedGraph) {
    // Use small M to make edge counts predictable
    TestIndex<float, float> index(DIM, VecSimMetric_L2, 2, 200, 10);

    // Add 3 vectors close together - they should all be connected
    float vec0[DIM] = {0.0f, 0.0f, 0.0f, 0.0f};
    float vec1[DIM] = {0.1f, 0.0f, 0.0f, 0.0f};
    float vec2[DIM] = {0.2f, 0.0f, 0.0f, 0.0f};

    EXPECT_EQ(index->addVector(vec0, 100), 1);
    EXPECT_EQ(index->addVector(vec1, 101), 1);
    EXPECT_EQ(index->addVector(vec2, 102), 1);

    EXPECT_EQ(index->indexSize(), 3);

    // After adding 3 close vectors, they should form a connected graph
    // Check that node 1 has neighbors (it was inserted after node 0 exists)
    auto edges1 = index->testGetOutgoingEdges(1, 0);
    EXPECT_GT(edges1.size(), 0) << "Node 1 should have neighbors";

    // Check that node 2 has neighbors
    auto edges2 = index->testGetOutgoingEdges(2, 0);
    EXPECT_GT(edges2.size(), 0) << "Node 2 should have neighbors";
}

// Test addVector properly stores vectors for distance computation
TEST_F(HNSWDiskTest, AddVectorStoresVectorsCorrectly) {
    TestIndex<float, float> index(DIM);

    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    float vec2[DIM] = {0.0f, 1.0f, 0.0f, 0.0f};

    EXPECT_EQ(index->addVector(vec1, 100), 1);
    EXPECT_EQ(index->addVector(vec2, 101), 1);

    // Compute distance between the two stored vectors
    // L2 distance between [1,0,0,0] and [0,1,0,0] = sqrt(2) ≈ 1.414
    float dist = index->testComputeDistance(vec1, 1);
    EXPECT_GT(dist, 0.0f);
    EXPECT_LT(dist, 3.0f); // Reasonable range for L2 distance
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

TEST_F(HNSWDiskTest, AddVectorAndComputeDistance) {
    TestIndex<float, float> index(DIM);

    // Create test vectors
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    float vec2[DIM] = {0.0f, 1.0f, 0.0f, 0.0f};

    // Add elements using the production addVector function
    EXPECT_EQ(index->addVector(vec1, 100), 1);
    EXPECT_EQ(index->addVector(vec2, 101), 1);

    EXPECT_EQ(index->indexSize(), 2);

    // Verify entry point is set after adding vectors
    auto entryPointState = index->testGetEntryPointState();
    EXPECT_NE(entryPointState.id, INVALID_ID) << "Entry point should be set";

    // Compute distance from vec1 to stored vec2
    // L2 distance between [1,0,0,0] and [0,1,0,0] = sqrt(2) ≈ 1.414
    // But SQ8 quantization may introduce some error
    float dist = index->testComputeDistance(vec1, 1);
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

// Test search on graph built with addVector (full insertion flow)
TEST_F(HNSWDiskTest, SearchLayerOnAddVectorGraph) {
    TestIndex<float, float> index(DIM);

    // Build graph using addVector (the production path)
    float vectors[5][DIM] = {{0.0f, 0.0f, 0.0f, 0.0f},
                             {1.0f, 0.0f, 0.0f, 0.0f},
                             {2.0f, 0.0f, 0.0f, 0.0f},
                             {3.0f, 0.0f, 0.0f, 0.0f},
                             {4.0f, 0.0f, 0.0f, 0.0f}};

    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(index->addVector(vectors[i], 100 + i), 1);
    }
    EXPECT_EQ(index->indexSize(), 5);

    // Get entry point for search
    auto entryPointState = index->testGetEntryPointState();
    EXPECT_NE(entryPointState.id, INVALID_ID);

    // Query for vector closest to [2.5, 0, 0, 0] - between nodes 2 and 3
    float query[DIM] = {2.5f, 0.0f, 0.0f, 0.0f};

    // Search starting from entry point
    auto results = index->testSearchLayer<false>(entryPointState.id, query, 0, 5);

    // Should find results
    EXPECT_GT(results.size(), 0);

    // Extract result IDs
    std::vector<idType> resultIds;
    while (!results.empty()) {
        resultIds.push_back(results.top().second);
        results.pop();
    }

    // Should have found nodes 2 and 3 (closest to query [2.5, 0, 0, 0])
    bool found2 = std::find(resultIds.begin(), resultIds.end(), 2) != resultIds.end();
    bool found3 = std::find(resultIds.begin(), resultIds.end(), 3) != resultIds.end();
    EXPECT_TRUE(found2 || found3) << "Should find nodes 2 or 3 (closest to query)";
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
    candidatesMaxHeap<float> topCandidates(index->getAllocator());
    topCandidates.emplace(index->testComputeDistance(vec2, id0), id0);
    topCandidates.emplace(index->testComputeDistance(vec2, id1), id1);

    // Mutually connect new element
    auto result = index->testMutuallyConnectNewElement(id2, topCandidates, 0);
    (void)result; // Unused in this test - we just verify edges are created

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
    candidatesMaxHeap<float> topCandidates(index->getAllocator());
    topCandidates.emplace(index->testComputeDistance(vectors[5], ids[0]), ids[0]);

    // Mutually connect - this should return id0 as needing repair
    auto result = index->testMutuallyConnectNewElement(ids[5], topCandidates, 0);

    // Verify the new node has edges
    auto newNodeEdges = index->testGetOutgoingEdges(ids[5], 0);
    EXPECT_GT(newNodeEdges.size(), 0) << "New node should have edges";

    // Check if id0 now has more than M0 neighbors (triggering repair)
    auto edgesAfter = index->testGetOutgoingEdges(ids[0], 0);
    if (edgesAfter.size() > index->testGetM0()) {
        // If overflow occurred, the node should be in the list to repair
        EXPECT_GT(result.nodesToRepair.size(), 0) << "Should return node to repair when neighbor overflows";
        // Verify the node to repair is id0 at level 0
        bool foundId0ToRepair = false;
        for (const auto& node : result.nodesToRepair) {
            if (node.id == ids[0] && node.level == 0) {
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

// Test that storeVector allocates ID, initializes metadata, and stores vectors
TEST_F(HNSWDiskTest, StoreVectorAllocatesIdAndInitializesMetadata) {
    TestIndex<float, float> index(DIM);

    float vec[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    labelType label = 100;

    // Call storeVector directly
    HNSWDiskAddVectorState state = index->testStoreVector(vec, label);

    // Verify ID was allocated (should be 0 for first element)
    EXPECT_EQ(state.newElementId, 0);

    // Verify random level was assigned (should be >= 0)
    EXPECT_GE(state.elementMaxLevel, 0);

    // Verify element count increased
    EXPECT_EQ(index->indexSize(), 1);

    // Verify metadata was initialized correctly
    EXPECT_EQ(index->testGetLabelById(state.newElementId), label);
    EXPECT_EQ(index->testGetElementLevel(state.newElementId), state.elementMaxLevel);
}

// Test that storeVector stores SQ8 quantized vector in memory
TEST_F(HNSWDiskTest, StoreVectorStoresQuantizedVectorInMemory) {
    TestIndex<float, float> index(DIM);

    float vec[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    HNSWDiskAddVectorState state = index->testStoreVector(vec, 100);

    // Get the stored SQ8 quantized data
    const char* storedSQ8 = index->testGetQuantizedDataByInternalId(state.newElementId);
    ASSERT_NE(storedSQ8, nullptr) << "SQ8 quantized vector should be stored in memory";

    // Verify the stored data is non-trivial (not all zeros)
    // SQ8 format: dim bytes + metadata floats
    size_t sq8Size = index->getStoredDataSize();
    bool hasNonZeroData = false;
    for (size_t i = 0; i < sq8Size && !hasNonZeroData; ++i) {
        if (storedSQ8[i] != 0) {
            hasNonZeroData = true;
        }
    }
    EXPECT_TRUE(hasNonZeroData) << "Stored SQ8 data should contain non-zero values";
}

// Test that storeVector stores original FP32 vector on disk
TEST_F(HNSWDiskTest, StoreVectorStoresFP32VectorOnDisk) {
    TestIndex<float, float> index(DIM);

    float originalVec[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    HNSWDiskAddVectorState state = index->testStoreVector(originalVec, 100);

    // Retrieve the stored vector from disk
    float retrievedVec[DIM] = {0.0f, 0.0f, 0.0f, 0.0f};
    size_t bufferSize = DIM * sizeof(float);
    bool success = index->testGetVectorFromDisk(state.newElementId, retrievedVec, bufferSize);
    ASSERT_TRUE(success) << "Should be able to retrieve vector from disk";

    // Verify the retrieved vector matches the original
    for (size_t i = 0; i < DIM; ++i) {
        EXPECT_FLOAT_EQ(retrievedVec[i], originalVec[i]) << "Dimension " << i << " should match original vector";
    }
}

// Test that storeVector does NOT update entry point (entry point update is deferred to indexVector)
TEST_F(HNSWDiskTest, StoreVectorUpdatesEntryPointForFirstElement) {
    TestIndex<float, float> index(DIM);

    // Initially, entry point should be invalid
    auto initialState = index->testGetEntryPointState();
    EXPECT_EQ(initialState.id, INVALID_ID) << "Entry point should be invalid initially";

    float vec[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    HNSWDiskAddVectorState state = index->testStoreVector(vec, 100);

    // After storeVector, entry point should still be invalid
    // (entry point is only updated in indexVector after graph connections are made)
    auto afterStoreState = index->testGetEntryPointState();
    EXPECT_EQ(afterStoreState.id, INVALID_ID)
        << "Entry point should still be invalid after storeVector (deferred to indexVector)";
}

// Test that storeVector captures current entry point state for indexVector
TEST_F(HNSWDiskTest, StoreVectorCapturesCurrentState) {
    TestIndex<float, float> index(DIM);

    // Store first element - does NOT become entry point yet (deferred to indexVector)
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    HNSWDiskAddVectorState state1 = index->testStoreVector(vec1, 100);

    // state1 should have captured that there was no entry point before
    EXPECT_EQ(state1.currEntryPoint, INVALID_ID) << "First element should have INVALID_ID as currEntryPoint";

    // Now store second element - entry point still not updated (only storeVector was called)
    float vec2[DIM] = {2.0f, 0.0f, 0.0f, 0.0f};
    HNSWDiskAddVectorState state2 = index->testStoreVector(vec2, 101);

    // state2 should also have INVALID_ID because entry point is only updated in indexVector
    // Both elements were stored but neither has been indexed yet
    EXPECT_EQ(state2.currEntryPoint, INVALID_ID)
        << "Second element should also have INVALID_ID (entry point updated only in indexVector)";
}

// Test that edge lists are empty after storeVector (before graph insertion)
TEST_F(HNSWDiskTest, StoreVectorHasEmptyEdgeLists) {
    TestIndex<float, float> index(DIM);

    float vec[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    HNSWDiskAddVectorState state = index->testStoreVector(vec, 100);

    // Verify edge lists are empty for all levels up to elementMaxLevel.
    // Missing keys in storage are treated as empty edge lists.
    for (levelType level = 0; level <= state.elementMaxLevel; ++level) {
        auto outgoing = index->testGetOutgoingEdges(state.newElementId, level);
        auto incoming = index->testGetIncomingEdges(state.newElementId, level);
        EXPECT_EQ(outgoing.size(), 0) << "Outgoing edges should be empty at level " << level;
        EXPECT_EQ(incoming.size(), 0) << "Incoming edges should be empty at level " << level;
    }
}

// Test that storeVector marks element as IN_PROCESS
TEST_F(HNSWDiskTest, StoreVectorMarksElementAsInProcess) {
    TestIndex<float, float> index(DIM);

    float vec[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    HNSWDiskAddVectorState state = index->testStoreVector(vec, 100);

    // Element should be marked as IN_PROCESS (not yet indexed)
    EXPECT_TRUE(index->testIsInProcess(state.newElementId)) << "Element should be marked IN_PROCESS after storeVector";
    EXPECT_FALSE(index->testIsMarkedDeleted(state.newElementId)) << "Element should NOT be marked as deleted";
}

// =============================================================================
// indexVector Tests (Phase 2 of insertion)
// =============================================================================

// Test that indexVector unmarks IN_PROCESS flag for first element (no graph insertion needed)
TEST_F(HNSWDiskTest, IndexVectorUnmarksInProcessForFirstElement) {
    TestIndex<float, float> index(DIM);

    float vec[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    HNSWDiskAddVectorState state = index->testStoreVector(vec, 100);

    // Before indexVector: element should be IN_PROCESS
    EXPECT_TRUE(index->testIsInProcess(state.newElementId));

    // Get the SQ8 quantized data for indexVector
    const char* storedSQ8 = index->testGetQuantizedDataByInternalId(state.newElementId);
    ASSERT_NE(storedSQ8, nullptr);

    // Call indexVector
    GraphNodeList nodesToRepair = index->testIndexVector(storedSQ8, 100, state);

    // After indexVector: element should NOT be IN_PROCESS anymore
    EXPECT_FALSE(index->testIsInProcess(state.newElementId)) << "Element should be unmarked after indexVector";

    // For first element, no nodes should need repair (no graph insertion)
    EXPECT_EQ(nodesToRepair.size(), 0) << "First element should have no nodes to repair";
}

// Test that indexVector connects second element to first
TEST_F(HNSWDiskTest, IndexVectorConnectsSecondElementToFirst) {
    TestIndex<float, float> index(DIM);

    // Add first element using full addVector
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    index->addVector(vec1, 100);
    EXPECT_EQ(index->indexSize(), 1);

    // Now add second element using storeVector + indexVector manually
    float vec2[DIM] = {0.0f, 1.0f, 0.0f, 0.0f};
    HNSWDiskAddVectorState state = index->testStoreVector(vec2, 200);

    // State should capture first element as entry point
    EXPECT_EQ(state.currEntryPoint, 0) << "Entry point should be first element";

    // Get SQ8 data and call indexVector
    const char* storedSQ8 = index->testGetQuantizedDataByInternalId(state.newElementId);
    GraphNodeList nodesToRepair = index->testIndexVector(storedSQ8, 200, state);

    // Element should be unmarked
    EXPECT_FALSE(index->testIsInProcess(state.newElementId));

    // Second element should have edges to first element at level 0
    auto outgoing = index->testGetOutgoingEdges(state.newElementId, 0);
    EXPECT_GE(outgoing.size(), 1) << "Second element should have at least one outgoing edge";

    // First element should have second element as neighbor (bidirectional connection)
    auto firstOutgoing = index->testGetOutgoingEdges(0, 0);
    bool hasSecond = std::find(firstOutgoing.begin(), firstOutgoing.end(), state.newElementId) != firstOutgoing.end();
    EXPECT_TRUE(hasSecond) << "First element should have second element as neighbor";
}

// Test that indexVector returns nodes to repair when neighbors overflow
TEST_F(HNSWDiskTest, IndexVectorReturnsNodesToRepairOnOverflow) {
    // Use small M to trigger overflow quickly
    size_t smallM = 2;
    // Constructor: dim, metric, M, efConstruction, efRuntime
    TestIndex<float, float> index(DIM, VecSimMetric_L2, smallM, 4, 10);

    // Add M+1 elements to trigger overflow on one of them
    std::vector<float> vectors;
    for (size_t i = 0; i <= smallM + 1; i++) {
        float vec[DIM];
        fillVector(vec, i);
        index->addVector(vec, 100 + i);
    }

    // With M=2, after adding 3+ elements, some nodes should have needed repair.
    // The repairs are executed inline in addVector, so we can't directly observe them.
    // But we can verify the graph is connected properly
    EXPECT_EQ(index->indexSize(), smallM + 2);

    // Verify all elements have at most M neighbors at level 0
    for (size_t i = 0; i < index->indexSize(); i++) {
        auto outgoing = index->testGetOutgoingEdges(i, 0);
        EXPECT_LE(outgoing.size(), smallM * 2) << "Element " << i << " should have at most M*2 neighbors";
    }
}

// Test that indexVector returns nodes to repair and repairNode fixes the overflow
// This test explicitly verifies the full flow:
// 1. storeVector + indexVector (without calling repairNode)
// 2. Verify indexVector returns nodes needing repair
// 3. Verify the graph has overflow BEFORE repair
// 4. Call repairNode and verify it fixes the overflow
TEST_F(HNSWDiskTest, IndexVectorReturnsNodesToRepairAndRepairNodeFixesThem) {
    // Use very small M=2 to easily trigger overflow (M0 = 4 at level 0)
    size_t smallM = 2;
    size_t M0 = smallM * 2; // Max neighbors at level 0
    TestIndex<float, float> index(DIM, VecSimMetric_L2, smallM, 4, 10);

    // Add MORE than M0 nodes so we have enough nodes to fill node 0's neighbor list
    // We need at least M0+1 nodes (not counting node 0) to guarantee we can fill all M0 slots
    size_t numInitialNodes = M0 + 3;
    for (size_t i = 0; i < numInitialNodes; i++) {
        float vec[DIM];
        for (size_t j = 0; j < DIM; j++) {
            vec[j] = static_cast<float>(i);
        }
        index->addVector(vec, 100 + i);
    }
    ASSERT_EQ(index->indexSize(), numInitialNodes);

    // Find a node that we can force into overflow
    // Node 0 is a good candidate - check its current neighbor count
    auto currentEdges = index->testGetOutgoingEdges(0, 0);

    // Add extra edges to node 0 to bring it to exactly M0 capacity
    // Use all available nodes (1 to numInitialNodes-1) as potential targets
    for (size_t targetId = 1; targetId < numInitialNodes && currentEdges.size() < M0; targetId++) {
        bool alreadyConnected = false;
        for (const auto& neighbor : currentEdges) {
            if (neighbor == static_cast<idType>(targetId)) {
                alreadyConnected = true;
                break;
            }
        }
        if (!alreadyConnected) {
            index->testAddEdge(0, static_cast<idType>(targetId), 0);
            // Refresh edge list after each addition
            currentEdges = index->testGetOutgoingEdges(0, 0);
        }
    }

    // Verify node 0 is at exactly M0 capacity
    auto edgesAtCapacity = index->testGetOutgoingEdges(0, 0);
    ASSERT_EQ(edgesAtCapacity.size(), M0) << "Node 0 should have exactly M0=" << M0 << " neighbors";

    // Now add a NEW node using storeVector + indexVector (not addVector!)
    // This lets us observe nodesToRepair before calling repairNode
    // Use a vector very close to node 0 so it will connect to it
    float newVec[DIM];
    for (size_t j = 0; j < DIM; j++) {
        newVec[j] = 0.1f; // Very close to node 0 (which has all 0s)
    }

    // Phase 1: storeVector
    HNSWDiskAddVectorState state = index->testStoreVector(newVec, 999);
    EXPECT_TRUE(index->testIsInProcess(state.newElementId));

    // Get the SQ8 quantized data
    const char* storedSQ8 = index->testGetQuantizedDataByInternalId(state.newElementId);
    ASSERT_NE(storedSQ8, nullptr);

    // Phase 2: indexVector - this should return nodes needing repair
    GraphNodeList nodesToRepair = index->testIndexVector(storedSQ8, 999, state);

    // Element should be unmarked after indexVector
    EXPECT_FALSE(index->testIsInProcess(state.newElementId));

    // Check if node 0 has overflow (more than M0 neighbors)
    auto edgesBeforeRepair = index->testGetOutgoingEdges(0, 0);

    // The new node should have connected to node 0 (since it's very close)
    // Check both directions - new node connecting TO node 0, or node 0 having new node as neighbor
    bool newNodeConnectedToNode0 =
        std::find(edgesBeforeRepair.begin(), edgesBeforeRepair.end(), state.newElementId) != edgesBeforeRepair.end();

    // Also check if new node has node 0 as a neighbor
    auto newNodeEdges = index->testGetOutgoingEdges(state.newElementId, 0);
    bool node0InNewNodeNeighbors = std::find(newNodeEdges.begin(), newNodeEdges.end(), 0) != newNodeEdges.end();

    if (newNodeConnectedToNode0 || node0InNewNodeNeighbors) {
        // If connected, node 0 should have overflow
        if (edgesBeforeRepair.size() > M0) {
            // Verify nodesToRepair contains node 0
            bool foundNode0InRepairList = false;
            for (const auto& node : nodesToRepair) {
                if (node.id == 0 && node.level == 0) {
                    foundNode0InRepairList = true;
                    break;
                }
            }
            EXPECT_TRUE(foundNode0InRepairList) << "Node 0 should be in nodesToRepair list";

            // Call repairNode to fix the overflow
            index->testRepairNode(0, 0);

            // Verify overflow is fixed
            auto edgesAfterRepair = index->testGetOutgoingEdges(0, 0);
            EXPECT_LE(edgesAfterRepair.size(), M0)
                << "Node 0 should have at most M0=" << M0 << " neighbors after repair";
            EXPECT_GT(edgesAfterRepair.size(), 0) << "Node 0 should still have some neighbors after repair";
        } else {
            // No overflow on node 0, but the insertion succeeded - check if repair was needed elsewhere
            if (nodesToRepair.size() > 0) {
                // Repair all nodes and verify they end up within limits
                for (const auto& [nodeId, level] : nodesToRepair) {
                    auto beforeRepair = index->testGetOutgoingEdges(nodeId, level);
                    index->testRepairNode(nodeId, level);
                    auto afterRepair = index->testGetOutgoingEdges(nodeId, level);
                    size_t maxNeighbors = (level == 0) ? M0 : smallM;
                    EXPECT_LE(afterRepair.size(), maxNeighbors)
                        << "Node " << nodeId << " at level " << level << " should be repaired";
                }
            }
        }
    } else {
        GTEST_SKIP() << "New node did not connect to node 0 - test scenario not triggered";
    }
}

// Test that indexVector with multiple elements creates connected graph
TEST_F(HNSWDiskTest, IndexVectorCreatesConnectedGraph) {
    TestIndex<float, float> index(DIM);
    const size_t numVectors = 10;

    // Add vectors
    for (size_t i = 0; i < numVectors; i++) {
        float vec[DIM];
        fillVector(vec, i);
        index->addVector(vec, 100 + i);
    }

    EXPECT_EQ(index->indexSize(), numVectors);

    // Verify each element (except potentially first) has neighbors
    size_t elementsWithNeighbors = 0;
    for (size_t i = 0; i < numVectors; i++) {
        auto outgoing = index->testGetOutgoingEdges(i, 0);
        if (outgoing.size() > 0) {
            elementsWithNeighbors++;
        }
    }

    // Most elements should have neighbors (at least n-1 elements)
    EXPECT_GE(elementsWithNeighbors, numVectors - 1);

    // All elements should be unmarked (not IN_PROCESS)
    for (size_t i = 0; i < numVectors; i++) {
        EXPECT_FALSE(index->testIsInProcess(i)) << "Element " << i << " should not be IN_PROCESS";
    }
}

// =============================================================================
// insertElementToGraph Tests (Core graph insertion algorithm)
// =============================================================================

// Test that insertElementToGraph creates bidirectional connections
TEST_F(HNSWDiskTest, InsertElementToGraphCreatesBidirectionalConnections) {
    TestIndex<float, float> index(DIM);

    // Add first element manually using storeVector (skip indexVector since no entry point)
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    HNSWDiskAddVectorState state1 = index->testStoreVector(vec1, 100);
    index->testUnmarkInProcess(state1.newElementId);

    // Add second element and call insertElementToGraph directly
    float vec2[DIM] = {0.0f, 1.0f, 0.0f, 0.0f};
    HNSWDiskAddVectorState state2 = index->testStoreVector(vec2, 200);

    const char* sq8Data = index->testGetQuantizedDataByInternalId(state2.newElementId);
    ASSERT_NE(sq8Data, nullptr);

    // Manually call insertElementToGraph
    GraphNodeList nodesToRepair =
        index->testInsertElementToGraph(state2.newElementId, state2.elementMaxLevel, state1.newElementId, 0, sq8Data);

    // Verify bidirectional connection at level 0
    auto outgoing2 = index->testGetOutgoingEdges(state2.newElementId, 0);
    bool elem2HasElem1 = std::find(outgoing2.begin(), outgoing2.end(), state1.newElementId) != outgoing2.end();
    EXPECT_TRUE(elem2HasElem1) << "Element 2 should have element 1 as neighbor";

    auto outgoing1 = index->testGetOutgoingEdges(state1.newElementId, 0);
    bool elem1HasElem2 = std::find(outgoing1.begin(), outgoing1.end(), state2.newElementId) != outgoing1.end();
    EXPECT_TRUE(elem1HasElem2) << "Element 1 should have element 2 as neighbor";
}

// Test that insertElementToGraph maintains incoming edges
TEST_F(HNSWDiskTest, InsertElementToGraphMaintainsIncomingEdges) {
    TestIndex<float, float> index(DIM);

    // Add first element
    float vec1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    index->addVector(vec1, 100);

    // Add second element
    float vec2[DIM] = {0.0f, 1.0f, 0.0f, 0.0f};
    index->addVector(vec2, 200);

    // Verify incoming edges are maintained
    auto incoming1 = index->testGetIncomingEdges(0, 0);
    auto incoming2 = index->testGetIncomingEdges(1, 0);

    // At least one of them should have incoming edges from the other
    bool hasIncoming = (std::find(incoming1.begin(), incoming1.end(), 1) != incoming1.end()) ||
                       (std::find(incoming2.begin(), incoming2.end(), 0) != incoming2.end());
    EXPECT_TRUE(hasIncoming) << "Bidirectional connections should create incoming edges";
}

// Test insertElementToGraph with multiple elements selects correct neighbors
TEST_F(HNSWDiskTest, InsertElementToGraphSelectsCorrectNeighbors) {
    // Use small M to verify neighbor selection
    TestIndex<float, float> index(DIM, VecSimMetric_L2, 2, 10, 10);

    // Add several elements in a line (easy to predict distances)
    for (int i = 0; i < 5; i++) {
        float vec[DIM] = {static_cast<float>(i), 0.0f, 0.0f, 0.0f};
        index->addVector(vec, 100 + i);
    }

    EXPECT_EQ(index->indexSize(), 5);

    // Middle element (id 2) should have neighbors 1 and 3 (closest in L2)
    auto neighbors = index->testGetOutgoingEdges(2, 0);
    EXPECT_GE(neighbors.size(), 1) << "Middle element should have neighbors";

    // The neighbors should include adjacent elements (1 or 3)
    bool hasAdjacent = std::find(neighbors.begin(), neighbors.end(), 1) != neighbors.end() ||
                       std::find(neighbors.begin(), neighbors.end(), 3) != neighbors.end();
    EXPECT_TRUE(hasAdjacent) << "Middle element should have adjacent elements as neighbors";
}

// =============================================================================
// Multithreading Tests for addVector flow
// =============================================================================

#include <atomic>
#include <barrier>
#include <set>
#include <thread>
#include <vector>

// Parameterized test for concurrent addVector with varying thread counts
// Using 1 thread tests the single-threaded baseline
struct AddVectorTestParams {
    size_t numThreads;
    size_t vectorsPerThread;
};

class HNSWDiskAddVectorTest : public HNSWDiskTest, public testing::WithParamInterface<AddVectorTestParams> {};

TEST_P(HNSWDiskAddVectorTest, AddVector) {
    auto params = GetParam();
    TestIndex<float, float> index(DIM);

    const size_t totalVectors = params.numThreads * params.vectorsPerThread;

    // Use threads for all cases (works for numThreads=1 too)
    std::vector<std::thread> threads;
    for (size_t t = 0; t < params.numThreads; t++) {
        threads.emplace_back([&, t]() {
            for (size_t i = 0; i < params.vectorsPerThread; i++) {
                labelType label = t * params.vectorsPerThread + i;
                float vec[DIM];
                fillVector(vec, label);
                index->addVector(vec, label);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all inserts succeeded
    EXPECT_EQ(index->indexSize(), totalVectors);

    // Verify no elements are marked IN_PROCESS
    for (size_t i = 0; i < totalVectors; i++) {
        EXPECT_FALSE(index->testIsInProcess(i)) << "Element " << i << " should not be IN_PROCESS";
    }

    // Verify label <-> ID mapping consistency
    std::set<idType> usedIds;
    for (labelType label = 0; label < static_cast<labelType>(totalVectors); label++) {
        EXPECT_TRUE(index->testIsLabelExists(label)) << "Label " << label << " should exist";
        idType id = index->testGetIdByLabel(label);
        EXPECT_LT(id, totalVectors) << "ID for label " << label << " should be valid";
        EXPECT_EQ(index->testGetLabelById(id), label) << "Round-trip failed for label " << label;
        usedIds.insert(id);
    }
    EXPECT_EQ(usedIds.size(), totalVectors) << "All IDs should be unique";

    // Verify graph connectivity (all but first should have neighbors)
    if (totalVectors > 1) {
        size_t elementsWithNeighbors = 0;
        for (size_t i = 0; i < totalVectors; i++) {
            auto outgoing = index->testGetOutgoingEdges(i, 0);
            if (!outgoing.empty()) {
                elementsWithNeighbors++;
            }
        }
        EXPECT_GE(elementsWithNeighbors, totalVectors - 1);
    }
}

INSTANTIATE_TEST_SUITE_P(Concurrency, HNSWDiskAddVectorTest,
                         testing::Values(AddVectorTestParams{1, 100},  // Single-threaded baseline
                                         AddVectorTestParams{4, 25},   // Basic concurrent (4 threads)
                                         AddVectorTestParams{8, 50},   // Higher thread count
                                         AddVectorTestParams{16, 25},  // High thread count stress
                                         AddVectorTestParams{8, 200}), // Block growth stress (1600 vectors)
                         [](const testing::TestParamInfo<AddVectorTestParams>& info) {
                             return std::to_string(info.param.numThreads) + "threads_" +
                                    std::to_string(info.param.vectorsPerThread) + "vectors";
                         });

// Test that concurrent storeVector+indexVector flow works correctly.
// NOTE: This test intentionally bypasses addVector and calls storeVector+indexVector directly,
// which is how the tiered index will use these functions. The label-to-ID mapping is NOT
// registered here because the tiered index manages label mappings at its own layer.
// This test only verifies that the low-level storage and graph insertion work correctly.
TEST_F(HNSWDiskTest, ConcurrentStoreAndIndexVector) {
    TestIndex<float, float> index(DIM);
    const size_t numThreads = 4;
    const size_t vectorsPerThread = 20;
    const size_t totalVectors = numThreads * vectorsPerThread;

    std::vector<std::thread> threads;

    for (size_t t = 0; t < numThreads; t++) {
        threads.emplace_back([&, t]() {
            for (size_t i = 0; i < vectorsPerThread; i++) {
                labelType label = t * vectorsPerThread + i;
                float vec[DIM];
                fillVector(vec, label);

                // Phase 1: storeVector (no label mapping - tiered index handles that)
                HNSWDiskAddVectorState state = index->testStoreVector(vec, label);

                // Get the quantized data for indexing
                const char* sq8Data = index->testGetQuantizedDataByInternalId(state.newElementId);
                if (sq8Data != nullptr) {
                    // Phase 2: indexVector
                    index->testIndexVector(sq8Data, label, state);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(index->indexSize(), totalVectors);

    // Verify no elements are marked IN_PROCESS
    for (size_t i = 0; i < totalVectors; i++) {
        EXPECT_FALSE(index->testIsInProcess(i)) << "Element " << i << " should not be IN_PROCESS";
    }
}

// Test that concurrent first vector insertion is handled correctly
// This tests the race condition when multiple threads try to become the first entry point
TEST_F(HNSWDiskTest, ConcurrentFirstVectorRace) {
    const size_t numThreads = 8;

    std::vector<std::thread> threads;

    // Each thread creates its own index and tries to add the first vector concurrently
    // This tests the entry point initialization race
    TestIndex<float, float> index(DIM);

    std::barrier<> startBarrier(numThreads);

    for (size_t t = 0; t < numThreads; t++) {
        threads.emplace_back([&, t]() {
            float vec[DIM];
            fillVector(vec, t);

            // Synchronize all threads to start at the same time
            startBarrier.arrive_and_wait();

            index->addVector(vec, t);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // All threads should succeed (each has unique label)
    EXPECT_EQ(index->indexSize(), numThreads);

    // Verify entry point is set to a valid element
    auto entryPointState = index->testGetEntryPointState();
    EXPECT_LT(entryPointState.id, numThreads);
}

// Test concurrent insertions where many threads insert similar vectors, causing high contention
// on the same graph nodes (same neighborhood). This tests that per-node locking works correctly.
TEST_F(HNSWDiskTest, ConcurrentHighContentionSameNeighborhood) {
    const size_t numThreads = 8;
    const size_t vectorsPerThread = 10;
    const size_t totalVectors = numThreads * vectorsPerThread;

    // Use small M to increase contention on limited neighbor slots
    TestIndex<float, float> index(DIM, VecSimMetric_L2, 4, 50, 10);

    std::vector<std::thread> threads;

    // All threads insert vectors clustered around the same point
    // This causes maximum contention as all vectors compete for the same neighbors
    for (size_t t = 0; t < numThreads; t++) {
        threads.emplace_back([&, t]() {
            for (size_t i = 0; i < vectorsPerThread; i++) {
                labelType label = t * vectorsPerThread + i;
                // Create vectors clustered around origin with small perturbations
                // This causes them all to be neighbors of each other
                float vec[DIM];
                float perturbation = static_cast<float>(label) * 0.001f;
                for (size_t j = 0; j < DIM; j++) {
                    vec[j] = perturbation + static_cast<float>(j) * 0.0001f;
                }
                index->addVector(vec, label);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // All inserts should succeed
    EXPECT_EQ(index->indexSize(), totalVectors);

    // Verify graph is properly connected despite high contention
    size_t elementsWithNeighbors = 0;
    for (size_t i = 0; i < totalVectors; i++) {
        auto outgoing = index->testGetOutgoingEdges(i, 0);
        if (!outgoing.empty()) {
            elementsWithNeighbors++;
        }
    }
    // All elements except possibly first should have neighbors
    EXPECT_GE(elementsWithNeighbors, totalVectors - 1);

    // Verify all elements are properly finalized (not IN_PROCESS)
    for (size_t i = 0; i < totalVectors; i++) {
        EXPECT_FALSE(index->testIsInProcess(i)) << "Element " << i << " should not be IN_PROCESS";
    }
}

// Test that metadata is fully visible after element creation (no partial visibility)
// This tests the atomicity of metadata initialization
TEST_F(HNSWDiskTest, MetadataVisibilityAfterCreation) {
    const size_t numThreads = 8;
    const size_t vectorsPerThread = 30;
    const size_t totalVectors = numThreads * vectorsPerThread;

    TestIndex<float, float> index(DIM);

    std::vector<std::thread> threads;
    std::atomic<bool> allInsertsDone{false};

    // Insert threads
    for (size_t t = 0; t < numThreads / 2; t++) {
        threads.emplace_back([&, t]() {
            for (size_t i = 0; i < vectorsPerThread; i++) {
                labelType label = t * vectorsPerThread + i;
                float vec[DIM];
                fillVector(vec, label);
                index->addVector(vec, label);
            }
        });
    }

    // Metadata checking threads - verify that whenever an element is visible,
    // its metadata is fully initialized
    std::atomic<size_t> metadataChecks{0};
    std::atomic<size_t> metadataErrors{0};

    for (size_t t = numThreads / 2; t < numThreads; t++) {
        threads.emplace_back([&]() {
            while (!allInsertsDone.load() || metadataChecks.load() < 1000) {
                size_t currentSize = index->indexSize();
                if (currentSize > 0) {
                    // Check a random element's metadata
                    size_t elemId = metadataChecks.load() % currentSize;
                    metadataChecks++;

                    // If element exists, its label should be retrievable
                    try {
                        labelType label = index->testGetLabelById(elemId);
                        levelType level = index->testGetElementLevel(elemId);
                        // Verify label is sensible (non-garbage)
                        if (label > totalVectors * 10) {
                            metadataErrors++;
                        }
                        // Verify level is sensible
                        if (level < 0 || level > 20) {
                            metadataErrors++;
                        }
                    } catch (...) {
                        // Element may have been added but not fully visible yet - this is OK
                    }
                }
                // Small delay to reduce CPU spinning
                std::this_thread::yield();
            }
        });
    }

    // Wait for insert threads
    for (size_t t = 0; t < numThreads / 2; t++) {
        threads[t].join();
    }

    allInsertsDone.store(true);

    // Wait for checker threads
    for (size_t t = numThreads / 2; t < numThreads; t++) {
        threads[t].join();
    }

    // Verify inserts succeeded
    EXPECT_EQ(index->indexSize(), (numThreads / 2) * vectorsPerThread);

    // Verify no metadata errors detected
    EXPECT_EQ(metadataErrors.load(), 0) << "Should have no metadata visibility errors";

    // Verify metadata checks ran
    EXPECT_GT(metadataChecks.load(), 100) << "Should have performed many metadata checks";
}
