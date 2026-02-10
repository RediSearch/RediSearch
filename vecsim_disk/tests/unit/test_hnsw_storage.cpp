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
#include "storage/encoding.h"
#include "storage/edge_merge_operator.h"
#include "VecSim/utils/vecsim_stl.h"

#include <memory>
#include <cstring>
#include <limits>

using namespace test_utils;

class HNSWStorageTest : public ::testing::Test {
protected:
    static constexpr size_t DIM = 4;
    static constexpr size_t VECTOR_SIZE = DIM * sizeof(float);

    std::unique_ptr<TempSpeeDB> db_;
    std::shared_ptr<VecSimAllocator> allocator_;

    void SetUp() override {
        db_ = std::make_unique<TempSpeeDB>();
        allocator_ = VecSimAllocator::newVecsimAllocator();
    }

    void TearDown() override {
        db_.reset();
        allocator_.reset();
    }

    std::unique_ptr<HNSWStorage<float>> CreateStorage() { return db_->createStorage<float>(); }

    // Helper to create vecsim_stl::vector for edges
    vecsim_stl::vector<idType> makeEdges(std::initializer_list<idType> init) {
        vecsim_stl::vector<idType> edges(allocator_);
        for (auto e : init) {
            edges.push_back(e);
        }
        return edges;
    }

    // Helper to append multiple edges using single-edge API
    static void AppendEdges(HNSWStorage<float>* storage, idType id, levelType level,
                            std::initializer_list<idType> edges) {
        for (idType edge : edges) {
            storage->append_incoming_edge(id, level, edge);
        }
    }
};

// =============================================================================
// Endianness Detection Test
// =============================================================================

TEST_F(HNSWStorageTest, EndiannessMode) {
    // Report the endianness mode for this test run
#if defined(FORCE_BIG_ENDIAN_TEST)
    std::cout << "Running HNSW storage tests in FORCED BIG-ENDIAN mode" << std::endl;
    EXPECT_FALSE(encoding::kIsLittleEndian) << "Should be in forced big-endian mode";
#else
    std::cout << "Running HNSW storage tests in " << (encoding::kIsLittleEndian ? "LITTLE-ENDIAN" : "BIG-ENDIAN")
              << " mode (auto-detected)" << std::endl;
#endif
}

// =============================================================================
// Vector Storage Tests
// =============================================================================

TEST_F(HNSWStorageTest, PutAndGetVector) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    float vec[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    idType id = 42;

    // Put vector
    ASSERT_NO_THROW(storage->put_vector(id, vec, VECTOR_SIZE));

    // Get vector back
    float retrieved[DIM];
    ASSERT_NO_THROW(storage->get_vector(id, retrieved, VECTOR_SIZE));

    // Verify data
    for (size_t i = 0; i < DIM; i++) {
        EXPECT_FLOAT_EQ(retrieved[i], vec[i]);
    }
}

TEST_F(HNSWStorageTest, GetNonExistentVector) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    float retrieved[DIM];
    idType id = 999;

    // Should throw for non-existent vector
    EXPECT_THROW(storage->get_vector(id, retrieved, VECTOR_SIZE), std::runtime_error);
}

TEST_F(HNSWStorageTest, DeleteVector) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    float vec[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    idType id = 42;

    ASSERT_NO_THROW(storage->put_vector(id, vec, VECTOR_SIZE));

    // Delete vector
    ASSERT_NO_THROW(storage->del_vector(id));

    // Should no longer exist
    float retrieved[DIM];
    EXPECT_THROW(storage->get_vector(id, retrieved, VECTOR_SIZE), std::runtime_error);
}

TEST_F(HNSWStorageTest, OverwriteVector) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    float vec1[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    float vec2[DIM] = {5.0f, 6.0f, 7.0f, 8.0f};
    idType id = 42;

    // Put first vector
    ASSERT_NO_THROW(storage->put_vector(id, vec1, VECTOR_SIZE));

    // Overwrite with second vector
    ASSERT_NO_THROW(storage->put_vector(id, vec2, VECTOR_SIZE));

    // Should get second vector
    float retrieved[DIM];
    ASSERT_NO_THROW(storage->get_vector(id, retrieved, VECTOR_SIZE));
    for (size_t i = 0; i < DIM; i++) {
        EXPECT_FLOAT_EQ(retrieved[i], vec2[i]);
    }
}

// =============================================================================
// Edge Storage Tests
// =============================================================================

TEST_F(HNSWStorageTest, PutAndGetOutgoingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 0;
    auto edges = makeEdges({1, 2, 3, 4, 5});

    // Put edges
    ASSERT_NO_THROW(storage->put_outgoing_edges(id, level, edges));

    // Get edges back
    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, level, retrieved));

    // Verify data
    ASSERT_EQ(retrieved.size(), edges.size());
    for (size_t i = 0; i < edges.size(); i++) {
        EXPECT_EQ(retrieved[i], edges[i]);
    }
}

TEST_F(HNSWStorageTest, PutAndGetIncomingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 1;
    auto edges = makeEdges({10, 20, 30});

    // Put edges
    ASSERT_NO_THROW(storage->put_incoming_edges(id, level, edges));

    // Get edges back
    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));

    // Verify data
    ASSERT_EQ(retrieved.size(), edges.size());
    for (size_t i = 0; i < edges.size(); i++) {
        EXPECT_EQ(retrieved[i], edges[i]);
    }
}

TEST_F(HNSWStorageTest, GetNonExistentOutgoingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    vecsim_stl::vector<idType> retrieved(allocator_);

    // Should succeed with empty list for non-existent edges (not found is valid)
    ASSERT_NO_THROW(storage->get_outgoing_edges(999, 0, retrieved));
    EXPECT_TRUE(retrieved.empty());
}

TEST_F(HNSWStorageTest, GetNonExistentIncomingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    vecsim_stl::vector<idType> retrieved(allocator_);

    // Should succeed with empty list for non-existent edges (not found is valid)
    ASSERT_NO_THROW(storage->get_incoming_edges(999, 0, retrieved));
    EXPECT_TRUE(retrieved.empty());
}

TEST_F(HNSWStorageTest, EmptyEdgeList) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 0;
    vecsim_stl::vector<idType> empty_edges(allocator_);

    // Put empty edge list
    ASSERT_NO_THROW(storage->put_outgoing_edges(id, level, empty_edges));

    // Get it back
    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, level, retrieved));

    // Should be empty
    EXPECT_EQ(retrieved.size(), 0);
}

TEST_F(HNSWStorageTest, DeleteOutgoingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 0;
    auto edges = makeEdges({1, 2, 3});

    ASSERT_NO_THROW(storage->put_outgoing_edges(id, level, edges));

    // Delete edges
    ASSERT_NO_THROW(storage->del_outgoing_edges(id, level));

    // Should succeed with empty list after deletion (not found is valid)
    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, level, retrieved));
    EXPECT_TRUE(retrieved.empty());
}

TEST_F(HNSWStorageTest, DeleteIncomingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 1;
    auto edges = makeEdges({10, 20});

    ASSERT_NO_THROW(storage->put_incoming_edges(id, level, edges));

    // Delete edges
    ASSERT_NO_THROW(storage->del_incoming_edges(id, level));

    // Should succeed with empty list after deletion (not found is valid)
    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_TRUE(retrieved.empty());
}

TEST_F(HNSWStorageTest, OverwriteOutgoingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 0;
    auto edges1 = makeEdges({1, 2, 3});
    auto edges2 = makeEdges({4, 5, 6, 7});

    // Put first edge list
    ASSERT_NO_THROW(storage->put_outgoing_edges(id, level, edges1));

    // Overwrite with second edge list
    ASSERT_NO_THROW(storage->put_outgoing_edges(id, level, edges2));

    // Should get second edge list
    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, level, retrieved));
    ASSERT_EQ(retrieved.size(), edges2.size());
    for (size_t i = 0; i < edges2.size(); i++) {
        EXPECT_EQ(retrieved[i], edges2[i]);
    }
}

TEST_F(HNSWStorageTest, DifferentLevelsSameNode) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    auto edges_level0 = makeEdges({1, 2, 3});
    auto edges_level1 = makeEdges({4, 5});
    auto edges_level2 = makeEdges({6});

    // Put edges at different levels
    ASSERT_NO_THROW(storage->put_outgoing_edges(id, 0, edges_level0));
    ASSERT_NO_THROW(storage->put_outgoing_edges(id, 1, edges_level1));
    ASSERT_NO_THROW(storage->put_outgoing_edges(id, 2, edges_level2));

    // Get edges from each level
    vecsim_stl::vector<idType> retrieved0(allocator_), retrieved1(allocator_), retrieved2(allocator_);
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, 0, retrieved0));
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, 1, retrieved1));
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, 2, retrieved2));

    // Verify each level has correct edges
    EXPECT_EQ(retrieved0, edges_level0);
    EXPECT_EQ(retrieved1, edges_level1);
    EXPECT_EQ(retrieved2, edges_level2);
}

TEST_F(HNSWStorageTest, OutgoingAndIncomingEdgesSeparate) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 0;
    auto outgoing = makeEdges({1, 2, 3});
    auto incoming = makeEdges({4, 5, 6});

    // Put both outgoing and incoming edges
    ASSERT_NO_THROW(storage->put_outgoing_edges(id, level, outgoing));
    ASSERT_NO_THROW(storage->put_incoming_edges(id, level, incoming));

    // Get both back
    vecsim_stl::vector<idType> retrieved_out(allocator_), retrieved_in(allocator_);
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, level, retrieved_out));
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved_in));

    // Verify they are separate
    EXPECT_EQ(retrieved_out, outgoing);
    EXPECT_EQ(retrieved_in, incoming);
}

TEST_F(HNSWStorageTest, LargeEdgeList) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 0;

    // Create a large edge list
    vecsim_stl::vector<idType> edges(allocator_);
    for (idType i = 0; i < 1000; i++) {
        edges.push_back(i);
    }

    // Put and get large edge list
    ASSERT_NO_THROW(storage->put_outgoing_edges(id, level, edges));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, level, retrieved));

    // Verify all edges
    ASSERT_EQ(retrieved.size(), edges.size());
    for (size_t i = 0; i < edges.size(); i++) {
        EXPECT_EQ(retrieved[i], edges[i]);
    }
}

// =============================================================================
// Mixed Operations Tests
// =============================================================================

TEST_F(HNSWStorageTest, MixedVectorAndEdgeOperations) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    float vec[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    auto edges = makeEdges({1, 2, 3});

    // Store vector and edges
    ASSERT_NO_THROW(storage->put_vector(id, vec, VECTOR_SIZE));
    ASSERT_NO_THROW(storage->put_outgoing_edges(id, 0, edges));
    ASSERT_NO_THROW(storage->put_incoming_edges(id, 0, edges));

    // Retrieve and verify vector
    float retrieved_vec[DIM];
    ASSERT_NO_THROW(storage->get_vector(id, retrieved_vec, VECTOR_SIZE));
    for (size_t i = 0; i < DIM; i++) {
        EXPECT_FLOAT_EQ(retrieved_vec[i], vec[i]);
    }

    // Retrieve and verify edges
    vecsim_stl::vector<idType> retrieved_out(allocator_), retrieved_in(allocator_);
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, 0, retrieved_out));
    ASSERT_NO_THROW(storage->get_incoming_edges(id, 0, retrieved_in));
    EXPECT_EQ(retrieved_out, edges);
    EXPECT_EQ(retrieved_in, edges);

    // Delete vector but keep edges
    ASSERT_NO_THROW(storage->del_vector(id));
    EXPECT_THROW(storage->get_vector(id, retrieved_vec, VECTOR_SIZE), std::runtime_error);
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, 0, retrieved_out));
    ASSERT_NO_THROW(storage->get_incoming_edges(id, 0, retrieved_in));
}

TEST_F(HNSWStorageTest, OverwriteVectorAndEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 0;

    // Store initial vector
    float vec1[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    ASSERT_NO_THROW(storage->put_vector(id, vec1, VECTOR_SIZE));

    // Store initial edges
    auto edges1_out = makeEdges({10, 20, 30});
    auto edges1_in = makeEdges({100, 200});
    ASSERT_NO_THROW(storage->put_outgoing_edges(id, level, edges1_out));
    ASSERT_NO_THROW(storage->put_incoming_edges(id, level, edges1_in));

    // Verify initial data
    float retrieved_vec[DIM];
    ASSERT_NO_THROW(storage->get_vector(id, retrieved_vec, VECTOR_SIZE));
    for (size_t i = 0; i < DIM; i++) {
        EXPECT_FLOAT_EQ(retrieved_vec[i], vec1[i]);
    }

    vecsim_stl::vector<idType> retrieved_out(allocator_), retrieved_in(allocator_);
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, level, retrieved_out));
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved_in));
    EXPECT_EQ(retrieved_out, edges1_out);
    EXPECT_EQ(retrieved_in, edges1_in);

    // Overwrite with new vector (different values)
    float vec2[DIM] = {10.0f, 20.0f, 30.0f, 40.0f};
    ASSERT_NO_THROW(storage->put_vector(id, vec2, VECTOR_SIZE));

    // Overwrite with new edges (different sizes and values)
    auto edges2_out = makeEdges({50, 60, 70, 80, 90}); // More edges
    auto edges2_in = makeEdges({500});                 // Fewer edges
    ASSERT_NO_THROW(storage->put_outgoing_edges(id, level, edges2_out));
    ASSERT_NO_THROW(storage->put_incoming_edges(id, level, edges2_in));

    // Verify we get the NEW vector back (not the old one)
    ASSERT_NO_THROW(storage->get_vector(id, retrieved_vec, VECTOR_SIZE));
    for (size_t i = 0; i < DIM; i++) {
        EXPECT_FLOAT_EQ(retrieved_vec[i], vec2[i]) << "Vector component " << i << " should be updated";
        EXPECT_NE(vec2[i], vec1[i]) << "Test setup: vec2 should differ from vec1";
    }

    // Verify we get the NEW edges back (not the old ones)
    retrieved_out.clear();
    retrieved_in.clear();
    ASSERT_NO_THROW(storage->get_outgoing_edges(id, level, retrieved_out));
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved_in));

    EXPECT_EQ(retrieved_out.size(), edges2_out.size()) << "Outgoing edges should have new size";
    EXPECT_EQ(retrieved_in.size(), edges2_in.size()) << "Incoming edges should have new size";
    EXPECT_EQ(retrieved_out, edges2_out) << "Should get new outgoing edges";
    EXPECT_EQ(retrieved_in, edges2_in) << "Should get new incoming edges";

    // Verify old edges are completely gone
    EXPECT_NE(retrieved_out, edges1_out) << "Old outgoing edges should be replaced";
    EXPECT_NE(retrieved_in, edges1_in) << "Old incoming edges should be replaced";
}

// =============================================================================
// Serialization Format Tests
// =============================================================================

// Test that verifies cross-platform compatibility by checking that
// serialization/deserialization produces consistent results regardless
// of the host system's endianness
TEST_F(HNSWStorageTest, VectorSerializationRoundTrip) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    // Test with various float values including edge cases
    float vec[DIM] = {1.0f, -2.5f, 3.14159f, 0.0f};
    idType id = 42;

    // Store the vector
    ASSERT_NO_THROW(storage->put_vector(id, vec, VECTOR_SIZE));

    // Retrieve it back
    float retrieved[DIM];
    ASSERT_NO_THROW(storage->get_vector(id, retrieved, VECTOR_SIZE));

    // Verify exact bit-for-bit equality (not just approximate)
    for (size_t i = 0; i < DIM; ++i) {
        uint32_t original_bits, retrieved_bits;
        std::memcpy(&original_bits, &vec[i], sizeof(uint32_t));
        std::memcpy(&retrieved_bits, &retrieved[i], sizeof(uint32_t));
        EXPECT_EQ(original_bits, retrieved_bits) << "Bit pattern mismatch at index " << i << " (original=" << vec[i]
                                                 << ", retrieved=" << retrieved[i] << ")";
    }
}

// Test with double precision to verify 64-bit serialization
TEST_F(HNSWStorageTest, DoubleVectorSerializationRoundTrip) {
    auto storage_double = db_->createStorage<double>();
    ASSERT_NE(storage_double, nullptr);

    constexpr size_t DIM_DOUBLE = 4;
    constexpr size_t VECTOR_SIZE_DOUBLE = DIM_DOUBLE * sizeof(double);

    double vec[DIM_DOUBLE] = {1.0, -2.5, 3.141592653589793, 0.0};
    idType id = 100;

    // Store the vector
    ASSERT_NO_THROW(storage_double->put_vector(id, vec, VECTOR_SIZE_DOUBLE));

    // Retrieve it back
    double retrieved[DIM_DOUBLE];
    ASSERT_NO_THROW(storage_double->get_vector(id, retrieved, VECTOR_SIZE_DOUBLE));

    // Verify exact bit-for-bit equality
    for (size_t i = 0; i < DIM_DOUBLE; ++i) {
        uint64_t original_bits, retrieved_bits;
        std::memcpy(&original_bits, &vec[i], sizeof(uint64_t));
        std::memcpy(&retrieved_bits, &retrieved[i], sizeof(uint64_t));
        EXPECT_EQ(original_bits, retrieved_bits) << "Bit pattern mismatch at index " << i << " (original=" << vec[i]
                                                 << ", retrieved=" << retrieved[i] << ")";
    }
}

// =============================================================================
// Merge Operator Tests
// =============================================================================

TEST_F(HNSWStorageTest, AppendIncomingEdge) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    levelType level = 0;

    // Append single edges one at a time
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, 10));
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, 20));
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, 30));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({10, 20, 30}));
}

TEST_F(HNSWStorageTest, MergeDeleteIncomingEdge) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    levelType level = 0;
    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {1, 2, 3}));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 2));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({1, 3}));
}

// Test: FullMergeV2 with existing base value (Put) followed by merge operands.
// This specifically tests the existing_value path in FullMergeV2, which differs
// from tests that only use merge operations (no base Put value).
TEST_F(HNSWStorageTest, FullMergeV2WithExistingValue) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    levelType level = 0;

    // First, set a base value using Put (not Merge)
    auto initial_edges = makeEdges({10, 20, 30});
    ASSERT_NO_THROW(storage->put_incoming_edges(id, level, initial_edges));

    // Now apply merge operations on top of the existing value
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, 40));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 20));
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, 50));

    // FullMergeV2 is invoked during read with:
    // - existing_value = {10, 20, 30} (from Put)
    // - operand_list = [append(40), delete(20), append(50)]
    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({10, 30, 40, 50}));
}

// =============================================================================
// Edge Merge Operator Edge Cases
// =============================================================================

// Test: Append to empty (no existing value)
TEST_F(HNSWStorageTest, MergeAppendToEmpty) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 100;
    levelType level = 0;

    // Append to non-existent key
    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {10, 20, 30}));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({10, 20, 30}));
}

// Test: Multiple consecutive appends
TEST_F(HNSWStorageTest, MergeMultipleAppends) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 101;
    levelType level = 0;

    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {1, 2}));
    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {3, 4}));
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, 5));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({1, 2, 3, 4, 5}));
}

// Test: Delete from empty list (edge doesn't exist)
TEST_F(HNSWStorageTest, MergeDeleteFromEmpty) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 102;
    levelType level = 0;

    // Delete from non-existent key - should not crash
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 999));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_TRUE(retrieved.empty());
}

// Test: Delete non-existent edge from populated list
TEST_F(HNSWStorageTest, MergeDeleteNonExistent) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 103;
    levelType level = 0;

    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {1, 2, 3}));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 999)); // Edge doesn't exist

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({1, 2, 3})); // Unchanged
}

// Test: Delete all edges one by one
TEST_F(HNSWStorageTest, MergeDeleteAllEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 104;
    levelType level = 0;

    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {1, 2, 3}));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 1));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 2));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 3));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_TRUE(retrieved.empty());
}

// Test: Delete first edge
TEST_F(HNSWStorageTest, MergeDeleteFirstEdge) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 105;
    levelType level = 0;

    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {1, 2, 3, 4, 5}));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 1));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({2, 3, 4, 5}));
}

// Test: Delete last edge
TEST_F(HNSWStorageTest, MergeDeleteLastEdge) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 106;
    levelType level = 0;

    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {1, 2, 3, 4, 5}));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 5));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({1, 2, 3, 4}));
}

// Test: Delete middle edge
TEST_F(HNSWStorageTest, MergeDeleteMiddleEdge) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 107;
    levelType level = 0;

    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {1, 2, 3, 4, 5}));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 3));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({1, 2, 4, 5}));
}

// Test: Delete only removes first occurrence (duplicates shouldn't exist in HNSW)
TEST_F(HNSWStorageTest, MergeDeleteDuplicateEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 108;
    levelType level = 0;

    // Append edges with duplicates (shouldn't happen in practice, but test the behavior)
    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {1, 2, 2, 3, 2}));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 2)); // Removes only first occurrence

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({1, 2, 3, 2}));
}

// Test: Interleaved append and delete operations
TEST_F(HNSWStorageTest, MergeInterleavedOperations) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 109;
    levelType level = 0;

    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {1, 2, 3}));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 2));
    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {4, 5}));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 1));
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, 6));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 5));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({3, 4, 6}));
}

// Test: Single edge operations
TEST_F(HNSWStorageTest, MergeSingleEdge) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 111;
    levelType level = 0;

    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, 42));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({42}));

    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 42));
    retrieved.clear();
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_TRUE(retrieved.empty());
}

// Test: Large edge IDs (boundary values)
TEST_F(HNSWStorageTest, MergeLargeEdgeIds) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 112;
    levelType level = 0;

    idType maxId = std::numeric_limits<idType>::max();
    idType almostMax = maxId - 1;

    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {0, 1, almostMax, maxId}));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({0, 1, almostMax, maxId}));

    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, maxId));
    retrieved.clear();
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({0, 1, almostMax}));
}

// Test: Operations on different levels
TEST_F(HNSWStorageTest, MergeDifferentLevels) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 113;

    ASSERT_NO_THROW(AppendEdges(storage.get(), id, 0, {1, 2, 3}));
    ASSERT_NO_THROW(AppendEdges(storage.get(), id, 1, {10, 20}));
    ASSERT_NO_THROW(storage->append_incoming_edge(id, 2, 100));

    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, 0, 2));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, 1, 10));

    vecsim_stl::vector<idType> level0(allocator_), level1(allocator_), level2(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, 0, level0));
    ASSERT_NO_THROW(storage->get_incoming_edges(id, 1, level1));
    ASSERT_NO_THROW(storage->get_incoming_edges(id, 2, level2));

    EXPECT_EQ(level0, makeEdges({1, 3}));
    EXPECT_EQ(level1, makeEdges({20}));
    EXPECT_EQ(level2, makeEdges({100}));
}

// Test: Large number of edges
TEST_F(HNSWStorageTest, MergeLargeEdgeList) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 114;
    levelType level = 0;

    // Append a large number of edges one by one
    for (idType i = 0; i < 1000; ++i) {
        ASSERT_NO_THROW(storage->append_incoming_edge(id, level, i));
    }

    // Delete every other edge
    for (idType i = 0; i < 1000; i += 2) {
        ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, i));
    }

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));

    // Should have only odd numbers
    EXPECT_EQ(retrieved.size(), 500u);
    for (size_t i = 0; i < retrieved.size(); ++i) {
        EXPECT_EQ(retrieved[i], static_cast<idType>(i * 2 + 1));
    }
}

// Test: Delete same edge multiple times
TEST_F(HNSWStorageTest, MergeDeleteSameEdgeTwice) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 115;
    levelType level = 0;

    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {1, 2, 3}));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 2));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 2)); // Delete again (no-op)

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({1, 3}));
}

// Test: Incoming edges with merge operator
TEST_F(HNSWStorageTest, MergeIncomingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 116;
    levelType level = 0;

    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {100, 200, 300}));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 200));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({100, 300}));
}

// Test: Append after delete restores edge
TEST_F(HNSWStorageTest, MergeAppendAfterDelete) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 117;
    levelType level = 0;

    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {1, 2, 3}));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 2));
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, 2)); // Re-add deleted edge

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({1, 3, 2})); // 2 is at the end now
}

// =============================================================================
// PartialMerge Direct Tests
// =============================================================================

class EdgeMergeOperatorTest : public ::testing::Test {
protected:
    rocksdb::EdgeListMergeOperator merge_op_;

    // Helper to decode edges from a merged operand (skip the op byte)
    static std::vector<idType> DecodeEdges(const std::string& operand) {
        std::vector<idType> edges;
        if (operand.size() < 1)
            return edges;
        for (size_t i = 1; i < operand.size(); i += sizeof(idType)) {
            edges.push_back(encoding::DecodeFixedLE<idType>(operand.data() + i));
        }
        return edges;
    }
};

TEST_F(EdgeMergeOperatorTest, PartialMergeTwoAppends) {
    auto left = rocksdb::EdgeListMergeOperator::CreateAppendOperand(100);
    auto right = rocksdb::EdgeListMergeOperator::CreateAppendOperand(200);

    std::string result;
    bool merged =
        merge_op_.PartialMerge(rocksdb::Slice("key"), rocksdb::Slice(left), rocksdb::Slice(right), &result, nullptr);

    ASSERT_TRUE(merged);
    ASSERT_EQ(result[0], rocksdb::EdgeListMergeOperator::OP_APPEND);
    auto edges = DecodeEdges(result);
    EXPECT_EQ(edges, std::vector<idType>({100, 200}));
}

TEST_F(EdgeMergeOperatorTest, PartialMergeTwoDeletes) {
    auto left = rocksdb::EdgeListMergeOperator::CreateDeleteOperand(10);
    auto right = rocksdb::EdgeListMergeOperator::CreateDeleteOperand(20);

    std::string result;
    bool merged =
        merge_op_.PartialMerge(rocksdb::Slice("key"), rocksdb::Slice(left), rocksdb::Slice(right), &result, nullptr);

    ASSERT_TRUE(merged);
    ASSERT_EQ(result[0], rocksdb::EdgeListMergeOperator::OP_DELETE);
    auto edges = DecodeEdges(result);
    EXPECT_EQ(edges, std::vector<idType>({10, 20}));
}

TEST_F(EdgeMergeOperatorTest, PartialMergeAppendAndDeleteFails) {
    auto append = rocksdb::EdgeListMergeOperator::CreateAppendOperand(100);
    auto del = rocksdb::EdgeListMergeOperator::CreateDeleteOperand(10);

    std::string result;

    // APPEND + DELETE should fail
    bool merged1 =
        merge_op_.PartialMerge(rocksdb::Slice("key"), rocksdb::Slice(append), rocksdb::Slice(del), &result, nullptr);
    EXPECT_FALSE(merged1);

    // DELETE + APPEND should fail
    bool merged2 =
        merge_op_.PartialMerge(rocksdb::Slice("key"), rocksdb::Slice(del), rocksdb::Slice(append), &result, nullptr);
    EXPECT_FALSE(merged2);
}

TEST_F(EdgeMergeOperatorTest, PartialMergeMultipleAppends) {
    auto op1 = rocksdb::EdgeListMergeOperator::CreateAppendOperand(1);
    auto op2 = rocksdb::EdgeListMergeOperator::CreateAppendOperand(2);
    auto op3 = rocksdb::EdgeListMergeOperator::CreateAppendOperand(3);

    std::string merged12;
    ASSERT_TRUE(
        merge_op_.PartialMerge(rocksdb::Slice("key"), rocksdb::Slice(op1), rocksdb::Slice(op2), &merged12, nullptr));

    std::string merged123;
    ASSERT_TRUE(merge_op_.PartialMerge(rocksdb::Slice("key"), rocksdb::Slice(merged12), rocksdb::Slice(op3), &merged123,
                                       nullptr));

    ASSERT_EQ(merged123[0], rocksdb::EdgeListMergeOperator::OP_APPEND);
    auto edges = DecodeEdges(merged123);
    EXPECT_EQ(edges, std::vector<idType>({1, 2, 3}));
}

TEST_F(EdgeMergeOperatorTest, PartialMergeMultipleDeletes) {
    auto op1 = rocksdb::EdgeListMergeOperator::CreateDeleteOperand(10);
    auto op2 = rocksdb::EdgeListMergeOperator::CreateDeleteOperand(20);
    auto op3 = rocksdb::EdgeListMergeOperator::CreateDeleteOperand(30);

    std::string merged12;
    ASSERT_TRUE(
        merge_op_.PartialMerge(rocksdb::Slice("key"), rocksdb::Slice(op1), rocksdb::Slice(op2), &merged12, nullptr));

    std::string merged123;
    ASSERT_TRUE(merge_op_.PartialMerge(rocksdb::Slice("key"), rocksdb::Slice(merged12), rocksdb::Slice(op3), &merged123,
                                       nullptr));

    ASSERT_EQ(merged123[0], rocksdb::EdgeListMergeOperator::OP_DELETE);
    auto edges = DecodeEdges(merged123);
    EXPECT_EQ(edges, std::vector<idType>({10, 20, 30}));
}

// =============================================================================
// Repeated Add/Delete Tests (via storage layer)
// =============================================================================

// Test: Add and delete same edge multiple times, final state reflects last operation
TEST_F(HNSWStorageTest, RepeatedAddDeleteEdge) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 200;
    levelType level = 0;
    idType target_edge = 42;

    // Add edge
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, target_edge));

    // Delete edge
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, target_edge));

    // Add edge again
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, target_edge));

    // Delete edge again
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, target_edge));

    // Add edge one more time (final operation is ADD)
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, target_edge));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({target_edge})); // Edge should be present
}

TEST_F(HNSWStorageTest, RepeatedAddDeleteEdgeFinalDelete) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 201;
    levelType level = 0;
    idType target_edge = 99;

    // Add edge
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, target_edge));

    // Delete edge
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, target_edge));

    // Add edge again
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, target_edge));

    // Delete edge again (final operation is DELETE)
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, target_edge));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_TRUE(retrieved.empty()); // Edge should NOT be present
}

TEST_F(HNSWStorageTest, RepeatedAddDeleteWithOtherEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 202;
    levelType level = 0;

    // Add some edges
    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {1, 2, 3}));

    // Repeatedly add/delete edge 2
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 2));
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, 2));
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 2));
    ASSERT_NO_THROW(storage->append_incoming_edge(id, level, 2));

    // Add more edges
    ASSERT_NO_THROW(AppendEdges(storage.get(), id, level, {4, 5}));

    // Final delete of edge 2
    ASSERT_NO_THROW(storage->delete_edge_from_incoming(id, level, 2));

    vecsim_stl::vector<idType> retrieved(allocator_);
    ASSERT_NO_THROW(storage->get_incoming_edges(id, level, retrieved));
    EXPECT_EQ(retrieved, makeEdges({1, 3, 4, 5})); // 2 should be gone
}
