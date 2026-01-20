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

#include <memory>
#include <cstring>

using namespace test_utils;

class HNSWStorageTest : public ::testing::Test {
protected:
    static constexpr size_t DIM = 4;
    static constexpr size_t VECTOR_SIZE = DIM * sizeof(float);

    std::unique_ptr<TempSpeeDB> db_;

    void SetUp() override { db_ = std::make_unique<TempSpeeDB>(); }

    void TearDown() override { db_.reset(); }

    std::unique_ptr<HNSWStorage<float>> CreateStorage() { return db_->createStorage<float>(); }
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
    ASSERT_TRUE(storage->put_vector(id, vec, VECTOR_SIZE));

    // Get vector back
    float retrieved[DIM];
    ASSERT_TRUE(storage->get_vector(id, retrieved, VECTOR_SIZE));

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

    // Should return false for non-existent vector
    EXPECT_FALSE(storage->get_vector(id, retrieved, VECTOR_SIZE));
}

TEST_F(HNSWStorageTest, DeleteVector) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    float vec[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    idType id = 42;

    ASSERT_TRUE(storage->put_vector(id, vec, VECTOR_SIZE));

    // Delete vector
    ASSERT_TRUE(storage->del_vector(id));

    // Should no longer exist
    float retrieved[DIM];
    EXPECT_FALSE(storage->get_vector(id, retrieved, VECTOR_SIZE));
}

TEST_F(HNSWStorageTest, OverwriteVector) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    float vec1[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    float vec2[DIM] = {5.0f, 6.0f, 7.0f, 8.0f};
    idType id = 42;

    // Put first vector
    ASSERT_TRUE(storage->put_vector(id, vec1, VECTOR_SIZE));

    // Overwrite with second vector
    ASSERT_TRUE(storage->put_vector(id, vec2, VECTOR_SIZE));

    // Should get second vector
    float retrieved[DIM];
    ASSERT_TRUE(storage->get_vector(id, retrieved, VECTOR_SIZE));
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
    std::vector<idType> edges = {1, 2, 3, 4, 5};

    // Put edges
    ASSERT_TRUE(storage->put_outgoing_edges(id, level, edges));

    // Get edges back
    std::vector<idType> retrieved;
    ASSERT_TRUE(storage->get_outgoing_edges(id, level, retrieved));

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
    std::vector<idType> edges = {10, 20, 30};

    // Put edges
    ASSERT_TRUE(storage->put_incoming_edges(id, level, edges));

    // Get edges back
    std::vector<idType> retrieved;
    ASSERT_TRUE(storage->get_incoming_edges(id, level, retrieved));

    // Verify data
    ASSERT_EQ(retrieved.size(), edges.size());
    for (size_t i = 0; i < edges.size(); i++) {
        EXPECT_EQ(retrieved[i], edges[i]);
    }
}

TEST_F(HNSWStorageTest, GetNonExistentOutgoingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    std::vector<idType> retrieved;

    // Should return false for non-existent edges
    EXPECT_FALSE(storage->get_outgoing_edges(999, 0, retrieved));
}

TEST_F(HNSWStorageTest, GetNonExistentIncomingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    std::vector<idType> retrieved;

    // Should return false for non-existent edges
    EXPECT_FALSE(storage->get_incoming_edges(999, 0, retrieved));
}

TEST_F(HNSWStorageTest, EmptyEdgeList) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 0;
    std::vector<idType> empty_edges;

    // Put empty edge list
    ASSERT_TRUE(storage->put_outgoing_edges(id, level, empty_edges));

    // Get it back
    std::vector<idType> retrieved;
    ASSERT_TRUE(storage->get_outgoing_edges(id, level, retrieved));

    // Should be empty
    EXPECT_EQ(retrieved.size(), 0);
}

TEST_F(HNSWStorageTest, DeleteOutgoingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 0;
    std::vector<idType> edges = {1, 2, 3};

    ASSERT_TRUE(storage->put_outgoing_edges(id, level, edges));

    // Delete edges
    ASSERT_TRUE(storage->del_outgoing_edges(id, level));

    // Should no longer exist
    std::vector<idType> retrieved;
    EXPECT_FALSE(storage->get_outgoing_edges(id, level, retrieved));
}

TEST_F(HNSWStorageTest, DeleteIncomingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 1;
    std::vector<idType> edges = {10, 20};

    ASSERT_TRUE(storage->put_incoming_edges(id, level, edges));

    // Delete edges
    ASSERT_TRUE(storage->del_incoming_edges(id, level));

    // Should no longer exist
    std::vector<idType> retrieved;
    EXPECT_FALSE(storage->get_incoming_edges(id, level, retrieved));
}

TEST_F(HNSWStorageTest, OverwriteOutgoingEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 0;
    std::vector<idType> edges1 = {1, 2, 3};
    std::vector<idType> edges2 = {4, 5, 6, 7};

    // Put first edge list
    ASSERT_TRUE(storage->put_outgoing_edges(id, level, edges1));

    // Overwrite with second edge list
    ASSERT_TRUE(storage->put_outgoing_edges(id, level, edges2));

    // Should get second edge list
    std::vector<idType> retrieved;
    ASSERT_TRUE(storage->get_outgoing_edges(id, level, retrieved));
    ASSERT_EQ(retrieved.size(), edges2.size());
    for (size_t i = 0; i < edges2.size(); i++) {
        EXPECT_EQ(retrieved[i], edges2[i]);
    }
}

TEST_F(HNSWStorageTest, DifferentLevelsSameNode) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    std::vector<idType> edges_level0 = {1, 2, 3};
    std::vector<idType> edges_level1 = {4, 5};
    std::vector<idType> edges_level2 = {6};

    // Put edges at different levels
    ASSERT_TRUE(storage->put_outgoing_edges(id, 0, edges_level0));
    ASSERT_TRUE(storage->put_outgoing_edges(id, 1, edges_level1));
    ASSERT_TRUE(storage->put_outgoing_edges(id, 2, edges_level2));

    // Get edges from each level
    std::vector<idType> retrieved0, retrieved1, retrieved2;
    ASSERT_TRUE(storage->get_outgoing_edges(id, 0, retrieved0));
    ASSERT_TRUE(storage->get_outgoing_edges(id, 1, retrieved1));
    ASSERT_TRUE(storage->get_outgoing_edges(id, 2, retrieved2));

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
    std::vector<idType> outgoing = {1, 2, 3};
    std::vector<idType> incoming = {4, 5, 6};

    // Put both outgoing and incoming edges
    ASSERT_TRUE(storage->put_outgoing_edges(id, level, outgoing));
    ASSERT_TRUE(storage->put_incoming_edges(id, level, incoming));

    // Get both back
    std::vector<idType> retrieved_out, retrieved_in;
    ASSERT_TRUE(storage->get_outgoing_edges(id, level, retrieved_out));
    ASSERT_TRUE(storage->get_incoming_edges(id, level, retrieved_in));

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
    std::vector<idType> edges;
    for (idType i = 0; i < 1000; i++) {
        edges.push_back(i);
    }

    // Put and get large edge list
    ASSERT_TRUE(storage->put_outgoing_edges(id, level, edges));

    std::vector<idType> retrieved;
    ASSERT_TRUE(storage->get_outgoing_edges(id, level, retrieved));

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
    std::vector<idType> edges = {1, 2, 3};

    // Store vector and edges
    ASSERT_TRUE(storage->put_vector(id, vec, VECTOR_SIZE));
    ASSERT_TRUE(storage->put_outgoing_edges(id, 0, edges));
    ASSERT_TRUE(storage->put_incoming_edges(id, 0, edges));

    // Retrieve and verify vector
    float retrieved_vec[DIM];
    ASSERT_TRUE(storage->get_vector(id, retrieved_vec, VECTOR_SIZE));
    for (size_t i = 0; i < DIM; i++) {
        EXPECT_FLOAT_EQ(retrieved_vec[i], vec[i]);
    }

    // Retrieve and verify edges
    std::vector<idType> retrieved_out, retrieved_in;
    ASSERT_TRUE(storage->get_outgoing_edges(id, 0, retrieved_out));
    ASSERT_TRUE(storage->get_incoming_edges(id, 0, retrieved_in));
    EXPECT_EQ(retrieved_out, edges);
    EXPECT_EQ(retrieved_in, edges);

    // Delete vector but keep edges
    ASSERT_TRUE(storage->del_vector(id));
    EXPECT_FALSE(storage->get_vector(id, retrieved_vec, VECTOR_SIZE));
    EXPECT_TRUE(storage->get_outgoing_edges(id, 0, retrieved_out));
    EXPECT_TRUE(storage->get_incoming_edges(id, 0, retrieved_in));
}

TEST_F(HNSWStorageTest, OverwriteVectorAndEdges) {
    auto storage = CreateStorage();
    ASSERT_NE(storage, nullptr);

    idType id = 42;
    unsigned short level = 0;

    // Store initial vector
    float vec1[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    ASSERT_TRUE(storage->put_vector(id, vec1, VECTOR_SIZE));

    // Store initial edges
    std::vector<idType> edges1_out = {10, 20, 30};
    std::vector<idType> edges1_in = {100, 200};
    ASSERT_TRUE(storage->put_outgoing_edges(id, level, edges1_out));
    ASSERT_TRUE(storage->put_incoming_edges(id, level, edges1_in));

    // Verify initial data
    float retrieved_vec[DIM];
    ASSERT_TRUE(storage->get_vector(id, retrieved_vec, VECTOR_SIZE));
    for (size_t i = 0; i < DIM; i++) {
        EXPECT_FLOAT_EQ(retrieved_vec[i], vec1[i]);
    }

    std::vector<idType> retrieved_out, retrieved_in;
    ASSERT_TRUE(storage->get_outgoing_edges(id, level, retrieved_out));
    ASSERT_TRUE(storage->get_incoming_edges(id, level, retrieved_in));
    EXPECT_EQ(retrieved_out, edges1_out);
    EXPECT_EQ(retrieved_in, edges1_in);

    // Overwrite with new vector (different values)
    float vec2[DIM] = {10.0f, 20.0f, 30.0f, 40.0f};
    ASSERT_TRUE(storage->put_vector(id, vec2, VECTOR_SIZE));

    // Overwrite with new edges (different sizes and values)
    std::vector<idType> edges2_out = {50, 60, 70, 80, 90}; // More edges
    std::vector<idType> edges2_in = {500};                 // Fewer edges
    ASSERT_TRUE(storage->put_outgoing_edges(id, level, edges2_out));
    ASSERT_TRUE(storage->put_incoming_edges(id, level, edges2_in));

    // Verify we get the NEW vector back (not the old one)
    ASSERT_TRUE(storage->get_vector(id, retrieved_vec, VECTOR_SIZE));
    for (size_t i = 0; i < DIM; i++) {
        EXPECT_FLOAT_EQ(retrieved_vec[i], vec2[i]) << "Vector component " << i << " should be updated";
        EXPECT_NE(vec2[i], vec1[i]) << "Test setup: vec2 should differ from vec1";
    }

    // Verify we get the NEW edges back (not the old ones)
    retrieved_out.clear();
    retrieved_in.clear();
    ASSERT_TRUE(storage->get_outgoing_edges(id, level, retrieved_out));
    ASSERT_TRUE(storage->get_incoming_edges(id, level, retrieved_in));

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
    ASSERT_TRUE(storage->put_vector(id, vec, VECTOR_SIZE));

    // Retrieve it back
    float retrieved[DIM];
    ASSERT_TRUE(storage->get_vector(id, retrieved, VECTOR_SIZE));

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
    ASSERT_TRUE(storage_double->put_vector(id, vec, VECTOR_SIZE_DOUBLE));

    // Retrieve it back
    double retrieved[DIM_DOUBLE];
    ASSERT_TRUE(storage_double->get_vector(id, retrieved, VECTOR_SIZE_DOUBLE));

    // Verify exact bit-for-bit equality
    for (size_t i = 0; i < DIM_DOUBLE; ++i) {
        uint64_t original_bits, retrieved_bits;
        std::memcpy(&original_bits, &vec[i], sizeof(uint64_t));
        std::memcpy(&retrieved_bits, &retrieved[i], sizeof(uint64_t));
        EXPECT_EQ(original_bits, retrieved_bits) << "Bit pattern mismatch at index " << i << " (original=" << vec[i]
                                                 << ", retrieved=" << retrieved[i] << ")";
    }
}
