#include "redisearch.h"
#include "disk/document_metadata/document_metadata.hpp"
#include <cstdio>
#include <filesystem>
#include <gtest/gtest.h>
#include <sstream>

using namespace std;

class DocumentMetadataTest : public ::testing::Test {
protected:
    void SetUp() override {
        // No setup needed for these tests
    }

    void TearDown() override {
        // No teardown needed for these tests
    }
};

TEST_F(DocumentMetadataTest, SerializeDeserialize) {
    search::disk::DocumentMetadata dmd;
    dmd.keyPtr = "testkey";
    dmd.score = 42.42;
    dmd.maxFreq = 123456;
    dmd.flags = 0xDEADBEEF;

    // Serialize
    std::string serialized = dmd.serialize();
    // Deserialize
    auto dmd2_opt = search::disk::DocumentMetadata::deserialize(serialized);

    // Verify we got a valid result
    ASSERT_TRUE(dmd2_opt.has_value());

    // Access the value and verify it matches
    const auto& dmd2 = *dmd2_opt;
    ASSERT_EQ(dmd2.keyPtr, dmd.keyPtr);
    ASSERT_EQ(dmd2.score, dmd.score);
    ASSERT_EQ(dmd2.maxFreq, dmd.maxFreq);
    ASSERT_EQ(dmd2.flags, dmd.flags);
}

TEST_F(DocumentMetadataTest, SerializeToStream) {
    search::disk::DocumentMetadata dmd;
    dmd.keyPtr = "streamkey";
    dmd.score = 99.99;
    dmd.maxFreq = 54321;
    dmd.flags = 0xCAFEBABE;

    // Serialize to stream
    std::ostringstream oss;
    dmd.serialize(oss);
    std::string serialized = oss.str();

    // Deserialize
    auto dmd2_opt = search::disk::DocumentMetadata::deserialize(serialized);

    // Verify we got a valid result
    ASSERT_TRUE(dmd2_opt.has_value());

    // Access the value and verify it matches
    const auto& dmd2 = *dmd2_opt;

    // Verify the data was correctly serialized and deserialized
    ASSERT_EQ(dmd2.keyPtr, dmd.keyPtr);
    ASSERT_EQ(dmd2.score, dmd.score);
    ASSERT_EQ(dmd2.maxFreq, dmd.maxFreq);
    ASSERT_EQ(dmd2.flags, dmd.flags);

    // Also verify that both serialization methods produce the same result
    std::string serialized2 = dmd.serialize();
    ASSERT_EQ(serialized, serialized2);
}

TEST_F(DocumentMetadataTest, EmptyKey) {
    search::disk::DocumentMetadata dmd;
    dmd.keyPtr = ""; // Empty key
    dmd.score = 1.0;
    dmd.maxFreq = 0;
    dmd.flags = 0;

    // Serialize and deserialize
    std::string serialized = dmd.serialize();
    auto dmd2_opt = search::disk::DocumentMetadata::deserialize(serialized);

    // Verify we got a valid result
    ASSERT_TRUE(dmd2_opt.has_value());

    // Access the value and verify it matches
    const auto& dmd2 = *dmd2_opt;

    // Verify empty key is preserved
    ASSERT_EQ(dmd2.keyPtr, "");
    ASSERT_EQ(dmd2.score, 1.0);
    ASSERT_EQ(dmd2.maxFreq, 0);
    ASSERT_EQ(dmd2.flags, 0);
}

TEST_F(DocumentMetadataTest, InvalidData) {
    // Test with empty data
    auto result1 = search::disk::DocumentMetadata::deserialize("");
    ASSERT_FALSE(result1.has_value());

    // Test with invalid data
    auto result2 = search::disk::DocumentMetadata::deserialize("not valid serialized data");
    ASSERT_FALSE(result2.has_value());

    // Test with truncated data (just the version byte)
    std::string serialized = "\x01";
    auto result3 = search::disk::DocumentMetadata::deserialize(serialized);
    ASSERT_FALSE(result3.has_value());
}
