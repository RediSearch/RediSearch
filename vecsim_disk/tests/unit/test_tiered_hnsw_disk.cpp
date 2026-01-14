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
#include "vecsim_disk_api.h"
#include "algorithms/hnsw/hnsw_disk_tiered.h"

using namespace test_utils;

class TieredHNSWDiskTest : public ::testing::Test {
protected:
    static constexpr size_t DIM = 4;

    // Helper to create tiered disk params
    struct TieredDiskParamsHolder {
        VecSimParams primary_params;
        TieredIndexParams tiered_params;
        VecSimParams tiered_vecsim_params;
        VecSimDiskContext disk_context;
        VecSimParamsDisk params_disk;
    };

    std::unique_ptr<TieredDiskParamsHolder> createTieredDiskParams(const HNSWParams& hnsw_params) {
        auto holder = std::make_unique<TieredDiskParamsHolder>();

        // Create primary (HNSW) params
        holder->primary_params = {
            .algo = VecSimAlgo_HNSWLIB,
            .algoParams = {.hnswParams = hnsw_params},
            .logCtx = nullptr,
        };

        // Create tiered params pointing to primary
        holder->tiered_params = {
            .jobQueue = nullptr,
            .jobQueueCtx = nullptr,
            .submitCb = nullptr,
            .flatBufferLimit = SIZE_MAX,
            .primaryIndexParams = &holder->primary_params,
            .specificParams = {.tieredHnswDiskParams = TieredHNSWDiskParams{}},
        };

        // Create VecSimParams with algo = TIERED
        holder->tiered_vecsim_params = {
            .algo = VecSimAlgo_TIERED,
            .algoParams = {.tieredParams = holder->tiered_params},
            .logCtx = nullptr,
        };

        // Create disk context
        holder->disk_context = {
            .storage = nullptr,
            .indexName = "test_tiered",
            .indexNameLen = strlen("test_tiered"),
        };

        // Create the final VecSimParamsDisk
        holder->params_disk = {
            .indexParams = &holder->tiered_vecsim_params,
            .diskContext = &holder->disk_context,
        };

        return holder;
    }
};

TEST_F(TieredHNSWDiskTest, CreateTieredIndex) {
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_IP,
        .blockSize = DEFAULT_BLOCK_SIZE + 4,
        .M = HNSW_DEFAULT_M + 4,
        .efConstruction = HNSW_DEFAULT_EF_C + 4,
        .efRuntime = HNSW_DEFAULT_EF_RT + 4,
    };

    auto params_holder = createTieredDiskParams(hnsw_params);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);

    ASSERT_NE(index, nullptr);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    VecSimIndexBasicInfo basic_info = VecSimIndex_BasicInfo(index);

    EXPECT_FALSE(basic_info.isMulti);
    EXPECT_TRUE(basic_info.isTiered);
    EXPECT_TRUE(basic_info.isDisk);
    EXPECT_EQ(basic_info.algo, VecSimAlgo_HNSWLIB);
    EXPECT_EQ(basic_info.dim, DIM);
    EXPECT_EQ(basic_info.metric, VecSimMetric_IP);
    EXPECT_EQ(basic_info.type, VecSimType_FLOAT32);

    // Check specific algo info
    EXPECT_EQ(info.tieredInfo.backendInfo.hnswInfo.M, hnsw_params.M);
    EXPECT_EQ(info.tieredInfo.backendInfo.hnswInfo.efConstruction, hnsw_params.efConstruction);
    EXPECT_EQ(info.tieredInfo.backendInfo.hnswInfo.efRuntime, hnsw_params.efRuntime);

    VecSimDisk_FreeIndex(index);
}

TEST_F(TieredHNSWDiskTest, TieredIndexStubBehavior) {
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

    auto params_holder = createTieredDiskParams(hnsw_params);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);
    ASSERT_NE(index, nullptr);

    // Stub behavior: addVector returns 0
    float vec[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    EXPECT_EQ(index->addVector(vec, 1), 0);

    // Stub behavior: indexSize returns 0
    EXPECT_EQ(index->indexSize(), 0);

    // Stub behavior: deleteVector returns 0
    EXPECT_EQ(index->deleteVector(1), 0);

    // Stub behavior: topKQuery returns empty results
    float query[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    auto* reply = index->topKQuery(query, 10, nullptr);
    ASSERT_NE(reply, nullptr);
    EXPECT_EQ(reply->results.size(), 0);
    delete reply;

    VecSimDisk_FreeIndex(index);
}

TEST_F(TieredHNSWDiskTest, CreateTieredIndexWithDefaults) {
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_IP,
    };

    auto params_holder = createTieredDiskParams(hnsw_params);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);
    ASSERT_NE(index, nullptr);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    VecSimIndexBasicInfo basic_info = VecSimIndex_BasicInfo(index);

    EXPECT_FALSE(basic_info.isMulti);
    EXPECT_TRUE(basic_info.isTiered);
    EXPECT_TRUE(basic_info.isDisk);
    EXPECT_EQ(basic_info.algo, VecSimAlgo_HNSWLIB);
    EXPECT_EQ(basic_info.dim, DIM);
    EXPECT_EQ(basic_info.metric, VecSimMetric_IP);
    EXPECT_EQ(basic_info.type, VecSimType_FLOAT32);

    // Check specific algo info
    EXPECT_EQ(info.tieredInfo.backendInfo.hnswInfo.M, HNSW_DEFAULT_M);
    EXPECT_EQ(info.tieredInfo.backendInfo.hnswInfo.efConstruction, HNSW_DEFAULT_EF_C);
    EXPECT_EQ(info.tieredInfo.backendInfo.hnswInfo.efRuntime, HNSW_DEFAULT_EF_RT);

    VecSimDisk_FreeIndex(index);
}
