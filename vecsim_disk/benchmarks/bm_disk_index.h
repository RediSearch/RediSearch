/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <benchmark/benchmark.h>
#include <memory>

#include "bm_common.h"
#include "test_utils.h"

using namespace test_utils;

/**
 * Benchmark fixture for disk-based vector indexes.
 *
 * Uses TempSpeeDB from test_utils.h - the exact same code that works in unit tests.
 */
class BM_DiskIndex : public benchmark::Fixture {
protected:
    // Use TempSpeeDB from test_utils - same as working unit tests
    std::unique_ptr<TempSpeeDB> temp_db_;

    // Vector index (owned by this fixture)
    VecSimIndex* index_ = nullptr;

    // Default index parameters
    static constexpr size_t kDefaultDim = 128;
    static constexpr size_t kDefaultM = 16;
    static constexpr size_t kDefaultEfConstruction = 200;

public:
    void SetUp(benchmark::State& state) override {
        // Create TempSpeeDB - same as unit tests
        temp_db_ = std::make_unique<TempSpeeDB>();
    }

    void TearDown(benchmark::State& state) override {
        // Clean up in reverse order of creation
        if (index_) {
            VecSimDisk_FreeIndex(index_);
            index_ = nullptr;
        }

        // TempSpeeDB destructor handles database cleanup
        temp_db_.reset();
    }

    /**
     * Create a DiskParamsHolder with all nested structures properly initialized.
     * Uses test_utils::createDiskParams for consistency with unit tests.
     */
    std::unique_ptr<DiskParamsHolder> CreateDiskParams(size_t dim = kDefaultDim, VecSimMetric metric = VecSimMetric_L2,
                                                       size_t M = kDefaultM,
                                                       size_t efConstruction = kDefaultEfConstruction) {
        HNSWParams hnsw_params = {
            .type = VecSimType_FLOAT32,
            .dim = dim,
            .metric = metric,
            .multi = false,
            .blockSize = 1024,
            .M = M,
            .efConstruction = efConstruction,
            .efRuntime = 10,
        };

        // Use test_utils::createDiskParams and then set storage
        auto holder = test_utils::createDiskParams(hnsw_params);
        holder->diskContext.storage = temp_db_->getHandles();
        holder->diskContext.indexName = "benchmark_index";
        holder->diskContext.indexNameLen = 15;

        return holder;
    }
};
