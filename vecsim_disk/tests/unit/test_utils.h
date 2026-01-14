/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "hnsw_disk.h"
#include "vector_storage.h"
#include "VecSim/memory/vecsim_malloc.h"
#include "factory/disk_index_factory.h"

#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>

namespace test_utils {

struct DiskParamsHolder {
    VecSimParams index_params;
    VecSimDiskContext diskContext;
    VecSimParamsDisk params_disk;
};

std::unique_ptr<DiskParamsHolder> createDiskParams(const HNSWParams& hnsw_params);

/**
 * @brief In-memory VectorStore implementation for testing.
 */
class MockStorage : public VectorStore {
public:
    bool put(labelType label, const void* data, size_t size) override {
        data_[label] = std::string(static_cast<const char*>(data), size);
        return true;
    }

    bool get(labelType label, void* data, size_t size) const override {
        auto it = data_.find(label);
        if (it == data_.end())
            return false;
        if (it->second.size() != size)
            return false;
        std::memcpy(data, it->second.data(), size);
        return true;
    }

    bool del(labelType label) override { return data_.erase(label) > 0; }

    size_t size() const { return data_.size(); }

private:
    std::unordered_map<labelType, std::string> data_;
};

/**
 * @brief RAII wrapper for HNSWDiskIndex with MockStorage.
 *
 * The index takes ownership of the storage via unique_ptr.
 */
template <typename DataType = float, typename DistType = float>
class TestIndex {
public:
    TestIndex(size_t dim, VecSimMetric metric = VecSimMetric_L2, size_t M = 16, size_t efConstruction = 200,
              size_t efRuntime = 10)
        : allocator_(VecSimAllocator::newVecsimAllocator()) {

        HNSWParams params = {
            .type = VecSimType_FLOAT32,
            .dim = dim,
            .metric = metric,
            .multi = false,
            .blockSize = 1024,
            .M = M,
            .efConstruction = efConstruction,
            .efRuntime = efRuntime,
        };
        auto params_disk_holder = createDiskParams(params);

        // Create abstract init params
        auto abstractInitParams = VecSimDiskFactory::NewAbstractInitParams(&params, nullptr, false);

        // Create components
        auto indexComponents = CreateIndexComponents<float, float>(allocator_, params.metric, params.dim, false);

        // Create storage and pass ownership to the index
        auto storage = std::make_unique<MockStorage>();
        storagePtr_ = storage.get(); // Keep raw pointer for test access
        index_ = new (allocator_) HNSWDiskIndex<DataType, DistType>(
            &params_disk_holder->params_disk, abstractInitParams, indexComponents, std::move(storage));
    }

    ~TestIndex() {
        delete index_;
        // storage_ is owned by index_, deleted automatically
    }

    HNSWDiskIndex<DataType, DistType>* get() { return index_; }
    HNSWDiskIndex<DataType, DistType>* operator->() { return index_; }
    MockStorage* storage() { return storagePtr_; }

    // Disable copy
    TestIndex(const TestIndex&) = delete;
    TestIndex& operator=(const TestIndex&) = delete;

private:
    std::shared_ptr<VecSimAllocator> allocator_;
    HNSWDiskIndex<DataType, DistType>* index_;
    MockStorage* storagePtr_; // Raw pointer for test access (index owns storage)
};

inline VecSimParams createParams(const HNSWParams& params) {
    VecSimParams params_disk = {
        .algo = VecSimAlgo_HNSWLIB,
        .algoParams = {.hnswParams = params},
        .logCtx = nullptr,
    };
    return params_disk;
}

inline std::unique_ptr<DiskParamsHolder> createDiskParams(const HNSWParams& hnsw_params) {
    auto holder = std::make_unique<DiskParamsHolder>();
    holder->index_params = createParams(hnsw_params);
    holder->diskContext = {
        .storage = nullptr,
        .indexName = "test",
        .indexNameLen = 4,
    };
    holder->params_disk = {
        .indexParams = &holder->index_params,
        .diskContext = &holder->diskContext,
    };
    return holder;
}

} // namespace test_utils
