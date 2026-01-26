/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "algorithms/hnsw/hnsw_disk.h"
#include "storage/hnsw_storage.h"
#include "VecSim/memory/vecsim_malloc.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/merge_operator.h"
#include "storage/edge_merge_operator.h"
#include "factory/disk_index_factory.h"
#include "vecsim_disk_api.h"

#include <cstring>
#include <memory>
#include <string>
#include <filesystem>
#include <iostream>
#include <unistd.h>

// C API wrapper structures (from rocksdb/c.cc)
struct rocksdb_t {
    rocksdb::DB* rep;
};

struct rocksdb_column_family_handle_t {
    rocksdb::ColumnFamilyHandle* rep;
};

namespace fs = std::filesystem;

namespace test_utils {

struct DiskParamsHolder {
    VecSimParams index_params;
    VecSimDiskContext diskContext;
    VecSimParamsDisk params_disk;
};

std::unique_ptr<DiskParamsHolder> createDiskParams(const HNSWParams& hnsw_params);

/**
 * @brief RAII wrapper for temporary SpeedB database.
 *
 * Creates a temporary database directory and manages the lifecycle of
 * RocksDB handles for testing.
 */
class TempSpeeDB {
public:
    TempSpeeDB() {
        // Create temporary directory for test database
        db_path_ =
            fs::temp_directory_path() / ("test_speedb_" + std::to_string(getpid()) + "_" + std::to_string(counter_++));

        // Destroy old database if it exists
        rocksdb::Options options;
        rocksdb::Status status = rocksdb::DestroyDB(db_path_, options);
        // Ignore errors from DestroyDB (database might not exist)

        // Configure options
        options.create_if_missing = true;

        // Configure column family options with merge operator for incoming edge operations (append and delete)
        rocksdb::ColumnFamilyOptions cf_options;
        cf_options.merge_operator = rocksdb::CreateEdgeListMergeOperator();

        // Open database with default column family
        std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
        column_families.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, cf_options));

        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        status = rocksdb::DB::Open(options, db_path_, column_families, &handles, &db_);
        if (!status.ok()) {
            throw std::runtime_error("Failed to open database: " + status.ToString());
        }

        cf_ = handles[0];
        std::cout << "[TempSpeeDB] Database created successfully at: " << db_path_ << std::endl;
    }

    ~TempSpeeDB() {
        if (cf_) {
            delete cf_;
        }
        if (db_) {
            delete db_;
        }

        // Clean up test directory
        if (!db_path_.empty() && fs::exists(db_path_)) {
            fs::remove_all(db_path_);
            std::cout << "[TempSpeeDB] Cleaned up directory: " << db_path_ << std::endl;
        }
    }

    rocksdb::DB* db() { return db_; }
    rocksdb::ColumnFamilyHandle* cf() { return cf_; }

    template <typename DataType = float>
    std::unique_ptr<HNSWStorage<DataType>> createStorage() {
        return std::make_unique<HNSWStorage<DataType>>(db_, cf_);
    }

    // Disable copy
    TempSpeeDB(const TempSpeeDB&) = delete;
    TempSpeeDB& operator=(const TempSpeeDB&) = delete;

private:
    rocksdb::DB* db_ = nullptr;
    rocksdb::ColumnFamilyHandle* cf_ = nullptr;
    std::string db_path_;
    static inline int counter_ = 0;
};

/**
 * @brief RAII wrapper for HNSWDiskIndex with real SpeedB storage.
 *
 * The index takes ownership of the storage via unique_ptr.
 * Creates a temporary SpeedB database for each test.
 */
template <typename DataType = float, typename DistType = float>
class TestIndex {
public:
    TestIndex(size_t dim, VecSimMetric metric = VecSimMetric_L2, size_t M = 16, size_t efConstruction = 200,
              size_t efRuntime = 10)
        : allocator_(VecSimAllocator::newVecsimAllocator()), db_(std::make_unique<TempSpeeDB>()) {

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

        // Create abstract init params for disk backend:
        // - storedDataSize = SQ8 quantized size
        // - inputBlobSize = FP32 size
        auto abstractInitParams = VecSimDiskFactory::NewDiskInitParams(&params, nullptr);

        // Create components
        auto indexComponents = CreateIndexComponents<DataType, DistType>(allocator_, params.metric, params.dim, false);

        // Create SpeedB storage and pass ownership to the index
        auto storage = db_->createStorage<DataType>();
        storagePtr_ = storage.get(); // Keep raw pointer for test access
        index_ = new (allocator_) HNSWDiskIndex<DataType, DistType>(
            &params_disk_holder->params_disk, abstractInitParams, indexComponents, std::move(storage));
    }

    ~TestIndex() {
        delete index_;
        // storage_ is owned by index_, deleted automatically
        // db_ is cleaned up by unique_ptr
    }

    HNSWDiskIndex<DataType, DistType>* get() { return index_; }
    HNSWDiskIndex<DataType, DistType>* operator->() { return index_; }
    HNSWStorage<DataType>* storage() { return storagePtr_; }
    TempSpeeDB* db() { return db_.get(); }

    // Disable copy
    TestIndex(const TestIndex&) = delete;
    TestIndex& operator=(const TestIndex&) = delete;

private:
    std::shared_ptr<VecSimAllocator> allocator_;
    std::unique_ptr<TempSpeeDB> db_;
    HNSWDiskIndex<DataType, DistType>* index_;
    HNSWStorage<DataType>* storagePtr_; // Raw pointer for test access (index owns storage)
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

// Helper to get expected SQ8 metadata floats based on metric
// L2 needs 4 metadata floats, IP/Cosine need 3
inline size_t getExpectedMetadataFloats(VecSimMetric metric) { return (metric == VecSimMetric_L2) ? 4 : 3; }

inline size_t getExpectedSQ8Size(size_t dim, VecSimMetric metric) {
    return dim * sizeof(uint8_t) + getExpectedMetadataFloats(metric) * sizeof(float);
}

} // namespace test_utils
