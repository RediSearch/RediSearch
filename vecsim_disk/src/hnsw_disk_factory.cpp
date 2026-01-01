/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// C API for creating disk-based HNSW indexes.
// After creation, use standard VecSimIndex_* functions for all operations.

#include "hnsw_disk_factory.h"
#include "hnsw_disk.h"
#include "vector_storage.h"
#include "VecSim/memory/vecsim_malloc.h"

#include <memory>

extern "C" VecSimDiskIndexHandle VecSimDisk_CreateIndex(const VecSimHNSWDiskParams* params) {
    if (!params) {
        return nullptr;
    }

    auto allocator = VecSimAllocator::newVecsimAllocator();

    // Apply defaults for zero values
    VecSimHNSWDiskParams paramsWithDefaults = *params;
    if (paramsWithDefaults.M == 0)
        paramsWithDefaults.M = 16;
    if (paramsWithDefaults.efConstruction == 0)
        paramsWithDefaults.efConstruction = 200;
    if (paramsWithDefaults.efRuntime == 0)
        paramsWithDefaults.efRuntime = 10;
    if (paramsWithDefaults.blockSize == 0)
        paramsWithDefaults.blockSize = 1024;

    // Create storage from SpeeDBHandles if provided
    // params->storage is a SpeeDBHandles* from Rust FFI
    // CreateSpeeDBStore is defined in speedb_store.cpp (requires SpeedB linkage)
    std::unique_ptr<VectorStore> storage;
    if (params->storage) {
        auto* handles = static_cast<const SpeeDBHandles*>(params->storage);
        storage = CreateSpeeDBStore(handles);
    }

    auto* index = new (allocator) HNSWDiskIndex<float, float>(&paramsWithDefaults, allocator, std::move(storage));
    return static_cast<VecSimDiskIndexHandle>(index);
}

extern "C" void VecSimDisk_FreeIndex(VecSimDiskIndexHandle handle) {
    if (handle) {
        auto* index = static_cast<HNSWDiskIndex<float, float>*>(handle);
        delete index;
    }
}
