/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "hnsw_disk_factory.h"
#include "disk_index_factory.h"
#include "algorithms/hnsw/hnsw_disk.h"
#include "storage/hnsw_storage.h"

namespace HNSWDiskFactory {
VecSimIndex* NewIndex(const VecSimParamsDisk* params, bool is_input_normalized) {
    auto allocator = VecSimAllocator::newVecsimAllocator();

    const HNSWParams* hnswParams = &params->indexParams->algoParams.hnswParams;
    assert(hnswParams->type == VecSimType_FLOAT32 && "Only FLOAT32 type is currently supported");

    // Create storage from SpeeDBHandles if provided
    // params->diskContext->storage is a SpeeDBHandles* from Rust FFI
    // CreateHNSWStorage is defined in speedb_store.cpp (requires SpeedB linkage)
    std::unique_ptr<HNSWStorage<float>> storage;
    if (params->diskContext->storage) {
        auto* handles = static_cast<const SpeeDBHandles*>(params->diskContext->storage);
        storage = CreateHNSWStorage<float>(handles);
    }

    // Create abstract params
    auto abstractParams =
        VecSimDiskFactory::NewAbstractInitParams(hnswParams, params->indexParams->logCtx, is_input_normalized);
    auto indexComponents =
        CreateIndexComponents<float, float>(allocator, hnswParams->metric, abstractParams.dim, is_input_normalized);

    auto* index =
        new (allocator) HNSWDiskIndex<float, float>(params, abstractParams, indexComponents, std::move(storage));
    return index;
}

} // namespace HNSWDiskFactory
