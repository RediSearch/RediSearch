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
#include "hnsw_disk.h"

namespace HNSWDiskFactory {
VecSimIndex* NewIndex(const VecSimParamsDisk* params, bool is_input_normalized) {
    auto allocator = VecSimAllocator::newVecsimAllocator();

    const HNSWParams* hnswParams = &params->indexParams->algoParams.hnswParams;

    // Create storage from SpeeDBHandles if provided
    // params->storage is a SpeeDBHandles* from Rust FFI
    // CreateSpeeDBStore is defined in speedb_store.cpp (requires SpeedB linkage)
    std::unique_ptr<VectorStore> storage;
    if (params->diskContext->storage) {
        auto* handles = static_cast<const SpeeDBHandles*>(params->diskContext->storage);
        storage = CreateSpeeDBStore(handles);
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
