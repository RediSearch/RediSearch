/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include <memory>

#include "vecsim_disk_api.h"
#include "factory/disk_index_factory.h"

VecSimIndex* VecSimDisk_CreateIndex(const VecSimParamsDisk* params) {
    if (!params) {
        return nullptr;
    }

    return VecSimDiskFactory::NewIndex(params);
}

void VecSimDisk_FreeIndex(VecSimIndex* index) {
    if (index) {
        // CRITICAL: Save allocator so it will not deallocate itself during destruction.
        // The VecsimBaseObject::operator delete reads obj->allocator AFTER the destructor
        // runs (which destroys the shared_ptr member). By keeping a reference here,
        // the allocator stays alive until after delete completes.
        // This pattern is copied from VecSimIndex_Free in VectorSimilarity.
        std::shared_ptr<VecSimAllocator> allocator = index->getAllocator();
        delete index;
    }
}
