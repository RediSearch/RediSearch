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
#include "utils/consistency_lock.h"

VecSimIndex* VecSimDisk_CreateIndex(const VecSimParamsDisk* params) {
    if (!params) {
        return nullptr;
    }

    return VecSimDiskFactory::NewIndex(params);
}

void VecSimDisk_FreeIndex(VecSimIndex* index) {
    if (index) {
        // CRITICAL: We cannot use `delete index` for VecsimBaseObject-derived classes!
        //
        // The problem is that VecsimBaseObject::operator delete(void*, size_t) tries to
        // access obj->allocator, but by the time operator delete is called, the destructor
        // has already run and destroyed the allocator member variable.
        //
        // The workaround is to:
        // 1. Save the allocator before destruction
        // 2. Call the destructor explicitly (std::destroy_at uses virtual dispatch)
        // 3. Free the memory using the saved allocator
        //
        // This avoids calling operator delete entirely.
        std::shared_ptr<VecSimAllocator> allocator = index->getAllocator();
        std::destroy_at(index);            // Virtual dispatch calls derived destructor
        allocator->free_allocation(index); // Free memory without calling operator delete
    }
}

void VecSimDisk_AcquireConsistencyLock() { vecsim_disk::getConsistencyMutex().lock(); }

void VecSimDisk_ReleaseConsistencyLock() { vecsim_disk::getConsistencyMutex().unlock(); }
