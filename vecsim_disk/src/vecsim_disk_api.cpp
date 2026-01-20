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
        delete index;
    }
}
