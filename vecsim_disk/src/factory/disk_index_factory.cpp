/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "disk_index_factory.h"
#include "tiered_disk_factory.h"
#include "hnsw_disk_factory.h"

namespace VecSimDiskFactory {
VecSimIndex* NewIndex(const VecSimParamsDisk* params) {
    if (!params)
        return nullptr;

    switch (params->indexParams->algo) {
    case VecSimAlgo_HNSWLIB:
        // Standalone HNSW disk is not currently used; tiered path is the production path.
        // Pass is_normalized=true since disk backend assumes normalization is done upstream.
        return HNSWDiskFactory::NewIndex(params, true);
    case VecSimAlgo_TIERED:
        return TieredDiskFactory::NewIndex(params);
    default:
        return nullptr;
    }
}

} // namespace VecSimDiskFactory
