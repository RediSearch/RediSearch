/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// SpeeDBStore implementation - creates VectorStore from SpeeDBHandles
// This file requires linking against SpeedB/RocksDB.

#include "vector_storage.h"
#include "hnsw_disk_factory.h"

#include <memory>

std::unique_ptr<VectorStore> CreateSpeeDBStore(const SpeeDBHandles* handles) {
    if (!handles) {
        return nullptr;
    }
    return std::make_unique<SpeeDBStore>(handles->db, handles->cf);
}
