/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// Stub implementation of CreateSpeeDBStore for unit tests.
// Returns nullptr to simulate no SpeedB storage.
// Real implementation is in src/speedb_store.cpp.

#include "vecsim_disk_api.h"
#include "vector_storage.h"

#include <memory>

std::unique_ptr<VectorStore> CreateSpeeDBStore(const SpeeDBHandles* /*handles*/) {
    // In unit tests, we don't have SpeedB available.
    // Return nullptr - tests should use MockStorage instead.
    return nullptr;
}
