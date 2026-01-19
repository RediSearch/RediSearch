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

// Stub RocksDB C API functions to satisfy linker when SpeeDBStore class is compiled
// but not actually used (CreateSpeeDBStore returns nullptr in tests).
extern "C" {
void rocksdb_readoptions_destroy(rocksdb_readoptions_t*) {}
void rocksdb_writeoptions_destroy(rocksdb_writeoptions_t*) {}
void rocksdb_put_cf(rocksdb_t*, const rocksdb_writeoptions_t*, rocksdb_column_family_handle_t*, const char*, size_t,
                    const char*, size_t, char**) {}
char* rocksdb_get_cf(rocksdb_t*, const rocksdb_readoptions_t*, rocksdb_column_family_handle_t*, const char*, size_t,
                     size_t*, char**) {
    return nullptr;
}
void rocksdb_delete_cf(rocksdb_t*, const rocksdb_writeoptions_t*, rocksdb_column_family_handle_t*, const char*, size_t,
                       char**) {}
void rocksdb_free(void*) {}
}

std::unique_ptr<VectorStore> CreateSpeeDBStore(const SpeeDBHandles* /*handles*/) {
    // In unit tests, we don't have SpeedB available.
    // Return nullptr - tests should use MockStorage instead.
    return nullptr;
}
