/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// HNSWStorage implementation - creates HNSWStorage from SpeeDBHandles
// This file requires linking against SpeedB/RocksDB.

#include "storage/hnsw_storage.h"
#include "vecsim_disk_api.h"
#include "rocksdb/c.h"

#include <memory>

// Internal C API wrapper structures (from rocksdb/c.cc)
// These wrap the C++ types for the C API
struct rocksdb_t {
    rocksdb::DB* rep;
};

struct rocksdb_column_family_handle_t {
    rocksdb::ColumnFamilyHandle* rep;
};

template <typename DataType>
std::unique_ptr<HNSWStorage<DataType>> CreateHNSWStorage(const SpeeDBHandles* handles) {
    if (!handles || !handles->db || !handles->cf) {
        return nullptr;
    }
    // Extract C++ pointers from C API wrappers
    rocksdb::DB* db = handles->db->rep;
    rocksdb::ColumnFamilyHandle* cf = handles->cf->rep;
    return std::make_unique<HNSWStorage<DataType>>(db, cf);
}

// Explicit template instantiations for supported types
template std::unique_ptr<HNSWStorage<float>> CreateHNSWStorage<float>(const SpeeDBHandles* handles);
template std::unique_ptr<HNSWStorage<double>> CreateHNSWStorage<double>(const SpeeDBHandles* handles);
