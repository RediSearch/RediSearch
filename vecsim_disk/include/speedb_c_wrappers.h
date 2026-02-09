/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

/**
 * @file speedb_c_wrappers.h
 * @brief C API wrapper structures for SpeedB/RocksDB.
 *
 * These structs wrap C++ RocksDB types to bridge the C API.
 * They mirror the internal structures from rocksdb/c.cc but are defined here
 * for use in code that needs to create C API wrappers from C++ objects.
 *
 * Used by:
 * - speedb_store.cpp: To extract C++ pointers from SpeeDBHandles
 * - test_utils.h: To create test fixtures with SpeeDBHandles
 * - benchmarks: To create benchmark fixtures with SpeeDBHandles
 */

// Forward declarations of RocksDB C++ types
namespace rocksdb {
class DB;
class ColumnFamilyHandle;
} // namespace rocksdb

// C API wrapper structures
// These must match the internal structs in rocksdb/c.cc
struct rocksdb_t {
    rocksdb::DB* rep;
};

struct rocksdb_column_family_handle_t {
    rocksdb::ColumnFamilyHandle* rep;
};
