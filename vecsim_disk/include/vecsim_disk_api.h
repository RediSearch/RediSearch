/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "VecSim/vec_sim.h"
// C API for creating disk-based  indexes.
// After creation, use standard VecSimIndex_* functions for all operations.

#ifdef __cplusplus
extern "C" {
#endif

VecSimIndex* VecSimDisk_CreateIndex(const VecSimParamsDisk* params);

void VecSimDisk_FreeIndex(VecSimIndex* index);

// Forward declarations (defined in rocksdb/c.h)
typedef struct rocksdb_t rocksdb_t;
typedef struct rocksdb_column_family_handle_t rocksdb_column_family_handle_t;

typedef void* VecSimDiskIndexHandle;

// Storage handles passed from Rust to C++.
// Caller owns these pointers and must keep them valid for index lifetime.
typedef struct SpeeDBHandles {
    rocksdb_t* db;
    rocksdb_column_family_handle_t* cf;
} SpeeDBHandles;

#ifdef __cplusplus
}
#endif // __cplusplus
