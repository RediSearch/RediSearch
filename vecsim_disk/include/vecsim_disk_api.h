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

/**
 * @brief Acquire the global consistency lock for exclusive access.
 * Call from main thread before fork to ensure disk and in-memory data structures
 * are in a consistent state. Async jobs hold a shared lock while applying changes;
 * this blocks until all such jobs complete. Safe to call even if no indexes exist.
 * Must be paired with VecSimDisk_ReleaseConsistencyLock().
 */
void VecSimDisk_AcquireConsistencyLock();

/**
 * @brief Release the global consistency lock.
 * Call from main thread after fork to allow async jobs to resume.
 */
void VecSimDisk_ReleaseConsistencyLock();

/**
 * @brief Perform edge list merge operation (for use as SpeedB merge operator).
 *
 * This exposes the C++ EdgeListMergeOperator::FullMergeV2 logic for FFI use.
 * The caller is responsible for freeing the returned buffer with VecSimDisk_FreeMergeResult.
 *
 * @param existing_value Existing value bytes (may be NULL for no existing value)
 * @param existing_value_len Length of existing value (ignored if existing_value is NULL)
 * @param operands_list Array of operand pointers (must be non-NULL if num_operands > 0)
 * @param operands_len_list Array of operand lengths (must be non-NULL if num_operands > 0)
 * @param num_operands Number of operands (may be 0)
 * @param result_len Output: length of result buffer (must be non-NULL)
 * @return Non-NULL pointer to result buffer on success (even if result_len is 0 for empty results).
 *         NULL on failure: merge failed or memory allocation failed.
 *         Caller must free non-NULL result with VecSimDisk_FreeMergeResult.
 *
 * @note Passing NULL for required parameters triggers an assertion failure in debug builds.
 */
char* VecSimDisk_EdgeListMerge(const char* existing_value, size_t existing_value_len, const char* const* operands_list,
                               const size_t* operands_len_list, size_t num_operands, size_t* result_len);

/**
 * @brief Free a buffer returned by VecSimDisk_EdgeListMerge or VecSimDisk_EdgeListPartialMerge.
 */
void VecSimDisk_FreeMergeResult(char* result);

/**
 * @brief Perform edge list partial merge operation (for use as SpeedB merge operator).
 *
 * This exposes the C++ EdgeListMergeOperator::PartialMerge logic for FFI use.
 * Partial merge combines operands without an existing value - used during compaction
 * to reduce the number of operands before a full merge.
 *
 * Unlike FullMerge, PartialMerge preserves the operand format (with 'A'/'D' prefix)
 * so the result can be used as an operand in subsequent merges.
 *
 * @param operands_list Array of operand pointers (must be non-NULL, at least 2 operands)
 * @param operands_len_list Array of operand lengths (must be non-NULL)
 * @param num_operands Number of operands (must be >= 2)
 * @param result_len Output: length of result buffer (must be non-NULL)
 * @param success Output: 1 if partial merge succeeded, 0 if cannot partial merge (must be non-NULL)
 * @return Non-NULL pointer to result buffer on success (caller must free with VecSimDisk_FreeMergeResult).
 *         NULL if partial merge cannot be performed (success=0) or on allocation failure.
 *
 * @note Returns success=0 (with NULL result) when operands cannot be partially merged
 *       (e.g., different operation types like APPEND + DELETE). This is not an error;
 *       SpeedB will fall back to full merge.
 */
char* VecSimDisk_EdgeListPartialMerge(const char* const* operands_list, const size_t* operands_len_list,
                                      size_t num_operands, size_t* result_len, int* success);

#ifdef __cplusplus
}
#endif // __cplusplus
