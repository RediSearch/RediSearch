/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include <cassert>
#include <cstring>
#include <memory>

#include "vecsim_disk_api.h"
#include "factory/disk_index_factory.h"
#include "utils/consistency_lock.h"
#include "storage/edge_merge_operator.h"
#include "VecSim/memory/vecsim_malloc.h"

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

char* VecSimDisk_EdgeListMerge(const char* existing_value, size_t existing_value_len, const char* const* operands_list,
                               const size_t* operands_len_list, size_t num_operands, size_t* result_len) {
    // Validate required parameters (internal API - use assertions)
    assert(result_len != nullptr && "result_len must not be null");
    assert((num_operands == 0 || operands_list != nullptr) && "operands_list required when num_operands > 0");
    assert((num_operands == 0 || operands_len_list != nullptr) && "operands_len_list required when num_operands > 0");

    // Build operands vector
    std::vector<rocksdb::Slice> operands;
    operands.reserve(num_operands);
    for (size_t i = 0; i < num_operands; ++i) {
        operands.emplace_back(operands_list[i], operands_len_list[i]);
    }

    // Build MergeOperationInput
    rocksdb::Slice key; // Not used by EdgeListMergeOperator
    rocksdb::Slice existing_slice;
    const rocksdb::Slice* existing_ptr = nullptr;
    if (existing_value != nullptr) {
        existing_slice = rocksdb::Slice(existing_value, existing_value_len);
        existing_ptr = &existing_slice;
    }
    rocksdb::MergeOperator::MergeOperationInput merge_in(key, existing_ptr, operands, nullptr);

    // Build MergeOperationOutput
    std::string new_value;
    rocksdb::Slice existing_operand;
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);

    // Perform merge
    rocksdb::EdgeListMergeOperator merge_op;
    bool success = merge_op.FullMergeV2(merge_in, &merge_out);

    if (!success) {
        *result_len = 0;
        return nullptr;
    }

    // Allocate result buffer and copy
    *result_len = new_value.size();

    // Empty result is valid (e.g., all edges deleted) - return non-null empty allocation
    // We allocate 1 byte to guarantee a non-null pointer for empty results
    size_t alloc_size = (*result_len > 0) ? *result_len : 1;
    char* result = static_cast<char*>(vecsim_malloc(alloc_size));
    if (result == nullptr) {
        // malloc failed
        *result_len = 0;
        return nullptr;
    }

    if (*result_len > 0) {
        std::memcpy(result, new_value.data(), *result_len);
    }
    return result;
}

void VecSimDisk_FreeMergeResult(char* result) { vecsim_free(result); }

char* VecSimDisk_EdgeListPartialMerge(const char* const* operands_list, const size_t* operands_len_list,
                                      size_t num_operands, size_t* result_len, int* success) {
    // Validate required parameters
    assert(result_len != nullptr && "result_len must not be null");
    assert(success != nullptr && "success must not be null");
    assert(num_operands >= 2 && "partial merge requires at least 2 operands");
    assert(operands_list != nullptr && "operands_list must not be null");
    assert(operands_len_list != nullptr && "operands_len_list must not be null");

    *result_len = 0;
    *success = 0;

    // Partial merge only works on pairs, so we chain them: ((op1 + op2) + op3) + ...
    rocksdb::EdgeListMergeOperator merge_op;
    rocksdb::Slice key; // Not used by EdgeListMergeOperator

    // Start with the first operand
    std::string current(operands_list[0], operands_len_list[0]);

    for (size_t i = 1; i < num_operands; ++i) {
        std::string merged;
        rocksdb::Slice left(current);
        rocksdb::Slice right(operands_list[i], operands_len_list[i]);

        if (!merge_op.PartialMerge(key, left, right, &merged, nullptr)) {
            // Cannot partial merge (e.g., different operation types)
            // Return failure - SpeedB will fall back to full merge
            return nullptr;
        }
        current = std::move(merged);
    }

    // Success - allocate and copy result
    *result_len = current.size();
    *success = 1;

    size_t alloc_size = (*result_len > 0) ? *result_len : 1;
    char* result = static_cast<char*>(vecsim_malloc(alloc_size));
    if (result == nullptr) {
        *result_len = 0;
        *success = 0;
        return nullptr;
    }

    if (*result_len > 0) {
        std::memcpy(result, current.data(), *result_len);
    }
    return result;
}
