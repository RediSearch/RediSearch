/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 * SPDX-FileCopyrightText: Copyright 2026 Arm Limited and/or its affiliates
 * <open-source-office@arm.com>
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "VecSim/vec_sim_common.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"
template <VecSimType type, typename DataType, typename DistType = DataType>
struct IndexType {

    static VecSimType get_index_type() { return type; }

    typedef DataType data_t;
    typedef DistType dist_t;
};

// Array indices for accessing different index types in the indices array
// Note: Updated variants are offset by 1 from their base types (e.g., INDEX_BF_UPDATED = INDEX_BF +
// 1)
enum IndexTypeIndex {
    INDEX_BF = 0,
    INDEX_BF_UPDATED,
    INDEX_HNSW,
    INDEX_HNSW_UPDATED,
    INDEX_TIERED_HNSW,
    INDEX_SVS,
    INDEX_TIERED_SVS,
    INDEX_SVS_QUANTIZED,
    INDEX_HNSW_SQ8,
    INDEX_TIERED_HNSW_SQ8,
    NUMBER_OF_INDEX_TYPES // Keep last
};

// Bit flags for selectively enabling index types in benchmarks via
// BM_VecSimGeneral::enabled_index_types bitmask Limited to 32 index types because IndexTypeFlags is
// stored in a 32-bit mask (uint32_t)
// Note: Bit positions currently match IndexTypeIndex values but this is not required by the code
enum IndexTypeFlags {
    INDEX_MASK_BF = 1 << 0,
    INDEX_MASK_BF_UPDATED = 1 << 1,
    INDEX_MASK_HNSW = 1 << 2,
    INDEX_MASK_HNSW_UPDATED = 1 << 3,
    INDEX_MASK_TIERED_HNSW = 1 << 4,
    INDEX_MASK_SVS = 1 << 5,
    INDEX_MASK_TIERED_SVS = 1 << 6,
    INDEX_MASK_SVS_COMPRESSED = 1 << 7,
    INDEX_MASK_HNSW_SQ8 = 1 << 8,
    INDEX_MASK_TIERED_HNSW_SQ8 = 1 << 9
};

// Smart pointer wrapper for VecSimIndex with configurable ownership
// Supports:
// 1. Ownership control via release_ownership()
// 2. Sharing with thread pool's shared_ptr
// 3. Safe transfer of ownership to tiered index
class IndexPtr {
private:
    std::shared_ptr<VecSimIndex> ptr;
    std::shared_ptr<bool> owns_ptr;

public:
    // Default constructor - creates empty pointer with ownership
    IndexPtr() : ptr(nullptr), owns_ptr(std::make_shared<bool>(true)) {}

    // Constructor - always starts with ownership
    explicit IndexPtr(VecSimIndex *p) : owns_ptr(std::make_shared<bool>(true)) {
        if (p) {
            ptr = std::shared_ptr<VecSimIndex>(p, [owns_ptr = this->owns_ptr](VecSimIndex *p) {
                if (*owns_ptr) {
                    VecSimIndex_Free(p);
                }
            });
        }
    }

    // Prevent copying to ensure clear ownership
    IndexPtr(const IndexPtr &) = delete;
    IndexPtr &operator=(const IndexPtr &) = delete;

    // Allow moving
    IndexPtr(IndexPtr &&) = default;
    IndexPtr &operator=(IndexPtr &&) = default;

    // Access methods
    VecSimIndex *get() const { return ptr.get(); }
    std::shared_ptr<VecSimIndex> get_shared() { return ptr; }

    // Implicit conversion to raw pointer for ease of use
    operator VecSimIndex *() const { return ptr.get(); }

    // Ownership control
    void release_ownership() { *owns_ptr = false; }
};

using fp32_index_t = IndexType<VecSimType_FLOAT32, float, float>;
using fp64_index_t = IndexType<VecSimType_FLOAT64, double, double>;
using bf16_index_t = IndexType<VecSimType_BFLOAT16, vecsim_types::bfloat16, float>;
using fp16_index_t = IndexType<VecSimType_FLOAT16, vecsim_types::float16, float>;
using int8_index_t = IndexType<VecSimType_INT8, int8_t, float>;
using uint8_index_t = IndexType<VecSimType_UINT8, uint8_t, float>;

#define INDICES   BM_VecSimIndex<index_type_t>::indices
#define GET_INDEX BM_VecSimIndex<index_type_t>::get_index
#define QUERIES   BM_VecSimIndex<index_type_t>::queries
#define N_QUERIES BM_VecSimGeneral::n_queries
#define N_VECTORS BM_VecSimGeneral::n_vectors
#define DIM       BM_VecSimGeneral::dim
#define IS_MULTI  BM_VecSimGeneral::is_multi

constexpr uint32_t DEFAULT_BM_INDEXES_MASK = IndexTypeFlags::INDEX_MASK_BF |
                                             IndexTypeFlags::INDEX_MASK_HNSW |
                                             IndexTypeFlags::INDEX_MASK_TIERED_HNSW;
