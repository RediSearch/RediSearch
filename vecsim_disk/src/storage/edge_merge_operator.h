/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "VecSim/vec_sim_common.h"
#include <vector>
#include <memory>
#include <string>

namespace rocksdb {

/**
 * @brief Custom merge operator for HNSW edge lists.
 *
 * Supports two operations:
 * - APPEND ('A'): Append edges to the list (incoming edges)
 * - DELETE ('D'): Remove a specific edge from the list
 *
 * Operation format:
 * - Append: 'A' + serialized edge (4 bytes, little-endian)
 * - Delete: 'D' + edge_id (4 bytes, little-endian)
 */
class EdgeListMergeOperator : public MergeOperator {
public:
    static constexpr char OP_APPEND = 'A';
    static constexpr char OP_DELETE = 'D';

    static const char* kClassName() { return "EdgeListMergeOperator"; }
    const char* Name() const override;

    bool FullMergeV2(const MergeOperationInput& merge_in, MergeOperationOutput* merge_out) const override;

    bool PartialMerge(const Slice& key, const Slice& left_operand, const Slice& right_operand, std::string* new_value,
                      Logger* logger) const override;

    // Helper to create an append operand
    static std::string CreateAppendOperand(idType edge);

    // Helper to create a delete operand
    static std::string CreateDeleteOperand(idType edge);
};

// Factory function to create the merge operator
std::shared_ptr<MergeOperator> CreateEdgeListMergeOperator();

} // namespace rocksdb
