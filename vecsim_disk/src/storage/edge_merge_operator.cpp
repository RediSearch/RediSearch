/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "edge_merge_operator.h"
#include "encoding.h"
#include <cstring>

namespace rocksdb {

namespace {

constexpr size_t kEdgeSize = sizeof(idType);

// Read an edge from binary data in little-endian format
inline idType readEdgeLE(const char* ptr) noexcept { return encoding::DecodeFixedLE<idType>(ptr); }

// Write an edge to binary data in little-endian format
inline void writeEdgeLE(char* ptr, idType value) noexcept { encoding::EncodeFixedLE<idType>(ptr, value); }

} // anonymous namespace

const char* EdgeListMergeOperator::Name() const { return kClassName(); }

bool EdgeListMergeOperator::FullMergeV2(const MergeOperationInput& merge_in, MergeOperationOutput* merge_out) const {
    std::string& result = merge_out->new_value;
    result.clear();

    // Estimate final size to reduce reallocations (upper bound - deletes may reduce)
    size_t estimated_size = merge_in.existing_value ? merge_in.existing_value->size() : 0;
    for (const auto& operand : merge_in.operand_list) {
        if (operand.size() > 0 && operand[0] == OP_APPEND) {
            estimated_size += operand.size() - 1;
        }
    }
    result.reserve(estimated_size);

    // Start with existing value if present
    if (merge_in.existing_value != nullptr) {
        result.append(merge_in.existing_value->data(), merge_in.existing_value->size());
    }

    // Apply each operand in order - working directly on binary data
    for (const auto& operand : merge_in.operand_list) {
        // CreateAppendOperand and CreateDeleteOperand ensure operand size >= 1
        assert(operand.size() >= 1);

        char op = operand[0];
        const char* payload = operand.data() + 1;
        size_t payload_size = operand.size() - 1;

        if (op == OP_APPEND) {
            // Assumes no duplicate edges are added - HNSW logic should prevent this
            result.append(payload, payload_size);
        } else if (op == OP_DELETE) {
            // Delete operand may contain multiple edges (if partial-merged)
            for (size_t i = 0; i < payload_size; i += kEdgeSize) {
                idType edge_to_delete = readEdgeLE(payload + i);
                const size_t count = result.size() / kEdgeSize;

                // Find the edge position
                size_t pos = 0;
                for (; pos < count; ++pos) {
                    if (readEdgeLE(result.data() + pos * kEdgeSize) == edge_to_delete) {
                        break;
                    }
                }

                // If found, shift everything after it back by one edge
                if (pos < count) {
                    size_t byte_pos = pos * kEdgeSize;
                    size_t remaining = result.size() - byte_pos - kEdgeSize;
                    if (remaining > 0) {
                        std::memmove(&result[byte_pos], &result[byte_pos + kEdgeSize], remaining);
                    }
                    result.resize(result.size() - kEdgeSize);
                }
            }
        } else {
            assert(false && "Unknown operation type in edge merge operator");
        }
    }

    return true;
}

bool EdgeListMergeOperator::PartialMerge(const Slice& /*key*/, const Slice& left_operand, const Slice& right_operand,
                                         std::string* new_value, Logger* /*logger*/) const {
    assert(left_operand.size() >= 1 && right_operand.size() >= 1);

    char leftOp = left_operand[0];
    char rightOp = right_operand[0];

    // Merge if both are the same operation type (APPEND+APPEND or DELETE+DELETE)
    if (leftOp == rightOp && (leftOp == OP_APPEND || leftOp == OP_DELETE)) {
        new_value->reserve(left_operand.size() + right_operand.size() - 1);
        new_value->assign(left_operand.data(), left_operand.size());
        new_value->append(right_operand.data() + 1, right_operand.size() - 1);
        return true;
    }

    return false; // Cannot partially merge different operation types
}

std::string EdgeListMergeOperator::CreateAppendOperand(idType edge) {
    std::string result;
    result.resize(1 + kEdgeSize);
    result[0] = OP_APPEND;
    writeEdgeLE(&result[1], edge);
    return result;
}

std::string EdgeListMergeOperator::CreateDeleteOperand(idType edge) {
    std::string result;
    result.resize(1 + kEdgeSize);
    result[0] = OP_DELETE;
    writeEdgeLE(&result[1], edge);
    return result;
}

std::shared_ptr<MergeOperator> CreateEdgeListMergeOperator() { return std::make_shared<EdgeListMergeOperator>(); }

} // namespace rocksdb
