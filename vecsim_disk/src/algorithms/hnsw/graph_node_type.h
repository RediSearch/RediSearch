#pragma once

#include "VecSim/vec_sim_common.h"
#include "VecSim/utils/vecsim_stl.h"
#include <functional>

/**
 * @brief Represents a node in the HNSW graph at a specific level.
 *
 * Used for tracking nodes that need repair operations, storing neighbor
 * information, and as keys in unordered_maps for job deduplication.
 */
struct GraphNodeType {
    idType id;
    levelType level;

    GraphNodeType(idType id, levelType level) : id(id), level(level) {}

    bool operator==(const GraphNodeType& other) const { return id == other.id && level == other.level; }
};

using GraphNodeList = vecsim_stl::vector<GraphNodeType>;

// Hash function for GraphNodeType to use in unordered_map
namespace std {
template <>
struct hash<GraphNodeType> {
    size_t operator()(const GraphNodeType& node) const noexcept {
        // Combine hashes of the two elements
        static_assert(sizeof(size_t) >= sizeof(idType) + sizeof(levelType));
        size_t combined = node.id + (size_t(node.level) << (sizeof(idType) * 8));
        return std::hash<size_t>()(combined);
    }
};
} // namespace std
