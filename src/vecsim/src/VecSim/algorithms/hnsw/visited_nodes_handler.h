/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <mutex>
#include <vector>
#include "VecSim/memory/vecsim_malloc.h"
#include "VecSim/memory/vecsim_base.h"

typedef unsigned short tag_t;

/**
 * Used as a singleton that is responsible for marking nodes that were visited in the graph scan.
 * Every scan has a "pseudo unique" tag which is associated with this specific scan, and nodes
 * that were visited in this particular scan are tagged with this tag. The tags range from
 * 1-MAX_USHORT, and we reset the tags after we complete MAX_USHORT scans.
 */
class VisitedNodesHandler : public VecsimBaseObject {
private:
    tag_t cur_tag;
    tag_t *elements_tags;
    unsigned int num_elements;

public:
    VisitedNodesHandler(unsigned int cap, const std::shared_ptr<VecSimAllocator> &allocator);

    // Return unused tag for marking the visited nodes. The tags are cyclic, so whenever we reach
    // zero, we reset the tags of all the nodes (and use 1 as the fresh tag)
    tag_t getFreshTag();

    inline tag_t *getElementsTags() { return elements_tags; }

    void reset();

    void resize(size_t new_size);

    // Mark node_id with tag, to have an indication that this node has been visited.
    inline void tagNode(unsigned int node_id, tag_t tag) { elements_tags[node_id] = tag; }

    // Get the tag in which node_id is marked currently.
    inline tag_t getNodeTag(unsigned int node_id) { return elements_tags[node_id]; }

    ~VisitedNodesHandler() noexcept override;
};

/**
 * A wrapper class for using a pool of VisitedNodesHandler (relevant for parallel graph scans).
 */
class VisitedNodesHandlerPool : public VecsimBaseObject {
private:
    std::vector<VisitedNodesHandler *, VecsimSTLAllocator<VisitedNodesHandler *>> pool;
    std::mutex pool_guard;
    unsigned int num_elements;
    unsigned short total_handlers_in_use;

public:
    VisitedNodesHandlerPool(int cap, const std::shared_ptr<VecSimAllocator> &allocator);

    VisitedNodesHandler *getAvailableVisitedNodesHandler();

    void returnVisitedNodesHandlerToPool(VisitedNodesHandler *handler);

    // This should be called under a guarded section only (NOT in parallel).
    void resize(size_t new_size);

    size_t getPoolSize() { return pool.size(); }

    void clearPool();

    ~VisitedNodesHandlerPool() override;
};
