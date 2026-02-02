/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

// Local extensions to vecsim_stl namespace.
// This file adds STL containers that are not in the original VectorSimilarity repo.

#include "VecSim/memory/vecsim_base.h"
#include <deque>

namespace vecsim_stl {

/**
 * @brief A deque container with custom VecSim allocator.
 *
 * This is similar to vecsim_stl::vector but for std::deque.
 * Deque is useful when you need:
 * - Stable references (elements don't move when container grows)
 * - Efficient insertion/removal at both ends
 *
 * Used in HNSWDiskIndex for nodeLocks_ since std::atomic_bool is not movable.
 *
 * @tparam T The element type stored in the deque.
 */
template <typename T>
class deque : public VecsimBaseObject, public std::deque<T, VecsimSTLAllocator<T>> {
    using base_deque = std::deque<T, VecsimSTLAllocator<T>>;

public:
    /**
     * @brief Construct an empty deque with the given allocator.
     * @param alloc The VecSim allocator to use for memory management.
     */
    explicit deque(const std::shared_ptr<VecSimAllocator>& alloc) : VecsimBaseObject(alloc), base_deque(alloc) {}

    /**
     * @brief Construct a deque with n default-initialized elements.
     * @param n The number of elements to initialize.
     * @param alloc The VecSim allocator to use for memory management.
     */
    explicit deque(size_t n, const std::shared_ptr<VecSimAllocator>& alloc)
        : VecsimBaseObject(alloc), base_deque(n, alloc) {}

    /**
     * @brief Construct a deque with n copies of the given value.
     * @param n The number of elements.
     * @param val The value to copy into each element.
     * @param alloc The VecSim allocator to use for memory management.
     */
    explicit deque(size_t n, const T& val, const std::shared_ptr<VecSimAllocator>& alloc)
        : VecsimBaseObject(alloc), base_deque(n, val, alloc) {}
};

} // namespace vecsim_stl
