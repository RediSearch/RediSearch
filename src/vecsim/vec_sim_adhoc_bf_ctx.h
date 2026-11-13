/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "vec_sim_common.h"
#include "memory/vecsim_base.h"

/**
 * @brief Base class for ad-hoc brute force context.
 *
 * Provides the interface for distance lookups during hybrid queries.
 * Derived classes implement index-specific logic (e.g., disk SQ8 vs RAM FP32).
 *
 * Inherits from VecsimBaseObject to support allocator-aware memory management,
 * allowing derived classes to use placement new with VecSimAllocator.
 *
 * Usage:
 * 1. Create context with VecSimIndex_AdhocBfCtx_New() - preprocesses query once
 * 2. Call getDistanceFrom() for each candidate label during ad-hoc BF
 * 3. Optionally call getExactDistances() for batch reranking with exact FP32 distances
 * 4. Free context with VecSimIndex_AdhocBfCtx_Free()
 */
struct VecSimAdhocBfCtx : public VecsimBaseObject {
    explicit VecSimAdhocBfCtx(std::shared_ptr<VecSimAllocator> allocator)
        : VecsimBaseObject(std::move(allocator)) {}

    ~VecSimAdhocBfCtx() override = default;

    /**
     * @brief Get distance from preprocessed query to a single label.
     *
     * For disk indexes: tries flat buffer first (exact FP32), then SQ8 backend (approximate).
     * For RAM indexes: returns exact distance.
     *
     * @param label The label to compute distance to.
     * @return Distance, or NAN if label not found.
     */
    virtual double getDistanceFrom(labelType label) const = 0;

    /**
     * @brief Get exact distances for a batch of labels.
     *
     * For disk indexes: fetches FP32 vectors from disk, computes exact distances.
     * For RAM indexes: same as calling getDistanceFrom() for each label.
     *
     * @param labels Input array of labels.
     * @param distances_out Output array, filled with exact distances (NAN if not found).
     * @param count Number of labels.
     */
    virtual void getExactDistances(const labelType *labels, double *distances_out,
                                   size_t count) const = 0;
};
