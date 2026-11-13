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
#include "VecSim/memory/vecsim_base.h"

/**
 * An abstract class for performing search in batches. Every index type should implement its own
 * batch iterator class.
 * A batch iterator instance is NOT meant to be shared between threads, but the iterated index can
 * be and in this case the iterator should be able to iterate the index concurrently and safely.
 */
struct VecSimBatchIterator : public VecsimBaseObject {
private:
    void *query_vector;
    size_t returned_results_count;
    void *timeoutCtx;

public:
    explicit VecSimBatchIterator(void *query_vector, void *tctx,
                                 std::shared_ptr<VecSimAllocator> allocator)
        : VecsimBaseObject(allocator), query_vector(query_vector), returned_results_count(0),
          timeoutCtx(tctx) {};

    virtual inline const void *getQueryBlob() const { return query_vector; }

    inline void *getTimeoutCtx() const { return timeoutCtx; }

    inline size_t getResultsCount() const { return returned_results_count; }

    inline void updateResultsCount(size_t num) { returned_results_count += num; }

    inline void resetResultsCount() { returned_results_count = 0; }

    // Returns the Top n_res results that *hasn't been returned* in the previous calls.
    // The implementation is specific to the underline index algorithm.
    virtual VecSimQueryReply *getNextResults(size_t n_res, VecSimQueryReply_Order order) = 0;

    // Indicates whether there are additional results from the index to return
    virtual bool isDepleted() = 0;

    // Reset the iterator to the initial state, before any results has been returned.
    virtual void reset() = 0;

    virtual ~VecSimBatchIterator() noexcept { allocator->free_allocation(this->query_vector); };
};
