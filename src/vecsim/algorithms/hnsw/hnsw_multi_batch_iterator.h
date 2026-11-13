/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "hnsw_batch_iterator.h"

template <typename DataType, typename DistType>
class HNSWMulti_BatchIterator : public HNSW_BatchIterator<DataType, DistType> {
private:
    vecsim_stl::unordered_set<labelType> returned;

    inline void fillFromExtras(candidatesLabelsMaxHeap<DistType> *top_candidates) override;
    inline void prepareResults(VecSimQueryReply *rep,
                               candidatesLabelsMaxHeap<DistType> *top_candidates,
                               size_t n_res) override;
    inline void updateHeaps(candidatesLabelsMaxHeap<DistType> *top_candidates, DistType dist,
                            idType id) override;

public:
    HNSWMulti_BatchIterator(void *query_vector, const HNSWIndex<DataType, DistType> *index,
                            VecSimQueryParams *queryParams,
                            std::shared_ptr<VecSimAllocator> allocator)
        : HNSW_BatchIterator<DataType, DistType>(query_vector, index, queryParams, allocator),
          returned(this->index->indexSize(), this->allocator) {}

    ~HNSWMulti_BatchIterator() override = default;

    void reset() override;
};

/******************** Implementation **************/

template <typename DataType, typename DistType>
void HNSWMulti_BatchIterator<DataType, DistType>::prepareResults(
    VecSimQueryReply *rep, candidatesLabelsMaxHeap<DistType> *top_candidates, size_t n_res) {

    // Put the "spare" results (if exist) in the extra candidates heap.
    while (top_candidates->size() > n_res) {
        this->top_candidates_extras.emplace(top_candidates->top().first,
                                            top_candidates->top().second); // (distance, label)
        top_candidates->pop();
    }
    // Return results from the top candidates heap, put them in reverse order in the batch results
    // array.
    rep->results.resize(top_candidates->size());
    for (auto result = rep->results.rbegin(); result != rep->results.rend(); ++result) {
        std::tie(result->score, result->id) = top_candidates->top();
        this->returned.insert(result->id);
        top_candidates->pop();
    }
}

template <typename DataType, typename DistType>
void HNSWMulti_BatchIterator<DataType, DistType>::fillFromExtras(
    candidatesLabelsMaxHeap<DistType> *top_candidates) {
    while (top_candidates->size() < this->ef && !this->top_candidates_extras.empty()) {
        if (returned.find(this->top_candidates_extras.top().second) == returned.end()) {
            top_candidates->emplace(this->top_candidates_extras.top().first,
                                    this->top_candidates_extras.top().second);
        }
        this->top_candidates_extras.pop();
    }
}

template <typename DataType, typename DistType>
void HNSWMulti_BatchIterator<DataType, DistType>::updateHeaps(
    candidatesLabelsMaxHeap<DistType> *top_candidates, DistType dist, idType id) {

    if (this->lower_bound > dist || top_candidates->size() < this->ef) {
        labelType label = this->index->getExternalLabel(id);
        if (returned.find(label) == returned.end()) {
            top_candidates->emplace(dist, label);
            if (top_candidates->size() > this->ef) {
                // If the top candidates queue is full, pass the "worst" results to the "extras",
                // for the next iterations.
                this->top_candidates_extras.emplace(top_candidates->top().first,
                                                    top_candidates->top().second);
                top_candidates->pop();
            }
            this->lower_bound = top_candidates->top().first;
        }
    }
}

template <typename DataType, typename DistType>
void HNSWMulti_BatchIterator<DataType, DistType>::reset() {
    this->returned.clear();
    HNSW_BatchIterator<DataType, DistType>::reset();
}
