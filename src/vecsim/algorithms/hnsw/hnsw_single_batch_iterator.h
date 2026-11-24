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
class HNSWSingle_BatchIterator : public HNSW_BatchIterator<DataType, DistType> {
private:
    inline void fillFromExtras(candidatesLabelsMaxHeap<DistType> *top_candidates) override;
    inline void prepareResults(VecSimQueryReply *rep,
                               candidatesLabelsMaxHeap<DistType> *top_candidates,
                               size_t n_res) override;

    inline void updateHeaps(candidatesLabelsMaxHeap<DistType> *top_candidates, DistType dist,
                            idType id) override;

public:
    HNSWSingle_BatchIterator(void *query_vector, const HNSWIndex<DataType, DistType> *index,
                             VecSimQueryParams *queryParams,
                             std::shared_ptr<VecSimAllocator> allocator)
        : HNSW_BatchIterator<DataType, DistType>(query_vector, index, queryParams, allocator) {}

    ~HNSWSingle_BatchIterator() override = default;
};

/******************** Implementation **************/

template <typename DataType, typename DistType>
void HNSWSingle_BatchIterator<DataType, DistType>::prepareResults(
    VecSimQueryReply *rep, candidatesLabelsMaxHeap<DistType> *top_candidates, size_t n_res) {

    if (this->prefetch_batch_vectors && top_candidates->size() > n_res &&
        this->top_candidates_extras.hasOrderedDistances()) {
        using MaxHeap = vecsim_stl::max_priority_queue<DistType, labelType>;
        auto *heap = dynamic_cast<MaxHeap *>(top_candidates);
        if (heap && std::none_of(heap->begin(), heap->end(),
                                 [](const auto &item) { return std::isnan(item.first); })) {
            // Partition by (distance, label), the same key used by the legacy heaps.
            auto entries = heap->takeEntries();
            auto extra = entries.begin() + n_res;
            std::nth_element(entries.begin(), extra, entries.end());
            std::sort(entries.begin(), extra);
            rep->results.resize(n_res);
            for (size_t i = 0; i < n_res; ++i) {
                std::tie(rep->results[i].score, rep->results[i].id) = entries[i];
            }
            this->top_candidates_extras.appendEntries(extra, entries.end());
            return;
        }
    }

    // Put the "spare" results (if exist) in the extra candidates heap.
    while (top_candidates->size() > n_res) {
        this->top_candidates_extras.emplace(top_candidates->top()); // (distance, label)
        top_candidates->pop();
    }
    // Return results from the top candidates heap, put them in reverse order in the batch results
    // array.
    rep->results.resize(top_candidates->size());
    for (auto result = rep->results.rbegin(); result != rep->results.rend(); ++result) {
        std::tie(result->score, result->id) = top_candidates->top();
        top_candidates->pop();
    }
}

template <typename DataType, typename DistType>
void HNSWSingle_BatchIterator<DataType, DistType>::fillFromExtras(
    candidatesLabelsMaxHeap<DistType> *top_candidates) {
    auto &extras = this->top_candidates_extras;
    if (this->prefetch_batch_vectors && top_candidates->empty() && !extras.empty() &&
        extras.size() <= this->ef && extras.hasOrderedDistances()) {
        using MaxHeap = vecsim_stl::max_priority_queue<DistType, labelType>;
        if (auto *heap = dynamic_cast<MaxHeap *>(top_candidates)) {
            // The entire saved batch fits, so it needs one heap build and no per-item moves.
            heap->replaceEntries(extras.takeEntries());
            return;
        }
    }
    while (top_candidates->size() < this->ef && !this->top_candidates_extras.empty()) {
        top_candidates->emplace(this->top_candidates_extras.top().first,
                                this->top_candidates_extras.top().second);
        this->top_candidates_extras.pop();
    }
}

template <typename DataType, typename DistType>
void HNSWSingle_BatchIterator<DataType, DistType>::updateHeaps(
    candidatesLabelsMaxHeap<DistType> *top_candidates, DistType dist, idType id) {
    if (top_candidates->size() < this->ef) {
        top_candidates->emplace(dist, this->index->getExternalLabel(id));
        this->lower_bound = top_candidates->top().first;
    } else if (this->lower_bound > dist) {
        top_candidates->emplace(dist, this->index->getExternalLabel(id));
        // If the top candidates queue is full, pass the "worst" results to the "extras",
        // for the next iterations.
        this->top_candidates_extras.emplace(top_candidates->top().first,
                                            top_candidates->top().second);
        top_candidates->pop();
        this->lower_bound = top_candidates->top().first;
    }
}
