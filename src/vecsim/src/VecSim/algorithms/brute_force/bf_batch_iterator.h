/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/batch_iterator.h"
#include "VecSim/algorithms/brute_force/brute_force.h"

#include <vector>
#include <limits>
#include <cassert>
#include <algorithm> //nth_element
#include <iostream>
#include <cmath>
#include <functional>

using std::pair;

template <typename DataType, typename DistType>
class BF_BatchIterator : public VecSimBatchIterator {
protected:
    const BruteForceIndex<DataType, DistType> *index;
    size_t index_label_count; // number of labels in the index when calculating the scores,
                              // which is the only time we access the index.
    vecsim_stl::vector<pair<DistType, labelType>> scores; // vector of scores for every label.
    size_t scores_valid_start_pos; // the first index in the scores vector that contains a vector
                                   // that hasn't been returned already.

    VecSimQueryReply *searchByHeuristics(size_t n_res, VecSimQueryReply_Order order);
    VecSimQueryReply *selectBasedSearch(size_t n_res);
    VecSimQueryReply *heapBasedSearch(size_t n_res);
    void swapScores(const vecsim_stl::unordered_map<labelType, size_t> &TopCandidatesIndices,
                    size_t res_num);

    virtual inline VecSimQueryReply_Code calculateScores() = 0;

public:
    BF_BatchIterator(void *query_vector, const BruteForceIndex<DataType, DistType> *bf_index,
                     VecSimQueryParams *queryParams, std::shared_ptr<VecSimAllocator> allocator);

    VecSimQueryReply *getNextResults(size_t n_res, VecSimQueryReply_Order order) override;

    bool isDepleted() override;

    void reset() override;

    ~BF_BatchIterator() override = default;
};

/******************** Implementation **************/

// heuristics: decide if using heap or select search, based on the ratio between the
// number of remaining results and the index size.
template <typename DataType, typename DistType>
VecSimQueryReply *
BF_BatchIterator<DataType, DistType>::searchByHeuristics(size_t n_res,
                                                         VecSimQueryReply_Order order) {
    if ((this->index_label_count - this->getResultsCount()) / 1000 > n_res) {
        // Heap based search always returns the results ordered by score
        return this->heapBasedSearch(n_res);
    }
    VecSimQueryReply *rep = this->selectBasedSearch(n_res);
    if (order == BY_SCORE) {
        sort_results_by_score(rep);
    } else if (order == BY_SCORE_THEN_ID) {
        sort_results_by_score_then_id(rep);
    }
    return rep;
}

template <typename DataType, typename DistType>
void BF_BatchIterator<DataType, DistType>::swapScores(
    const vecsim_stl::unordered_map<labelType, size_t> &TopCandidatesIndices, size_t res_num) {
    // Create a set of the indices in the scores array for every results that we return.
    vecsim_stl::set<size_t> indices(this->allocator);
    for (auto pos : TopCandidatesIndices) {
        indices.insert(pos.second);
    }
    // Get the first valid position in the next iteration.
    size_t next_scores_valid_start_pos = this->scores_valid_start_pos + res_num;
    // Get the first index of a results in this iteration which is greater or equal to
    // next_scores_valid_start_pos.
    auto reuse_index_it = indices.lower_bound(next_scores_valid_start_pos);
    auto it = indices.begin();
    size_t ind = this->scores_valid_start_pos;
    // Swap elements which are in the first res_num positions in the scores array, and place them
    // in indices of results that we return now (reuse these indices).
    while (ind < next_scores_valid_start_pos) {
        // don't swap if there is a result in one of the heading indices which will be invalid from
        // next iteration.
        if (*it == ind) {
            it++;
        } else {
            this->scores[*reuse_index_it] = this->scores[ind];
            reuse_index_it++;
        }
        ind++;
    }
    this->scores_valid_start_pos = next_scores_valid_start_pos;
}

template <typename DataType, typename DistType>
VecSimQueryReply *BF_BatchIterator<DataType, DistType>::heapBasedSearch(size_t n_res) {
    auto rep = new VecSimQueryReply(this->allocator);
    DistType upperBound = std::numeric_limits<DistType>::lowest();
    vecsim_stl::max_priority_queue<DistType, labelType> TopCandidates(this->allocator);
    // map vector's label to its index in the scores vector.
    vecsim_stl::unordered_map<labelType, size_t> TopCandidatesIndices(n_res, this->allocator);
    for (size_t i = this->scores_valid_start_pos; i < this->scores.size(); i++) {
        if (TopCandidates.size() >= n_res) {
            if (this->scores[i].first < upperBound) {
                // remove the furthest vector from the candidates and from the label->index mappings
                // we first remove the worst candidate so we wont exceed the allocated size
                TopCandidatesIndices.erase(TopCandidates.top().second);
                TopCandidates.pop();
            } else {
                continue;
            }
        }
        // top candidate heap size is smaller than n either because we didn't reach n_res yet,
        // or we popped the heap top since the the current score is closer
        TopCandidates.emplace(this->scores[i].first, this->scores[i].second);
        TopCandidatesIndices[this->scores[i].second] = i;
        upperBound = TopCandidates.top().first;
    }

    // Save the top results to return.
    rep->results.resize(TopCandidates.size());
    for (auto result = rep->results.rbegin(); result != rep->results.rend(); result++) {
        std::tie(result->score, result->id) = TopCandidates.top();
        TopCandidates.pop();
    }
    swapScores(TopCandidatesIndices, rep->results.size());
    return rep;
}

template <typename DataType, typename DistType>
VecSimQueryReply *BF_BatchIterator<DataType, DistType>::selectBasedSearch(size_t n_res) {
    auto rep = new VecSimQueryReply(this->allocator);
    size_t remaining_vectors_count = this->scores.size() - this->scores_valid_start_pos;
    // Get an iterator to the effective first element in the scores array, which is the first
    // element that hasn't been returned in previous iterations.
    auto valid_begin_it = this->scores.begin() + (int)(this->scores_valid_start_pos);
    // We return up to n_res vectors, the remaining vectors size is an upper bound.
    if (n_res > remaining_vectors_count) {
        n_res = remaining_vectors_count;
    }
    auto n_th_element_pos = valid_begin_it + (int)n_res;
    // This will perform an in-place partition of the elements in the slice of the array that
    // contains valid results, based on the n-th element as the pivot - every element with a lower
    // will be placed before it, and all the rest will be placed after.
    std::nth_element(valid_begin_it, n_th_element_pos, this->scores.end());

    rep->results.reserve(n_res);
    for (size_t i = this->scores_valid_start_pos; i < this->scores_valid_start_pos + n_res; i++) {
        rep->results.push_back(VecSimQueryResult{this->scores[i].second, this->scores[i].first});
    }
    // Update the valid results start position after returning the results.
    this->scores_valid_start_pos += rep->results.size();
    return rep;
}

template <typename DataType, typename DistType>
BF_BatchIterator<DataType, DistType>::BF_BatchIterator(
    void *query_vector, const BruteForceIndex<DataType, DistType> *bf_index,
    VecSimQueryParams *queryParams, std::shared_ptr<VecSimAllocator> allocator)
    : VecSimBatchIterator(query_vector, queryParams ? queryParams->timeoutCtx : nullptr, allocator),
      index(bf_index), index_label_count(index->indexLabelCount()), scores(allocator),
      scores_valid_start_pos(0) {}

template <typename DataType, typename DistType>
VecSimQueryReply *
BF_BatchIterator<DataType, DistType>::getNextResults(size_t n_res, VecSimQueryReply_Order order) {
    // Only in the first iteration we need to compute all the scores
    if (this->scores.empty()) {
        assert(getResultsCount() == 0);

        // The only time we access the index. This function also updates the iterator's label count.
        auto rc = calculateScores();

        if (VecSim_OK != rc) {
            return new VecSimQueryReply(this->allocator, rc);
        }
    }
    if (VECSIM_TIMEOUT(this->getTimeoutCtx())) {
        return new VecSimQueryReply(this->allocator, VecSim_QueryReply_TimedOut);
    }
    VecSimQueryReply *rep = searchByHeuristics(n_res, order);

    this->updateResultsCount(VecSimQueryReply_Len(rep));
    if (order == BY_ID) {
        sort_results_by_id(rep);
    }
    return rep;
}

template <typename DataType, typename DistType>
bool BF_BatchIterator<DataType, DistType>::isDepleted() {
    assert(this->getResultsCount() <= this->index_label_count);
    bool depleted = this->getResultsCount() == this->index_label_count;
    return depleted;
}

template <typename DataType, typename DistType>
void BF_BatchIterator<DataType, DistType>::reset() {
    this->scores.clear();
    this->resetResultsCount();
    this->scores_valid_start_pos = 0;
}
