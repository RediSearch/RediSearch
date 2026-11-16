/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once
#include "bf_batch_iterator.h"

#include <limits>

template <typename DataType, typename DistType>
class BFM_BatchIterator : public BF_BatchIterator<DataType, DistType> {
public:
    BFM_BatchIterator(void *query_vector, const BruteForceIndex<DataType, DistType> *index,
                      VecSimQueryParams *queryParams, std::shared_ptr<VecSimAllocator> allocator)
        : BF_BatchIterator<DataType, DistType>(query_vector, index, queryParams, allocator) {}

    ~BFM_BatchIterator() override = default;

private:
    VecSimQueryReply_Code calculateScores() override {
        this->index_label_count = this->index->indexLabelCount();
        this->scores.reserve(this->index_label_count);
        vecsim_stl::unordered_map<labelType, DistType> tmp_scores(this->index_label_count,
                                                                  this->allocator);

        idType curr_id = 0;
        auto vectors_it = this->index->getVectorsIterator();
        while (auto *vector = vectors_it->next()) {
            // Compute the scores for every vector and extend the scores array.
            if (VECSIM_TIMEOUT(this->getTimeoutCtx())) {
                return VecSim_QueryReply_TimedOut;
            }
            auto score = this->index->calcDistance(vector, this->getQueryBlob());
            labelType curr_label = this->index->getVectorLabel(curr_id);
            auto curr_pair = tmp_scores.find(curr_label);
            // For each score, emplace or update the score of the label.
            if (curr_pair == tmp_scores.end()) {
                tmp_scores.emplace(curr_label, score);
            } else if (curr_pair->second > score) {
                curr_pair->second = score;
            }
            ++curr_id;
        }
        assert(curr_id == this->index->indexSize());
        for (auto p : tmp_scores) {
            this->scores.emplace_back(p.second, p.first);
        }
        return VecSim_QueryReply_OK;
    }
};
