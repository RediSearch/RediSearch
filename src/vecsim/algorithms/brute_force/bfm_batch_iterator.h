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

        bool completed = this->index->scanVectors(
            this->getTimeoutCtx(), [&](const char *vector, idType curr_id) {
                auto score = this->index->calcDistance(vector, this->getQueryBlob());
                labelType curr_label = this->index->getVectorLabel(curr_id);
                auto curr_pair = tmp_scores.find(curr_label);
                // For each score, emplace or update the score of the label.
                if (curr_pair == tmp_scores.end()) {
                    tmp_scores.emplace(curr_label, score);
                } else if (curr_pair->second > score) {
                    curr_pair->second = score;
                }
            });
        if (!completed) {
            return VecSim_QueryReply_Stopped;
        }
        for (auto p : tmp_scores) {
            this->scores.emplace_back(p.second, p.first);
        }
        return VecSim_QueryReply_OK;
    }
};
