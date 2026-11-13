
/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "bm_vecsim_general.h"

#include <unordered_set>

void BM_VecSimGeneral::MeasureRecall(VecSimQueryReply *hnsw_results, VecSimQueryReply *bf_results,
                                     std::atomic_int &correct) {
    std::unordered_set<labelType> bf_ids;
    bf_ids.reserve(VecSimQueryReply_Len(bf_results));
    auto bf_it = VecSimQueryReply_GetIterator(bf_results);
    while (VecSimQueryReply_IteratorHasNext(bf_it)) {
        bf_ids.insert(VecSimQueryResult_GetId(VecSimQueryReply_IteratorNext(bf_it)));
    }
    VecSimQueryReply_IteratorFree(bf_it);

    auto hnsw_it = VecSimQueryReply_GetIterator(hnsw_results);
    while (VecSimQueryReply_IteratorHasNext(hnsw_it)) {
        auto hnsw_res_item = VecSimQueryReply_IteratorNext(hnsw_it);
        if (bf_ids.contains(VecSimQueryResult_GetId(hnsw_res_item))) {
            correct++;
        }
    }
    VecSimQueryReply_IteratorFree(hnsw_it);
}

tieredIndexMock *BM_VecSimGeneral::mock_thread_pool = nullptr;
