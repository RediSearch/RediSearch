/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/*
 * Minimal C shims for M2b benchmarks.
 *
 * These functions drive VecSim directly (bypassing the HybridIterator wrapper)
 * to produce a C baseline that is directly comparable to the Rust implementation.
 * Both sides use the same VecSim APIs in the same order, so the comparison is fair.
 *
 * Declared via `extern "C" { ... }` in the Rust bench file — no separate header needed.
 */

#include <stddef.h>
#include <stdlib.h>
#include <math.h>

#include "VecSim/vec_sim.h"
#include "VecSim/query_results.h"

/* ── Unfiltered (standard KNN) ─────────────────────────────────────────────
 *
 * Mirrors VectorTopKIterator::new_unfiltered: call TopKQuery, iterate reply,
 * count results.
 */
size_t bench_c_unfiltered(VecSimIndex *index, const void *query_vec, size_t k) {
    VecSimQueryParams params = {0};
    VecSimQueryReply *reply = VecSimIndex_TopKQuery(index, query_vec, k, &params, BY_ID);
    VecSimQueryReply_Iterator *iter = VecSimQueryReply_GetIterator(reply);

    size_t count = 0;
    while (VecSimQueryReply_IteratorHasNext(iter)) {
        VecSimQueryResult *res = VecSimQueryReply_IteratorNext(iter);
        (void)VecSimQueryResult_GetId(res);
        (void)VecSimQueryResult_GetScore(res);
        count++;
    }

    VecSimQueryReply_IteratorFree(iter);
    VecSimQueryReply_Free(reply);
    return count;
}

/* ── Batches ────────────────────────────────────────────────────────────────
 *
 * Mirrors VectorScoreSource::next_batch + intersect_batch_with_child:
 *  - create batch iterator
 *  - for each batch, scan linearly for IDs present in child_ids
 *  - collect into a fixed-size top-k accumulator (count, not a real heap)
 *
 * `child_ids` must be sorted ascending. `child_count` is the array length.
 */
size_t bench_c_batches(VecSimIndex *index, const void *query_vec, size_t k,
                       const size_t *child_ids, size_t child_count) {
    VecSimQueryParams params = {0};
    VecSimBatchIterator *batch_it = VecSimBatchIterator_New(index, query_vec, &params);

    size_t index_size = VecSimIndex_IndexSize(index);
    size_t child_est = child_count > 0 ? child_count : 1;
    size_t heap_count = 0;

    while (VecSimBatchIterator_HasNext(batch_it) && heap_count < k) {
        size_t k_remaining = k - heap_count;
        size_t batch_size = (k_remaining * index_size / child_est) + 1;

        VecSimQueryReply *reply = VecSimBatchIterator_Next(batch_it, batch_size, BY_ID);
        VecSimQueryReply_Iterator *iter = VecSimQueryReply_GetIterator(reply);

        /* merge-join: batch (sorted by id) vs child_ids (sorted by id) */
        size_t ci = 0;
        while (VecSimQueryReply_IteratorHasNext(iter) && ci < child_count) {
            VecSimQueryResult *res = VecSimQueryReply_IteratorNext(iter);
            size_t vid = (size_t)VecSimQueryResult_GetId(res);
            while (ci < child_count && child_ids[ci] < vid) ci++;
            if (ci < child_count && child_ids[ci] == vid) {
                (void)VecSimQueryResult_GetScore(res);
                heap_count++;
                ci++;
                if (heap_count >= k) break;
            }
        }

        VecSimQueryReply_IteratorFree(iter);
        VecSimQueryReply_Free(reply);
    }

    VecSimBatchIterator_Free(batch_it);
    return heap_count;
}

/* ── Adhoc BF ───────────────────────────────────────────────────────────────
 *
 * Mirrors the RAM path of VectorScoreSource::lookup_score:
 *  - acquire/release shared locks around each GetDistanceFrom_Unsafe call
 *  - count non-NaN results
 */
size_t bench_c_adhoc(VecSimIndex *index, const void *query_vec,
                     const size_t *child_ids, size_t child_count) {
    VecSimTieredIndex_AcquireSharedLocks(index);
    size_t count = 0;
    for (size_t i = 0; i < child_count; i++) {
        double dist = VecSimIndex_GetDistanceFrom_Unsafe(index, child_ids[i], query_vec);
        if (!isnan(dist)) count++;
    }
    VecSimTieredIndex_ReleaseSharedLocks(index);
    return count;
}
