/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/index_factories/tiered_factory.h"
#include "utils/mock_thread_pool.h"
#include "VecSim/algorithms/svs/svs_utils.h"

namespace test_utils {
static TieredIndexParams
CreateTieredSVSParams(VecSimParams &svs_params, tieredIndexMock &mock_thread_pool,
                      size_t training_threshold, size_t update_threshold,
                      size_t update_job_wait_time = SVS_DEFAULT_UPDATE_JOB_WAIT_TIME) {
    return TieredIndexParams{.jobQueue = &mock_thread_pool.jobQ,
                             .jobQueueCtx = mock_thread_pool.ctx,
                             .submitCb = tieredIndexMock::submit_callback,
                             .primaryIndexParams = &svs_params,
                             .specificParams = {.tieredSVSParams = TieredSVSParams{
                                                    .trainingTriggerThreshold = training_threshold,
                                                    .updateTriggerThreshold = update_threshold,
                                                    .updateJobWaitTime = update_job_wait_time}}};
}

template <typename data_t>
static void verifyNumThreads(TieredSVSIndex<data_t> *tiered_index, size_t expected_num_threads,
                             size_t expected_capcity, std::string msg = "") {
    ASSERT_EQ(tiered_index->GetSVSIndex()->getPoolSize(), expected_capcity)
        << msg << ": thread pool capacity mismatch";
    size_t num_reserved_threads = tiered_index->GetSVSIndex()->getParallelism();
    if (num_reserved_threads < expected_num_threads) {
        std::cout << msg << ": WARNING: last reserved threads (" << num_reserved_threads
                  << ") is less than expected (" << expected_num_threads << ")." << std::endl;
    }
}
} // namespace test_utils
