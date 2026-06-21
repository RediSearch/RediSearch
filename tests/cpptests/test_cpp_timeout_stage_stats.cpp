/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "info/global_stats.h"
#include "query_error.h"

// Verifies the per-stage timeout breakdown added for MOD-13192: each stage has
// its own counter, the aggregate `timeout` counter tracks the sum of the stages,
// and the `stage` argument is ignored for non-timeout codes.
class TimeoutStageStatsTest : public ::testing::Test {};

// Update the timeout error/warning counters and assert the targeted per-stage
// sub-counter moved by exactly one while the aggregate moved with it, leaving the
// other stages untouched. Restores the global counters afterwards so the
// process-global stats are unaffected by the test.
static void checkStage(bool isError, bool coord, QueryTimeoutStage stage) {
  QueriesGlobalStats before = TotalGlobalStats_GetQueryStats();
  if (isError) {
    QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, stage, 1, coord);
  } else {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_TIMED_OUT, stage, 1, coord);
  }
  QueriesGlobalStats after = TotalGlobalStats_GetQueryStats();

  // Pick the struct the update targeted.
  const QueryTimeoutStageStats &b = isError ? (coord ? before.coord_errors.timeout_by_stage
                                                     : before.shard_errors.timeout_by_stage)
                                            : (coord ? before.coord_warnings.timeout_by_stage
                                                     : before.shard_warnings.timeout_by_stage);
  const QueryTimeoutStageStats &a =
      isError
          ? (coord ? after.coord_errors.timeout_by_stage : after.shard_errors.timeout_by_stage)
          : (coord ? after.coord_warnings.timeout_by_stage : after.shard_warnings.timeout_by_stage);
  size_t aggBefore = isError
                         ? (coord ? before.coord_errors.timeout : before.shard_errors.timeout)
                         : (coord ? before.coord_warnings.timeout : before.shard_warnings.timeout);
  size_t aggAfter = isError ? (coord ? after.coord_errors.timeout : after.shard_errors.timeout)
                            : (coord ? after.coord_warnings.timeout : after.shard_warnings.timeout);

  EXPECT_EQ(a.queue, b.queue + (stage == QUERY_TIMEOUT_STAGE_QUEUE ? 1u : 0u));
  EXPECT_EQ(a.pipeline, b.pipeline + (stage == QUERY_TIMEOUT_STAGE_PIPELINE ? 1u : 0u));
  EXPECT_EQ(a.reply, b.reply + (stage == QUERY_TIMEOUT_STAGE_REPLY ? 1u : 0u));
  // Aggregate moved by one and equals the sum of the per-stage parts.
  EXPECT_EQ(aggAfter, aggBefore + 1u);
  EXPECT_EQ(aggAfter, a.queue + a.pipeline + a.reply);

  // Restore the global counters.
  if (isError) {
    QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, stage, -1, coord);
  } else {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_TIMED_OUT, stage, -1, coord);
  }
}

TEST_F(TimeoutStageStatsTest, ErrorStagesShardAndCoord) {
  for (bool coord : {false, true}) {
    checkStage(/*isError=*/true, coord, QUERY_TIMEOUT_STAGE_QUEUE);
    checkStage(/*isError=*/true, coord, QUERY_TIMEOUT_STAGE_PIPELINE);
    checkStage(/*isError=*/true, coord, QUERY_TIMEOUT_STAGE_REPLY);
  }
}

TEST_F(TimeoutStageStatsTest, WarningStagesShardAndCoord) {
  for (bool coord : {false, true}) {
    checkStage(/*isError=*/false, coord, QUERY_TIMEOUT_STAGE_QUEUE);
    checkStage(/*isError=*/false, coord, QUERY_TIMEOUT_STAGE_PIPELINE);
    checkStage(/*isError=*/false, coord, QUERY_TIMEOUT_STAGE_REPLY);
  }
}

// The stage argument must be ignored for non-timeout codes: the per-stage timeout
// counters must not move when recording an OOM error.
TEST_F(TimeoutStageStatsTest, NonTimeoutCodeIgnoresStage) {
  QueriesGlobalStats before = TotalGlobalStats_GetQueryStats();
  QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_OUT_OF_MEMORY, QUERY_TIMEOUT_STAGE_REPLY, 1,
                                     true);
  QueriesGlobalStats after = TotalGlobalStats_GetQueryStats();

  EXPECT_EQ(after.coord_errors.timeout_by_stage.queue, before.coord_errors.timeout_by_stage.queue);
  EXPECT_EQ(after.coord_errors.timeout_by_stage.pipeline,
            before.coord_errors.timeout_by_stage.pipeline);
  EXPECT_EQ(after.coord_errors.timeout_by_stage.reply, before.coord_errors.timeout_by_stage.reply);
  EXPECT_EQ(after.coord_errors.timeout, before.coord_errors.timeout);
  EXPECT_EQ(after.coord_errors.oom, before.coord_errors.oom + 1u);

  // Restore.
  QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_OUT_OF_MEMORY, QUERY_TIMEOUT_STAGE_REPLY, -1,
                                     true);
}
