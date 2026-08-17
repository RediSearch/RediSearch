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

// Verifies the per-stage blocked-client timeout breakdown (MOD-13192):
// QueryTimeoutStageStats_Record bumps the matching per-stage sub-counter, while
// the aggregate `timeout` counter is owned separately by
// Query{Errors,Warnings}GlobalStats_UpdateError/Warning. The breakdown counts
// blocked-client timeout callback invocations and is intentionally independent
// of the aggregate counters.
class TimeoutStageStatsTest : public ::testing::Test {};

static const QueryTimeoutStageStats &stagesOf(const QueriesGlobalStats &s, bool isError, bool coord) {
  return isError ? (coord ? s.coord_errors.timeout_by_stage : s.shard_errors.timeout_by_stage)
                 : (coord ? s.coord_warnings.timeout_by_stage : s.shard_warnings.timeout_by_stage);
}

// Record one timeout for `stage` and assert only that per-stage sub-counter moved
// by one, the other stages are untouched, and the aggregate `timeout` counter is
// NOT affected by the recorder. Restores the counter afterwards.
static void checkStage(bool isError, bool coord, QueryTimeoutStage stage) {
  QueriesGlobalStats before = TotalGlobalStats_GetQueryStats();
  QueryTimeoutStageStats_Record(stage, isError, coord);
  QueriesGlobalStats after = TotalGlobalStats_GetQueryStats();

  const QueryTimeoutStageStats &b = stagesOf(before, isError, coord);
  const QueryTimeoutStageStats &a = stagesOf(after, isError, coord);
  EXPECT_EQ(a.queue, b.queue + (stage == QUERY_TIMEOUT_STAGE_QUEUE ? 1u : 0u));
  EXPECT_EQ(a.pipeline, b.pipeline + (stage == QUERY_TIMEOUT_STAGE_PIPELINE ? 1u : 0u));
  EXPECT_EQ(a.reply, b.reply + (stage == QUERY_TIMEOUT_STAGE_REPLY ? 1u : 0u));
  // The recorder only touches the per-stage breakdown, never the aggregate.
  size_t aggBefore = isError ? (coord ? before.coord_errors.timeout : before.shard_errors.timeout)
                             : (coord ? before.coord_warnings.timeout : before.shard_warnings.timeout);
  size_t aggAfter = isError ? (coord ? after.coord_errors.timeout : after.shard_errors.timeout)
                            : (coord ? after.coord_warnings.timeout : after.shard_warnings.timeout);
  EXPECT_EQ(aggAfter, aggBefore);

  // Restore by decrementing the same stage via the inverse... the recorder has no
  // toAdd, so just leave the +1: tests run in one process but assertions are delta
  // based, so a residual +1 does not affect other tests. (Kept minimal on purpose.)
}

TEST_F(TimeoutStageStatsTest, RecordErrorStagesShardAndCoord) {
  for (bool coord : {false, true}) {
    checkStage(/*isError=*/true, coord, QUERY_TIMEOUT_STAGE_QUEUE);
    checkStage(/*isError=*/true, coord, QUERY_TIMEOUT_STAGE_PIPELINE);
    checkStage(/*isError=*/true, coord, QUERY_TIMEOUT_STAGE_REPLY);
  }
}

TEST_F(TimeoutStageStatsTest, RecordWarningStagesShardAndCoord) {
  for (bool coord : {false, true}) {
    checkStage(/*isError=*/false, coord, QUERY_TIMEOUT_STAGE_QUEUE);
    checkStage(/*isError=*/false, coord, QUERY_TIMEOUT_STAGE_PIPELINE);
    checkStage(/*isError=*/false, coord, QUERY_TIMEOUT_STAGE_REPLY);
  }
}

// The aggregate timeout counter (UpdateError/UpdateWarning) and the per-stage
// breakdown (Record) are independent: each moves only its own counters.
TEST_F(TimeoutStageStatsTest, AggregateAndBreakdownAreIndependent) {
  QueriesGlobalStats before = TotalGlobalStats_GetQueryStats();
  QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, 1, true);
  QueriesGlobalStats after = TotalGlobalStats_GetQueryStats();
  // Aggregate moved, per-stage breakdown did not.
  EXPECT_EQ(after.coord_errors.timeout, before.coord_errors.timeout + 1u);
  EXPECT_EQ(after.coord_errors.timeout_by_stage.queue, before.coord_errors.timeout_by_stage.queue);
  EXPECT_EQ(after.coord_errors.timeout_by_stage.pipeline, before.coord_errors.timeout_by_stage.pipeline);
  EXPECT_EQ(after.coord_errors.timeout_by_stage.reply, before.coord_errors.timeout_by_stage.reply);
}
