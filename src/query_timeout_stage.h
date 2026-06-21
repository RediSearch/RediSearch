/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef QUERY_TIMEOUT_STAGE_H__
#define QUERY_TIMEOUT_STAGE_H__

#ifdef __cplusplus
extern "C" {
#endif

/**
 * The stage of the query-processing pipeline in which a timeout occurred.
 * Used to break down the query-timeout metric (see global_stats.h) by where in
 * the lifecycle the deadline was exceeded.
 *
 * The values double as an "execution phase" marker on RequestSyncCtx: the
 * executing (background) thread advances it monotonically QUEUE -> PIPELINE ->
 * REPLY, and the main-thread timeout callbacks read it to classify a timeout.
 */
typedef enum {
  QUERY_TIMEOUT_STAGE_QUEUE = 0,     // before the result-processor pipeline started running
  QUERY_TIMEOUT_STAGE_PIPELINE = 1,  // during result-processor pipeline execution
  QUERY_TIMEOUT_STAGE_REPLY = 2,     // during reply serialization
  QUERY_TIMEOUT_STAGE_COUNT
} QueryTimeoutStage;

#ifdef __cplusplus
}
#endif

#endif  // QUERY_TIMEOUT_STAGE_H__
