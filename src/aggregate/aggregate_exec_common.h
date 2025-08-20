/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once
#include "redismodule.h"
#include "query_error.h"
#include "result_processor.h"

bool hasTimeoutError(QueryError *err);

bool ShouldReplyWithError(QueryError *status, RSTimeoutPolicy timeoutPolicy, bool isProfile);

bool ShouldReplyWithTimeoutError(int rc, RSTimeoutPolicy timeoutPolicy, bool isProfile);

void ReplyWithTimeoutError(RedisModule_Reply *reply);

void destroyResults(SearchResult **results);

SearchResult **AggregateResults(ResultProcessor *rp, int *rc);

void startPipelineCommon(RSTimeoutPolicy timeoutPolicy, struct timespec *timeout,
                              ResultProcessor *rp, SearchResult ***results, SearchResult *r, int *rc);