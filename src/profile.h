/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "value.h"
#include "aggregate/aggregate.h"
#include "util/timeout.h"

#define printProfileType(vtype) RedisModule_ReplyKV_SimpleString(reply, "Type", (vtype))
#define printProfileTime(vtime) RedisModule_ReplyKV_Double(reply, "Time", (vtime))
#define printProfileCounter(vcounter) RedisModule_ReplyKV_LongLong(reply, "Counter", (vcounter))
#define printProfileGILTime(vtime) RedisModule_ReplyKV_Double(reply, "GIL-Time", (rs_timer_ms(&(vtime))))
#define printProfileNumBatches(hybrid_reader) \
  RedisModule_ReplyKV_LongLong(reply, "Batches number", (hybrid_reader)->numIterations)
#define printProfileOptimizationType(oi) \
  RedisModule_ReplyKV_SimpleString(reply, "Optimizer mode", QOptimizer_PrintType((oi)->optim))

// Print the profile of a single shard
void Profile_Print(RedisModule_Reply *reply, void *ctx);
// Print the profile of a single shard, in full format
void Profile_PrintDefault(RedisModule_Reply *reply, AREQ *req, bool timedout, bool reachedMaxPrefixExpansions);

void printReadIt(RedisModule_Reply *reply, IndexIterator *root, size_t counter, double cpuTime,
                 PrintProfileConfig *config);

#define PROFILE_STR "Profile"
#define PROFILE_SHARDS_STR "Shards"
#define PROFILE_COORDINATOR_STR "Coordinator"

void Profile_PrepareMapForReply(RedisModule_Reply *reply);

typedef struct {
  AREQ *req;
  bool timedout;
  bool reachedMaxPrefixExpansions;
} ProfilePrinterCtx; // Context for the profile printing callback

typedef void (*ProfilePrinterCB)(RedisModule_Reply *reply, void *ctx);

void Profile_PrintInFormat(RedisModule_Reply *reply,
                           ProfilePrinterCB shards_cb, void *shards_ctx,
                           ProfilePrinterCB coordinator_cb, void *coordinator_ctx);
