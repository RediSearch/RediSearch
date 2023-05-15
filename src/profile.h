/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once


#include "value.h"
#include "aggregate/aggregate.h"

#define CLOCKS_PER_MILLISEC  (CLOCKS_PER_SEC / 1000)

#define printProfileType(vtype)                       \
  do {                                                \
    RedisModule_ReplyWithSimpleString(ctx, "Type");   \
    RedisModule_ReplyWithSimpleString(ctx, vtype);    \
  } while (0)

#define printProfileTime(vtime)                       \
  do {                                                \
    RedisModule_ReplyWithSimpleString(ctx, "Time");   \
    RedisModule_ReplyWithDouble(ctx, vtime);          \
  } while (0)

#define printProfileCounter(vcounter)                 \
  do {                                                \
    RedisModule_ReplyWithSimpleString(ctx, "Counter");\
    RedisModule_ReplyWithLongLong(ctx, vcounter);     \
  } while (0)

#define printProfileNumBatches(hybrid_reader)                         \
  do {                                                                \
    RedisModule_ReplyWithSimpleString(ctx, "Batches number");         \
    RedisModule_ReplyWithLongLong(ctx, hybrid_reader->numIterations); \
  } while (0)

#define printProfileOptimizationType(oi)                              \
  do {                                                                \
    RedisModule_ReplyWithSimpleString(ctx, "Optimizer mode");         \
    RedisModule_ReplyWithSimpleString(ctx,                            \
            QOptimizer_PrintType(oi->optim));                         \
  } while (0)

int Profile_Print(RedisModuleCtx *ctx, AREQ *req);

void printReadIt(RedisModuleCtx *ctx, IndexIterator *root, size_t counter,
                 double cpuTime, PrintProfileConfig *config);
