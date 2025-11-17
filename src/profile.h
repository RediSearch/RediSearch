/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once


#include "value.h"
#include "aggregate/aggregate.h"

#define printProfileType(vtype) RedisModule_ReplyKV_SimpleString(reply, "Type", (vtype))
#define printProfileTime(vtime) RedisModule_ReplyKV_Double(reply, "Time", (vtime))
#define printProfileIteratorCounter(vcount) RedisModule_ReplyKV_LongLong(reply, "Number of reading operations", (vcount))
#define printProfileRPCounter(vcount) RedisModule_ReplyKV_LongLong(reply, "Results processed", (vcount))
#define printProfileNumBatches(hybrid_reader) \
  RedisModule_ReplyKV_LongLong(reply, "Batches number", (hybrid_reader)->numIterations)
#define printProfileOptimizationType(oi) \
  RedisModule_ReplyKV_SimpleString(reply, "Optimizer mode", QOptimizer_PrintType((oi)->optim))

  typedef struct ProfilePrinterCtx {
    AREQ *req;
    bool timedout;
    bool reachedMaxPrefixExpansions;
    bool bgScanOOM;
  } ProfilePrinterCtx; // Context for the profile printing callback

void Profile_Print(RedisModule_Reply *reply, ProfilePrinterCtx *ctx);

void printReadIt(RedisModule_Reply *reply, IndexIterator *root, size_t counter,
                 double cpuTime, PrintProfileConfig *config);
