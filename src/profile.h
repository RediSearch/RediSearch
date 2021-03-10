#pragma once


#include "value.h"
#include "aggregate/aggregate.h"

#define CLOCKS_PER_MILLISEC  (CLOCKS_PER_SEC / 1000)
#define PROFILE_VERBOSE RSGlobalConfig.printProfileClock

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

int Profile_Print(RedisModuleCtx *ctx, AREQ *req);

void printReadIt(RedisModuleCtx *ctx, IndexIterator *root, size_t counter,
                 double cpuTime);
