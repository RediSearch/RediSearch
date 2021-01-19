#pragma once


#include "value.h"
#include "aggregate/aggregate.h"

#define CLOCKS_PER_MILLISEC  (CLOCKS_PER_SEC / 1000)
#define PROFILE_VERBOSE RSGlobalConfig.printProfileClock

int Profile_Print(RedisModuleCtx *ctx, AREQ *req);

void printReadIt(RedisModuleCtx *ctx, IndexIterator *root, size_t counter,
                 double cpuTime);
