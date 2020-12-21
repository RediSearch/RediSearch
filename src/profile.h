#pragma once


#include "value.h"
#include "aggregate/aggregate.h"

#define CLOCKS_PER_MILLISEC  ((__clock_t) 1000)
#define PROFILE_VERBOSE RSGlobalConfig.printProfileClock

#define IsProfile(r) ((r)->reqflags & QEXEC_F_PROFILE)

int Profile_Print(RedisModuleCtx *ctx, AREQ *req, size_t *nelem);
void printReadIt(RedisModuleCtx *ctx, IndexIterator *root, size_t counter, double cpuTime);
void printIteratorProfile(RedisModuleCtx *ctx, IndexIterator *root, size_t counter, double cpuTime);
