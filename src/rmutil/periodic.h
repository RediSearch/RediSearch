#ifndef RMUTIL_PERIODIC_H_
#define RMUTIL_PERIODIC_H_

#include <time.h>
#include <redismodule.h>
#include <pthread.h>
#include <errno.h>
#include <stdlib.h>

typedef void (*RMutilTimerFunc)(RedisModuleCtx *, void *);

struct RMUtilTimer;

struct RMUtilTimer *RMUtil_NewPeriodicTimer(RMutilTimerFunc cb, void *privdata,
                                            struct timespec interval);
int RMUtilTimer_Stop(struct RMUtilTimer *t);

void RMUtilTimer_Free(struct RMUtilTimer *t);
#endif