#ifndef RMUTIL_PERIODIC_H_
#define RMUTIL_PERIODIC_H_
#include <time.h>
#include <redismodule.h>

/** periodic.h - Utility periodic timer running a task repeatedly every given time interval */

/* RMUtilTimer - opaque context for the timer */
struct RMUtilTimer;

/* RMutilTimerFunc - callback type for timer tasks. The ctx is a thread-safe redis module context
 * that should be locked/unlocked by the callback when running stuff against redis. privdata is
 * pre-existing private data */
typedef void (*RMutilTimerFunc)(RedisModuleCtx *ctx, void *privdata);

/* Create and start a new periodic timer. Each timer has its own thread and can only be run and
 * stopped once. The timer runs `cb` every `interval` with `privdata` passed to the callback. */
struct RMUtilTimer *RMUtil_NewPeriodicTimer(RMutilTimerFunc cb, void *privdata,
                                            struct timespec interval);

/* Stop the timer loop. This should return immediately and join the thread */
int RMUtilTimer_Stop(struct RMUtilTimer *t);

/* Free the timer context. The caller should be responsible for freeing the private data at this
 * point */
void RMUtilTimer_Free(struct RMUtilTimer *t);
#endif