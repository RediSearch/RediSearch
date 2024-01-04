/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef RMUTIL_PERIODIC_H_
#define RMUTIL_PERIODIC_H_
#include <time.h>
#include <redismodule.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/** periodic.h - Utility periodic timer running a task repeatedly every given time interval */

/* RMUtilTimer - opaque context for the timer */
struct RMUtilTimer;

/* RMutilTimerFunc - callback type for timer tasks. The ctx is a thread-safe redis module context
 * that should be locked/unlocked by the callback when running stuff against redis. privdata is
 * pre-existing private data */
typedef int (*RMutilTimerFunc)(RedisModuleCtx *ctx, void *privdata);

typedef void (*RMUtilTimerTerminationFunc)(void *privdata);

/* Create and start a new periodic timer. Each timer has its own thread and can only be run and
 * stopped once. The timer runs `cb` every `interval` with `privdata` passed to the callback. */
struct RMUtilTimer *RMUtil_NewPeriodicTimer(RMutilTimerFunc cb, RMUtilTimerTerminationFunc onTerm,
                                            void *privdata, struct timespec interval);

/* set a new frequency for the timer. This will take effect AFTER the next trigger */
void RMUtilTimer_SetInterval(struct RMUtilTimer *t, struct timespec newInterval);

/* Stop the timer loop, call the termination callbck to free up any resources linked to the timer,
 * and free the timer after stopping.
 *
 * This function doesn't wait for the thread to terminate, as it may cause a race condition if the
 * timer's callback is waiting for the redis global lock.
 * Instead you should make sure any resources are freed by the callback after the thread loop is
 * finished.
 *
 * The timer is freed automatically, so the callback doesn't need to do anything about it.
 * The callback gets the timer's associated privdata as its argument.
 *
 * If no callback is specified we do not free up privdata. If privdata is NULL we still call the
 * callback, as it may log stuff or free global resources.
 */
int RMUtilTimer_Terminate(struct RMUtilTimer *t);

/*
 * This function cause the callback function to execute with out waiting for timetout.
 * It does it by sending a signal to the conditional variable causing the pthread_cond_timedwait
 * to return. It receive a blocked client which added to the blocks client chain.
 * After finish executing the cb the block client is unblocked.
 *
 * We use this to allow running gc with ft.debug FORCE_GCINVOKE and receive a 'done' reply when
 * the gc finished its run.
 */
void RMUtilTimer_ForceInvoke(struct RMUtilTimer *t);

int RMUtilTimer_Signal(struct RMUtilTimer *t);

/* DEPRECATED - do not use this function (well now you can't), use terminate instead
    Free the timer context. The caller should be responsible for freeing the private data at this
 * point */
// void RMUtilTimer_Free(struct RMUtilTimer *t);

#ifdef __cplusplus
}
#endif

#endif
