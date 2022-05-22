#include "periodic.h"
#include "rmalloc.h"
#include <pthread.h>
#include <stdlib.h>
#include <errno.h>

typedef struct RMUtilTimer {
  RMutilTimerFunc cb;
  RMUtilTimerTerminationFunc onTerm;
  void *privdata;
  struct timespec interval;
  pthread_t thread;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  volatile bool isCanceled;
} RMUtilTimer;

static struct timespec timespecAdd(struct timespec *a, struct timespec *b) {
  struct timespec ret;
  ret.tv_sec = a->tv_sec + b->tv_sec;

  long long ns = a->tv_nsec + b->tv_nsec;
  ret.tv_sec += ns / 1000000000;
  ret.tv_nsec = ns % 1000000000;
  return ret;
}

static void *rmutilTimer_Loop(void *ctx) {
  RMUtilTimer *tm = ctx;

  int rc = ETIMEDOUT;
  struct timespec ts;

  pthread_mutex_lock(&tm->lock);
  while (true) {
    clock_gettime(CLOCK_REALTIME, &ts);
    struct timespec timeout = timespecAdd(&ts, &tm->interval);
    rc = pthread_cond_timedwait(&tm->cond, &tm->lock, &timeout);
    if (rc == EINVAL) {
      perror("Error waiting for condition");
      break;
    }

    if (tm->isCanceled) {
      break;
    }

    // Create a thread safe context if we're running inside redis
    RedisModuleCtx *rctx = NULL;
    if (RedisModule_GetThreadSafeContext) rctx = RedisModule_GetThreadSafeContext(NULL);

    // call our callback...
    if (!tm->cb(rctx, tm->privdata)) {
      if (rctx) RedisModule_FreeThreadSafeContext(rctx);
      break;
    }

    // If needed - free the thread safe context.
    // It's up to the user to decide whether automemory is active there
    if (rctx) RedisModule_FreeThreadSafeContext(rctx);
  }

  // call the termination callback if needed
  if (tm->onTerm != NULL) {
    tm->onTerm(tm->privdata);
  }

  // free resources associated with the timer
  pthread_cond_destroy(&tm->cond);
  pthread_mutex_unlock(&tm->lock);
  rm_free(tm);

  return NULL;
}

/* set a new frequency for the timer. This will take effect AFTER the next trigger */
void RMUtilTimer_SetInterval(struct RMUtilTimer *t, struct timespec newInterval) {
  t->interval = newInterval;
}

RMUtilTimer *RMUtil_NewPeriodicTimer(RMutilTimerFunc cb, RMUtilTimerTerminationFunc onTerm,
                                     void *privdata, struct timespec interval) {
  RMUtilTimer *ret = rm_malloc(sizeof(*ret));
  *ret = (RMUtilTimer){
      .privdata = privdata,
      .interval = interval,
      .cb = cb,
      .onTerm = onTerm,
      .isCanceled = false,
  };
  pthread_cond_init(&ret->cond, NULL);
  pthread_mutex_init(&ret->lock, NULL);

  pthread_create(&ret->thread, NULL, rmutilTimer_Loop, ret);
  pthread_detach(ret->thread);
  return ret;
}

void RMUtilTimer_ForceInvoke(struct RMUtilTimer *t) {
  RMUtilTimer_Signal(t);
}

int RMUtilTimer_Signal(struct RMUtilTimer *t) {
  return pthread_cond_signal(&t->cond);
}

int RMUtilTimer_Terminate(struct RMUtilTimer *t) {
  t->isCanceled = true;
  return RMUtilTimer_Signal(t);
}
