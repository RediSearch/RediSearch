#include <time.h>
#include <signal.h>
#include <redismodule.h>
#include <pthread.h>
#include <errno.h>
#include <stdio.h>
#include <unistd.h>

typedef void (*RMutilTimerFunc)(RedisModuleCtx *, void *);

typedef struct {
  RMutilTimerFunc cb;
  RedisModuleCtx *redisCtx;
  void *privdata;
  struct timespec interval;
  pthread_t thread;
  pthread_mutex_t lock;
  pthread_cond_t cond;
} RMUtilTimer;

void timespecAdd(struct timespec *dst, struct timespec *other) {
  dst->tv_sec += other->tv_sec;

  long long ns = dst->tv_nsec + other->tv_nsec;
  dst->tv_sec += ns / 100000000;
  dst->tv_nsec = ns % 1000000;
}

static void *rmutilTimer_Loop(void *ctx) {
  RMUtilTimer *tm = ctx;

  int rc = ETIMEDOUT;
  while (rc == ETIMEDOUT) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    timespecAdd(&ts, &tm->interval);

    if ((rc = pthread_cond_timedwait(&tm->cond, &tm->lock, &ts)) == ETIMEDOUT) {
      tm->cb(tm->redisCtx, tm->privdata);
    }
  }
  printf("Timer cancelled\n");

  return NULL;
}

RMUtilTimer *RMUtil_NewPeriodicTimer(RMutilTimerFunc cb, void *privdata, struct timespec interval) {
  RMUtilTimer *ret = RedisModule_Alloc(sizeof(*ret));
  *ret = (RMUtilTimer){
      .privdata = privdata,
      .redisCtx = RedisModule_GetThreadSafeContext(NULL),
      .interval = interval,
      .cb = cb,
  };
  pthread_cond_init(&ret->cond, NULL);
  pthread_mutex_init(&ret->lock, NULL);

  pthread_create(&ret->thread, NULL, rmutilTimer_Loop, ret);
  return ret;
}

int RMUtilTimer_Stop(RMUtilTimer *t) {
  int rc;
  if (0 != (rc = pthread_cond_signal(&t->cond))) {
    return rc;
  }
  if (0 != (rc = pthread_join(t->thread, NULL))) {
    return rc;
  }
  printf("Stopped timer!\n");
  return 0;
}

void RMUtilTimer_Free(RMUtilTimer *t) {
}

void timerCb(RedisModuleCtx *ctx, void *p) {
  // int *x = p;
  printf("!\n");
}
int main(int argc, char **argv) {
  int x = 0;
  RMUtilTimer *tm =
      RMUtil_NewPeriodicTimer(timerCb, &x, (struct timespec){.tv_sec = 0, .tv_nsec = 1000000});

  sleep(5);
  printf("Done! %d\n", x);
  return 0;
}