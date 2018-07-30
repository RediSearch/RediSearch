#include "periodic.h"
#include "rmalloc.h"
#include <pthread.h>
#include <stdlib.h>
#include <errno.h>

typedef struct BlockClient {
  RedisModuleBlockedClient* bClient;
  struct BlockClient* next;
  struct BlockClient* prev;
}BlockClient;

typedef struct BlockClients {
  BlockClient* head;
  BlockClient* tail;
  pthread_mutex_t lock;
}BlockClients;

typedef struct RMUtilTimer {
  RMutilTimerFunc cb;
  RMUtilTimerTerminationFunc onTerm;
  void *privdata;
  struct timespec interval;
  pthread_t thread;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  volatile bool isCanceled;
  BlockClients bClients;
} RMUtilTimer;

static void BlockClients_push(BlockClients* ctx, RedisModuleBlockedClient *bClient){
  pthread_mutex_lock(&ctx->lock);
  BlockClient* bc = rm_calloc(1, sizeof(BlockClient));
  bc->bClient = bClient;

  if(ctx->head == NULL){
    ctx->head = ctx->tail = bc;
    pthread_mutex_unlock(&ctx->lock);
    return;
  }

  bc->next = ctx->head;
  ctx->head->prev = bc;
  ctx->head = bc;
  pthread_mutex_unlock(&ctx->lock);
}

static RedisModuleBlockedClient* BlockClients_pop(BlockClients* ctx){
  pthread_mutex_lock(&ctx->lock);
  BlockClient* bc = ctx->tail;
  if(!bc){
    pthread_mutex_unlock(&ctx->lock);
    return NULL;
  }

  ctx->tail = bc->prev;
  if(ctx->tail){
    ctx->tail->next = NULL;
  }else{
    ctx->head = NULL;
  }

  RedisModuleBlockedClient* ret = bc->bClient;
  rm_free(bc);

  pthread_mutex_unlock(&ctx->lock);
  return ret;
}

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

    RedisModuleBlockedClient* bClient = BlockClients_pop(&tm->bClients);
    if(bClient){
      RedisModule_UnblockClient(bClient, NULL);
    }
  }

  // call the termination callback if needed
  if (tm->onTerm != NULL) {
    tm->onTerm(tm->privdata);
  }

  // free resources associated with the timer
  pthread_cond_destroy(&tm->cond);
  pthread_mutex_unlock(&tm->lock);
  free(tm);

  return NULL;
}

/* set a new frequency for the timer. This will take effect AFTER the next trigger */
void RMUtilTimer_SetInterval(struct RMUtilTimer *t, struct timespec newInterval) {
  t->interval = newInterval;
}

RMUtilTimer *RMUtil_NewPeriodicTimer(RMutilTimerFunc cb, RMUtilTimerTerminationFunc onTerm,
                                     void *privdata, struct timespec interval) {
  RMUtilTimer *ret = malloc(sizeof(*ret));
  *ret = (RMUtilTimer){
      .privdata = privdata,
      .interval = interval,
      .cb = cb,
      .onTerm = onTerm,
      .isCanceled=false,
      .bClients = {0},
  };
  pthread_cond_init(&ret->cond, NULL);
  pthread_mutex_init(&ret->lock, NULL);
  pthread_mutex_init(&ret->bClients.lock, NULL);

  pthread_create(&ret->thread, NULL, rmutilTimer_Loop, ret);
  return ret;
}

void RMUtilTimer_ForceInvoke(struct RMUtilTimer *t, RedisModuleBlockedClient *bClient){
  BlockClients_push(&t->bClients, bClient);
  RMUtilTimer_Signal(t);
}

int RMUtilTimer_Signal(struct RMUtilTimer *t){
  return pthread_cond_signal(&t->cond);
}

int RMUtilTimer_Terminate(struct RMUtilTimer *t) {
  t->isCanceled = true;
  return RMUtilTimer_Signal(t);
}
