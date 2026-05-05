/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "spec_snapshot_service.h"

#include "rmalloc.h"
#include "rmutil/rm_assert.h"

#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdatomic.h>

struct SpecSnapshotServiceRequest {
  RedisSearchCtx managerSctx;
  sem_t stateSem;
  bool lockAcquired;
  _Atomic(uint32_t) remainingSignals;
  _Atomic(bool) completionQueued;
  struct SpecSnapshotServiceRequest *next;
  struct SpecSnapshotServiceRequest *activeNext;
};

typedef struct {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  SpecSnapshotServiceRequest *pendingHead;
  SpecSnapshotServiceRequest *pendingTail;
  SpecSnapshotServiceRequest *completedHead;
  SpecSnapshotServiceRequest *completedTail;
  SpecSnapshotServiceRequest *activeHead;
} SpecSnapshotService;

static SpecSnapshotService specSnapshotService = {0};
static pthread_once_t specSnapshotServiceInitOnce = PTHREAD_ONCE_INIT;

static int WaitOnSemaphore(sem_t *sem) {
  int rc;
  do {
    rc = sem_wait(sem);
  } while (rc == -1 && errno == EINTR);
  return rc;
}

static inline void EnqueuePendingRequest(SpecSnapshotServiceRequest *req) {
  req->next = NULL;
  if (!specSnapshotService.pendingHead) {
    specSnapshotService.pendingHead = req;
    specSnapshotService.pendingTail = req;
  } else {
    specSnapshotService.pendingTail->next = req;
    specSnapshotService.pendingTail = req;
  }
}

static inline void EnqueueCompletedRequest(SpecSnapshotServiceRequest *req) {
  req->next = NULL;
  if (!specSnapshotService.completedHead) {
    specSnapshotService.completedHead = req;
    specSnapshotService.completedTail = req;
  } else {
    specSnapshotService.completedTail->next = req;
    specSnapshotService.completedTail = req;
  }
}

static inline void AddActiveRequest(SpecSnapshotServiceRequest *req) {
  pthread_mutex_lock(&specSnapshotService.mutex);
  req->activeNext = specSnapshotService.activeHead;
  specSnapshotService.activeHead = req;
  pthread_mutex_unlock(&specSnapshotService.mutex);
}

static inline void RemoveActiveRequest(SpecSnapshotServiceRequest *req) {
  pthread_mutex_lock(&specSnapshotService.mutex);
  SpecSnapshotServiceRequest **prev = &specSnapshotService.activeHead;
  while (*prev && *prev != req) {
    prev = &(*prev)->activeNext;
  }
  if (*prev == req) {
    *prev = req->activeNext;
  }
  pthread_mutex_unlock(&specSnapshotService.mutex);
}

static void *SpecSnapshotService_Run(void *arg) {
  (void)arg;
  while (true) {
    pthread_mutex_lock(&specSnapshotService.mutex);
    while (!specSnapshotService.pendingHead && !specSnapshotService.completedHead) {
      pthread_cond_wait(&specSnapshotService.cond, &specSnapshotService.mutex);
    }
    SpecSnapshotServiceRequest *pending = specSnapshotService.pendingHead;
    SpecSnapshotServiceRequest *completed = specSnapshotService.completedHead;
    specSnapshotService.pendingHead = NULL;
    specSnapshotService.pendingTail = NULL;
    specSnapshotService.completedHead = NULL;
    specSnapshotService.completedTail = NULL;
    pthread_mutex_unlock(&specSnapshotService.mutex);

    while (completed) {
      SpecSnapshotServiceRequest *req = completed;
      completed = completed->next;
      RemoveActiveRequest(req);
      RedisSearchCtx_UnlockSpec(&req->managerSctx);
      rm_free(req);
    }

    while (pending) {
      SpecSnapshotServiceRequest *req = pending;
      pending = pending->next;

      int rc = RedisSearchCtx_TryLockSpecRead(&req->managerSctx);
      req->lockAcquired = (rc == REDISMODULE_OK);
      if (req->lockAcquired) {
        AddActiveRequest(req);
      }
      RS_ASSERT_ALWAYS(sem_post(&req->stateSem) == 0);
    }
  }
  return NULL;
}

static void SpecSnapshotService_InitOnce(void) {
  pthread_mutex_init(&specSnapshotService.mutex, NULL);
  pthread_cond_init(&specSnapshotService.cond, NULL);

  pthread_t tid;
  int rc = pthread_create(&tid, NULL, SpecSnapshotService_Run, NULL);
  RS_ASSERT_ALWAYS(rc == 0);
  pthread_detach(tid);
}

SpecSnapshotServiceRequest *SpecSnapshotService_Request(RedisSearchCtx *searchCtx, uint32_t expectedSignals) {
  pthread_once(&specSnapshotServiceInitOnce, SpecSnapshotService_InitOnce);
  RS_ASSERT(expectedSignals > 0);

  SpecSnapshotServiceRequest *req = rm_calloc(1, sizeof(*req));
  req->managerSctx = SEARCH_CTX_STATIC(searchCtx->redisCtx, searchCtx->spec);
  atomic_store(&req->remainingSignals, expectedSignals);
  atomic_store(&req->completionQueued, false);
  RS_ASSERT_ALWAYS(sem_init(&req->stateSem, 0, 0) == 0);

  pthread_mutex_lock(&specSnapshotService.mutex);
  EnqueuePendingRequest(req);
  pthread_cond_signal(&specSnapshotService.cond);
  pthread_mutex_unlock(&specSnapshotService.mutex);

  RS_ASSERT_ALWAYS(WaitOnSemaphore(&req->stateSem) == 0);
  sem_destroy(&req->stateSem);
  if (!req->lockAcquired) {
    rm_free(req);
    return NULL;
  }
  return req;
}

void SpecSnapshotService_Signal(SpecSnapshotServiceRequest *request) {
  if (!request) {
    return;
  }

  uint32_t prev = atomic_fetch_sub_explicit(&request->remainingSignals, 1, memory_order_acq_rel);
  RS_ASSERT(prev > 0);
  if (prev == 1) {
    bool expected = false;
    if (atomic_compare_exchange_strong_explicit(&request->completionQueued,
                                                &expected,
                                                true,
                                                memory_order_acq_rel,
                                                memory_order_relaxed)) {
      pthread_mutex_lock(&specSnapshotService.mutex);
      EnqueueCompletedRequest(request);
      pthread_cond_signal(&specSnapshotService.cond);
      pthread_mutex_unlock(&specSnapshotService.mutex);
    }
  }
}
