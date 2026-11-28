/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "hybrid_executor.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

#include <pthread.h>
#include <stdbool.h>

typedef struct {
  pthread_mutex_t lock;
  size_t next;
  size_t children;
  size_t remaining;
  size_t tickets;
  void (*runChild)(void *, size_t);
  void (*finish)(void *);
  void *ctx;
} HybridTickets;

static void releaseTicket(HybridTickets *group) {
  pthread_mutex_lock(&group->lock);
  const bool last = --group->tickets == 0;
  pthread_mutex_unlock(&group->lock);
  if (last) {
    pthread_mutex_destroy(&group->lock);
    rm_free(group);
  }
}

static void runClaimedChild(HybridTickets *group, size_t child) {
  for (;;) {
    group->runChild(group->ctx, child);

    pthread_mutex_lock(&group->lock);
    if (--group->remaining == 0) {
      void *ctx = group->ctx;
      group->ctx = NULL;
      pthread_mutex_unlock(&group->lock);
      group->finish(ctx);
      return;
    }
    if (group->next == group->children) {
      pthread_mutex_unlock(&group->lock);
      return;
    }
    child = group->next++;
    pthread_mutex_unlock(&group->lock);
  }
}

static void runTicket(void *data) {
  HybridTickets *group = data;
  pthread_mutex_lock(&group->lock);
  if (group->next < group->children) {
    const size_t child = group->next++;
    pthread_mutex_unlock(&group->lock);
    runClaimedChild(group, child);
  } else {
    pthread_mutex_unlock(&group->lock);
  }
  releaseTicket(group);
}

void HybridExecutor_Run(size_t children, size_t runners, HybridSubmitTicket submit, void *pool,
                        void (*runChild)(void *, size_t), void (*finish)(void *), void *ctx) {
  RS_ASSERT(children > 0 && runners > 0);
  if (runners > children) runners = children;
  if (runners == 1) {
    for (size_t i = 0; i < children; ++i) runChild(ctx, i);
    finish(ctx);
    return;
  }

  // Reserve child zero for the caller so the request survives until submission is over.
  HybridTickets *group = rm_new(HybridTickets);
  *group = (HybridTickets){.next = 1,
                           .children = children,
                           .remaining = children,
                           .tickets = runners,
                           .runChild = runChild,
                           .finish = finish,
                           .ctx = ctx};
  int rc = pthread_mutex_init(&group->lock, NULL);
  RS_ASSERT_ALWAYS(rc == 0);
  for (size_t i = 1; i < runners; ++i) {
    if (submit(pool, runTicket, group) != 0) releaseTicket(group);
  }
  runClaimedChild(group, 0);
  releaseTicket(group);
}
