/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stdlib.h>
#include <stddef.h>
#include <uv.h>

typedef void (*MRQueueCallback)(void *);

typedef struct queueItem {
  void *privdata;
  MRQueueCallback cb;
  struct queueItem *next;
} queueItem;

typedef struct MRWorkQueue {
  size_t id;
  queueItem *head;
  queueItem *tail;
  int pending;
  int maxPending;
  size_t sz;
  struct {
    queueItem *head;
    size_t warnSize;
  } pendingInfo;
  uv_mutex_t lock;
} MRWorkQueue;

MRWorkQueue *RQ_New(int maxPending, size_t id);

void RQ_Free(MRWorkQueue *q);

void RQ_UpdateMaxPending(MRWorkQueue *q, int maxPending);

void RQ_Done(MRWorkQueue *q);

void RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata);

queueItem *RQ_Pop(MRWorkQueue *q, uv_async_t* async);
