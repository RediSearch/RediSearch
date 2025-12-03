/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <pthread.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <time.h>

typedef struct chanItem {
  void *ptr;
  struct chanItem *next;
} chanItem;

struct MRChannel {
  chanItem *head;
  chanItem *tail;
  size_t size;
  volatile bool wait;
  pthread_mutex_t lock;
  pthread_cond_t cond;
};

#include "chan.h"
#include "rmalloc.h"
#include "util/timeout.h"

MRChannel *MR_NewChannel() {
  MRChannel *chan = rm_malloc(sizeof(*chan));
  *chan = (MRChannel){
      .head = NULL,
      .tail = NULL,
      .size = 0,
      .wait = true,
  };
  pthread_cond_init(&chan->cond, NULL);
  pthread_mutex_init(&chan->lock, NULL);
  return chan;
}

void MRChannel_Free(MRChannel *chan) {
  pthread_mutex_destroy(&chan->lock);
  pthread_cond_destroy(&chan->cond);
  rm_free(chan);
}

size_t MRChannel_Size(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  size_t ret = chan->size;
  pthread_mutex_unlock(&chan->lock);
  return ret;
}

void MRChannel_Push(MRChannel *chan, void *ptr) {
  chanItem *item = rm_malloc(sizeof(*item));
  item->next = NULL;
  item->ptr = ptr;
  pthread_mutex_lock(&chan->lock);
  if (chan->tail) {
    // make it the next of the current tail
    chan->tail->next = item;
    // set a new tail
    chan->tail = item;
  } else {  // no tail means no head - empty queue
    chan->head = chan->tail = item;
  }
  chan->size++;
  pthread_cond_broadcast(&chan->cond);
  pthread_mutex_unlock(&chan->lock);
}

void *MRChannel_UnsafeForcePop(MRChannel *chan) {
  chanItem *item = chan->head;
  if (!item) {
    return NULL;
  }
  chan->head = item->next;
  // empty queue...
  if (!chan->head) chan->tail = NULL;
  chan->size--;
  // discard the item (TODO: recycle items)
  void* ret = item->ptr;
  rm_free(item);
  return ret;
}

void *MRChannel_Pop(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  while (!chan->size) {
    if (!chan->wait) {
      chan->wait = true;  // reset the flag
      pthread_mutex_unlock(&chan->lock);
      return NULL;
    }
    pthread_cond_wait(&chan->cond, &chan->lock);
  }

  chanItem *item = chan->head;
  chan->head = item->next;
  // empty queue...
  if (!chan->head) chan->tail = NULL;
  chan->size--;
  pthread_mutex_unlock(&chan->lock);
  // discard the item (TODO: recycle items)
  void *ret = item->ptr;
  rm_free(item);
  return ret;
}

// Calculate remaining time until a CLOCK_MONOTONIC deadline
// Returns: true if time remains (stored in *remaining), false if deadline passed
static bool getRemainingTime(const struct timespec *abstimeMono, struct timespec *remaining) {
  struct timespec nowMono;
  clock_gettime(CLOCK_MONOTONIC, &nowMono);
  rs_timerdelta((struct timespec *)abstimeMono, &nowMono, remaining);
  return remaining->tv_sec != 0 || remaining->tv_nsec != 0;
}

// Convert a relative duration to platform-specific format for pthread_cond_timedwait
// macOS: returns relative duration (for pthread_cond_timedwait_relative_np)
// Linux: returns absolute CLOCK_REALTIME (for pthread_cond_timedwait)
static void setCondTimeout(const struct timespec *remaining, struct timespec *out) {
#if defined(__APPLE__) && defined(__MACH__)
  *out = *remaining;
#else
  struct timespec nowReal;
  clock_gettime(CLOCK_REALTIME, &nowReal);
  out->tv_sec = nowReal.tv_sec + remaining->tv_sec;
  out->tv_nsec = nowReal.tv_nsec + remaining->tv_nsec;
  if (out->tv_nsec >= 1000000000) {
    out->tv_sec += 1;
    out->tv_nsec -= 1000000000;
  }
#endif
}

// Platform-specific timed wait on condition variable
static int condTimedWait(pthread_cond_t *cond, pthread_mutex_t *lock, struct timespec *timeout) {
#if defined(__APPLE__) && defined(__MACH__)
  return pthread_cond_timedwait_relative_np(cond, lock, timeout);
#else
  return pthread_cond_timedwait(cond, lock, timeout);
#endif
}

void *MRChannel_PopWithTimeout(MRChannel *chan, const struct timespec *abstimeMono, bool *timedOut) {
  *timedOut = false;

  // If no timeout specified, behave like regular Pop
  if (!abstimeMono) {
    return MRChannel_Pop(chan);
  }

  pthread_mutex_lock(&chan->lock);
  while (!chan->size) {
    if (!chan->wait) {
      chan->wait = true;  // reset the flag
      pthread_mutex_unlock(&chan->lock);
      return NULL;
    }

    struct timespec remaining, timeout;
    if (!getRemainingTime(abstimeMono, &remaining)) {
      *timedOut = true;
      pthread_mutex_unlock(&chan->lock);
      return NULL;
    }
    setCondTimeout(&remaining, &timeout);

    if (condTimedWait(&chan->cond, &chan->lock, &timeout) == ETIMEDOUT) {
      *timedOut = true;
      pthread_mutex_unlock(&chan->lock);
      return NULL;
    }
  }

  chanItem *item = chan->head;
  chan->head = item->next;
  // empty queue...
  if (!chan->head) chan->tail = NULL;
  chan->size--;
  pthread_mutex_unlock(&chan->lock);
  // discard the item (TODO: recycle items)
  void *ret = item->ptr;
  rm_free(item);
  return ret;
}

void MRChannel_Unblock(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  chan->wait = false;
  // unblock any waiting readers
  pthread_cond_signal(&chan->cond);
  pthread_mutex_unlock(&chan->lock);
}
