/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
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
#include "search_ctx.h"
#include "util/timeout.h"

// Note: pthread_condattr_setclock only supports CLOCK_MONOTONIC (not CLOCK_MONOTONIC_RAW)
// The timeout parameter (abstimeMono) is in CLOCK_MONOTONIC_RAW, so we convert it
// to CLOCK_MONOTONIC in condTimedWait()

MRChannel *MR_NewChannel() {
  MRChannel *chan = rm_malloc(sizeof(*chan));
  *chan = (MRChannel){
      .head = NULL,
      .tail = NULL,
      .size = 0,
      .wait = true,
  };
#if defined(__APPLE__) && defined(__MACH__)
  // macOS doesn't support pthread_condattr_setclock, use default clock
  pthread_cond_init(&chan->cond, NULL);
#else
  // Initialize with CLOCK_MONOTONIC for use with pthread_cond_timedwait
  pthread_condattr_t cond_attr;
  pthread_condattr_init(&cond_attr);
  pthread_condattr_setclock(&cond_attr, CLOCK_MONOTONIC);
  pthread_cond_init(&chan->cond, &cond_attr);
  pthread_condattr_destroy(&cond_attr);
#endif
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

// Must be called with chan->lock held and chan->size > 0
// Releases chan->lock before returning
static void *popHeadAndUnlock(MRChannel *chan) {
  chanItem *item = chan->head;
  chan->head = item->next;
  if (!chan->head) chan->tail = NULL;
  chan->size--;
  pthread_mutex_unlock(&chan->lock);
  void *ret = item->ptr;
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

  return popHeadAndUnlock(chan);
}

// Platform-specific timed wait on condition variable
// Returns: true if timed out, false if signaled (or spurious wakeup)
// abstimeMono is an absolute time in CLOCK_MONOTONIC_RAW
// macOS: uses pthread_cond_timedwait_relative_np with relative timeout
// Linux/FreeBSD: converts to CLOCK_MONOTONIC for pthread_cond_timedwait
static bool condTimedWait(pthread_cond_t *cond, pthread_mutex_t *lock,
                          const struct timespec *abstimeMono) {
  // Calculate remaining time from CLOCK_MONOTONIC_RAW
  struct timespec nowRaw, remaining;
  clock_gettime(CLOCK_MONOTONIC_RAW, &nowRaw);
  rs_timerremaining((struct timespec *)abstimeMono, &nowRaw, &remaining);
  // Check if already past deadline
  if (remaining.tv_sec == 0 && remaining.tv_nsec == 0) {
    return true;  // timed out
  }
#if defined(__APPLE__) && defined(__MACH__)
  return pthread_cond_timedwait_relative_np(cond, lock, &remaining) == ETIMEDOUT;
#else
  // Convert to CLOCK_MONOTONIC absolute time for the condition variable
  struct timespec nowMono, absMono;
  clock_gettime(CLOCK_MONOTONIC, &nowMono);
  rs_timeradd(&nowMono, &remaining, &absMono);
  return pthread_cond_timedwait(cond, lock, &absMono) == ETIMEDOUT;
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

    if (condTimedWait(&chan->cond, &chan->lock, abstimeMono)) {
      *timedOut = true;
      pthread_mutex_unlock(&chan->lock);
      return NULL;
    }
  }

  return popHeadAndUnlock(chan);
}

void MRChannel_Unblock(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  chan->wait = false;
  // unblock any waiting readers
  pthread_cond_signal(&chan->cond);
  pthread_mutex_unlock(&chan->lock);
}
