/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <pthread.h>
#include <stdlib.h>
#include <stdbool.h>

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

void MRChannel_Unblock(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  chan->wait = false;
  // unblock any waiting readers
  pthread_cond_broadcast(&chan->cond);
  pthread_mutex_unlock(&chan->lock);
}

void MRChannel_Block(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  chan->wait = true;
  pthread_mutex_unlock(&chan->lock);
}
