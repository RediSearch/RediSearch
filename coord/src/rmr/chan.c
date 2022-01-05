#define MR_CHAN_C_
#include <pthread.h>
#include <sys/time.h>
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <assert.h>

void *MRCHANNEL_CLOSED = (void *)"MRCHANNEL_CLOSED";

typedef struct chanItem {
  void *ptr;
  struct chanItem *next;
} chanItem;

typedef struct MRChannel {
  chanItem *head;
  chanItem *tail;
  size_t size;
  size_t maxSize;
  volatile int open;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  // condition used to wait for closing
  pthread_cond_t closeCond;
} MRChannel;

#include "chan.h"

MRChannel *MR_NewChannel(size_t max) {
  MRChannel *chan = malloc(sizeof(*chan));
  *chan = (MRChannel){
      .head = NULL,
      .tail = NULL,
      .size = 0,
      .maxSize = max,
      .open = 1,
  };
  pthread_cond_init(&chan->cond, NULL);
  pthread_cond_init(&chan->closeCond, NULL);

  pthread_mutex_init(&chan->lock, NULL);
  return chan;
}

/* Safely wait until the channel is closed */
void MRChannel_WaitClose(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  while (chan->open) {
    pthread_cond_wait(&chan->closeCond, &chan->lock);
  }
  pthread_mutex_unlock(&chan->lock);
}

void MRChannel_Free(MRChannel *chan) {

  // TODO: proper drain and stop routine

  pthread_mutex_destroy(&chan->lock);
  pthread_cond_destroy(&chan->cond);
  free(chan);
}

size_t MRChannel_Size(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  size_t ret = chan->size;
  pthread_mutex_unlock(&chan->lock);
  return ret;
}

size_t MRChannel_MaxSize(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  size_t ret = chan->maxSize;
  pthread_mutex_unlock(&chan->lock);
  return ret;
}

int MRChannel_Push(MRChannel *chan, void *ptr) {

  pthread_mutex_lock(&chan->lock);
  int rc = 1;
  if (!chan->open || (chan->maxSize > 0 && chan->size == chan->maxSize)) {
    rc = 0;
    goto end;
  }

  chanItem *item = malloc(sizeof(*item));
  item->next = NULL;
  item->ptr = ptr;
  if (chan->tail) {
    // make it the next of the current tail
    chan->tail->next = item;
    // set a new tail
    chan->tail = item;
  } else {  // no tail means no head - empty queue
    chan->head = chan->tail = item;
  }
  chan->size++;
end:
  if (pthread_cond_broadcast(&chan->cond)) rc = 0;
  pthread_mutex_unlock(&chan->lock);
  return rc;
}

void *MRChannel_ForcePop(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  chanItem *item = chan->head;
  if(!item){
      pthread_mutex_unlock(&chan->lock);
      return NULL;
  }
  chan->head = item->next;
  // empty queue...
  if (!chan->head) chan->tail = NULL;
  chan->size--;
  pthread_mutex_unlock(&chan->lock);
  // discard the item (TODO: recycle items)
  void* ret = item->ptr;
  free(item);
  return ret;
}

// todo wait is not actually used anywhere...
void *MRChannel_Pop(MRChannel *chan) {
  void *ret = NULL;

  pthread_mutex_lock(&chan->lock);
  while (!chan->size) {
    if (!chan->open) {
      pthread_mutex_unlock(&chan->lock);
      return MRCHANNEL_CLOSED;
    }

    int rc = pthread_cond_wait(&chan->cond, &chan->lock);
    assert(rc == 0 && "cond_wait failed");
    if (!chan->size) {
      // otherwise, spurious wakeup
      printf("spurious cond_wait wakeup\n");
      // continue..
    }
  }

  chanItem *item = chan->head;
  assert(item);
  chan->head = item->next;
  // empty queue...
  if (!chan->head) chan->tail = NULL;
  chan->size--;
  pthread_mutex_unlock(&chan->lock);
  // discard the item (TODO: recycle items)
  ret = item->ptr;
  free(item);
  return ret;
}

void MRChannel_Close(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  chan->open = 0;
  // notify any waiting readers
  pthread_cond_broadcast(&chan->cond);
  pthread_cond_broadcast(&chan->closeCond);

  pthread_mutex_unlock(&chan->lock);
}
