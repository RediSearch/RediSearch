#ifndef RULES_Q_H
#define RULES_Q_H

#include <pthread.h>
#include "redismodule.h"
#include "spec.h"
#include "rules.h"
#include "util/minmax_heap.h"
#include "rmutil/sds.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct IndexQueueItem {
  struct IndexQueueItem *next;
  sds key;
  IndexItemAttrs *attrs;
} IndexQueueItem;

typedef struct IndexQueue {
  IndexSpec *spec;
  IndexQueueItem *head;
  IndexQueueItem *tail;
  size_t count;
} IndexQueue;

IndexQueue *IndexQueue_New();
void IndexQueue_Free(IndexQueue *queue);

// Add an item to the queue
int IndexQueue_Add(IndexQueue *queue, const char *key, size_t nkey, const IndexItemAttrs *attrs);

#ifdef __cplusplus
}
#endif
#endif