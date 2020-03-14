#include "util/arr.h"
#include "rules.h"
#include "module.h"
#include <sys/time.h>

static void *aiThreadInit(void *privdata);

AsyncIndexQueue *AIQ_Create(size_t interval, size_t batchSize) {
  AsyncIndexQueue *q = rm_calloc(1, sizeof(*q));

  q->interval = interval;
  q->indexBatchSize = batchSize;
  q->pending = array_new(SpecDocQueue *, 8);
  pthread_mutex_init(&q->lock, NULL);
  pthread_cond_init(&q->cond, NULL);
  // didn't find pthread.c when in function AsyncIndexCtx_Create
  pthread_create(&q->aiThread, NULL, aiThreadInit, q);
  pthread_detach(q->aiThread);
  return q;
}

void AIQ_Destroy(AsyncIndexQueue *aq) {
  aq->state = AIQ_S_CANCELLED;
  pthread_cond_signal(&aq->cond);
  pthread_join(aq->aiThread, NULL);
  pthread_mutex_destroy(&aq->lock);
  pthread_cond_destroy(&aq->cond);
  array_free(aq->pending);
  rm_free(aq);
}

void AIQ_Submit(AsyncIndexQueue *aq, IndexSpec *spec, MatchAction *result, RuleKeyItem *item) {
  // submit to queue
  // 1. Create a queue per index
  // 2. Add `item` to queue
  // Somewhere else
  // 3. As a callback, index items into the index
  // 4. Remove items from queue

  // Queue indexes with documents to be indexed
  // Queue documents to be indexed for each index
  // Schedule

  // index points to a retained ptr at SchemaRules_g
  RuleIndexableDocument *rid = rm_calloc(1, sizeof(*rid));
  rid->kstr = item->kstr;
  rid->iia = result->attrs;
  RedisModule_RetainString(NULL, rid->kstr);
  SpecDocQueue *dq = spec->queue;
  if (!dq) {
    dq = SpecDocQueue_Create(spec);
  }

  pthread_mutex_lock(&aq->lock);
  int rv = dictAdd(dq->entries, rid->kstr, rid);
  if (rv != DICT_OK) {
    // item already exists?
    pthread_mutex_unlock(&aq->lock);
    rm_free(rid);
    RedisModule_FreeString(NULL, item->kstr);
    return;
  }
  int flags = dq->state;
  size_t nqueued = dictSize(dq->entries);
  if ((flags & (SDQ_S_PENDING | SDQ_S_PROCESSING)) == 0) {
    printf("adding to list of pending indexes...\n");
    // the pending flag isn't set yet, and we aren't processing either,
    // go ahead and add this to the pending array
    aq->pending = array_append(aq->pending, dq);
    dq->state |= SDQ_S_PENDING;
  }
  pthread_mutex_unlock(&aq->lock);
  if ((flags & SDQ_S_PROCESSING) == 0 && nqueued >= aq->indexBatchSize) {
    printf("signalling thread..\n");
    pthread_cond_signal(&aq->cond);
  }
}

static void scanCallback(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *keyobj,
                         void *privdata) {
  // body here should be similar to keyspace notification callback, except that
  // async is always forced
  printf("hi: %s\n", RedisModule_StringPtrLen(keyname, NULL));
  RuleKeyItem item = {.kstr = keyname, .kobj = keyobj};
  SchemaRules_ProcessItem(ctx, &item, 1);
}

static void scanRedis6(void) {
  printf("r6 scan..\n");
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
  while (RedisModule_Scan(RSDummyContext, cursor, scanCallback, NULL)) {
    // no body
  }
  RedisModule_ScanCursorDestroy(cursor);
}

static void scanRedis5(void) {
  printf("r5 scan..\n");
  char cursorbuf[1024] = {'0', 0};
  RedisModuleCtx *ctx = RSDummyContext;
  do {
    // RedisModuleCallReply *r =
    //     RedisModule_Call(ctx, "SCAN", "lcccl", cursor, "TYPE", "hash", "COUNT", 100);
    // printf("cursor: %s\n", cursorbuf);
    RedisModuleCallReply *r = RedisModule_Call(ctx, "SCAN", "c", cursorbuf);

    assert(r != NULL);
    assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ARRAY);

    if (RedisModule_CallReplyLength(r) < 2) {
      RedisModule_FreeCallReply(r);
      break;
    }

    // cursor is the first element
    size_t ncursor = 0;
    const char *newcur =
        RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(r, 0), &ncursor);
    memcpy(cursorbuf, newcur, ncursor);
    cursorbuf[ncursor] = 0;

    RedisModuleCallReply *keys = RedisModule_CallReplyArrayElement(r, 1);
    assert(RedisModule_CallReplyType(keys) == REDISMODULE_REPLY_ARRAY);
    size_t nelem = RedisModule_CallReplyLength(keys);

    for (size_t ii = 0; ii < nelem; ++ii) {
      size_t len;
      const char *kcstr =
          RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(keys, ii), &len);
      RuleKeyItem rki = {.kstr = RedisModule_CreateString(NULL, kcstr, len)};
      SchemaRules_ProcessItem(ctx, &rki, 1);
      RedisModule_FreeString(ctx, rki.kstr);
    }
    RedisModule_FreeCallReply(r);
  } while (cursorbuf[0] != '0');
}

void SchemaRules_ScanAll(const SchemaRules *rules) {
  if (RedisModule_Scan) {
    scanRedis6();
  } else {
    scanRedis5();
  }
}

int SchemaRules_ScanAllCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // no arguments
  SchemaRules_ScanAll(SchemaRules_g);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int SchemaRules_QueueInfoCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  const char *idxname = RedisModule_StringPtrLen(argv[1], NULL);
  IndexSpec *sp = IndexSpec_Load(ctx, idxname, 0);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "No such index");
  }
  if (!(sp->flags & Index_UseRules)) {
    return RedisModule_ReplyWithError(
        ctx, "This command can only be used on indexes created using `WITHRULES`");
  }
  ssize_t ret = SchemaRules_GetPendingCount(sp);
  return RedisModule_ReplyWithLongLong(ctx, ret);
}

static void ms2ts(struct timespec *ts, unsigned long ms) {
  ts->tv_sec = ms / 1000;
  ts->tv_nsec = (ms % 1000) * 1000000;
}

static void indexBatch(AsyncIndexQueue *aiq, SpecDocQueue *dq, dict *entries) {
  printf("indexBatch!\n");
  IndexSpec *spec = dq->spec;
  dictIterator *iter = dictGetIterator(entries);
  dictEntry *e = NULL;
  while ((e = dictNext(iter))) {
    RuleIndexableDocument *rid = e->v.val;
    QueryError err = {0};
    RuleKeyItem rki = {.kstr = rid->kstr};
    RedisModule_ThreadSafeContextLock(RSDummyContext);
    printf("Indexing..\n");
    int rc = SchemaRules_IndexDocument(RSDummyContext, spec, &rki, &rid->iia, &err);
    RedisModule_FreeString(NULL, rid->kstr);
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    // deal with Key when applicable
    rm_free(rid);
    assert(rc == REDISMODULE_OK);
  }
  dictReleaseIterator(iter);

  // now that we're done, lock the dq and see if we need to place it back into
  // pending again:
  pthread_mutex_lock(&aiq->lock);
  dq->state &= ~SDQ_S_PROCESSING;
  if (dictSize(dq->entries)) {
    dq->state = SDQ_S_PENDING;
    aiq->pending = array_append(aiq->pending, dq);
  }
  dq->nactive = 0;
  pthread_mutex_unlock(&aiq->lock);
}

static int sortPending(const void *a, const void *b) {
  const SpecDocQueue *qa = a, *qb = b;
  return dictSize(qa->entries) - dictSize(qb->entries);
}

static void *aiThreadInit(void *privdata) {
  AsyncIndexQueue *q = privdata;
  struct timespec base_ts = {0};
  ms2ts(&base_ts, q->interval);
  dict *newdict = dictCreate(&dictTypeHeapRedisStrings, NULL);

  while (1) {
    /**
     * Wait until the specified interval expires, OR when we are signalled with
     * a given amount of items.
     */
    if (q->state == AIQ_S_CANCELLED) {
      break;
    }
    pthread_mutex_lock(&q->lock);
    size_t nq;

    while ((nq = array_len(q->pending)) == 0) {
      struct timespec ts = {0};
      struct timeval tv;
      gettimeofday(&tv, NULL);
      ts.tv_sec = tv.tv_sec + base_ts.tv_sec;
      ts.tv_nsec = (tv.tv_usec * 1000) + base_ts.tv_nsec;
      int rv = pthread_cond_timedwait(&q->cond, &q->lock, &ts);
      assert(rv != EINVAL);
      // todo: cancel/exit scenarios?
    }

    // sort in ascending order. The queue with the fewest items comes first.
    // this makes it easier to delete items when done.
    qsort(q->pending, nq, sizeof(*q->pending), sortPending);
    SpecDocQueue *dq = q->pending[nq - 1];
    array_del_fast(q->pending, nq - 1);
    dict *oldEntries = dq->entries;
    dq->entries = newdict;
    dq->nactive = dictSize(oldEntries);
    dq->state = SDQ_S_PROCESSING;

    newdict = NULL;
    pthread_mutex_unlock(&q->lock);
    indexBatch(q, dq, oldEntries);

    if (!newdict) {
      newdict = dictCreate(&dictTypeHeapRedisStrings, NULL);
    }
  }
  if (newdict) {
    dictRelease(newdict);
  }
  return NULL;
}

ssize_t SchemaRules_GetPendingCount(const IndexSpec *spec) {
  if (!spec->queue) {
    printf("No queue. returning -1\n");
    return -1;
  }
  SpecDocQueue *dq = spec->queue;
  AsyncIndexQueue *aiq = asyncQueue_g;
  ssize_t ret;
  pthread_mutex_lock(&aiq->lock);
  pthread_mutex_lock(&dq->lock);

  ret = dq->nactive + dictSize(dq->entries);

  pthread_mutex_unlock(&dq->lock);
  pthread_mutex_unlock(&aiq->lock);
  return ret;
}

void SDQ_RemoveDoc(SpecDocQueue *sdq, AsyncIndexQueue *aiq, RedisModuleString *keyname) {
}