/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "cursor.h"
#include "resp3.h"
#include <time.h>
#include "rmutil/rm_assert.h"
#include <err.h>

#define Cursor_IsIdle(cur) ((cur)->pos != -1)
CursorList RSCursors;

static uint64_t curTimeNs() {
  struct timespec tv;
  clock_gettime(CLOCK_MONOTONIC, &tv);
  return tv.tv_nsec + (tv.tv_sec * 1000000000);
}

static void CursorList_Lock(CursorList *cl) {
  pthread_mutex_lock(&cl->lock);
}

static void CursorList_Unlock(CursorList *cl) {
  pthread_mutex_unlock(&cl->lock);
}

void CursorList_Init(CursorList *cl) {
  *cl = (CursorList) {0};
  pthread_mutex_init(&cl->lock, NULL);
  cl->lookup = kh_init(cursors);
  Array_Init(&cl->idle);
  srand48(getpid());
  cl->specsDict = dictCreate(&dictTypeHeapStrings, NULL);
}

static CursorSpecInfo *findInfo(const CursorList *cl, const char *keyName) {
  return dictFetchValue(cl->specsDict, keyName);;
}

static void Cursor_RemoveFromIdle(Cursor *cur) {
  Array *idle = &cur->parent->idle;
  Cursor **ll = ARRAY_GETARRAY_AS(idle, Cursor **);
  size_t n = ARRAY_GETSIZE_AS(idle, Cursor *);

  if (n > 1) {
    Cursor *last = ll[n - 1]; /** Last cursor - move to current position */
    last->pos = cur->pos;
    ll[last->pos] = last;
  }

  Array_Resize(idle, sizeof(Cursor *) * (n - 1));
  if (cur->nextTimeoutNs == cur->parent->nextIdleTimeoutNs) {
    cur->parent->nextIdleTimeoutNs = 0;
  }
  cur->pos = -1;
}

/* Doesn't lock - simply deallocates and decrements */
static void Cursor_FreeInternal(Cursor *cur, khiter_t khi) {
  /* Decrement the used count */
  RS_LOG_ASSERT(khi != kh_end(cur->parent->lookup), "Iterator shouldn't be at end of cursor list");
  RS_LOG_ASSERT(kh_get(cursors, cur->parent->lookup, cur->id) != kh_end(cur->parent->lookup),
                                                    "Cursor was not found");
  kh_del(cursors, cur->parent->lookup, khi);
  RS_LOG_ASSERT(kh_get(cursors, cur->parent->lookup, cur->id) == kh_end(cur->parent->lookup),
                                                    "Failed to delete cursor");
  cur->specInfo->used--;
  if (cur->execState) {
    Cursor_FreeExecState(cur->execState);
    cur->execState = NULL;
  }
  rm_free(cur);
}

static void Cursors_ForEach(CursorList *cl, void (*callback)(CursorList *, Cursor *, void *),
                            void *arg) {
  for (size_t ii = 0; ii < ARRAY_GETSIZE_AS(&cl->idle, Cursor *); ++ii) {
    Cursor *cur = *ARRAY_GETITEM_AS(&cl->idle, ii, Cursor **);
    Cursor *oldCur = NULL;
    /**
     * The cursor `cur` might have been changed in the callback, if it has been
     * swapped with another one, as deletion means swapping the last cursor to
     * the current position. We ensure that we do not 'skip' over this cursor
     * (effectively skipping over the cursor that was just relocated).
     */

    while (cur && cur != oldCur) {
      callback(cl, cur, arg);
      oldCur = cur;
      if (cl->idle.len > ii) {
        cur = *ARRAY_GETITEM_AS(&cl->idle, ii, Cursor **);
      }
    }
  }
}

typedef struct {
  uint64_t now;
  int numCollected;
} cursorGcCtx;

static void cursorGcCb(CursorList *cl, Cursor *cur, void *arg) {
  cursorGcCtx *ctx = arg;
  if (cur->nextTimeoutNs <= ctx->now) {
    Cursor_RemoveFromIdle(cur);
    Cursor_FreeInternal(cur, kh_get(cursors, cl->lookup, cur->id));
    ctx->numCollected++;
  }
}

/**
 * Garbage collection:
 *
 * Garbage collection is performed:
 *
 * - Every <n> operations
 * - If there are too many active cursors and we want to create a cursor
 * - If NextTimeout is set and is earlier than the current time.
 *
 * Garbage collection is throttled within a given interval as well.
 */
static int Cursors_GCInternal(CursorList *cl, int force) {
  uint64_t now = curTimeNs();
  if (cl->nextIdleTimeoutNs && cl->nextIdleTimeoutNs > now) {
    return -1;
  } else if (!force && now - cl->lastCollect < RSCURSORS_SWEEP_THROTTLE) {
    return -1;
  }

  cl->lastCollect = now;
  cursorGcCtx ctx = {.now = now};
  Cursors_ForEach(cl, cursorGcCb, &ctx);
  return ctx.numCollected;
}

int Cursors_CollectIdle(CursorList *cl) {
  CursorList_Lock(cl);
  int rc = Cursors_GCInternal(cl, 1);
  CursorList_Unlock(cl);
  return rc;
}

void CursorList_AddSpec(CursorList *cl, const char *k, size_t capacity) {
  CursorSpecInfo *info = findInfo(cl, k);
  if (!info) {
    info = rm_malloc(sizeof(*info));
    info->keyName = rm_strdup(k);
    info->used = 0;
    dictAdd(cl->specsDict, (void *)k, info);
  }
  info->cap = capacity;
}

void CursorList_RemoveSpec(CursorList *cl, const char *k) {
  CursorSpecInfo *info = findInfo(cl, k);
  if (info) {
    dictDelete(cl->specsDict, k);
    rm_free(info->keyName);
    rm_free(info);
  }
}

static void CursorList_IncrCounter(CursorList *cl) {
  if (++cl->counter % RSCURSORS_SWEEP_INTERVAL == 0) {
    Cursors_GCInternal(cl, 0);
  }
}

/**
 * Cursor ID is a 64 bit opaque integer. The upper 32 bits consist of the PID
 * of the process which generated the cursor, and the lower 32 bits consist of
 * the counter at the time at which it was generated. This doesn't make it
 * particularly "secure" but it does prevent accidental collisions from both
 * a stuck client and a crashed server
 */
static uint64_t CursorList_GenerateId(CursorList *curlist) {
  uint64_t id = lrand48() + 1;  // 0 should never be returned as cursor id
  return id;
}

Cursor *Cursors_Reserve(CursorList *cl, const char *lookupName, unsigned interval,
                        QueryError *status) {
  CursorList_Lock(cl);
  CursorList_IncrCounter(cl);
  CursorSpecInfo *spec = findInfo(cl, lookupName);
  Cursor *cur = NULL;

  if (spec == NULL) {
    QueryError_SetErrorFmt(status, QUERY_ENOINDEX, "Index `%s` does not have cursors enabled",
                           lookupName);
    goto done;
  }

  if (spec->used >= spec->cap) {
    /** Collect idle cursors now */
    Cursors_GCInternal(cl, 0);
    if (spec->used >= spec->cap) {
      QueryError_SetError(status, QUERY_ELIMIT, "Too many cursors allocated for index");
      goto done;
    }
  }

  cur = rm_calloc(1, sizeof(*cur));
  cur->parent = cl;
  cur->specInfo = spec;
  cur->id = CursorList_GenerateId(cl);
  cur->pos = -1;
  cur->timeoutIntervalMs = interval;

  int dummy;
  khiter_t iter = kh_put(cursors, cl->lookup, cur->id, &dummy);
  kh_value(cl->lookup, iter) = cur;

done:
  if (cur) {
    cur->specInfo->used++;
  }
  CursorList_Unlock(cl);
  return cur;
}

int Cursor_Pause(Cursor *cur) {
  CursorList *cl = cur->parent;
  cur->nextTimeoutNs = curTimeNs() + ((uint64_t)cur->timeoutIntervalMs * 1000000);

  CursorList_Lock(cl);
  CursorList_IncrCounter(cl);

  if (cur->nextTimeoutNs < cl->nextIdleTimeoutNs || cl->nextIdleTimeoutNs == 0) {
    cl->nextIdleTimeoutNs = cur->nextTimeoutNs;
  }

  /* Add to idle list */
  *(Cursor **)(ARRAY_ADD_AS(&cl->idle, Cursor *)) = cur;
  cur->pos = ARRAY_GETSIZE_AS(&cl->idle, Cursor **) - 1;
  CursorList_Unlock(cl);

  return REDISMODULE_OK;
}

Cursor *Cursors_TakeForExecution(CursorList *cl, uint64_t cid) {
  CursorList_Lock(cl);
  CursorList_IncrCounter(cl);

  Cursor *cur = NULL;
  khiter_t iter = kh_get(cursors, cl->lookup, cid);
  if (iter != kh_end(cl->lookup)) {
    cur = kh_value(cl->lookup, iter);
    if (cur->pos == -1) {
      // Cursor is not idle!
      cur = NULL;
    } else {
      // Remove from idle
      Cursor_RemoveFromIdle(cur);
    }
  }

  CursorList_Unlock(cl);
  return cur;
}

int Cursors_Purge(CursorList *cl, uint64_t cid) {
  CursorList_Lock(cl);
  CursorList_IncrCounter(cl);

  int rc;
  khiter_t iter = kh_get(cursors, cl->lookup, cid);
  if (iter != kh_end(cl->lookup)) {
    Cursor *cur = kh_value(cl->lookup, iter);
    if (Cursor_IsIdle(cur)) {
      Cursor_RemoveFromIdle(cur);
    }
    Cursor_FreeInternal(cur, iter);
    rc = REDISMODULE_OK;

  } else {
    rc = REDISMODULE_ERR;
  }
  CursorList_Unlock(cl);
  return rc;
}

int Cursor_Free(Cursor *cur) {
  return Cursors_Purge(cur->parent, cur->id);
}

void Cursors_RenderStats(RedisModule_Reply *reply, CursorList *cl, const char *name) {
  CursorList_Lock(cl);
  CursorSpecInfo *info = findInfo(cl, name);

  RedisModule_ReplyKV_Map(reply, "cursor_stats");

    RedisModule_ReplyKV_LongLong(reply, "global_idle", ARRAY_GETSIZE_AS(&cl->idle, Cursor **));
    RedisModule_ReplyKV_LongLong(reply, "global_total", kh_size(cl->lookup));
    RedisModule_ReplyKV_LongLong(reply, "index_capacity", info->cap);
    RedisModule_ReplyKV_LongLong(reply, "index_total", info->used);

  RedisModule_Reply_MapEnd(reply);

  CursorList_Unlock(cl);
}

#ifdef FTINFO_FOR_INFO_MODULES
void Cursors_RenderStatsForInfo(CursorList *cl, const char *name, RedisModuleInfoCtx *ctx) {
  CursorList_Lock(cl);
  CursorSpecInfo *info = findInfo(cl, name);

  RedisModule_InfoBeginDictField(ctx, "cursor_stats");
  RedisModule_InfoAddFieldLongLong(ctx, "global_idle", ARRAY_GETSIZE_AS(&cl->idle, Cursor **));
  RedisModule_InfoAddFieldLongLong(ctx, "global_total", kh_size(cl->lookup));
  RedisModule_InfoAddFieldLongLong(ctx, "index_capacity", info->cap);
  RedisModule_InfoAddFieldLongLong(ctx, "index_total", info->used);
  RedisModule_InfoEndDictField(ctx);

  CursorList_Unlock(cl);
}
#endif // FTINFO_FOR_INFO_MODULES

static void purgeCb(CursorList *cl, Cursor *cur, void *arg) {
  CursorSpecInfo *info = arg;
  if (cur->specInfo != info) {
    return;
  }

  Cursor_RemoveFromIdle(cur);
  Cursor_FreeInternal(cur, kh_get(cursors, cl->lookup, cur->id));
}

void Cursors_PurgeWithName(CursorList *cl, const char *lookupName) {
  CursorSpecInfo *info = findInfo(cl, lookupName);
  if (!info) {
    return;
  }
  Cursors_ForEach(cl, purgeCb, info);
}

void CursorList_Empty(CursorList *cl) {
  CursorList_Destroy(cl);
  Array_Free(&cl->idle);
  CursorList_Init(cl);
}

void CursorList_Destroy(CursorList *cl) {
  Cursors_GCInternal(cl, 1);
  for (khiter_t ii = 0; ii != kh_end(cl->lookup); ++ii) {
    if (!kh_exist(cl->lookup, ii)) {
      continue;
    }
    Cursor *c = kh_val(cl->lookup, ii);
    fprintf(stderr, "[redisearch] leaked cursor at %p\n", c);
    Cursor_FreeInternal(c, ii);
  }
  kh_destroy(cursors, cl->lookup);

  // free the dictionary
  dictIterator *iter = dictGetIterator(cl->specsDict);
  dictEntry *entry;
  while ((entry = dictNext(iter))) {
    CursorSpecInfo *sp = dictGetVal(entry);
    rm_free(sp->keyName);
    rm_free(sp);
  }
  dictReleaseIterator(iter);
  dictRelease(cl->specsDict);

  pthread_mutex_destroy(&cl->lock);
}
