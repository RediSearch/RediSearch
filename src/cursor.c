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

// coord cursors will have odd ids and regular cursors will have even ids
CursorList g_CursorsList;
CursorList g_CursorsListCoord;


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

void CursorList_Init(CursorList *cl, bool is_coord) {
  *cl = (CursorList) {0};
  pthread_mutex_init(&cl->lock, NULL);
  cl->lookup = kh_init(cursors);
  Array_Init(&cl->idle);
  cl->is_coord = is_coord;
  srand48(getpid());
}

static void Cursor_RemoveFromIdle(Cursor *cur) {
  CursorList *cl = getCursorList(cur->is_coord);
  Array *idle = &cl->idle;
  Cursor **ll = ARRAY_GETARRAY_AS(idle, Cursor **);
  size_t n = ARRAY_GETSIZE_AS(idle, Cursor *);

  if (n > 1) {
    Cursor *last = ll[n - 1]; /** Last cursor - move to current position */
    last->pos = cur->pos;
    ll[last->pos] = last;
  }

  Array_Resize(idle, sizeof(Cursor *) * (n - 1));
  if (cur->nextTimeoutNs == cl->nextIdleTimeoutNs) {
    cl->nextIdleTimeoutNs = 0;
  }
  cur->pos = -1;
}

#define get_g_CursorsList(is_coord) ((is_coord) ? &g_CursorsListCoord : &g_CursorsList)

/* Assumed to be called under the cursors global lock or upon server shut down. */
static void Cursor_FreeInternal(Cursor *cur, khiter_t khi) {
  CursorList *cl = get_g_CursorsList(cur->is_coord);
  /* Decrement the used count */
  RS_LOG_ASSERT(khi != kh_end(cl->lookup), "Iterator shouldn't be at end of cursor list");
  RS_LOG_ASSERT(kh_get(cursors, cl->lookup, cur->id) != kh_end(cl->lookup),
                                                    "Cursor was not found");
  kh_del(cursors, cl->lookup, khi);
  RS_LOG_ASSERT(kh_get(cursors, cl->lookup, cur->id) == kh_end(cl->lookup),
                                                    "Failed to delete cursor");
  if (cur->execState) {
    Cursor_FreeExecState(cur->execState);
    cur->execState = NULL;
  }
  // if There's a spec associated with the cursor
  if(cur->spec_ref.rm) {
    StrongRef spec_ref = WeakRef_Promote(cur->spec_ref);
    IndexSpec *spec = StrongRef_Get(spec_ref);
    // the spec may have been dropped, so we need to make sure it is still valid.
    if(spec) {
      spec->activeCursors--;
      StrongRef_Release(spec_ref);
    }
    WeakRef_Release(cur->spec_ref);

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
      if (ARRAY_GETSIZE_AS(&cl->idle, Cursor *) > ii) {
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
 *
 * Assumed to be called under the cursors global lock or upon server shut down.
 *
 */
static int Cursors_GCInternal(CursorList *cl, int force) {
  uint64_t now = curTimeNs();
  if ((cl->nextIdleTimeoutNs && cl->nextIdleTimeoutNs > now) ||
      (!force && now - cl->lastCollect < RSCURSORS_SWEEP_THROTTLE)) {
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

// The cursors list is assumed to be locked upon calling this function
static void CursorList_IncrCounter(CursorList *cl) {
  if (++cl->counter % RSCURSORS_SWEEP_INTERVAL == 0) {
    Cursors_GCInternal(cl, 0);
  }
}

#define mask31(x) ((x) & 0x7fffffffUL) // mask to prevent overflow when adding 1 to the id
#define rand_even48() (mask31(lrand48()) & ~(1UL))
#define rand_odd48() (mask31(lrand48()) | (1UL))

/**
 * Cursor ID is a 64 bit opaque integer. The upper 32 bits consist of the PID
 * of the process which generated the cursor, and the lower 32 bits consist of
 * the counter at the time at which it was generated. This doesn't make it
 * particularly "secure" but it does prevent accidental collisions from both
 * a stuck client and a crashed server
 */
static uint64_t CursorList_GenerateId(CursorList *curlist) {
  uint64_t id = (curlist->is_coord ? rand_even48() : rand_odd48()) + 1;  // 0 should never be returned as cursor id

  // For fast lookup we would like the coord cusors to have odd ids and the non-coord to have even
  while((kh_get(cursors, curlist->lookup, id) != kh_end(curlist->lookup))) {
    id = (curlist->is_coord ? rand_even48() : rand_odd48()) + 1;  // 0 should never be returned as cursor id
  }
  return id;
}

Cursor *Cursors_Reserve(CursorList *cl, StrongRef global_spec_ref, unsigned interval,
                        QueryError *status) {
  CursorList_Lock(cl);
  CursorList_IncrCounter(cl);
  Cursor *cur = NULL;

  // If the cursor should be associated with a spec,
  // we assume that global_spec_ref points to a valid spec, else the function returns NULL.
  IndexSpec *spec = StrongRef_Get(global_spec_ref);

  // If we are in a coordinator ctx, the spec is NULL
  if (spec && (spec->activeCursors >= spec->cursorsCap)) {
    /** Collect idle cursors now */
    Cursors_GCInternal(cl, 0);
    if (spec->activeCursors >= spec->cursorsCap) {
      QueryError_SetError(status, QUERY_ELIMIT, "Too many cursors allocated for index");
      goto done;
    }

  }

  cur = rm_calloc(1, sizeof(*cur));
  cur->id = CursorList_GenerateId(cl);
  cur->pos = -1;
  cur->timeoutIntervalMs = interval;
  cur->is_coord = cl->is_coord;
  if(spec) {
    // Get a a weak reference to the spec out of the strong ref, and save it in the
    // cursor's struct.
    cur->spec_ref = StrongRef_Demote(global_spec_ref);
    spec->activeCursors++;
  }

  int dummy;
  khiter_t iter = kh_put(cursors, cl->lookup, cur->id, &dummy);
  kh_value(cl->lookup, iter) = cur;

done:
  CursorList_Unlock(cl);
  return cur;
}

int Cursor_Pause(Cursor *cur) {
  CursorList *cl = get_g_CursorsList(cur->is_coord);

  CursorList_Lock(cl);
  CursorList_IncrCounter(cl);

  cur->nextTimeoutNs = curTimeNs() + ((uint64_t)cur->timeoutIntervalMs * 1000000);
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
  return Cursors_Purge(get_g_CursorsList(cur->is_coord), cur->id);
}

void Cursors_RenderStats(CursorList *cl, CursorList *cl_coord, IndexSpec *spec, RedisModule_Reply *reply) {
  CursorList_Lock(cl);

  RedisModule_ReplyKV_Map(reply, "cursor_stats");

    RedisModule_ReplyKV_LongLong(reply, "global_idle", ARRAY_GETSIZE_AS(&cl->idle, Cursor **) +
                                                        ARRAY_GETSIZE_AS(&cl_coord->idle, Cursor **));
    RedisModule_ReplyKV_LongLong(reply, "global_total", kh_size(cl->lookup) + kh_size(cl_coord->lookup));
    RedisModule_ReplyKV_LongLong(reply, "index_capacity", spec->cursorsCap);
    RedisModule_ReplyKV_LongLong(reply, "index_total", spec->activeCursors);

  RedisModule_Reply_MapEnd(reply);

  CursorList_Unlock(cl);
}

#ifdef FTINFO_FOR_INFO_MODULES
void Cursors_RenderStatsForInfo(CursorList *cl, CursorList *cl_coord, IndexSpec *spec, RedisModuleInfoCtx *ctx) {
  CursorList_Lock(cl);

  RedisModule_InfoBeginDictField(ctx, "cursor_stats");
  RedisModule_InfoAddFieldLongLong(ctx, "global_idle", ARRAY_GETSIZE_AS(&cl->idle, Cursor **) +
                                                        ARRAY_GETSIZE_AS(&cl_coord->idle, Cursor **));
  RedisModule_InfoAddFieldLongLong(ctx, "global_total", kh_size(cl->lookup) + kh_size(cl_coord->lookup));
  RedisModule_InfoAddFieldLongLong(ctx, "index_capacity", spec->cursorsCap);
  RedisModule_InfoAddFieldLongLong(ctx, "index_total", spec->activeCursors);
  RedisModule_InfoEndDictField(ctx);

  CursorList_Unlock(cl);
}
#endif // FTINFO_FOR_INFO_MODULES

void CursorList_Destroy(CursorList *cl) {
  Cursors_GCInternal(cl, 1);
  for (khiter_t ii = 0; ii != kh_end(cl->lookup); ++ii) {
    if (!kh_exist(cl->lookup, ii)) {
      continue;
    }
    Cursor *c = kh_val(cl->lookup, ii);
    Cursor_FreeInternal(c, ii);
  }
  kh_destroy(cursors, cl->lookup);

  pthread_mutex_destroy(&cl->lock);
  Array_Free(&cl->idle);
}

void CursorList_Empty(CursorList *cl) {
  bool is_coord = cl->is_coord;
  CursorList_Destroy(cl);
  CursorList_Init(cl, is_coord);
}
