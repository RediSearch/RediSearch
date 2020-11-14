
#include "cursor.h"
#include "query_error.h"

#include "rmalloc.h"
#include "rmutil/rm_assert.h"

#include <time.h>
#include <err.h>

///////////////////////////////////////////////////////////////////////////////////////////////

Cursor::Cursor(CursorList *cl, CursorSpecInfo *info_, uint32_t interval) {
  parent = cl;
  info = info_;
  id = CursorList::GenerateId();
  pos = -1;
  timeoutIntervalMs = interval;
  nextIdleTimeoutNs = 0;
  execState = NULL;
}

//---------------------------------------------------------------------------------------------

// Doesn't lock - simply deallocates and decrements

Cursor::~Cursor() {
  info->used--;
  if (execState) {
    delete execState;
  }
}

//---------------------------------------------------------------------------------------------

int Cursor::Free() {
  return parent->Purge(id);
}

//---------------------------------------------------------------------------------------------

// Pause a cursor, setting it to idle and placing it back in the cursor list

int Cursor::Pause() {
  CursorList &cl = *parent;
  nextTimeoutNs = curTimeNs() + ((uint64_t)timeoutIntervalMs * 1000000);

  MutexGuard guard(cl.lock);
  cl.IncrCounter();

  if (nextTimeoutNs < cl.nextIdleTimeoutNs || cl.nextIdleTimeoutNs == 0) {
    cl.nextIdleTimeoutNs = nextTimeoutNs;
  }

  // Add to idle list
  *(Cursor **)(ARRAY_ADD_AS(&cl.idle, Cursor *)) = this;
  pos = ARRAY_GETSIZE_AS(&cl.idle, Cursor **) - 1;

  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

void Cursor::RemoveFromIdle() {
  Array *idle = &parent->idle;
  Cursor **ll = ARRAY_GETARRAY_AS(idle, Cursor **);
  size_t n = ARRAY_GETSIZE_AS(idle, Cursor *);

  if (n > 1) {
    Cursor *last = ll[n - 1]; // Last cursor - move to current position
    last->pos = pos;
    ll[last->pos] = last;
  }

  Array_Resize(idle, sizeof(Cursor *) * (n - 1));
  if (nextTimeoutNs == parent->nextIdleTimeoutNs) {
    parent->nextIdleTimeoutNs = 0;
  }
  pos = -1;
}

///////////////////////////////////////////////////////////////////////////////////////////////

CursorList *RSCursors;

//---------------------------------------------------------------------------------------------

static uint64_t curTimeNs() {
  struct timespec tv;
  clock_gettime(CLOCK_MONOTONIC, &tv);
  return tv.tv_nsec + (tv.tv_sec * 1000000000);
}

//---------------------------------------------------------------------------------------------

CursorList::CursorList() {
  cursorCount = 0;
  counter = 0;
  lastCollect = 0;
  nextIdleTimeoutNs = 0;

  lookup = kh_init(cursors);
  Array_Init(&idle);
  infos = NULL;

  srand48(getpid());
}

//---------------------------------------------------------------------------------------------

CursorSpecInfo *CursorList::Find(const char *keyName, size_t *index) const {
  for (size_t i = 0; i < cursorCount; ++i) {
    if (!strcmp(infos[i]->keyName, keyName)) {
      if (index) {
        *index = i;
      }
      return infos[i];
    }
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

void CursorList::Free(Cursor *cur, khiter_t khi) {
  // Decrement the used count
  auto &lookup = parent->lookup;
  RS_LOG_ASSERT(khi != kh_end(lookup), "Iterator shouldn't be at end of cursor list");
  RS_LOG_ASSERT(kh_get(cursors, lookup, id) != kh_end(lookup), "Cursor was not found");
  kh_del(cursors, lookup, khi);
  RS_LOG_ASSERT(kh_get(cursors, lookup, id) == kh_end(lookup), "Failed to delete cursor");
  delete cur;
}

//---------------------------------------------------------------------------------------------

void CursorList::ForEach(std::function<void(CursorList&, Cursor&, void *)> f, void *arg) {
  for (size_t i = 0; i < ARRAY_GETSIZE_AS(&idle, Cursor *); ++i) {
    Cursor *cur = *ARRAY_GETITEM_AS(&idle, ii, Cursor **);
    Cursor *oldCur = NULL;

    // The cursor `cur` might have been changed in the callback, if it has been
    // swapped with another one, as deletion means swapping the last cursor to
    // the current position. We ensure that we do not 'skip' over this cursor
    // (effectively skipping over the cursor that was just relocated).

    while (cur && cur != oldCur) {
      f(*this, *cur, arg);
      oldCur = cur;
      if (idle.len > i) {
        cur = *ARRAY_GETITEM_AS(&idle, i, Cursor **);
      }
    }
  }
}

//---------------------------------------------------------------------------------------------

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

int CursorList::GCInternal(bool force) {
  uint64_t now = curTimeNs();
  if (nextIdleTimeoutNs && nextIdleTimeoutNs > now) {
    return -1;
  } else if (!force && now - lastCollect < RSCURSORS_SWEEP_THROTTLE) {
    return -1;
  }

  lastCollect = now;

  struct GcData {
    uint64_t now;
    int numCollected;
  } gc_data = {now, 0};

  auto cb = [](CursorList &cl, Cursor &cur, void *gc_data) {
    GcData *data = gc_data;
    if (cur.nextTimeoutNs <= data->now) {
      cur.RemoveFromIdle();
      cl.Free(&cur, kh_get(cursors, cl.lookup, cur.id));
      data->numCollected++;
    }
  }
  ForEach(cb, &gc_data);
  return gc_data.numCollected;
}

//---------------------------------------------------------------------------------------------

int CursorList::CollectIdle() {
  MutexGuard guard(lock);
  int rc = GCInternal(true);
  return rc;
}

//---------------------------------------------------------------------------------------------

// Add an index spec to the cursor list.
// This has the effect of adding the spec (via its key) along with its capacity.

void CursorList::Add(const char *keyname, size_t capacity) {
  CursorSpecInfo *info = Find(keyname, NULL);
  if (!info) {
    info = new CursorSpecInfo(keyname, capacity);
    infos = rm_realloc(infos, sizeof(*infos) * ++cursorCount);
    infos[cursorCount - 1] = info;
  }
}

//---------------------------------------------------------------------------------------------

void CursorList::Remove(const char *keyname) {
  size_t index;
  CursorSpecInfo *info = Find(keyname, &index);
  if (info) {
    infos[index] = infos[cursorCount - 1];
    infos = rm_realloc(infos, sizeof(*infos) * --cursorCount);
    delete info
  }
}

//---------------------------------------------------------------------------------------------

void CursorList::IncrCounter() {
  if (++counter % RSCURSORS_SWEEP_INTERVAL) {
    GCInternal();
  }
}

//---------------------------------------------------------------------------------------------

/**
 * Cursor ID is a 64 bit opaque integer. The upper 32 bits consist of the PID of the process 
 * which generated the cursor, and the lower 32 bits consist of the counter at the time at 
 * which it was generated.
 * This doesn't make it particularly "secure" but it does prevent accidental collisions from 
 * both a stuck client and a crashed server.
 */
CursorId CursorList::GenerateId() {
  CursorId id = lrand48() + 1;  // 0 should never be returned as cursor id
  return id;
}

//---------------------------------------------------------------------------------------------

/**
 * Reserve a cursor for use with a given query.
 * Returns NULL if the index does not exist or if there are too many cursors currently in use.
 *
 * Timeout is the max idle timeout (activated at each call to Pause()) in milliseconds.
 */

Cursor *CursorList::Reserve(const char *lookupName, unsigned interval, QueryError *status) {
  MutexGuard guard(lock);;
  IncrCounter();
  CursorSpecInfo *spec = Find(lookupName, NULL);
  Cursor *cur = NULL;

  if (spec == NULL) {
    status->SetErrorFmt(QUERY_ENOINDEX, "Index `%s` does not have cursors enabled", lookupName);
    goto done;
  }

  if (spec->used >= spec->cap) {
    GCInternal();
    if (spec->used >= spec->cap) {
      // Collect idle cursors now
      status->SetError(QUERY_ELIMIT, "Too many cursors allocated for index");
      goto done;
    }
  }

  cur = new Cursor(*this, spec, inteval)

  int dummy;
  khiter_t iter = kh_put(cursors, cl->lookup, cur->id, &dummy);
  kh_value(cl->lookup, iter) = cur;

done:
  if (cur) {
    cur->specInfo->used++;
  }
  return cur;
}

//---------------------------------------------------------------------------------------------

// Retrieve a cursor for execution. This locates the cursor, removes it
// from the idle list, and returns it

Cursor *CursorList::TakeForExecution(CursorId cid) {
  MutexGuard guard(lock);
  IncrCounter();

  Cursor *cur = NULL;
  khiter_t iter = kh_get(cursors, lookup, cid);
  if (iter != kh_end(lookup)) {
    cur = kh_value(lookup, iter);
    if (cur->pos == -1) {
      // Cursor is not idle!
      cur = NULL;
    } else {
      // Remove from idle
      cur->RemoveFromIdle();
    }
  }

  return cur;
}

//---------------------------------------------------------------------------------------------

// Locate and free the cursor with the given ID

int CursorList::Purge(CursorId cid) {
  MutexGuard guard(lock);
  IncrCounter();

  int rc;
  khiter_t iter = kh_get(cursors, lookup, cid);
  if (iter != kh_end(lookup)) {
    Cursor *cur = kh_value(lookup, iter);
    if (cur->IsIdle()) {
      cur->RemoveFromIdle();
    }
    Free(cur, iter);
    rc = REDISMODULE_OK;

  } else {
    rc = REDISMODULE_ERR;
  }
  return rc;
}

//---------------------------------------------------------------------------------------------

void CursorList::RenderStats(const char *name, RedisModuleCtx *ctx) {
  MutexGuard guard(lock);
  CursorSpecInfo *info = Find(name, NULL);
  size_t n = 0;

  // Output total information
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithSimpleString(ctx, "global_idle");
  RedisModule_ReplyWithLongLong(ctx, ARRAY_GETSIZE_AS(&idle, Cursor **));
  n += 2;

  RedisModule_ReplyWithSimpleString(ctx, "global_total");
  RedisModule_ReplyWithLongLong(ctx, kh_size(lookup));
  n += 2;

  if (info) {
    RedisModule_ReplyWithSimpleString(ctx, "index_capacity");
    RedisModule_ReplyWithLongLong(ctx, info->cap);
    n += 2;

    RedisModule_ReplyWithSimpleString(ctx, "index_total");
    RedisModule_ReplyWithLongLong(ctx, info->used);
    n += 2;
  }

  RedisModule_ReplySetArrayLength(ctx, n);
}

//---------------------------------------------------------------------------------------------

// Remove all cursors with the given lookup name

void CursorList::Purge(const char *lookupName) {
  CursorSpecInfo *info = Find(lookupName, NULL);
  if (!info) {
    return;
  }

  auto cb = [](CursorList &cl, Cursor &cur, void *arg) {
    CursorSpecInfo *info = arg;
    if (cur.specInfo != info) {
      return;
    }

    cur.RemoveFromIdle();
    cl.Free(&cur, kh_get(cursors, cl.lookup, cur.id));
  }

  ForEach(cl, cb, info);
}

//---------------------------------------------------------------------------------------------

CursorList::~CursorList() {
  GCInternal(true);

  for (khiter_t i = 0; i != kh_end(lookup); ++i) {
    if (!kh_exist(lookup, i)) {
      continue;
    }
    Cursor *cur = kh_val(cl->lookup, ii);
    fprintf(stderr, "[redisearch] leaked cursor at %p\n", cur);
    Free(cur, ii);
  }
  kh_destroy(cursors, lookup);

  if (infos) {
    for (size_t i = 0; i < cursorCount; ++i) {
      delete infos[i];
    }
    rm_free(infos);
  }

  Array_Free(&idle);
}

///////////////////////////////////////////////////////////////////////////////////////////////

CursorSpecInfo::CursorSpecInfo(const char *keyname, size_t capacity) {
  keyName = rm_strdup(k);
  used = 0;
  cap = capacity;
}

//---------------------------------------------------------------------------------------------

CursorSpecInfo::~CursorSpecInfo() {
  rm_free(keyName);
}

///////////////////////////////////////////////////////////////////////////////////////////////
