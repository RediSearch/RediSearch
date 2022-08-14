
#include "cursor.h"
#include "query_error.h"

#include "rmalloc.h"
#include "rmutil/rm_assert.h"

#include <time.h>
#include <err.h>

///////////////////////////////////////////////////////////////////////////////////////////////

Cursor::Cursor(CursorList *cl, CursorSpecInfo *info, uint32_t interval) {
  parent = cl;
  specInfo = info;
  id = CursorList::GenerateId();
  pos = -1;
  timeoutIntervalMs = interval;
  nextTimeoutNs = 0;
  execState = NULL;
}

//---------------------------------------------------------------------------------------------

// Doesn't lock - simply deallocates and decrements

Cursor::~Cursor() {
  specInfo->used--;
  if (execState) {
    delete execState;
  }
  delete parent;
}

//---------------------------------------------------------------------------------------------

int Cursor::Free() {
  return parent->Purge(id);
}

//---------------------------------------------------------------------------------------------

static uint64_t curTimeNs() {
  struct timespec tv;
  clock_gettime(CLOCK_MONOTONIC, &tv);
  return tv.tv_nsec + (tv.tv_sec * 1000000000);
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
  cl.idle.push_back(this);
  pos = cl.idle.size() - 1;

  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

void Cursor::RemoveFromIdle() {
  Vector<Cursor *> idle = parent->idle;
  size_t n = idle.size();

  if (n > 1) {
    Cursor *last = idle.back(); // Last cursor - move to current position
    last->pos = pos;
    idle[last->pos] = last;
  }

  idle.pop_back();
  if (nextTimeoutNs == parent->nextIdleTimeoutNs) {
    parent->nextIdleTimeoutNs = 0;
  }
  pos = -1;
}

///////////////////////////////////////////////////////////////////////////////////////////////

CursorList *RSCursors;

//---------------------------------------------------------------------------------------------

CursorList::CursorList() {
  counter = 0;
  lastCollect = 0;
  nextIdleTimeoutNs = 0;

  srand48(getpid());
}

//---------------------------------------------------------------------------------------------

CursorSpecInfo *CursorList::Find(const char *keyName, size_t *index) const {
  for (size_t i = 0; i < infos.size(); ++i) {
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

void CursorList::ForEach(std::function<void(CursorList&, Cursor&, void *)> f, void *arg) {
  for (size_t i = 0; i < idle.size(); ++i) {
    Cursor *cur = idle[i];
    Cursor *oldCur = NULL;

    // The cursor `cur` might have been changed in the callback, if it has been
    // swapped with another one, as deletion means swapping the last cursor to
    // the current position. We ensure that we do not 'skip' over this cursor
    // (effectively skipping over the cursor that was just relocated).

    while (cur && cur != oldCur) {
      f(*this, *cur, arg);
      oldCur = cur;
      if (idle.size() > i) {
        cur = idle[i];
      }
    }
  }
}

//---------------------------------------------------------------------------------------------

// Garbage collection:
//
// Garbage collection is performed:
//
// - Every <n> operations
// - If there are too many active cursors and we want to create a cursor
// - If NextTimeout is set and is earlier than the current time.
//
// Garbage collection is throttled within a given interval as well.

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
      delete &cl;
      data->numCollected++;
    }
  };
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

// Add an index spec to the cursor list if it's not exist.
// This has the effect of adding the spec (via its key) along with its capacity.

void CursorList::Add(const char *keyname, size_t capacity) {
  CursorSpecInfo *info = Find(keyname, NULL);
  if (!info) {
    infos.push_back(new CursorSpecInfo(keyname, capacity));
  }
}

//---------------------------------------------------------------------------------------------

void CursorList::Remove(const char *keyname) {
  size_t index;
  CursorSpecInfo *info = Find(keyname, &index);
  if (info) {
    infos.erase(infos.begin() + index);
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

  cur = new Cursor(this, spec, interval);
  lookup.emplace(cur->id, cur);

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

  Cursor *cur = lookup[cid];
  if (cur->pos == -1) {
    // Cursor is not idle!
    cur = NULL;
  } else {
    // Remove from idle
    cur->RemoveFromIdle();
  }

  return cur;
}

//---------------------------------------------------------------------------------------------

// Locate and free the cursor with the given ID

int CursorList::Purge(CursorId cid) {
  MutexGuard guard(lock);
  IncrCounter();

  int rc;
  if (lookup.contains(cid)) {
    Cursor cur = *lookup[cid];
    if (cur.IsIdle()) {
      cur.RemoveFromIdle();
    }
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
  RedisModule_ReplyWithLongLong(ctx, idle.size());
  n += 2;

  RedisModule_ReplyWithSimpleString(ctx, "global_total");
  RedisModule_ReplyWithLongLong(ctx, lookup.size());
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
    delete &cl;
  };

  ForEach(cb, info);
}

//---------------------------------------------------------------------------------------------

CursorList::~CursorList() {
  GCInternal(true);
}

///////////////////////////////////////////////////////////////////////////////////////////////

CursorSpecInfo::CursorSpecInfo(const char *k, size_t capacity) {
  keyName = rm_strdup(k);
  used = 0;
  cap = capacity;
}

//---------------------------------------------------------------------------------------------

CursorSpecInfo::~CursorSpecInfo() {
  rm_free(keyName);
}

///////////////////////////////////////////////////////////////////////////////////////////////
