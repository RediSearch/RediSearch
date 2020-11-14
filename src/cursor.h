
#pragma once

#include "rmalloc.h"
#include "search_ctx.h"

#include "util/array.h"
#include "util/khash.h"

#include <unistd.h>
#include <pthread.h>
#include <functional>
#include <mutex>

///////////////////////////////////////////////////////////////////////////////////////////////

struct CursorSpecInfo : public Object {
  char *keyName; // Name of the key that refers to the spec
  size_t cap;    // Maximum number of cursors for the spec
  size_t used;   // Number of cursors currently open

  CursorSpecInfo(const char *keyname, size_t capacity);
  ~CursorSpecInfo();
};

//---------------------------------------------------------------------------------------------

struct CursorList;

struct Cursor : public Object {
  // Link to info on parent. Used to increment/decrement the count, and also to reopen the spec
  CursorSpecInfo *specInfo;

  // Parent - used for deletion, etc
  struct CursorList *parent;

  // Execution state. Opaque to the cursor - managed by consumer
  struct AREQ *execState;

  // Time when this cursor will no longer be valid, in nanos
  uint64_t nextTimeoutNs;

  // ID of this cursor
  uint64_t id;

  // Initial timeout interval
  uint32_t timeoutIntervalMs;

  // Position within idle list
  int pos;

  Cursor(CursorList &parent, CursorSpecInfo *spec);
  ~Cursor();

  void Free();

  bool IsIdle() const { return pos != -1; }

  int Pause();
  void RemoveFromIdle();
  void runCursor(RedisModuleCtx *outputCtx, size_t num);
};

//---------------------------------------------------------------------------------------------

// KHash type definition (name, value type) with key of int64
KHASH_MAP_INIT_INT64(cursors, Cursor *);

typedef uint64_t CursorId;

//---------------------------------------------------------------------------------------------

// Cursor list. This is the global cursor list and does not distinguish between different specs

struct CursorList : public Object {
  // Cursor lookup by ID
  khash_t(cursors) *lookup;

  // List of spec infos; we just iterate over this
  CursorSpecInfo **infos;
  size_t cursorCount;

  // List of idle cursors
  Array idle;

  std::mutex lock;

  // Counter - this serves two purposes:
  // 1) When counter % n == 0, a GC sweep is performed
  // 2) Used to calculate a monotonically incrementing cursor ID
  uint32_t counter;

  // Last time GC was performed
  uint64_t lastCollect;

  // Next timeout - set to the lowest entry. This is used as a hint to avoid excessive sweeps.
  uint64_t nextIdleTimeoutNs;

  CursorList();
  ~CursorList();

  void Add(const char *keyname, size_t capacity);
  void Remove(const char *keyname);
  CursorSpecInfo *Find(const char *keyName, size_t *index) const;

  Cursor *Reserve(const char *keyname, unsigned timeout, struct QueryError *status);
  Cursor *TakeForExecution(CursorId cid);
  int CollectIdle();

  int Purge(CursorId cid);
  void Purge(const char *keyname);
  void Free(Cursor *cursor);

  void ForEach(std::function<void(CursorList&, Cursor&, void *)> f, void *arg);

  void RenderStats(const char *key, RedisModuleCtx *ctx);

  int GCInternal(bool force = false);

  static CursorId GenerateId();
};

//---------------------------------------------------------------------------------------------

// This resides in the background as a global. We could in theory make this part of the spec structure.
extern CursorList *RSCursors;

//---------------------------------------------------------------------------------------------

/**
 * Threading/Concurrency behavior
 *
 * Any manipulation of the cursor list happens with the GIL locked.
 * Sequence is as follows:
 *
 * (1) New cursor is allocated -- happens from main thread. New cursor is
 *     allocated and is passed to query execution thread. The cursor is not
 *     placed inside the cursor list yet, but the total count is incremented
 *
 * (2) If cursor has results, GIL is locked and cursor is placed inside the idle list.
 *
 * (3) When cursor is subsequently accessed, it is again removed from the idle list.
 *
 * (4) When cursor is finally exhausted (or removed), it is removed from the idle list and freed.
 *
 * In essence, whenever the cursor is accessed by any internal API (i.e. not
 * a network API) it becomes invisible to the cursor subsystem, so there is
 * never any worry that the cursor is accessed from different threads, or
 * that a client might accidentally refer to the same cursor twice.
 */

//---------------------------------------------------------------------------------------------

#define RSCURSORS_DEFAULT_CAPACITY 128
#define RSCURSORS_SWEEP_INTERVAL 500                /* GC Every 500 requests */
#define RSCURSORS_SWEEP_THROTTLE (1 * (1000000000)) /* Throttle, in NS */

// Free a given cursor. This should be called on an already-obtained cursor
int Cursor_Free(Cursor *cl);

void Cursor_FreeExecState(void *);

///////////////////////////////////////////////////////////////////////////////////////////////
