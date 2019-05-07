#ifndef CURSOR_H
#define CURSOR_H

#include <unistd.h>
#include <pthread.h>
#include "util/khash.h"
#include "util/array.h"
#include "search_ctx.h"

typedef struct {
  char *keyName; /** Name of the key that refers to the spec */
  size_t cap;    /** Maximum number of cursors for the spec */
  size_t used;   /** Number of cursors currently open */
} CursorSpecInfo;

struct CursorList;

typedef struct Cursor {
  /**
   * Link to info on parent. This is used to increment/decrement the count,
   * and also to reopen the spec
   */
  CursorSpecInfo *specInfo;

  /** Parent - used for deletion, etc */
  struct CursorList *parent;

  /** Execution state. Opaque to the cursor - managed by consumer */
  void *execState;

  /** Time when this cursor will no longer be valid, in nanos */
  uint64_t nextTimeoutNs;

  /** ID of this cursor */
  uint64_t id;

  /** Initial timeout interval */
  unsigned timeoutIntervalMs;

  /** Position within idle list */
  int pos;
} Cursor;

KHASH_MAP_INIT_INT64(cursors, Cursor *);
/**
 * Cursor list. This is the global cursor list and does not distinguish
 * between different specs.
 */
typedef struct CursorList {
  /** Cursor lookup by ID */
  khash_t(cursors) * lookup;

  /** List of spec infos; we just iterate over this */
  CursorSpecInfo **specs;
  size_t specsCount;

  /** List of idle cursors */
  Array idle;

  pthread_mutex_t lock;

  /**
   * Counter - this serves two purposes:
   * 1) When counter % n == 0, a GC sweep is performed
   * 2) Used to calculate a monotonically incrementing cursor ID.
   */
  uint32_t counter;

  /**
   * Last time GC was performed.
   */
  uint64_t lastCollect;

  /**
   * Next timeout - set to the lowest entry.
   * This is used as a hint to avoid excessive sweeps.
   */
  uint64_t nextIdleTimeoutNs;
} CursorList;

// This resides in the background as a global. We could in theory make this
// part of the spec structure
extern CursorList RSCursors;

/**
 * Threading/Concurrency behavior
 *
 * Any manipulation of the cursor list happens with the GIL locked. Sequence
 * is as follows:
 *
 * (1) New cursor is allocated -- happens from main thread. New cursor is
 *     allocated and is passed to query execution thread. The cursor is not
 *     placed inside the cursor list yet, but the total count is incremented
 *
 * (2) If the cursor has results, the GIL is locked and the cursor is placed
 *     inside the idle list.
 *
 * (3) When the cursor is subsequently accessed, it is again removed from the
 *     idle list.
 *
 * (4) When the cursor is finally exhausted (or removed), it is removed from
 *     the idle list and freed.
 *
 * In essence, whenever the cursor is accessed by any internal API (i.e. not
 * a network API) it becomes invisible to the cursor subsystem, so there is
 * never any worry that the cursor is accessed from different threads, or
 * that a client might accidentally refer to the same cursor twice.
 */

/**
 * Initialize the cursor list
 */
void CursorList_Init(CursorList *cl);

/**
 * Clear the cursor list
 */
void CursorList_Destroy(CursorList *cl);

#define RSCURSORS_DEFAULT_CAPACITY 128
#define RSCURSORS_SWEEP_INTERVAL 500                /* GC Every 500 requests */
#define RSCURSORS_SWEEP_THROTTLE (1 * (1000000000)) /* Throttle, in NS */

/**
 * Add an index spec to the cursor list. This has the effect of adding the
 * spec (via its key) along with its capacity
 */
void CursorList_AddSpec(CursorList *cl, const char *k, size_t capacity);

void CursorList_RemoveSpec(CursorList *cl, const char *k);

/**
 * Reserve a cursor for use with a given query.
 * Returns NULL if the index does not exist or if there are too many
 * cursors currently in use.
 *
 * Timeout is the max idle timeout (activated at each call to Pause()) in
 * milliseconds.
 */
Cursor *Cursors_Reserve(CursorList *cl, const char *lookupName, unsigned timeout,
                        QueryError *status);

/**
 * Retrieve a cursor for execution. This locates the cursor, removes it
 * from the idle list, and returns it
 */
Cursor *Cursors_TakeForExecution(CursorList *cl, uint64_t cid);

/**
 * Pause a cursor, setting it to idle and placing it back in the cursor
 * list
 */
int Cursor_Pause(Cursor *cur);

/**
 * Free a given cursor. This should be called on an already-obtained cursor
 */
int Cursor_Free(Cursor *cl);

/**
 * Locate and free the cursor with the given ID
 */
int Cursors_Purge(CursorList *cl, uint64_t cid);

int Cursors_CollectIdle(CursorList *cl);

/** Remove all cursors with the given lookup name */
void Cursors_PurgeWithName(CursorList *cl, const char *lookupName);

void Cursors_RenderStats(CursorList *cl, const char *key, RedisModuleCtx *ctx);

void Cursor_FreeExecState(void *);
#endif
