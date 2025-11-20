/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef CURSOR_H
#define CURSOR_H

#include <unistd.h>
#include <pthread.h>
#include "util/khash.h"
#include "util/array.h"
#include "search_ctx.h"
#include "aggregate/aggregate.h"

#ifdef __cplusplus
extern "C" {
#endif

struct CursorList;

typedef struct Cursor {
  /**
   * The cursor is holding a weak reference to spec. When read cursor is called
   * we will try to promote the reference to a strong reference. if the promotion fails -
   *  it means that the index was dropped. The cursor is no longer valid and should be freed.
   */
  WeakRef spec_ref;

  /**
   * Hybrid request reference. This is a strong reference to the hybrid request.
   * If the hybrid request is NULL, this is a regular cursor.
   */
  StrongRef hybrid_ref;

  /** Execution state. Opaque to the cursor - managed by consumer */
  AREQ *execState;

  /** Time when this cursor will no longer be valid, in nanos */
  uint64_t nextTimeoutNs;

  /** ID of this cursor */
  uint64_t id;

  /** Initial timeout interval */
  unsigned timeoutIntervalMs;

  /** Position within idle list.
   * Should only be accessed under cursor list lock */
  int pos;

  /** Is it an internal coordinator cursor or a user cursor*/
  bool is_coord;

  /** If true, a call to `Cursor_Pause` should drop it instead.
   *  Should only be accessed under cursor list lock */
  bool delete_mark;

  /** Pre-calculated total results for WITHCOUNT + WITHCURSOR queries.
   *  This is calculated once when the cursor is created and used for all
   *  subsequent cursor reads to provide accurate total_results. */
  uint32_t precalculated_total;

  /** Flag indicating whether precalculated_total has been set.
   *  If false, precalculated_total should not be used. */
  bool has_precalculated_total;
} Cursor;

KHASH_MAP_INIT_INT64(cursors, Cursor *);
/**
 * Cursor list. This is the global cursor list and does not distinguish
 * between different specs.
 */
typedef struct CursorList {
  /** Cursor lookup by ID */
  khash_t(cursors) * lookup;

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

  /** Is it an internal coordinator cursor or a user cursor */
  bool is_coord;
} CursorList;

// This resides in the background as a global. We could in theory make this
// part of the spec structure
// Structs managing the cusrosrs
extern CursorList g_CursorsList;
extern CursorList g_CursorsListCoord;

static inline CursorList *GetGlobalCursor(uint64_t cid) {
  return cid % 2 == 1 ? &g_CursorsListCoord : &g_CursorsList;
}

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
void CursorList_Init(CursorList *cl, bool is_coord);

/**
 * Empty the cursor list.
 * This function is thread-safe and handles both idle and active cursors.
 * Idle cursors are freed immediately, while active cursors are marked for
 * deletion and will be freed when they are next accessed.
 */
void CursorList_Empty(CursorList *cl);

#define RSCURSORS_SWEEP_INTERVAL 500                /* GC Every 500 requests */
#define RSCURSORS_SWEEP_THROTTLE (1 * (1000000000)) /* Throttle, in NS */

/**
 * Check if the cursor has a reference to a spec.
 */
static inline bool cursor_HasSpecWeakRef(const Cursor *cursor) {
  return cursor->spec_ref.rm != NULL;
}

/**
 * Reserve a cursor for use with a given query.
 * Returns NULL if the index does not exist or if there are too many
 * cursors currently in use.
 *
 * Timeout is the max idle timeout (activated at each call to Pause()) in
 * milliseconds.
 */
Cursor *Cursors_Reserve(CursorList *cl, StrongRef global_spec_ref, unsigned timeout,
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
 * Locate and free the cursor with the given ID.
 * If the cursor is found but not idle, it is marked for deletion.
 */
int Cursors_Purge(CursorList *cl, uint64_t cid);

int Cursors_CollectIdle(CursorList *cl);

typedef struct CursorsInfoStats {
  size_t total_user;                // total number of cursors created explicitly by user commands
  size_t total_idle_user;           // number of cursors created by user commands that are currently idle
  size_t total_internal;            // total number of internal cursors created by the coordinator
  size_t total_idle_internal;       // number of internal cursors created by the coordinator that are currently idle
} CursorsInfoStats;

/**
 * Return the stats for the `INFO` command
*/
CursorsInfoStats Cursors_GetInfoStats(void);

/**
 * Assumed to be called by the main thread with a valid locked spec, under the cursors lock.
 */
void Cursors_RenderStats(CursorList *cl, CursorList *cl_coord, const IndexSpec *spec, RedisModule_Reply *reply);

#ifdef FTINFO_FOR_INFO_MODULES
void Cursors_RenderStatsForInfo(CursorList *cl, CursorList *cl_coord, const IndexSpec *spec, RedisModuleInfoCtx *ctx);
#endif

#define getCursorList(coord) ((coord) ? &g_CursorsListCoord : &g_CursorsList)

#ifdef __cplusplus
}
#endif
#endif // CURSOR_H
