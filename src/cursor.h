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
#include "redismodule.h"
#include "util/khash.h"
#include "util/array.h"
#include "search_ctx.h"
#include "aggregate/aggregate.h"

#ifdef __cplusplus
extern "C" {
#endif

struct CursorList;
struct Cursor;

typedef enum {
  CURSOR_PENDING_READ_NONE,
  CURSOR_PENDING_READ_AVAILABLE,
  CURSOR_PENDING_READ_CLAIMED,
  CURSOR_PENDING_READ_READY,
} CursorPendingReadState;

typedef struct CursorTakeInfo {
  struct Cursor *cursor;
  AREQ *reqRef;
  size_t queryTimeoutMS;
  bool isInternal;
} CursorTakeInfo;

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

  /** Query-deadline timeout (ms) copied from the originating AREQ at cursor
   * creation. Write-once before the first Cursor_Pause; read under the
   * cursor-list lock (see Cursors_PeekTimeoutInfo). */
  size_t queryTimeoutMS;

  /** Initial timeout interval */
  unsigned timeoutIntervalMs;

  /** Timeout policy copied from the originating AREQ at cursor creation.
   * Frozen for the life of the cursor: changes to the `search-on-timeout`
   * config between commands do not affect in-flight cursors. Same access
   * pattern as queryTimeoutMS. */
  RSTimeoutPolicy queryTimeoutPolicy;

  /** Position within idle list.
   * Should only be accessed under cursor list lock */
  int pos;

  /** Is it an internal coordinator cursor or a user cursor*/
  bool is_coord;

  /** If true, a call to `Cursor_Pause` should drop it instead.
   *  Should only be accessed under cursor list lock */
  bool delete_mark;

  /** RETURN_STRICT timeout retry handoff state.
   *  Should only be accessed under cursor list lock */
  CursorPendingReadState pendingReadState;

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
  pthread_cond_t pendingReadCond;

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

  /**
   * Module timer that fires at `nextIdleTimeoutNs` to reap expired idle
   * cursors without requiring further client traffic. Equal to
   * `IDLE_SWEEP_TIMER_NONE` when no timer is currently armed.
   */
  RedisModuleTimerID idleSweepTimerId;

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

/** Outcome of `Cursors_TakeForExecution`. */
typedef enum {
  /** Cursor was idle. Caller owns it and must release via `Cursor_Pause` /
   *  `Cursor_Free`. */
  CURSOR_TAKE_OK,
  /** No cursor with this id, or it is not currently available to this caller. */
  CURSOR_TAKE_NOT_FOUND,
  /** A RETURN_STRICT timeout exposed one pending-reader slot and this call
   *  claimed it. The caller must wait for the pending handoff before touching
   *  the cursor execution state. */
  CURSOR_TAKE_PENDING,
} CursorTakeStatus;

/**
 * Retrieve a cursor for execution, or claim the single pending-reader slot
 * exposed by `Cursors_MarkForPendingRead`.
 * `out` must be non-NULL. `out->cursor` is a strong execution handle when this
 * returns `CURSOR_TAKE_OK`. `CURSOR_TAKE_PENDING` returns a cursor pinned by
 * `CURSOR_PENDING_READ_CLAIMED`; the caller must wait on it with
 * `Cursor_WaitForPendingRead` before reading from it.
 *
 * When `claimPending` is false, pending-reader slots are ignored and reported as
 * `CURSOR_TAKE_NOT_FOUND`.
 */
CursorTakeStatus Cursors_TakeForExecution(CursorList *cl, uint64_t cid, bool claimPending,
                                          CursorTakeInfo *out);

/**
 * Mark an active cursor as able to accept one pending reader. Called by a
 * RETURN_STRICT cursor-read timeout after it replied with `cid` but before
 * the worker that still owns the cursor has paused/freed it.
 */
bool Cursors_MarkForPendingRead(uint64_t cid);

typedef enum {
  CURSOR_PENDING_WAIT_READY,
  CURSOR_PENDING_WAIT_TIMED_OUT,
  CURSOR_PENDING_WAIT_DELETED,
} CursorPendingWaitResult;

typedef struct CursorPendingTimeoutInfo {
  bool started;
  bool deleted;
} CursorPendingTimeoutInfo;

/**
 * Wait until a claimed pending cursor is ready for execution, deleted, or the
 * retry timed out. `timedOut`, `started`, and `deleted` are protected by the
 * cursor-list lock while this function runs.
 */
CursorPendingWaitResult Cursor_WaitForPendingRead(Cursor *cur, bool *timedOut, bool *started,
                                                  bool *deleted);

/**
 * Mark a pending retry as timed out and wake its waiting worker. Returns a
 * snapshot of whether the retry has already started execution or observed
 * cursor deletion.
 */
CursorPendingTimeoutInfo Cursors_TimeoutPendingRead(uint64_t cid, bool *timedOut, bool *started,
                                                    bool *deleted);

/**
 * Snapshot of an idle cursor's timeout configuration, returned by
 * Cursors_PeekTimeoutInfo without taking ownership of the cursor.
 */
typedef struct {
  /** Cached `queryTimeoutMS`. 0 means `TIMEOUT 0` on the originating
   * FT.AGGREGATE; maps to `RedisModule_BlockClient(timeoutMS=0)`. Use
   * `found` to distinguish "no cursor" from "cursor exists with TIMEOUT 0". */
  size_t queryTimeoutMS;
  /** Cached `timeoutPolicy`. Defaults to `TimeoutPolicy_Return` when the
   * cursor was not found (safe: the coord FAIL branch is then skipped). */
  RSTimeoutPolicy queryTimeoutPolicy;
  /** True if the cursor was present in the lookup table at peek time; when
   * false the other fields hold their defaults. Lets callers validate the
   * cid up-front instead of deferring "Cursor not found" to the worker. */
  bool found;
#ifdef ENABLE_ASSERT
  /** Today no hybrid cursor reaches this peek:
   * `_FT.HYBRID WITHCURSOR` cursors live on the shard cursor list and are
   * read via `_FT.CURSOR READ` which goes directly to RSCursorReadCommand,
   * bypassing CursorCommand (the only caller of Cursors_PeekTimeoutInfo).
   * User-facing `FT.HYBRID WITHCURSOR` is not supported. */
  bool isHybrid;
#endif
} CursorTimeoutInfo;

/**
 * Peek at an idle cursor's cached query-timeout and timeout-policy without
 * taking ownership. Values are captured at AREQ creation and frozen onto the
 * cursor at AREQ_StartCursor; reading them here (instead of live RSGlobalConfig)
 * keeps the cursor's timeout configuration frozen for the life of the cursor.
 *
 * Concurrency: the cursor-list lock is held only for the khash lookup and a
 * single scalar read of each write-once field.
 */
CursorTimeoutInfo Cursors_PeekTimeoutInfo(CursorList *cl, uint64_t cid);

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

/**
 * Mark all active cursors as potentially inaccurate due to ASM trimming.
 */
void CursorList_MarkASMInaccuracy();

void Cursors_RenderStatsForInfo(CursorList *cl, CursorList *cl_coord, const IndexSpec *spec, RedisModuleInfoCtx *ctx);

#define getCursorList(coord) ((coord) ? &g_CursorsListCoord : &g_CursorsList)

#ifdef __cplusplus
}
#endif
#endif // CURSOR_H
