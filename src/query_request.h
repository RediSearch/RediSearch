/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef QUERY_REQUEST_H__
#define QUERY_REQUEST_H__

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "query_error.h"
#include "util/rs_atomic.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct RLookup RLookup;
typedef struct PLN_ArrangeStep PLN_ArrangeStep;
typedef struct ResultProcessor ResultProcessor;
typedef struct SearchResult SearchResult;
struct Cursor;

/** Cached variables used while serializing stored results. */
typedef struct {
  RLookup *lastLookup;
  const PLN_ArrangeStep *lastAstp;
} cachedVars;

/**
 * State retained while results wait for the main-thread reply callback.
 *
 * The cursor owns its request through the request wrapper, not vice versa.
 * The cursor pointer stored here is non-owning and exists only so the reply
 * callback can pause or free it after serialization determines whether the
 * cursor is depleted. It must be cleared after the cursor is handled.
 */
typedef struct {
  SearchResult **results;  // Aggregated results array (NULL if not stored)
  int rc;                  // Pipeline return code (RS_RESULT_OK, RS_RESULT_EOF, etc.)
  bool hasStoredResults;   // Whether results are available to the reply callback
  QueryError err;          // Error copied from the pipeline's temporary QueryError
  cachedVars cv;           // Cached lookup variables used during serialization
  struct Cursor *cursor;   // Non-owning cursor handle for the reply callback
  size_t limit;            // Original limit, used to calculate the RESP2 result length
} ChunkReplyState;

typedef enum {
  QUERY_REQUEST_KIND_AREQ,
  QUERY_REQUEST_KIND_HYBRID,
} QueryRequestKind;

typedef struct {
  uint64_t id;
} CursorInfo;

/**
 * Timeout-only synchronization between query workers and main-thread callbacks.
 * TODO($$$): Remove this temporary state after MOD-17486 is merged.
 */
typedef struct {
  bool requiresAggregateResultsSync;
  RS_Atomic(bool) aggregatingResults;
  bool aggregateResultsClaimLost;
  bool aggregateResultsDone;
  int safeLoadersHoldingGIL;
  // TODO($$$): Plug both primitives into the async synchronization paths later in this PR.
  pthread_mutex_t aggregateResultsLock;
  pthread_cond_t aggregateResultsCond;
} QueryRequestAsyncState;

typedef struct QueryRequest {
  QueryRequestKind kind;
  CursorInfo cursorInfo;
  ChunkReplyState reply;
  QueryRequestAsyncState async;
  /**
   * Transitional reference to the legacy QueryProcessingCtx.endProc slot.
   * The extra indirection makes changes to that slot immediately visible here,
   * without mirroring every pipeline mutation.
   * TODO($$$): Once QueryRequest owns endProc, replace this with ResultProcessor *.
   */
  ResultProcessor **endProcRef;
} QueryRequest;

static inline void QueryRequest_SetEndProcRef(QueryRequest *request,
                                              ResultProcessor **endProcRef) {
  request->endProcRef = endProcRef;
}

static inline ResultProcessor *QueryRequest_GetEndProc(const QueryRequest *request) {
  return request->endProcRef ? *request->endProcRef : NULL;
}

void QueryRequest_Init(QueryRequest *request, QueryRequestKind kind);
void QueryRequest_ResetReply(QueryRequest *request);
void QueryRequest_Destroy(QueryRequest *request);

#ifdef __cplusplus
}
#endif

#endif
