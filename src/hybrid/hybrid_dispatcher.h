/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "redismodule.h"
#include "coord/rmr/rmr.h"
#include "hybrid/hybrid_request.h"
#include "query_error.h"
#include "util/references.h"

#ifdef __cplusplus
extern "C" {
#endif

// Cursor map structure for hybrid responses
typedef struct {
  long long search_cursor;
  long long vsim_cursor;
  bool has_search;
  bool has_vsim;
} HybridCursorMap;

// Hybrid dispatcher structure (shared coordination object)
typedef struct {
  // Hybrid-specific dispatch state
  volatile bool hybrid_dispatched;
  volatile bool setup_complete;
  MRIterator *it;
  MRCommand cmd;
  AREQ *areq;

  // Cursor lists from shard responses
  arrayof(long long) search_cursors;
  arrayof(long long) vsim_cursors;
  size_t num_shards;
} HybridDispatcher;

// Create a new HybridDispatcher (simplified for single-threaded use)
HybridDispatcher *HybridDispatcher_New(RedisSearchCtx *sctx, AREQ **requests, size_t nrequests);

// Free a HybridDispatcher
void HybridDispatcher_Free(HybridDispatcher *hd);

// Dispatch functionality
int hybridDispatcherNext_Start(HybridDispatcher *hd);

// Process one response at a time
int hybridDispatcherProcessResponse(HybridDispatcher *hd);

// Cursor map parsing
HybridCursorMap parseHybridCursorResponse(MRReply *rep);

#ifdef __cplusplus
}
#endif
