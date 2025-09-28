/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "module.h"
#include "config.h"
#include "result_processor.h"
#include "rmr/rmr.h"
#include "aggregate/aggregate.h"
#include "hybrid/hybrid_dispatcher.h"

#ifdef __cplusplus
extern "C" {
#endif

// NEW: Dispatch context for hybrid operations
typedef struct {
  StrongRef dispatcher_ref;  // Reference to the dispatcher this RPNet is associated with
  bool isSearch;  // true for search RPNet, false for vsim RPNet
  arrayof(CursorMapping *) mappings;  // The mappings this RPNet will take ownership of
} HybridDispatchContext;

typedef struct {
  ResultProcessor base;
  struct {
    MRReply *root;  // Root reply. We need to free this when done with the rows
    MRReply *rows;  // Array containing reply rows for quick access
    MRReply *meta;  // Metadata for the current reply, if any (RESP3)
  } current;
  // Lookup - the rows are written in here
  RLookup *lookup;
  size_t curIdx;
  MRIterator *it;
  MRCommand cmd;
  AREQ *areq;
  HybridDispatchContext *dispatchCtx;

  // profile vars
  arrayof(MRReply *) shardsProfile;
} RPNet;


void rpnetFree(ResultProcessor *rp);
RPNet *RPNet_New(const MRCommand *cmd, int (*nextFunc)(ResultProcessor *, SearchResult *));
void RPNet_SetDispatcher(RPNet *nc, HybridDispatcher *dispatcher);
void RPNet_resetCurrent(RPNet *nc);
int rpnetNext(ResultProcessor *self, SearchResult *r);

// NEW: Dispatch context management functions
HybridDispatchContext *HybridDispatchContext_New(StrongRef dispatcher_ref, bool isSearch);
void HybridDispatchContext_Free(HybridDispatchContext *ctx);
void RPNet_SetDispatchContext(RPNet *nc, StrongRef dispatcher_ref, bool isSearch);


#ifdef __cplusplus
}
#endif
