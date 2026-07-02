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
#include "hybrid/hybrid_cursor_mappings.h"

#ifdef __cplusplus
extern "C" {
#endif

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

  // NEW: Direct cursor mappings (no more dispatcher context)
  StrongRef mappings;  // Single mapping array per RPNet

  // profile vars
  arrayof(MRReply *) shardsProfile;

  // True when this is an async WITHCOUNT aggregate; total_results is
  // accumulated by withCountReplyCb on the IO thread, surfaced into
  // qctx->totalResults once at the start of Phase B by
  // executeAggregateDeferred, and preserved across cursor reads by
  // finishSendChunk.
  bool withCount;

  // Drain-only mode: rpnetNext pops already-queued replies without blocking
  // and maps timeouts to EOF. Set by the RETURN-STRICT timeout callback after
  // BG has exited the pipeline, so no concurrent reader - plain bool is safe.
  bool drainOnly;

  // KNN snapshot for SHARD_K_RATIO optimization in FT.AGGREGATE.
  // Populated by buildDistRPChain from the parsed VectorQuery on the main thread,
  // then used to initialize the iterator-owned AggregateKnnContext if needed.
  bool hasKnnContext;
  size_t knnQueryArgIndex;     // Index of query argument in MRCommand
  size_t knnOriginalK;         // K value from the parsed query
  double knnShardWindowRatio;  // SHARD_K_RATIO
  size_t knnKTokenPos;         // Byte offset of K within the query string
  size_t knnKTokenLen;         // Length of K token in bytes
} RPNet;


void rpnetFree(ResultProcessor *rp);
RPNet *RPNet_New(const MRCommand *cmd, int (*nextFunc)(ResultProcessor *, SearchResult *));
void RPNet_resetCurrent(RPNet *nc);
int rpnetNext(ResultProcessor *self, SearchResult *r);
int rpnetNext_EOF(ResultProcessor *self, SearchResult *r);
int rpnetNext_StartWithMappings(ResultProcessor *rp, SearchResult *r);

// Get the next reply from the channel.
// Return RS_RESULT_OK if there is a next reply to process, RS_RESULT_EOF if there are no more replies
// Or RS_RESULT_TIMEDOUT if we timed out
int getNextReply(RPNet *nc);

#ifdef __cplusplus
}
#endif
