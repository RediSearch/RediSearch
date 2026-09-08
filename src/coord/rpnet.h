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

#ifdef __cplusplus
extern "C" {
#endif

// Which FT.HYBRID subquery stream this RPNet consumes, if any. A hybrid RPNet
// reads a cursor stream whose per-shard commands are armed by the hybrid
// fan-out callback (see dist_hybrid.c), which also injects mapping-stage shard
// errors and warning strings into the stream — both need processing the plain
// aggregate stream never sees.
typedef enum {
  RPNET_HYBRID_NONE = 0,
  RPNET_HYBRID_SEARCH,
  RPNET_HYBRID_VSIM,
} RPNetHybridSubquery;

typedef struct {
  MRReply *root;
  MRReply *rows;
  MRReply *meta;
} RPNetReply;

typedef struct RPNet {
  ResultProcessor base;
  RPNetReply current;
  RS_Atomic(bool) stateLock;
  bool draining;
  bool drainEOF;
  struct MRChannel *drainChannel;
  int drainProtocol;
  bool drainProfiling;
  bool drainExplain;
  bool drainWithCount;
  RSTimeoutPolicy drainTimeoutPolicy;
  RSOomPolicy drainOomPolicy;
  RPNetHybridSubquery drainHybridSubquery;
  RLookup *drainLookup;
  RPNetReply drainCurrent;
  size_t drainIdx;
  // Drain-owned metadata is retained for the reply owner, never applied to AREQ here.
  arrayof(MRReply *) drainedReplies;
  uint64_t drainedCount;
  struct RPNet *owner;
  // Lookup - the rows are written in here
  RLookup *lookup;
  size_t curIdx;
  MRIterator *it;
  MRCommand cmd;
  AREQ *areq;

  // profile vars
  arrayof(MRReply *) shardsProfile;

  RPNetHybridSubquery hybridSubquery;

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
void RPNet_PublishIterator(RPNet *nc);
int rpnetNext_EOF(ResultProcessor *self, SearchResult *r);

// Get the next reply from the channel.
// Return RS_RESULT_OK if there is a next reply to process, RS_RESULT_EOF if there are no more
// replies Or RS_RESULT_TIMEDOUT if we timed out
int getNextReply(RPNet *nc);

#ifdef __cplusplus
}
#endif
