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
  size_t index;
} RPNetReply;

// The phase guards ownership, not cancellation (the request owns timeout).
typedef enum { RPNET_READING, RPNET_DRAINING, RPNET_DRAINED } RPNetPhase;

// Metadata observed by Drain, to merge into caller-owned reply state.
// Never merge into live AREQ/QueryProcessingCtx while Next is active.
typedef struct {
  // Cumulative source count since construction, independent of BG query bookkeeping.
  // Cursor reply owners subtract their source count at cycle start. WITHCOUNT
  // uses its separately published shard total instead (this count remains zero).
  uint64_t sourceResults;
  uint32_t formatFlags;
  uint32_t stateFlags;
  bool hasFormat;
  bool bgScanOOM;
  QueryError error;
  arrayof(MRReply *) profiles;
} RPNetDrainMetadata;

typedef struct RPNet {
  ResultProcessor base;
  // Next claims one row under stateLock. After takeover only Drain owns this cursor.
  RPNetReply current;
  RPNetReply *pendingBatch;  // Rare-race mailbox: a private Next batch offered to active Drain.
  uint64_t sourceResults;    // Updated at batch admission under stateLock, then Drain-owned.
  RS_Atomic(bool) stateLock;
  RPNetPhase phase;
  // Published once after lookup, command, policies and hybrid mode are initialized.
  struct MRChannel *drainChannel;
  bool explainScores;                 // reqflags itself remains mutable on the Next path.
  RPNetDrainMetadata *drainMetadata;  // Allocated only on first active Drain.
  // Lookup - the rows are written in here
  RLookup *lookup;
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
int rpnetNext(ResultProcessor *self, SearchResult *r);
void RPNet_PublishIterator(RPNet *nc);
// Drain caller only, between Drain calls (including a LIMIT stop). Transfers the
// metadata; repeated takes return NULL until another Drain produces metadata.
RPNetDrainMetadata *RPNet_TakeDrainMetadata(RPNet *nc);
void RPNetDrainMetadata_Free(RPNetDrainMetadata *metadata);
int rpnetNext_EOF(ResultProcessor *self, SearchResult *r);

// Get the next reply from the channel.
// Return RS_RESULT_OK if there is a next reply to process, RS_RESULT_EOF if there are no more
// replies Or RS_RESULT_TIMEDOUT if we timed out
int getNextReply(RPNet *nc, RPNetReply *reply);

#ifdef __cplusplus
}
#endif
