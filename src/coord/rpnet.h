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
#include "rs_wall_clock.h"

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

  // Breakdown of where this RP's wall time goes, accumulated only when the request is
  // profiled (see `profileBreakdown`). The Network RP dominates coordinator time on heavy
  // distributed aggregations, but its single "Time" figure conflates three very different
  // costs, so attributing it was guesswork. Splitting them shows whether the coordinator is
  // starved by the shards, spending its own CPU materializing rows, or paying for the
  // reply tree's destruction.
  struct {
    // Blocked popping the next shard reply off the channel: shard execution plus network
    // plus IO-thread parse latency. Not coordinator work.
    rs_wall_clock_ns_t waitTime;
    // Turning a reply's rows into lookup rows - MRReply to RSValue conversion and the
    // by-name row writes. Coordinator CPU, and where per-field allocation lands. Note
    // that MRReply_ToValue frees each value node as it consumes it, so the release of
    // value nodes is counted here rather than in `freeTime`.
    rs_wall_clock_ns_t convertTime;
    // Freeing exhausted reply trees, on the path that retires a fully consumed reply.
    // Excludes the error path and RP teardown, which are not per-reply costs. Since value
    // nodes are already gone (see `convertTime`), what this releases is the row and field
    // arrays plus one string node per field *name* - names that every row repeats.
    rs_wall_clock_ns_t freeTime;
    // Shard replies popped, and field values converted, over the request's lifetime.
    uint64_t replies;
    uint64_t fields;
  } breakdown;
  // Decoder state for a compact row block (see src/aggregate/row_block.h), when the shard
  // sent one instead of per-row RESP maps. Valid only while `current.rows` holds a block.
  struct {
    // Borrowed into the block buffer owned by `current.rows`; not freed here.
    const char *cur;
    const char *end;
    // Schema columns resolved once per block, so per-row writes go by key instead of by
    // name. Sized `ncols`, owned by the RPNet and reused across blocks.
    const RLookupKey **cols;
    uint16_t ncols;
    uint16_t colsCap;
    bool active;
  } block;

  // Whether to maintain `breakdown`. Timing costs two clock reads per row, so it is
  // confined to profiled requests. Deliberately per-row and not per-field: a clock pair
  // per field cost ~25% of query wall time on a wide distributed aggregation, enough to
  // make the profiled run unrepresentative of the unprofiled one.
  bool profileBreakdown;
} RPNet;


void rpnetFree(ResultProcessor *rp);

// Append the Network RP's time breakdown to the profile map already opened for it. A
// no-op when the request was not profiled.
void RPNet_ReplyProfileBreakdown(RedisModule_Reply *reply, const ResultProcessor *rp);
RPNet *RPNet_New(const MRCommand *cmd, int (*nextFunc)(ResultProcessor *, SearchResult *));
void RPNet_resetCurrent(RPNet *nc);
int rpnetNext(ResultProcessor *self, SearchResult *r);
int rpnetNext_EOF(ResultProcessor *self, SearchResult *r);

// Get the next reply from the channel.
// Return RS_RESULT_OK if there is a next reply to process, RS_RESULT_EOF if there are no more replies
// Or RS_RESULT_TIMEDOUT if we timed out
int getNextReply(RPNet *nc);

#ifdef __cplusplus
}
#endif
