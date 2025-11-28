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

// Separate structure for WITHCOUNT tracking that can be safely shared with I/O threads
// This structure is allocated separately and can outlive RPNet if callbacks are still running
typedef struct {
  uint32_t magic;                  // Magic number for validation (0xWITHC0UN)
  size_t numShards;                // Total number of shards
  _Atomic(bool) *shardResponded;   // Array: has each shard sent its first response?
  _Atomic(size_t) numResponded;    // Count of shards that have responded
  _Atomic(long long) accumulatedTotal;  // Sum of total_results from all shards
  _Atomic(int) refCount;           // Reference count for safe cleanup
} WithCountTracker;

#define WITHCOUNT_TRACKER_MAGIC 0x57495448  // "WITH" in hex

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

  // For WITHCOUNT: pointer to shared tracking structure (reference-counted)
  WithCountTracker *withCountTracker;  // NULL if not using WITHCOUNT

  // For WITHCOUNT: pending replies while waiting for all shards' first responses
  arrayof(MRReply *) pendingReplies;   // Replies accumulated while waiting
  bool waitedForAllShards;             // True once all shards have sent their first response
} RPNet;


void rpnetFree(ResultProcessor *rp);
RPNet *RPNet_New(const MRCommand *cmd, int (*nextFunc)(ResultProcessor *, SearchResult *));
void RPNet_resetCurrent(RPNet *nc);
int rpnetNext(ResultProcessor *self, SearchResult *r);
int rpnetNext_EOF(ResultProcessor *self, SearchResult *r);
int rpnetNext_StartWithMappings(ResultProcessor *rp, SearchResult *r);
int getNextReply(RPNet *nc);

#ifdef __cplusplus
}
#endif
