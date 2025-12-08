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

// Forward declaration
struct ShardResponseBarrier;

// Callback invoked by IO thread for each reply, before pushing to channel
// Parameters:
//   shardId: which shard sent this reply
//   totalResults: extracted total_results from the reply (-1 if error or not found)
//   isError: true if this is an error reply
//   privateData: the ShardResponseBarrier passed via MRIteratorCallback_GetPrivateData
typedef void (*ReplyNotifyCallback)(int16_t shardId, long long totalResults, bool isError, void *privateData);

// Structure for collecting first responses from all shards
// Shared with I/O threads via MRIterator's privateData
// Safe to free after MRIterator_Release returns (all callbacks complete)
typedef struct ShardResponseBarrier {
  _Atomic(size_t) numShards;       // Total number of shards (written by IO thread, read by main thread)
  bool *shardResponded;            // Array: has each shard sent its first response? (IO thread only, no atomic needed)
  _Atomic(size_t) numResponded;    // Count of shards that have responded
  _Atomic(long long) accumulatedTotal;  // Sum of total_results from all shards
  _Atomic(bool) hasShardError;     // Set to true if any shard returns an error
  ReplyNotifyCallback notifyCallback;  // Callback for processing replies (called from IO thread)
} ShardResponseBarrier;

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

  // Pointer to shared barrier structure for collecting first responses from all shards (reference-counted)
  ShardResponseBarrier *shardResponseBarrier;  // NULL if not using WITHCOUNT

  // Pending replies while waiting for all shards' first responses
  arrayof(MRReply *) pendingReplies;   // Replies accumulated while waiting
  bool waitedForAllShards;             // True once all shards have sent their first response
} RPNet;


void rpnetFree(ResultProcessor *rp);
RPNet *RPNet_New(const MRCommand *cmd);
void RPNet_resetCurrent(RPNet *nc);
int rpnetNext(ResultProcessor *self, SearchResult *r);
int rpnetNext_EOF(ResultProcessor *self, SearchResult *r);

// Get the next reply from the channel.
// Return RS_RESULT_OK if there is a next reply to process, RS_RESULT_EOF if there are no more replies
// Or RS_RESULT_TIMEDOUT if we timed out
int getNextReply(RPNet *nc);

// Allocate and initialize a new ShardResponseBarrier
// Notice: numShards and shardResponded init is postponed until shardResponseBarrier_Init is called
// Returns NULL on allocation failure
ShardResponseBarrier *shardResponseBarrier_New();

// Initialize ShardResponseBarrier (called from iterStartCb when topology is known)
void shardResponseBarrier_Init(void *ptr, MRIterator *it);

// Free a ShardResponseBarrier - used as destructor callback for MRIterator
void shardResponseBarrier_Free(void *ptr);

// Callback for accumulating total_results from shard replies (called from IO thread)
void shardResponseBarrier_Notify(int16_t shardId, long long totalResults, bool isError, void *privateData);

#ifdef __cplusplus
}
#endif
