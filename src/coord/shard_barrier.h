/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Forward declaration
struct MRIterator;

// Base barrier for tracking shard response counts
// Used by both FT.AGGREGATE (via ShardResponseBarrier) and FT.HYBRID (directly)
// numShards is set atomically from IO thread when topology is known
typedef struct ShardCountBarrier {
  _Atomic(size_t) numShards;       // Total number of shards (written by IO thread, read by coordinator thread)
  _Atomic(size_t) numResponded;    // Count of shards that have responded
} ShardCountBarrier;

// Callback invoked by IO thread for each reply, before pushing to channel
// Parameters:
//   shardIndex: which shard sent this reply
//   totalResults: extracted total_results from the reply (-1 if error or not found)
//   isError: true if this is an error reply
//   privateData: the ShardResponseBarrier passed via MRIteratorCallback_GetPrivateData
typedef void (*ReplyNotifyCallback)(uint16_t shardIndex, long long totalResults, bool isError, void *privateData);

// Extended barrier for WITHCOUNT functionality in FT.AGGREGATE
// Extends ShardCountBarrier with additional fields for accumulating totals
// Shared with I/O threads via MRIterator's privateData
// Safe to free after MRIterator_Release returns (all callbacks complete)
typedef struct ShardResponseBarrier {
  ShardCountBarrier base;          // Base barrier with numShards and numResponded
  bool *shardResponded;            // Array: has each shard sent its first response? (IO thread only, no atomic needed)
  _Atomic(long long) accumulatedTotal;  // Sum of total_results from all shards
  _Atomic(bool) hasShardError;     // Set to true if any shard returns an error
  ReplyNotifyCallback notifyCallback;  // Callback for processing replies (called from IO thread)
} ShardResponseBarrier;

// Initialize ShardCountBarrier base fields (called from iterStartCb when topology is known)
// This is a generic init function that can be used as privateDataInit callback
// when the privateData starts with a ShardCountBarrier (or is a ShardCountBarrier*)
void shardCountBarrier_Init(void *ptr, struct MRIterator *it);

// Allocate and initialize a new ShardResponseBarrier
// Notice: numShards and shardResponded init is postponed until shardResponseBarrier_Init is called
// Returns NULL on allocation failure
ShardResponseBarrier *shardResponseBarrier_New(void);

// Initialize ShardResponseBarrier (called from iterStartCb when topology is known)
// This initializes both the base ShardCountBarrier and the shardResponded array
void shardResponseBarrier_Init(void *ptr, struct MRIterator *it);

// Free a ShardResponseBarrier - used as destructor callback for MRIterator
void shardResponseBarrier_Free(void *ptr);

// Callback for accumulating total_results from shard replies (called from IO thread)
void shardResponseBarrier_Notify(uint16_t shardIndex, long long totalResults, bool isError, void *privateData);

#ifdef __cplusplus
}
#endif

