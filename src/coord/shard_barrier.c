/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "shard_barrier.h"
#include "rmr/rmr.h"
#include "rmalloc.h"

// Free a ShardResponseBarrier - used as destructor callback for MRIterator
void shardResponseBarrier_Free(void *ptr) {
  ShardResponseBarrier *barrier = (ShardResponseBarrier *)ptr;
  if (barrier) {
    rm_free(barrier->shardResponded);
    rm_free(barrier);
  }
}

// Initialize ShardCountBarrier base fields (called from iterStartCb when
// topology is known)
// This is a generic init function that can be used as privateDataInit callback
// when the privateData starts with a ShardCountBarrier (or is a
// ShardCountBarrier*)
void shardCountBarrier_Init(void *ptr, MRIterator *it) {
  ShardCountBarrier *barrier = (ShardCountBarrier *)ptr;
  if (!barrier || !it) {
    return;
  }

  size_t numShards = MRIterator_GetNumShards(it);
  // Use atomic_store (not atomic_init) because coord thread may already be
  // calling atomic_load on numShards concurrently
  atomic_store(&barrier->numShards, numShards);
}

// Allocate and initialize a new ShardResponseBarrier
// Notice: numShards and shardResponded init is postponed until NumShards is
// known
// Returns NULL on allocation failure
ShardResponseBarrier *shardResponseBarrier_New(void) {
  ShardResponseBarrier *barrier = rm_calloc(1, sizeof(ShardResponseBarrier));
  if (!barrier) {
    return NULL;
  }

  // numShards is initialized to 0 here and later updated via atomic_store in
  // shardResponseBarrier_Init when the actual shard count is known.
  // We must use atomic_init here (not rely on calloc zeroing)
  // because the coord thread may call atomic_load on numShards before
  // shardResponseBarrier_Init runs.
  atomic_init(&barrier->base.numShards, 0);
  atomic_init(&barrier->base.numResponded, 0);
  atomic_init(&barrier->accumulatedTotal, 0);
  atomic_init(&barrier->hasShardError, false);

  // Set the callback for processing replies in IO threads
  barrier->notifyCallback = shardResponseBarrier_Notify;

  return barrier;
}

// Initialize ShardResponseBarrier (called from iterStartCb when topology is
// known)
// This initializes both the base ShardCountBarrier and the shardResponded array
void shardResponseBarrier_Init(void *ptr, MRIterator *it) {
  ShardResponseBarrier *barrier = (ShardResponseBarrier *)ptr;
  if (!barrier || !it) {
    return;
  }

  size_t numShards = MRIterator_GetNumShards(it);
  barrier->shardResponded = rm_calloc(numShards, sizeof(*barrier->shardResponded));
  if (barrier->shardResponded) {
    // rm_calloc already zero-initializes, so all elements are false
    // Set numShards only after successful allocation to prevent
    // shardResponseBarrier_Notify from accessing NULL shardResponded array
    // Use atomic_store (not atomic_init) because coord thread may already be
    // calling atomic_load on numShards concurrently in getNextReply()
    atomic_store(&barrier->base.numShards, numShards);
  }
  // If allocation failed, numShards remains 0 (from atomic_init in
  // shardResponseBarrier_New) so Notify callback won't try to access the NULL
  // shardResponded array
}

// Callback invoked by IO thread for each shard reply to accumulate totals
// This function implements the ReplyNotifyCallback signature
void shardResponseBarrier_Notify(uint16_t shardIndex, long long totalResults,
                                 bool isError, void *privateData) {
  ShardResponseBarrier *barrier = (ShardResponseBarrier *)privateData;

  // Validate shardId bounds
  size_t numShards = atomic_load(&barrier->base.numShards);
  if (shardIndex >= numShards) {
    return;
  }

  // Check if this is the first response from this shard
  // No atomic needed - only one IO thread accesses shardResponded for this barrier
  if (!barrier->shardResponded[shardIndex]) {
    barrier->shardResponded[shardIndex] = true;
    if (!isError) {
      atomic_fetch_add(&barrier->accumulatedTotal, totalResults);
    } else {
      atomic_store(&barrier->hasShardError, true);
    }
    atomic_fetch_add(&barrier->base.numResponded, 1);
  }
}

