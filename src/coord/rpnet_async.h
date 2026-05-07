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
#include <pthread.h>

#include "rpnet.h"
#include "result_processor.h"
#include "query_error.h"
#include "util/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations
struct RLookup;
struct SearchResult;

/**
 * Async re-dispatchable version of RPNet's I/O draining logic.
 * Runs as a work item on the coordinator thread pool. When the channel is empty
 * and shards are still pending, the task yields the thread back to the pool and
 * is re-dispatched when new data arrives.
 */
typedef struct RPNetAsync {
  // Reference to the original RPNet (not owned). Provides access to MRIterator,
  // cmd, areq, shardResponseBarrier, etc.
  RPNet *rpnet;

  // Lookup used for deserializing reply rows into SearchResult fields
  struct RLookup *lookup;

  // Buffer: accumulated deserialized results (heap-allocated SearchResults)
  arrayof(struct SearchResult *) buffer;

  // Profile data collected during async draining (transferred to RPNet after completion)
  arrayof(MRReply *) shardsProfile;

  // Accumulated total_results count from shard replies
  long long totalResults;

  // Final status: RS_RESULT_EOF, RS_RESULT_TIMEDOUT, or RS_RESULT_ERROR
  int lastRc;

  // Error details (valid when lastRc == RS_RESULT_ERROR)
  QueryError error;

  // Completion signaling
  pthread_mutex_t mutex;
  pthread_cond_t done_cond;
  bool complete;

  // Yield/resume handoff (lock-free)
  _Atomic(bool) waiting;
} RPNetAsync;

/**
 * Trivial synchronous ResultProcessor backed by a pre-filled buffer
 * of SearchResults. No blocking, no I/O.
 */
typedef struct RPBufferedSource {
  ResultProcessor base;
  struct SearchResult **buffer;
  size_t bufferLen;
  size_t curIdx;
  int lastRc;
} RPBufferedSource;

// --- RPNetAsync API ---

// Allocate and initialize a new RPNetAsync.
// `rpnet` is borrowed (not owned) — caller must ensure it outlives the async task.
RPNetAsync *RPNetAsync_New(RPNet *rpnet, struct RLookup *lookup);

// Submit the async task to the coordinator thread pool.
void RPNetAsync_Start(RPNetAsync *self);

// Block the calling thread until the async task completes.
void RPNetAsync_WaitForCompletion(RPNetAsync *self);

// Notify that new data is available in the channel.
// Called from the IO thread after MRChannel_Push. If the task is yielded,
// this re-dispatches it to the thread pool.
void RPNetAsync_NotifyDataAvailable(RPNetAsync *self);

// Free the RPNetAsync structure and any owned resources.
// Does NOT free the referenced RPNet or the buffer contents (ownership transferred out).
void RPNetAsync_Free(RPNetAsync *self);

// --- RPBufferedSource API ---

// Create a new RPBufferedSource. Takes ownership of the buffer array.
RPBufferedSource *RPBufferedSource_New(struct SearchResult **buffer, size_t len, int lastRc);

#ifdef __cplusplus
}
#endif
