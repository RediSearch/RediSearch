/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <stdatomic.h>
#include "rpnet.h"
#include "rmr/reply.h"
#include "rmr/rmr.h"
#include "hiredis/sds.h"
#include "dist_utils.h"
#include "coord/src/coord_module.h"


#define CURSOR_EOF 0


RSValue *MRReply_ToValue(MRReply *r) {
  if (!r) return RS_NullVal();
  RSValue *v = NULL;
  switch (MRReply_Type(r)) {
    case MR_REPLY_STATUS:
    case MR_REPLY_STRING: {
      size_t l;
      const char *s = MRReply_String(r, &l);
      v = RS_NewCopiedString(s, l);
      break;
    }
    case MR_REPLY_ERROR: {
      double d = 42;
      MRReply_ToDouble(r, &d);
      v = RS_NumVal(d);
      break;
    }
    case MR_REPLY_INTEGER:
      v = RS_NumVal((double)MRReply_Integer(r));
      break;
    case MR_REPLY_DOUBLE:
      v = RS_NumVal(MRReply_Double(r));
      break;
    case MR_REPLY_MAP: {
      size_t n = MRReply_Length(r);
      RS_LOG_ASSERT(n % 2 == 0, "map of odd length");
      RSValue **map = rm_malloc(n * sizeof(*map));
      for (size_t i = 0; i < n; ++i) {
        MRReply *e = MRReply_ArrayElement(r, i);
        if (i % 2 == 0) {
          RS_LOG_ASSERT(MRReply_Type(e) == MR_REPLY_STRING, "non-string map key");
        }
        map[i] = MRReply_ToValue(e);
      }
      v = RSValue_NewMap(map, n / 2);
      break;
    }
    case MR_REPLY_ARRAY: {
      size_t n = MRReply_Length(r);
      RSValue **arr = RSValue_AllocateArray(n);
      for (size_t i = 0; i < n; ++i) {
        arr[i] = MRReply_ToValue(MRReply_ArrayElement(r, i));
      }
      v = RSValue_NewArray(arr, n);
      break;
    }
    case MR_REPLY_NIL:
      v = RS_NullVal();
      break;
    default:
      v = RS_NullVal();
      break;
  }
  return v;
}

// Free a ShardResponseBarrier - used as destructor callback for MRIterator
void shardResponseBarrier_Free(void *ptr) {
  ShardResponseBarrier *barrier = (ShardResponseBarrier *)ptr;
  if (barrier) {
    rm_free(barrier->shardResponded);
    rm_free(barrier);
  }
}

// Allocate and initialize a new ShardResponseBarrier
// Notice: numShards and shardResponded init is postponed until NumShards is known
// Returns NULL on allocation failure
ShardResponseBarrier *shardResponseBarrier_New() {
  ShardResponseBarrier *barrier = rm_calloc(1, sizeof(ShardResponseBarrier));
  if (!barrier) {
    return NULL;
  }

  // numShards is initialized to 0 here and later updated via atomic_store in
  // shardResponseBarrier_Init when the actual shard count is known.
  // We must use atomic_init here (not rely on calloc zeroing)
  // because the coord thread may call atomic_load on numShards before
  // shardResponseBarrier_Init runs.
  atomic_init(&barrier->numShards, 0);
  atomic_init(&barrier->numResponded, 0);
  atomic_init(&barrier->accumulatedTotal, 0);
  atomic_init(&barrier->hasShardError, false);

  // Set the callback for processing replies in IO threads
  barrier->notifyCallback = shardResponseBarrier_Notify;

  return barrier;
}

// Initialize ShardResponseBarrier (called from iterStartCb when topology is known)
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
    atomic_store(&barrier->numShards, numShards);
  }
  // If allocation failed, numShards remains 0 (from atomic_init in shardResponseBarrier_New)
  // so Notify callback won't try to access the NULL shardResponded array
}

// Callback invoked by IO thread for each shard reply to accumulate totals
// This function implements the ReplyNotifyCallback signature
void shardResponseBarrier_Notify(int16_t shardId, long long totalResults, bool isError, void *privateData) {
  ShardResponseBarrier *barrier = (ShardResponseBarrier *)privateData;

  // Validate shardId bounds
  size_t numShards = atomic_load(&barrier->numShards);
  if (shardId < 0 || shardId >= (int16_t)numShards) {
    return;
  }

  // Check if this is the first response from this shard
  // No atomic needed - only one IO thread accesses shardResponded for this barrier
  if (!barrier->shardResponded[shardId]) {
    barrier->shardResponded[shardId] = true;
    if (!isError) {
      atomic_fetch_add(&barrier->accumulatedTotal, totalResults);
    } else {
      atomic_store(&barrier->hasShardError, true);
    }
    atomic_fetch_add(&barrier->numResponded, 1);
  }
}

static void shardResponseBarrier_UpdateTotalResults(RPNet *nc) {
  // Set the accumulated total now that all shards have responded
  // numShards == 0 means IO thread never initialized the barrier (timeout before init)
  size_t numResponded = atomic_load(&nc->shardResponseBarrier->numResponded);
  size_t numShards = atomic_load(&nc->shardResponseBarrier->numShards);
  if (numShards > 0 && numResponded >= numShards) {
    long long accumulatedTotal = atomic_load(&nc->shardResponseBarrier->accumulatedTotal);
    nc->base.parent->totalResults = accumulatedTotal;
  }
}

static void shardResponseBarrier_PendingReplies_Free(RPNet *nc) {
  if (nc->pendingReplies) {
    array_foreach(nc->pendingReplies, reply, MRReply_Free(reply));
    array_free(nc->pendingReplies);
    nc->pendingReplies = NULL;
  }
}

// Get absolute timeout for MRIterator_NextWithTimeout
// Returns pointer to the CLOCK_MONOTONIC_RAW based timeout, or NULL if not available
static struct timespec *getAbsTimeout(RPNet *nc) {
  if (!nc->areq || !nc->areq->sctx) {
    return NULL;
  }
  return (struct timespec *)&nc->areq->sctx->timeout;
}

// Handle timeout (not enough shards responded) only if there were no errors
// Also handles the case where numShards == 0 (IO thread never initialized barrier)
static bool shardResponseBarrier_HandleTimeout(RPNet *nc) {
  size_t numShards = atomic_load(&nc->shardResponseBarrier->numShards);
  size_t numResponded = atomic_load(&nc->shardResponseBarrier->numResponded);
  // Timeout if: barrier not initialized (numShards == 0) OR not all shards responded
  if (!(atomic_load(&nc->shardResponseBarrier->hasShardError)) &&
      (numShards == 0 || numResponded < numShards)) {
    // cleanup pending replies
    shardResponseBarrier_PendingReplies_Free(nc);

    // Set timeout error in AREQ context
    QueryError_SetError(
      nc->areq->qiter.err,
      QUERY_ETIMEDOUT,
      "ShardResponseBarrier: Timeout while waiting for first responses from all shards");
    return true;
  }
  return false;
}

// Helper function to check for shard errors and keep only the first error reply
// Returns true if an error was found and set in nc->current.root, false otherwise
static bool shardResponseBarrier_HandleError(RPNet *nc) {
  // Check if any shard returned an error during the waiting period
  if (atomic_load(&nc->shardResponseBarrier->hasShardError)) {
    // Find the first error reply in pendingReplies and return it
    if (nc->pendingReplies) {
      for (size_t i = 0; i < array_len(nc->pendingReplies); i++) {
        MRReply *reply = nc->pendingReplies[i];
        if (MRReply_Type(reply) == MR_REPLY_ERROR) {
          // Move error reply to current
          nc->current.root = reply;
          array_del(nc->pendingReplies, i);
          shardResponseBarrier_PendingReplies_Free(nc);
          return true;  // Error found
        }
      }
    }
  }
  return false;  // No error
}

int getNextReply(RPNet *nc) {
  // Wait for all shards' first responses before returning any results
  // This ensures accurate total_results from the start
  MRReply *root = NULL;
  if (nc->shardResponseBarrier && !nc->waitedForAllShards) {
    // Get at least 1 response from each shard
    // Notice: numShards is re-read on each iteration because it may initially be 0
    // (in case the IO thread iterStartCb did not run yet and did not initialize the barrier yet).
    // Once a reply arrives, iterStartCb has finished and numShards will be set.
    size_t numShards;
    while ((numShards = atomic_load(&nc->shardResponseBarrier->numShards)) == 0 ||
           atomic_load(&nc->shardResponseBarrier->numResponded) < numShards) {
      // Check for timeout to avoid blocking indefinitely
      if (nc->areq && nc->areq->sctx && TimedOut(&nc->areq->sctx->timeout)) {
        break;
      }
      // Get next reply with timeout (uses CLOCK_MONOTONIC_RAW based timeout)
      bool nextTimedOut = false;
      MRReply *reply = MRIterator_NextWithTimeout(nc->it, getAbsTimeout(nc), &nextTimedOut);
      if (reply == NULL) {
        break;  // No more replies or timed out
      }

      // Store reply for later processing
      if (!nc->pendingReplies) {
        nc->pendingReplies = array_new(MRReply *, numShards);
      }
      array_append(nc->pendingReplies, reply);

      // Check for errors
      if (shardResponseBarrier_HandleError(nc)) {
        nc->waitedForAllShards = true;
        return RS_RESULT_OK;
      }
    }

    // Mark that we've waited (even if not all shards responded due to time out - to avoid infinite loop)
    nc->waitedForAllShards = true;

    // Handle timeout or not enough shards responded
    if (shardResponseBarrier_HandleTimeout(nc)) {
      return RS_RESULT_TIMEDOUT;
    }
    shardResponseBarrier_UpdateTotalResults(nc);
  }

    // First, return any pending replies collected during the wait
  if (nc->pendingReplies && array_len(nc->pendingReplies) > 0) {
    // Pop the first pending reply
    root = nc->pendingReplies[0];
    array_del(nc->pendingReplies, 0);
  } else {
    // No pending replies, get from channel
    if (nc->cmd.forCursor) {
      // if there are no more than `clusterConfig.cursorReplyThreshold` replies, trigger READs at the shards.
      // TODO: could be replaced with a query specific configuration
      if (!MR_ManuallyTriggerNextIfNeeded(nc->it, clusterConfig.cursorReplyThreshold)) {
        // No more replies
        RPNet_resetCurrent(nc);
        return RS_RESULT_EOF;
      }
    }
    root = MRIterator_Next(nc->it);
  }

  if (root == NULL) {
    // No more replies
    RPNet_resetCurrent(nc);
    return MRIterator_GetPending(nc->it) ? RS_RESULT_OK : RS_RESULT_EOF;
  }

  // Check if an error was returned
  if(MRReply_Type(root) == MR_REPLY_ERROR) {
    nc->current.root = root;
    return RS_RESULT_OK;
  }

  MRReply *rows = MRReply_ArrayElement(root, 0);
  // Perform sanity check to avoid processing empty replies
  bool is_empty;
  if (nc->cmd.protocol == 3) { // RESP3
    MRReply *results = MRReply_MapElement(rows, "results");
    is_empty = MRReply_Length(results) == 0;
  } else { // RESP2
    is_empty = MRReply_Length(rows) == 1;
  }

  if (is_empty) {
    MRReply_Free(root);
    root = NULL;
    rows = NULL;
    RedisModule_Log(RSDummyContext, "verbose", "An empty reply was received from a shard");
  }

  // invariant: either rows == NULL or least one row exists

  nc->current.root = root;
  nc->current.rows = rows;

  RS_ASSERT(!nc->current.rows
         || MRReply_Type(nc->current.rows) == MR_REPLY_ARRAY
         || MRReply_Type(nc->current.rows) == MR_REPLY_MAP);
  return RS_RESULT_OK;
}

void rpnetFree(ResultProcessor *rp) {
  RPNet *nc = (RPNet *)rp;

  // Note: shardResponseBarrier is freed by MRIterator_Free via the destructor callback
  // but pendingReplies must be freed by RPNet since it's used only in rpnetNext.
  // This ensures barrier is not freed while I/O callbacks may still be accessing it.

  // Free any pending replies that weren't consumed
  shardResponseBarrier_PendingReplies_Free(nc);

  if (nc->it) {
    RS_DEBUG_LOG("rpnetFree: calling MRIterator_Release");
    MRIterator_Release(nc->it);
  }

  if (nc->shardsProfile) {
    array_foreach(nc->shardsProfile, reply, {
      if (reply != nc->current.root) {
        MRReply_Free(reply);
      }
    });
    array_free(nc->shardsProfile);
  }

  MRReply_Free(nc->current.root);
  MRCommand_Free(&nc->cmd);

  rm_free(rp);
}

int rpnetNext_Start(ResultProcessor *rp, SearchResult *r) {
  RPNet *nc = (RPNet *)rp;

  // Initialize shard response barrier if WITHCOUNT is enabled
  if (HasWithCount(nc->areq) && IsAggregate(nc->areq)) {
    ShardResponseBarrier *barrier = shardResponseBarrier_New();
    if (!barrier) {
      return RS_RESULT_ERROR;
    }
    nc->shardResponseBarrier = barrier;
  }

  // Pass barrier as private data to callback (only if WITHCOUNT enabled)
  // The barrier is freed by MRIterator via shardResponseBarrier_Free destructor
  // shardResponseBarrier_Init is called from iterStartCb when numShards is known from topology
  MRIterator *it = nc->shardResponseBarrier
                   ? MR_IterateWithPrivateData(&nc->cmd, netCursorCallback, nc->shardResponseBarrier,
                                               shardResponseBarrier_Free, shardResponseBarrier_Init,
                                               iterStartCb, NULL)
                   : MR_Iterate(&nc->cmd, netCursorCallback);

  if (!it) {
    // Clean up on error - iterator never started so no callbacks running
    // Must free manually since iterator didn't take ownership
    if (nc->shardResponseBarrier) {
      shardResponseBarrier_Free(nc->shardResponseBarrier);
      nc->shardResponseBarrier = NULL;
    }
    return RS_RESULT_ERROR;
  }

  nc->it = it;
  nc->base.Next = rpnetNext;
  return rpnetNext(rp, r);
}


RPNet *RPNet_New(const MRCommand *cmd) {
  RPNet *nc = rm_calloc(1, sizeof(*nc));
  nc->cmd = *cmd; // Take ownership of the command's internal allocations
  nc->areq = NULL;
  nc->shardsProfile = NULL;
  nc->base.Free = rpnetFree;
  nc->base.Next = rpnetNext_Start;
  nc->base.type = RP_NETWORK;
  return nc;
}

void RPNet_resetCurrent(RPNet *nc) {
    nc->current.root = NULL;
    nc->current.rows = NULL;
}

int rpnetNext(ResultProcessor *self, SearchResult *r) {
  RPNet *nc = (RPNet *)self;
  MRReply *root = nc->current.root, *rows = nc->current.rows;

  // root (array) has similar structure for RESP2/3:
  // [0] array of results (rows) described right below
  // [1] cursor (int)
  // Or
  // Simple error

  // If root isn't a simple error:
  // rows:
  // RESP2: [ num_results, [ field, value, ... ], ... ]
  // RESP3: { ..., "results": [ { field: value, ... }, ... ], ... }

  // can also get an empty row:
  // RESP2: [] or [ 0 ]
  // RESP3: {}

  if (rows) {
      bool resp3 = MRReply_Type(rows) == MR_REPLY_MAP;
      size_t len;
      if (resp3) {
        MRReply *results = MRReply_MapElement(rows, "results");
        RS_LOG_ASSERT(results, "invalid results record: missing 'results' key");
        len = MRReply_Length(results);
      } else {
        len = MRReply_Length(rows);
      }

      if (nc->curIdx == len) {
        bool timed_out = false;
        // Check for a warning (resp3 only)
        MRReply *warning = MRReply_MapElement(rows, "warning");
        if (resp3 && MRReply_Length(warning) > 0) {
          const char *warning_str = MRReply_String(MRReply_ArrayElement(warning, 0), NULL);
          // Set an error to be later picked up and sent as a warning
          if (!strcmp(warning_str, QueryError_Strerror(QUERY_ETIMEDOUT))) {
            timed_out = true;
          } else if (!strcmp(warning_str, QUERY_WMAXPREFIXEXPANSIONS)) {
            nc->areq->qiter.err->reachedMaxPrefixExpansions = true;
          }
          if (!strcmp(warning_str, QUERY_WINDEXING_FAILURE)) {
              nc->areq->qiter.bgScanOOM = true;
          }
        }

        long long cursorId = MRReply_Integer(MRReply_ArrayElement(root, 1));

        // in profile mode, save shard's profile info to be returned later
        if (cursorId == 0 && nc->shardsProfile) {
          array_ensure_append_1(nc->shardsProfile, root);
        } else {

          MRReply_Free(root);
        }
        root = rows = NULL;
        RPNet_resetCurrent(nc);

        if (timed_out) {
          return RS_RESULT_TIMEDOUT;
        }
      }
  }

  int new_reply = !root;

  // get the next reply from the channel
  while (!root || !rows || MRReply_Length(rows) == 0) {
    if(TimedOut(&self->parent->sctx->timeout)) {
      // Set the `timedOut` flag in the MRIteratorCtx, later to be read by the
      // callback so that a `CURSOR DEL` command will be dispatched instead of
      // a `CURSOR READ` command.
      MRIteratorCallback_SetTimedOut(MRIterator_GetCtx(nc->it));

      return RS_RESULT_TIMEDOUT;
    } else if (MRIteratorCallback_GetTimedOut(MRIterator_GetCtx(nc->it))) {
      // if timeout was set in previous reads, reset it
      MRIteratorCallback_ResetTimedOut(MRIterator_GetCtx(nc->it));
    }

    int ret = getNextReply(nc);
    if (ret == RS_RESULT_EOF) {
      return RS_RESULT_EOF;
    } else if (ret == RS_RESULT_TIMEDOUT) {
      MRIteratorCallback_SetTimedOut(MRIterator_GetCtx(nc->it));
      return RS_RESULT_TIMEDOUT;
    }

    // If an error was returned, propagate it
    if (nc->current.root && MRReply_Type(nc->current.root) == MR_REPLY_ERROR) {
      const char *strErr = MRReply_String(nc->current.root, NULL);
      if (!strErr
          || strcmp(strErr, "Timeout limit was reached")
          || nc->areq->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) {
        QueryError_SetError(nc->areq->qiter.err, QUERY_EGENERIC, strErr);
        return RS_RESULT_ERROR;
      } else {
        MRReply_Free(nc->current.root);
        RPNet_resetCurrent(nc);
      }
    }

    root = nc->current.root;
    rows = nc->current.rows;
  }

  // invariant: at least one row exists

  bool resp3 = MRReply_Type(rows) == MR_REPLY_MAP;
  if (new_reply) {
    if (resp3) { // RESP3
      nc->curIdx = 0;
      // Note: For WITHCOUNT in multi-shard aggregate, totalResults is already set
      // by the waiting logic above. We skip accumulation here to avoid double-counting.
      // For non-WITHCOUNT or single-shard cases, we still need to count.
      if (!nc->shardResponseBarrier) {
        MRReply *results = MRReply_MapElement(rows, "results");
        RS_LOG_ASSERT(results, "invalid results record: missing 'results' key");
        nc->base.parent->totalResults += MRReply_Length(results);
      }
    } else { // RESP2
      // Get the index from the first
      nc->curIdx = 1;
      // For WITHCOUNT in multi-shard aggregate, totalResults is already set
      // by the callback accumulation logic. Skip to avoid double-counting.
      if (!nc->shardResponseBarrier) {
        // Without WITHCOUNT, accumulate total_results from each shard reply
        nc->base.parent->totalResults += MRReply_Integer(MRReply_ArrayElement(rows, 0));
      }
    }
  }

  if (resp3) // RESP3
  {
    MRReply *results = MRReply_MapElement(rows, "results");
    RS_LOG_ASSERT(results && MRReply_Type(results) == MR_REPLY_ARRAY, "invalid results record");
    MRReply *result = MRReply_ArrayElement(results, nc->curIdx++);
    RS_LOG_ASSERT(result && MRReply_Type(result) == MR_REPLY_MAP, "invalid result record");
    MRReply *fields = MRReply_MapElement(result, "extra_attributes");
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_MAP, "invalid fields record");

    processResultFormat(&nc->areq->reqflags, rows);

    for (size_t i = 0; i < MRReply_Length(fields); i += 2) {
      size_t len;
      const char *field = MRReply_String(MRReply_ArrayElement(fields, i), &len);
      MRReply *val = MRReply_ArrayElement(fields, i + 1);
      RSValue *v = MRReply_ToValue(val);
      RLookup_WriteOwnKeyByName(nc->lookup, field, len, &r->rowdata, v);
    }
  }
  else // RESP2
  {
    MRReply *rep = MRReply_ArrayElement(rows, nc->curIdx++);
    for (size_t i = 0; i < MRReply_Length(rep); i += 2) {
      size_t len;
      const char *field = MRReply_String(MRReply_ArrayElement(rep, i), &len);
      RSValue *v = RS_NullVal();
      if (i + 1 < MRReply_Length(rep)) {
        MRReply *val = MRReply_ArrayElement(rep, i + 1);
        v = MRReply_ToValue(val);
      }
      RLookup_WriteOwnKeyByName(nc->lookup, field, len, &r->rowdata, v);
    }
  }
  return RS_RESULT_OK;
}
