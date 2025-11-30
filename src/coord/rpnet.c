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
#include "coord/hybrid/dist_utils.h"


#define CURSOR_EOF 0


static RSValue *MRReply_ToValue(MRReply *r) {
  if (!r) return RSValue_NullStatic();
  RSValue *v = NULL;
  switch (MRReply_Type(r)) {
    case MR_REPLY_STATUS:
    case MR_REPLY_STRING: {
      size_t l;
      const char *s = MRReply_String(r, &l);
      v = RSValue_NewCopiedString(s, l);
      break;
    }
    case MR_REPLY_ERROR: {
      double d = 42;
      MRReply_ToDouble(r, &d);
      v = RSValue_NewNumber(d);
      break;
    }
    case MR_REPLY_INTEGER:
      v = RSValue_NewNumber((double)MRReply_Integer(r));
      break;
    case MR_REPLY_DOUBLE:
      v = RSValue_NewNumber(MRReply_Double(r));
      break;
    case MR_REPLY_MAP: {
      size_t n = MRReply_Length(r);
      RS_LOG_ASSERT(n % 2 == 0, "map of odd length");
      size_t map_len = n / 2;
      RSValueMap map = RSValueMap_AllocUninit(map_len);
      for (size_t i = 0; i < map_len; i++) {
        MRReply *e_k = MRReply_ArrayElement(r, i * 2);
        RS_LOG_ASSERT(MRReply_Type(e_k) == MR_REPLY_STRING, "non-string map key");
        MRReply *e_v = MRReply_ArrayElement(r, (i * 2) + 1);
        RSValueMap_SetEntry(&map, i,  MRReply_ToValue(e_k), MRReply_ToValue(e_v));
      }
      v = RSValue_NewMap(map);
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
      v = RSValue_NullStatic();
      break;
    default:
      v = RSValue_NullStatic();
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
// Returns NULL on allocation failure
ShardResponseBarrier *shardResponseBarrier_New(size_t numShards) {
  ShardResponseBarrier *barrier = rm_calloc(1, sizeof(ShardResponseBarrier));
  if (!barrier) {
    RedisModule_Log(RSDummyContext, "warning",
                    "ShardResponseBarrier: Failed to allocate for %zu shards", numShards);
    return NULL;
  }

  barrier->numShards = numShards;
  barrier->shardResponded = rm_calloc(numShards, sizeof(_Atomic(bool)));
  if (!barrier->shardResponded) {
    rm_free(barrier);
    RedisModule_Log(RSDummyContext, "warning",
                    "ShardResponseBarrier: Failed to allocate tracking array for %zu shards", numShards);
    return NULL;
  }

  atomic_init(&barrier->numResponded, 0);
  atomic_init(&barrier->accumulatedTotal, 0);
  atomic_init(&barrier->hasShardError, false);

  // Set the callback for processing replies in IO threads
  barrier->notifyCallback = shardResponseBarrier_Notify;

  return barrier;
}

// Callback invoked by IO thread for each shard reply to accumulate totals
// This function implements the ReplyNotifyCallback signature
void shardResponseBarrier_Notify(int16_t shardId, long long totalResults, bool isError, void *privateData) {
  ShardResponseBarrier *barrier = (ShardResponseBarrier *)privateData;

  // Validate shardId bounds
  if (shardId < 0 || shardId >= (int16_t)barrier->numShards) {
    // TODO: Remove this debug log
    RedisModule_Log(RSDummyContext, "warning",
                    "ShardResponseBarrier: Invalid shardId %d (numShards=%zu)", shardId, barrier->numShards);
    return;
  }

  // Check if this is the first response from this shard using atomic CAS
  bool expected = false;
  // Mark shard as responded, also for errors
  if (atomic_compare_exchange_strong(&barrier->shardResponded[shardId], &expected, true)) {
    // We won the race - this is the first response from this shard
    if (!isError) {
      atomic_fetch_add(&barrier->accumulatedTotal, totalResults);
    } else {
      atomic_store(&barrier->hasShardError, true);
    }
    size_t numResponded = atomic_fetch_add(&barrier->numResponded, 1) + 1;

    RedisModule_Log(RSDummyContext, "debug",
                    "ShardResponseBarrier: Shard %d first response: total=%lld (accumulated=%lld, responded=%zu/%zu, hasError=%d)",
                    shardId, totalResults,
                    atomic_load(&barrier->accumulatedTotal),
                    numResponded, barrier->numShards,
                    atomic_load(&barrier->hasShardError));
  }
}

static void shardResponseBarrier_UpdateTotalResults(RPNet *nc) {
  // Set the accumulated total now that all shards have responded
  size_t numResponded = atomic_load(&nc->shardResponseBarrier->numResponded);
  if (numResponded >= nc->shardResponseBarrier->numShards) {
    long long accumulatedTotal = atomic_load(&nc->shardResponseBarrier->accumulatedTotal);
    nc->base.parent->totalResults = accumulatedTotal;
    RedisModule_Log(RSDummyContext, "debug",
                    "ShardResponseBarrier: All %zu shards responded, total_results = %lld",
                    nc->shardResponseBarrier->numShards, accumulatedTotal);
  } else {
    RedisModule_Log(RSDummyContext, "warning",
                    "ShardResponseBarrier: Only %zu of %zu shards responded before returning results",
                    numResponded, nc->shardResponseBarrier->numShards);
  }
}

static void shardResponseBarrier_PendingReplies_Free(RPNet *nc) {
  if (nc->pendingReplies) {
    array_foreach(nc->pendingReplies, reply, MRReply_Free(reply));
    array_free(nc->pendingReplies);
    nc->pendingReplies = NULL;
  }
}

// Calculate absolute timeout for MRIterator_NextWithTimeout (using CLOCK_MONOTONIC)
// Converts from CLOCK_MONOTONIC_RAW based timeout to CLOCK_MONOTONIC
// Returns pointer to absTimeout if timeout is available, NULL otherwise
static struct timespec *calculateAbsTimeout(RPNet *nc, struct timespec *absTimeout) {
  if (!nc->areq || !nc->areq->sctx) {
    return NULL;
  }

  // Convert CLOCK_MONOTONIC_RAW based timeout to CLOCK_MONOTONIC
  // Get current time on both clocks and adjust
  struct timespec nowRaw, nowMono;
  clock_gettime(CLOCK_MONOTONIC_RAW, &nowRaw);
  clock_gettime(CLOCK_MONOTONIC, &nowMono);

  // Calculate remaining time from the original timeout
  struct timespec remaining;
  rs_timerdelta((struct timespec *)&nc->areq->sctx->time.timeout, &nowRaw, &remaining);

  // If already timed out, set a minimal timeout
  if (remaining.tv_sec < 0) {
    remaining.tv_sec = 0;
    remaining.tv_nsec = 0;
  }

  // Add remaining time to current CLOCK_MONOTONIC time
  rs_timeradd(&nowMono, &remaining, absTimeout);
  return absTimeout;
}

// Handle timeout (not enough shards responded) only if there were no errors
static bool shardResponseBarrier_HandleTimeout(RPNet *nc) {
  if (!(atomic_load(&nc->shardResponseBarrier->hasShardError)) &&
      atomic_load(&nc->shardResponseBarrier->numResponded) < nc->shardResponseBarrier->numShards) {
    // cleanup pending replies
    shardResponseBarrier_PendingReplies_Free(nc);

    // Set error in AREQ context
    QueryError_SetError(
      AREQ_QueryProcessingCtx(nc->areq)->err,
      QUERY_ERROR_CODE_TIMED_OUT,
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
          const char *error = MRReply_String(reply, NULL);
          RedisModule_Log(RSDummyContext, "warning",
                          "ShardResponseBarrier: Shard error during wait: %s", error);
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
  bool timedOut = false;
  if (nc->shardResponseBarrier && !nc->waitedForAllShards) {
    const size_t numShards = nc->shardResponseBarrier->numShards;

    // Calculate absolute timeout for MRIterator_NextWithTimeout
    struct timespec absTimeout;
    struct timespec *timeoutPtr = calculateAbsTimeout(nc, &absTimeout);

    // Collect replies until all shards have sent their first response
    // Also respect the query timeout to avoid blocking indefinitely
    while (atomic_load(&nc->shardResponseBarrier->numResponded) < numShards) {
      // Check for timeout to avoid blocking indefinitely
      if (nc->areq && nc->areq->sctx && TimedOut(&nc->areq->sctx->time.timeout)) {
        timedOut = true;
        RedisModule_Log(RSDummyContext, "debug",
                        "WITHCOUNT: Timeout while waiting for all shards' first responses");
        break;
      }

      if (nc->cmd.forCursor) {
        if (!MR_ManuallyTriggerNextIfNeeded(nc->it, clusterConfig.cursorReplyThreshold)) {
          break;  // No more replies available
        }
      }

      bool nextTimedOut = false;
      MRReply *reply = MRIterator_NextWithTimeout(nc->it, timeoutPtr, &nextTimedOut);
      if (reply == NULL) {
        if (nextTimedOut) {
          timedOut = true;
          RedisModule_Log(RSDummyContext, "debug",
                          "WITHCOUNT: MRIterator_NextWithTimeout timed out");
        }
        break;  // No more replies or timed out
      }

      // Store reply for later processing
      if (!nc->pendingReplies) {
        nc->pendingReplies = array_new(MRReply *, numShards);
      }
      array_append(nc->pendingReplies, reply);

      // Check for errors
      if (shardResponseBarrier_HandleError(nc)) {
        root = nc->current.root;
        break;
      }
    }

    // Mark that we've waited (even if not all shards responded due to time out - to avoid infinite loop)
    nc->waitedForAllShards = true;

    // Handle timeout or not enough shards responded
    if (timedOut) {
      shardResponseBarrier_HandleTimeout(nc);
      root = NULL;
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
      if (!MR_ManuallyTriggerNextIfNeeded(nc->it, clusterConfig.cursorReplyThreshold)) {
        RPNet_resetCurrent(nc);
        return 0;
      }
    }
    root = MRIterator_Next(nc->it);
  }

  if (root == NULL) {
    RPNet_resetCurrent(nc);
    return MRIterator_GetPending(nc->it);
  }

  // Check if an error was returned
  if (MRReply_Type(root) == MR_REPLY_ERROR) {
    nc->current.root = root;
    if (nc->cmd.forProfiling) {
      MRReply *error = MRReply_Clone(root);
      array_append(nc->shardsProfile, error);
    }
    return 1;
  }

  // For profile command, extract the profile data from the reply
  if (nc->cmd.forProfiling) {
    if (CURSOR_EOF == MRReply_Integer(MRReply_ArrayElement(root, 1))) {
      MRReply *profile_data;
      if (nc->cmd.protocol == 3) {
        MRReply *data = MRReply_ArrayElement(root, 0);
        profile_data = MRReply_TakeMapElement(data, "profile");
      } else {
        RS_ASSERT(nc->cmd.protocol == 2);
        RS_ASSERT(MRReply_Length(root) == 3);
        profile_data = MRReply_TakeArrayElement(root, 2);
      }
      array_append(nc->shardsProfile, profile_data);
    }
  }

  // Extract rows and meta from reply
  MRReply *rows = NULL, *meta = NULL;
  if (nc->cmd.protocol == 3) { // RESP3
    meta = MRReply_ArrayElement(root, 0);
    if (nc->cmd.forProfiling) {
      meta = MRReply_MapElement(meta, "results"); // profile has an extra level
    }
    rows = MRReply_MapElement(meta, "results");
  } else { // RESP2
    rows = MRReply_ArrayElement(root, 0);
  }

  // Debug log: print count after receiving data from shard
  size_t rows_count = MRReply_Length(rows);
  if (nc->cmd.protocol == 2 && rows_count > 0) {
    // RESP2: first element is the count
    long long total_count = MRReply_Integer(MRReply_ArrayElement(rows, 0));
    RedisModule_Log(RSDummyContext, "debug", "Nafraf: Shard reply: %lld total results, %zu rows in batch",
                    total_count, rows_count - 1);
  } else if (nc->cmd.protocol == 3) {
    long long total_count = MRReply_Integer(MRReply_MapElement(meta, "total_results"));
    RedisModule_Log(RSDummyContext, "debug", "Nafraf: Shard reply: %lld total results, %zu rows in batch",
                    total_count, rows_count);
  }

  const size_t empty_rows_len = nc->cmd.protocol == 3 ? 0 : 1; // RESP2 has the first element as the number of results.
  RS_ASSERT(rows && MRReply_Type(rows) == MR_REPLY_ARRAY);
  if (MRReply_Length(rows) <= empty_rows_len) {
    RedisModule_Log(RSDummyContext, "verbose", "An empty reply was received from a shard");
    MRReply_Free(root);
    root = NULL;
    rows = NULL;
    meta = NULL;
  }

  nc->current.root = root;
  nc->current.rows = rows;
  nc->current.meta = meta;
  return 1;
}

/**
 * Start function for RPNet with cursor mappings
 * Replaces rpnetNext_StartDispatcher
 */
int rpnetNext_StartWithMappings(ResultProcessor *rp, SearchResult *r) {
    RPNet *nc = (RPNet *)rp;

    CursorMappings *vsimOrSearch = StrongRef_Get(nc->mappings);
    // Mappings should already be populated by HybridRequest_executePlan
    if (!vsimOrSearch || array_len(vsimOrSearch->mappings) == 0) {
        RedisModule_Log(NULL, "error", "No cursor mappings available for RPNet");
        return REDISMODULE_ERR;
    }

    size_t idx_len;
    const char *idx = MRCommand_ArgStringPtrLen(&nc->cmd, 1, &idx_len);
    char *idx_copy = rm_strndup(idx, idx_len);
    MRCommand_Free(&nc->cmd);

    // Create cursor read command using the copied index name
    nc->cmd = MR_NewCommand(3, "_FT.CURSOR", "READ", idx_copy);
    nc->cmd.rootCommand = C_READ;
    nc->cmd.protocol = 3;
    rm_free(idx_copy);

    nc->it = MR_IterateWithPrivateData(&nc->cmd, netCursorCallback, NULL, NULL, iterCursorMappingCb, &nc->mappings);
    nc->base.Next = rpnetNext;

    return rpnetNext(rp, r);
}

void rpnetFree(ResultProcessor *rp) {
  RPNet *nc = (RPNet *)rp;

  // Note: shardResponseBarrier is freed by MRIterator_Free via the destructor callback
  // but pendingReplies must be freed by RPNet since it's used only in rpnetNext.
  // This ensures barrier is not freed while I/O callbacks may still be accessing it.

  // Free any pending replies that weren't consumed
  if (nc->pendingReplies) {
    shardResponseBarrier_PendingReplies_Free(nc->pendingReplies);
    nc->pendingReplies = NULL;
  }

  if (nc->it) {
    RS_DEBUG_LOG("rpnetFree: calling MRIterator_Release");
    MRIterator_Release(nc->it);
  }

  if (nc->shardsProfile) {
    array_foreach(nc->shardsProfile, reply, MRReply_Free(reply));
    array_free(nc->shardsProfile);
  }

  // NEW: Free cursor mappings
  if (nc->mappings.rm) {
    StrongRef_Release(nc->mappings);
  }
  MRReply_Free(nc->current.root);
  MRCommand_Free(&nc->cmd);

  rm_free(rp);
}


RPNet *RPNet_New(const MRCommand *cmd, int (*nextFunc)(ResultProcessor *, SearchResult *)) {
  RPNet *nc = rm_calloc(1, sizeof(*nc));
  nc->cmd = *cmd; // Take ownership of the command's internal allocations
  nc->areq = NULL;
  nc->shardsProfile = NULL;
  nc->base.Free = rpnetFree;
  nc->base.Next = nextFunc;
  nc->base.type = RP_NETWORK;
  return nc;
}

void RPNet_resetCurrent(RPNet *nc) {
    nc->current.root = NULL;
    nc->current.rows = NULL;
    nc->current.meta = NULL;
}

int rpnetNext(ResultProcessor *self, SearchResult *r) {
  RPNet *nc = (RPNet *)self;
  MRReply *root = nc->current.root, *rows = nc->current.rows;
  const bool resp3 = nc->cmd.protocol == 3;

  // root (array) has similar structure for RESP2/3:
  // [0] array of results (rows) described right below
  // [1] cursor (int)
  // Or
  // Simple error

  // If root isn't a simple error:
  // rows:
  // RESP2: [ num_results, [ field, value, ... ], ... ]
  // RESP3: [ { field: value, ... }, ... ]

  // can also get an empty row:
  // RESP2: [] or [ 0 ]
  // RESP3: {}

  if (rows) {
      size_t len = MRReply_Length(rows);

      if (nc->curIdx == len) {
        bool timed_out = false;
        // Check for a warning (resp3 only)
        if (resp3) {
          MRReply *warning = MRReply_MapElement(nc->current.meta, "warning");
          if (MRReply_Length(warning) > 0) {
            const char *warning_str = MRReply_String(MRReply_ArrayElement(warning, 0), NULL);
            // Set an error to be later picked up and sent as a warning
            if (!strcmp(warning_str, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT))) {
              timed_out = true;
            } else if (!strcmp(warning_str, QUERY_WMAXPREFIXEXPANSIONS)) {
              QueryError_SetReachedMaxPrefixExpansionsWarning(AREQ_QueryProcessingCtx(nc->areq)->err);
            } else if (!strcmp(warning_str, QUERY_WOOM_SHARD)) {
              QueryError_SetQueryOOMWarning(AREQ_QueryProcessingCtx(nc->areq)->err);
            }
            if (!strcmp(warning_str, QUERY_WINDEXING_FAILURE)) {
              AREQ_QueryProcessingCtx(nc->areq)->bgScanOOM = true;
            }
          }
        }

        MRReply_Free(root);
        root = rows = NULL;
        RPNet_resetCurrent(nc);

        if (timed_out) {
          return RS_RESULT_TIMEDOUT;
        }
      }
  }

  bool new_reply = !root;

  // get the next reply from the channel
  while (!root) {
    if (TimedOut(&nc->areq->sctx->time.timeout)) {
      // Set the `timedOut` flag in the MRIteratorCtx, later to be read by the
      // callback so that a `CURSOR DEL` command will be dispatched instead of
      // a `CURSOR READ` command.
      MRIteratorCallback_SetTimedOut(MRIterator_GetCtx(nc->it));

      return RS_RESULT_TIMEDOUT;
    } else if (MRIteratorCallback_GetTimedOut(MRIterator_GetCtx(nc->it))) {
      // if timeout was set in previous reads, reset it
      MRIteratorCallback_ResetTimedOut(MRIterator_GetCtx(nc->it));
    }

    if (!getNextReply(nc)) {
      return RS_RESULT_EOF;
    }

    // If an error was returned, propagate it
    if (nc->current.root && MRReply_Type(nc->current.root) == MR_REPLY_ERROR) {
      QueryErrorCode errCode = QueryError_GetCodeFromMessage(MRReply_String(nc->current.root, NULL));
      // TODO - use should_return_error after it is changed to support RequestConfig ptr
      if (errCode == QUERY_ERROR_CODE_GENERIC ||
          ((errCode == QUERY_ERROR_CODE_TIMED_OUT) && nc -> areq -> reqConfig.timeoutPolicy == TimeoutPolicy_Fail) ||
          ((errCode == QUERY_ERROR_CODE_OUT_OF_MEMORY) && nc -> areq -> reqConfig.oomPolicy == OomPolicy_Fail)) {
        // We need to pass the reply string as the error message, since the error code might be generic
        QueryError_SetError(AREQ_QueryProcessingCtx(nc->areq)->err, errCode,  MRReply_String(nc->current.root, NULL));
        return RS_RESULT_ERROR;
      } else {
        // Handle shards returning error unexpectedly
        // Might be from different Timeout/OOM policy (See MOD-10774)
        // Free the error reply before we override it and continue
        MRReply_Free(nc->current.root);
        // Set it as NULL avoid another free
        nc->current.root = NULL;
      }
    }

    root = nc->current.root;
    rows = nc->current.rows;
  }

  // invariant: at least one row exists
  if (new_reply) {
    if (resp3) { // RESP3
      nc->curIdx = 0;
      // Note: For WITHCOUNT in multi-shard aggregate, totalResults is already set
      // by the waiting logic above. We skip accumulation here to avoid double-counting.
      // For non-WITHCOUNT or single-shard cases, we still need to count.
      if (!HasWithCount(nc->areq) || !(nc->areq->reqflags & QEXEC_F_IS_AGGREGATE)) {
        // Without WITHCOUNT, count rows in batch for backward compatibility
        nc->base.parent->totalResults += MRReply_Length(rows);
      }
      processResultFormat(&nc->areq->reqflags, nc->current.meta);
    } else { // RESP2
      nc->curIdx = 1;
      // For WITHCOUNT in multi-shard aggregate, totalResults is already set
      // by the callback accumulation logic. Skip to avoid double-counting.
      if (!HasWithCount(nc->areq) || !(nc->areq->reqflags & QEXEC_F_IS_AGGREGATE)) {
        // Without WITHCOUNT, accumulate total_results from each shard reply
        nc->base.parent->totalResults += MRReply_Integer(MRReply_ArrayElement(rows, 0));
      }
    }
  }

  MRReply *score = NULL;
  MRReply *fields = MRReply_ArrayElement(rows, nc->curIdx++);
  if (resp3) {
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_MAP, "invalid result record");
    // extract score if it exists, WITHSCORES was specified
    score = MRReply_MapElement(fields, "score");
    fields = MRReply_MapElement(fields, "extra_attributes");
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_MAP, "invalid fields record");
  } else {
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_ARRAY, "invalid result record");
    RS_LOG_ASSERT(MRReply_Length(fields) % 2 == 0, "invalid fields record");
  }

  // The score is optional, in hybrid we need the score for the sorter and hybrid merger
  // We expect for it to exist in hybrid since we send WITHSCORES to the shard and we should use resp3
  // when opening shard connections
  if (score) {
    RS_LOG_ASSERT(MRReply_Type(score) == MR_REPLY_DOUBLE, "invalid score record");
    SearchResult_SetScore(r, MRReply_Double(score));
  }

  for (size_t i = 0; i < MRReply_Length(fields); i += 2) {
    size_t len;
    const char *field = MRReply_String(MRReply_ArrayElement(fields, i), &len);
    MRReply *val = MRReply_ArrayElement(fields, i + 1);
    RSValue *v = MRReply_ToValue(val);
    RLookup_WriteOwnKeyByName(nc->lookup, field, len, SearchResult_GetRowDataMut(r), v);
  }
  return RS_RESULT_OK;
}

int rpnetNext_EOF(ResultProcessor *self, SearchResult *r) {
  return RS_RESULT_EOF;
}
