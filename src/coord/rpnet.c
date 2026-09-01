/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <stdint.h>
#include <string.h>

#ifdef ENABLE_ASSERT
#include "debug_commands.h" // IWYU pragma: keep
#endif

#include "value_ffi.h"
#include "rpnet.h"
#include "rmr/reply.h"
#include "rmr/rmr.h"
#include "coord/dist_utils.h"
#include "score_explain_mr.h"
#include "rmalloc.h"
#include "config.h"
#include "hybrid/hybrid_exec.h"  // SEARCH_SUFFIX / VSIM_SUFFIX
#include "module.h"
#include "query_error.h"
#include "query_error_ffi.h"
#include "query_flags.h"
#include "redismodule.h"
#include "result_processor.h"
#include "rmutil/rm_assert.h"
#include "search_result.h"
#include "util/timeout.h"


#define CURSOR_EOF 0

// Converts an MRReply to an RSValue, consuming the reply. String buffers can be
// transferred directly because hiredis uses the Redis module allocator.
static RSValue *MRReply_ToValue(MRReply *r) {
  if (!r) return RSValue_NullStatic();
  RSValue *v = NULL;
  switch (MRReply_Type(r)) {
    case MR_REPLY_STATUS:
    case MR_REPLY_STRING: {
      size_t l;
      char *s = MRReply_TakeString(r, &l);
      RS_ASSERT(l <= UINT32_MAX);
      v = RSValue_NewString(s, (uint32_t)l);
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
      RSValueMapBuilder *map = RSValue_NewMapBuilder(map_len);
      for (size_t i = 0; i < map_len; i++) {
        MRReply *e_k = MRReply_TakeArrayElement(r, i * 2);
        RS_LOG_ASSERT(MRReply_Type(e_k) == MR_REPLY_STRING, "non-string map key");
        MRReply *e_v = MRReply_TakeArrayElement(r, (i * 2) + 1);
        RSValue_MapBuilderSetEntry(map, i,  MRReply_ToValue(e_k), MRReply_ToValue(e_v));
      }
      v = RSValue_NewMapFromBuilder(map);
      break;
    }
    case MR_REPLY_ARRAY: {
      size_t n = MRReply_Length(r);
      RSValue **arr = RSValue_NewArrayBuilder(n);
      for (size_t i = 0; i < n; ++i) {
        arr[i] = MRReply_ToValue(MRReply_TakeArrayElement(r, i));
      }
      v = RSValue_NewArrayFromBuilder(arr, n);
      break;
    }
    case MR_REPLY_NIL:
      v = RSValue_NullStatic();
      break;
    default:
      v = RSValue_NullStatic();
      break;
  }
  MRReply_Free(r);
  return v;
}

// Wall-clock deadline pointer for MRIterator_NextWithTimeout. NULL unless the
// hybrid stream is running a clock-based timeout cycle.
//
// Hybrid streams only: a shard that never publishes its cursor mapping leaves
// this RPNet's placeholder unarmed — no reply and no error ever arrives, so
// under RETURN the deadline must bound the pop (it replaces the mapping-stage
// deadline of the old blocking cursor-setup wait). Plain aggregate streams
// keep the legacy RETURN semantics — wait beyond the deadline for in-flight
// shard replies, observing the timeout only at reply boundaries — so they get
// no in-band pop deadline.
static const struct timespec *getAbsTimeout(const RPNet *nc) {
  RS_ASSERT(nc->areq);
  if (nc->hybridSubquery == RPNET_HYBRID_NONE ||
      nc->areq->base.timeout.kind != QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE) {
    return NULL;
  }
  return QueryRequestTimeout_GetClockDeadline(&nc->areq->base.timeout);
}

// Process warnings from nc->current.meta (RESP3 only), then free reply and reset state.
// Warning handling requires nc->current.meta to be set. Cleanup is done regardless of protocol.
//
// Shard warnings are always recorded on the AREQ / QueryError so the reply
// emitter can surface them. A shard's TIMEDOUT warning additionally controls
// whether the coord pipeline should keep draining:
//   - TimeoutPolicy_ReturnStrict: keep draining the remaining shards. The
//     warning flag is forwarded via QEXEC_S_SHARD_TIMED_OUT_WARNING; the
//     coord's own deadline (handled by the strict timeout callback) is the
//     authoritative stop signal.
//   - TimeoutPolicy_Return / TimeoutPolicy_Fail: a shard timeout
//     bails the coord pipeline early by returning RS_RESULT_TIMEDOUT.
static int processWarningsAndCleanup(RPNet *nc, bool is_resp3) {
  bool shard_timed_out = false;
  // Check for warnings (resp3 only)
  if (is_resp3) {
    RS_ASSERT(nc->current.meta);
    MRReply *warning = MRReply_MapElement(nc->current.meta, "warning");
    size_t num_warnings = MRReply_Length(warning);
    // Iterate over all warnings in the array
    for (size_t i = 0; i < num_warnings; i++) {
      const char *warning_str = MRReply_String(MRReply_ArrayElement(warning, i), NULL);
      // Set an error to be later picked up and sent as a warning
      if (!strcmp(warning_str, QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT))) {
        RS_ASSERT(nc->areq);
        shard_timed_out = true;
        nc->areq->stateflags |= QEXEC_S_SHARD_TIMED_OUT_WARNING;
      } else if (!strcmp(warning_str, QUERY_WMAXPREFIXEXPANSIONS)) {
        QueryError_SetReachedMaxPrefixExpansionsWarning(AREQ_QueryProcessingCtx(nc->areq)->err);
      } else if (!strcmp(warning_str, QUERY_WOOM_SHARD)) {
        QueryError_SetQueryOOMWarning(AREQ_QueryProcessingCtx(nc->areq)->err);
      } else if (!strcmp(warning_str, QUERY_WINDEXING_FAILURE)) {
        RS_ASSERT(nc->areq);
        AREQ_QueryProcessingCtx(nc->areq)->bgScanOOM = true;
      } else if (!strcmp(warning_str, QUERY_ASM_INACCURATE_RESULTS)) {
        RS_ASSERT(nc->areq);
        nc->areq->stateflags |= QEXEC_S_ASM_TRIMMING_DELAY_TIMEOUT;
      }
    }
  }

  MRReply_Free(nc->current.root);
  RPNet_resetCurrent(nc);

  if (shard_timed_out && nc->areq->reqConfig.timeoutPolicy != TimeoutPolicy_ReturnStrict) {
    return RS_RESULT_TIMEDOUT;
  }

  return RS_RESULT_OK;
}

// True when a popped reply is a mapping-stage warning injected into a hybrid
// stream by the arming fan-out callback (see forwardWarnings in
// hybrid_cursor_mappings.c): a bare warning string. The stream's other replies
// are arrays (rows) or errors, so a top-level string is unambiguous.
static bool isHybridMappingWarning(const RPNet *nc, MRReply *root) {
  if (nc->hybridSubquery == RPNET_HYBRID_NONE) {
    return false;
  }
  const int type = MRReply_Type(root);
  return type == MR_REPLY_STRING || type == MR_REPLY_STATUS;
}

// Apply one mapping-stage warning from a shard's _FT.HYBRID reply to this
// subquery's state, mirroring what processWarningsAndCleanup does for
// row-reply warnings — with two mapping-stage differences: a shard timeout
// warning aborts only under FAIL (under RETURN the mapping succeeded and the
// reads proceed), and suffix-tagged max-prefix warnings are routed to the
// subquery the shard tagged. Returns RS_RESULT_OK to keep reading, or the
// result code to propagate.
static int processHybridMappingWarning(RPNet *nc, const char *warning_str) {
  QueryError *err = AREQ_QueryProcessingCtx(nc->areq)->err;
  // Suffix-tagged max-prefix warnings don't exact-match the warning-code
  // lookup below; match by prefix. The arming callback already routed them to
  // the subquery stream they are tagged with (see forwardWarnings).
  if (!strncmp(warning_str, QUERY_WMAXPREFIXEXPANSIONS, strlen(QUERY_WMAXPREFIXEXPANSIONS))) {
    QueryError_SetReachedMaxPrefixExpansionsWarning(err);
    return RS_RESULT_OK;
  }
  // The remaining producer set is fixed: replyWithCursors emits a timeout
  // warning, and the early-bail empty reply emits a timeout or shard-OOM
  // warning (see common_hybrid_query_reply_empty).
  switch (QueryWarningCode_GetCodeFromMessage(warning_str)) {
    case QUERY_WARNING_CODE_TIMED_OUT:
      nc->areq->stateflags |= QEXEC_S_SHARD_TIMED_OUT_WARNING;
      if (nc->areq->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) {
        return RS_RESULT_TIMEDOUT;
      }
      break;
    case QUERY_WARNING_CODE_OUT_OF_MEMORY_SHARD:
      if (nc->areq->reqConfig.oomPolicy == OomPolicy_Fail) {
        // The shard ran under a milder OOM policy than this coordinator; FAIL
        // semantics still demand a hard error.
        QueryError_SetCode(err, QUERY_ERROR_CODE_OUT_OF_MEMORY);
        QueryError_SetDetail(err, warning_str);
        return RS_RESULT_ERROR;
      }
      QueryError_SetQueryOOMWarning(err);
      break;
    default:
      break;
  }
  return RS_RESULT_OK;
}

int getNextReply(RPNet *nc) {
  if (nc->cmd.forCursor) {
    if (!MR_ManuallyTriggerNextIfNeeded(nc->it, clusterConfig.cursorReplyThreshold)) {
      RPNet_resetCurrent(nc);
      return RS_RESULT_EOF;
    }
  }
  // Pop wake mechanisms: the abort flag is flipped by the FAIL / RETURN-STRICT
  // timeout callback via MRChannel_WakeAbort. Under RETURN the flag is never
  // flipped: aggregate streams degrade to a blocking pop (legacy RETURN waits
  // beyond the deadline for in-flight shard replies), while hybrid streams get
  // the in-band wall-clock deadline from getAbsTimeout — see its doc for why.
  // An unarmed timeout source provides neither mechanism and uses MRIterator_Next.
#ifdef ENABLE_ASSERT
  // Sync point (debug): park BG when it is about to wait for the next shard
  // reply. Reaching this site implies any previously admitted reply has been
  // fully drained downstream.
  SyncPoint_WaitUntil(SYNC_POINT_RPNET_WAITING_FOR_REPLY, areq_timed_out, nc->areq);
#endif
  RS_ASSERT(nc->areq);
  QueryRequestTimeout *timeout = &nc->areq->base.timeout;
  const struct timespec *deadline = getAbsTimeout(nc);
  RS_Atomic(bool) *abortFlag =
      timeout->kind == QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT
          ? QueryRequestTimeout_GetBlockedClientFlag(timeout)
          : NULL;
  bool popTimedOut = false;
  MRReply *root = deadline || abortFlag
                      ? MRIterator_NextWithTimeout(nc->it, deadline, abortFlag, &popTimedOut)
                      : MRIterator_Next(nc->it);

  if (root == NULL) {
    RPNet_resetCurrent(nc);
    // Drain-only: empty channel means end of queued replies, not a timeout —
    // the main-thread timeout callback already observed the deadline and is
    // now consuming whatever the I/O threads had already pushed.
    if (nc->drainOnly) {
      return RS_RESULT_EOF;
    }
    if (popTimedOut || QueryRequestTimeout_IsBlockedClientTimedOut(timeout)) {
      return RS_RESULT_TIMEDOUT;
    }
    return MRIterator_GetPending(nc->it) ? RS_RESULT_OK : RS_RESULT_EOF;
  }

  // Mapping-stage warnings ride the stream ahead of any rows; fold them into
  // this subquery's state and keep reading (current.root stays NULL, so the
  // rpnetNext pop loop re-enters).
  if (isHybridMappingWarning(nc, root)) {
    int rc = processHybridMappingWarning(nc, MRReply_String(root, NULL));
    MRReply_Free(root);
    RPNet_resetCurrent(nc);
    return rc;
  }

  // Check if an error was returned
  if (MRReply_Type(root) == MR_REPLY_ERROR) {
    nc->current.root = root;
    // If for profiling, clone and append the error
    if (nc->cmd.forProfiling) {
      // Clone the error and append it to the profile
      MRReply *error = MRReply_Clone(root);
      array_append(nc->shardsProfile, error);
    }
    return RS_RESULT_OK;
  }

  // For profile command, extract the profile data from the reply
  if (nc->cmd.forProfiling) {
    // if the cursor id is 0, this is the last reply from this shard, and it has the profile data
    if (CURSOR_EOF == MRReply_Integer(MRReply_ArrayElement(root, 1))) {
      MRReply *profile_data;
      if (nc->cmd.protocol == 3) {
        // [
        //   {
        //     "Results": { <FT.AGGREGATE reply> },
        //     "Profile": { <profile data> }
        //   },
        //   cursor_id
        // ]
        MRReply *data = MRReply_ArrayElement(root, 0);
        profile_data = MRReply_TakeMapElement(data, "profile");
      } else {
        // RESP2
        RS_ASSERT(nc->cmd.protocol == 2);
        // [
        //   <FT.AGGREGATE reply>,
        //   cursor_id,
        //   <profile data>
        // ]
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

  nc->current.root = root;
  nc->current.rows = rows;
  nc->current.meta = meta;

  const size_t empty_rows_len = nc->cmd.protocol == 3 ? 0 : 1; // RESP2 has the first element as the number of results.
  RS_LOG_ASSERT(rows && MRReply_Type(rows) == MR_REPLY_ARRAY, rows ? "rows is not an array" : "rows is NULL");
  if (MRReply_Length(rows) <= empty_rows_len) {
    RedisModule_Log(RSDummyContext, "verbose", "An empty reply was received from a shard");
    int ret = processWarningsAndCleanup(nc, nc->cmd.protocol == 3);

    if (ret == RS_RESULT_TIMEDOUT) {
      return RS_RESULT_TIMEDOUT;
    }
  }

  return RS_RESULT_OK;
}

void rpnetFree(ResultProcessor *rp) {
  RPNet *nc = (RPNet *)rp;

  if (nc->it) {
    // Unregister the abort-wake channel before releasing the iterator, so the main
    // thread's timeout callback cannot observe a channel that is about to be freed.
    QueryRequestAsyncState_UnregisterAbortWakeChannel(&nc->areq->base.async);
#ifdef ENABLE_ASSERT
    // Drop the FT.DEBUG BG_PENDING_REPLIES handle before releasing the iterator.
    DebugBgIterator_Clear(nc->it);
#endif
    // The reader is going away, so the request may be torn down (timeout,
    // fatal shard error, ...) before every shard exchange resolved — for
    // hybrid, even before the arming fan-out delivered some shards' cursor
    // ids. Flag the iterator so any late reply processing — getCursorCommand
    // on an in-flight read, or the hybrid arming callback — sends DEL instead
    // of READ: without it a healthy shard's cursor is read to depletion into
    // a channel nobody consumes. Unconditional: with nothing outstanding the
    // flag has no reader left to affect.
    MRIteratorCallback_SetTimedOut(MRIterator_GetCtx(nc->it));
    RS_DEBUG_LOG("rpnetFree: calling MRIterator_Release");
    MRIterator_Release(nc->it);
  }

  if (nc->shardsProfile) {
    array_foreach(nc->shardsProfile, reply, MRReply_Free(reply));
    array_free(nc->shardsProfile);
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
  AREQ *areq = nc->areq;
  RS_ASSERT(areq);

#ifdef ENABLE_ASSERT
  SyncPoint_WaitUntil(SYNC_POINT_BEFORE_RPNET_NEXT, areq_timed_out, areq);
#endif

  // Surface RETURN_STRICT timeouts on follow-up cursor reads where the channel
  // may already hold a buffered reply (the NULL-reply check below wouldn't fire
  // and we'd silently return rows). Skipped during the timer's own drain.
  if (QueryRequest_UsesReplyCallback(&areq->base) && !nc->drainOnly &&
      QueryRequestTimeout_IsBlockedClientTimedOut(&areq->base.timeout)) {
    return RS_RESULT_TIMEDOUT;
  }

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
      if (processWarningsAndCleanup(nc, resp3) == RS_RESULT_TIMEDOUT) {
        return RS_RESULT_TIMEDOUT;
      }

      root = rows = NULL;
    }
  }

  bool new_reply = !root;

  // get the next reply from the channel
  while (!root) {
    // RETURN_STRICT uses the blocked-client source, so only clock-based cycles
    // reach this check.
    if (areq->base.timeout.kind == QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE &&
        QueryRequestTimeout_IsTimedOutExact(&areq->base.timeout)) {
      // Set the `timedOut` flag in the MRIteratorCtx, later to be read by the
      // callback so that a `CURSOR DEL` command will be dispatched instead of
      // a `CURSOR READ` command.
      MRIteratorCallback_SetTimedOut(MRIterator_GetCtx(nc->it));
      return RS_RESULT_TIMEDOUT;
    } else if (!nc->drainOnly && MRIteratorCallback_GetTimedOut(MRIterator_GetCtx(nc->it))) {
      // if timeout was set in previous reads, reset it. Drain-only must keep
      // the flag set so the post-drain callback dispatches CURSOR DEL.
      MRIteratorCallback_ResetTimedOut(MRIterator_GetCtx(nc->it));
    }

    int ret = getNextReply(nc);
    if (ret == RS_RESULT_EOF) {
      return RS_RESULT_EOF;
    } else if (ret == RS_RESULT_TIMEDOUT) {
      MRIteratorCallback_SetTimedOut(MRIterator_GetCtx(nc->it));
      return RS_RESULT_TIMEDOUT;
    } else if (ret == RS_RESULT_ERROR) {
      // Mapping-stage warning escalated under a FAIL policy (see
      // processHybridMappingWarning); the QueryError is already set.
      return RS_RESULT_ERROR;
    }

    // If an error was returned, propagate it
    if (nc->current.root && MRReply_Type(nc->current.root) == MR_REPLY_ERROR) {
      QueryErrorCode errCode = QueryError_GetCodeFromMessage(MRReply_String(nc->current.root, NULL));
      // TODO - use should_return_error after it is changed to support RequestConfig ptr
      if (errCode == QUERY_ERROR_CODE_GENERIC ||
          errCode == QUERY_ERROR_CODE_UNAVAILABLE_SLOTS ||
          ((errCode == QUERY_ERROR_CODE_TIMED_OUT) && nc -> areq -> reqConfig.timeoutPolicy == TimeoutPolicy_Fail) ||
          ((errCode == QUERY_ERROR_CODE_OUT_OF_MEMORY) && nc -> areq -> reqConfig.oomPolicy == OomPolicy_Fail)) {
        // The shard reply already contains the prefixed error string — set it directly
        // without re-prefixing via QueryError_SetError.
        QueryError_SetCode(AREQ_QueryProcessingCtx(nc->areq)->err, errCode);
        // Hybrid mapping-stage timeout errors carry the shard's internal
        // phrasing (e.g. "Depleting timed out"); reply the canonical timeout
        // text instead, as the pre-arming-fan-out coordinator did.
        if (nc->hybridSubquery == RPNET_HYBRID_NONE || errCode != QUERY_ERROR_CODE_TIMED_OUT) {
          QueryError_SetDetail(AREQ_QueryProcessingCtx(nc->areq)->err,
                               MRReply_String(nc->current.root, NULL));
        }
        return RS_RESULT_ERROR;
      } else {
        // Handle shards returning error unexpectedly
        // Might be from different Timeout/OOM policy (See MOD-10774)
        if (nc->hybridSubquery != RPNET_HYBRID_NONE) {
          // Hybrid mapping-stage shard errors under a milder coordinator
          // policy degrade to the warnings the mapping's warning array would
          // have produced, instead of the aggregate path's silent drop.
          if (errCode == QUERY_ERROR_CODE_TIMED_OUT) {
            nc->areq->stateflags |= QEXEC_S_SHARD_TIMED_OUT_WARNING;
          } else if (errCode == QUERY_ERROR_CODE_OUT_OF_MEMORY) {
            QueryError_SetQueryOOMWarning(AREQ_QueryProcessingCtx(nc->areq)->err);
          } else {
            // No policy softens any other mapping-stage error (e.g. a shard
            // that lost the index) — fatal, as it was for the
            // pre-arming-fan-out coordinator; dropping it would silently
            // return incomplete results.
            QueryError_SetCode(AREQ_QueryProcessingCtx(nc->areq)->err, errCode);
            QueryError_SetDetail(AREQ_QueryProcessingCtx(nc->areq)->err,
                                 MRReply_String(nc->current.root, NULL));
            return RS_RESULT_ERROR;
          }
        }
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
#ifdef ENABLE_ASSERT
    // Sync point (debug): park BG after a shard reply has been admitted into the
    // pipeline (popped from the channel, about to emit its rows).
    SyncPoint_WaitUntil(SYNC_POINT_RPNET_REPLY_ADMITTED, areq_timed_out, nc->areq);
#endif
    if (resp3) { // RESP3
      nc->curIdx = 0;
      // For WITHCOUNT, totalResults was set once at Phase B start by
      // executeAggregateDeferred from the shard-summed total accumulated on the
      // IO thread; it is preserved across cursor reads by finishSendChunk.
      if (!nc->withCount) {
        // Without WITHCOUNT, count rows in batch for backward compatibility
        nc->base.parent->totalResults += MRReply_Length(rows);
      }
      processResultFormat(&nc->areq->reqflags, nc->current.meta);
    } else { // RESP2
      nc->curIdx = 1;
      // For WITHCOUNT, totalResults was set once at Phase B start by
      // executeAggregateDeferred (see RESP3 branch above).
      if (!nc->withCount) {
        // Without WITHCOUNT, accumulate total_results from each shard reply
        nc->base.parent->totalResults += MRReply_Integer(MRReply_ArrayElement(rows, 0));
      }
    }
  }

  MRReply *score = NULL;
  MRReply *fields = MRReply_ArrayElement(rows, nc->curIdx++);
  size_t fields_length = 0;
  if (resp3) {
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_MAP, "invalid result record");
    // extract score if it exists, WITHSCORES was specified
    score = MRReply_MapElement(fields, "score");
    fields = MRReply_MapElement(fields, "extra_attributes");
    // It could happen if Result_ExpiredDoc is set by the Loader on the shard, that no extra attributes is returned. In that case
    // we do not have keys to return.
    fields_length = fields && MRReply_Type(fields) == MR_REPLY_MAP ? MRReply_Length(fields) : 0;
  } else {
    fields_length = fields && MRReply_Type(fields) == MR_REPLY_ARRAY ? MRReply_Length(fields) : 0;
    RS_LOG_ASSERT(fields_length % 2 == 0, "invalid fields record");
  }

  // The score is optional, in hybrid we need the score for the sorter and hybrid merger
  // We expect for it to exist in hybrid since we send WITHSCORES to the shard and we should use resp3
  // when opening shard connections.
  if (score) {
    const bool expectExplain = (nc->areq->reqflags & QEXEC_F_SEND_SCOREEXPLAIN) != 0;
    if (expectExplain) {
      RS_LOG_ASSERT(MRReply_Type(score) == MR_REPLY_ARRAY &&
                        MRReply_Length(score) == SE_REPLY_NODE_ARITY,
                    "EXPLAINSCORE expected score paired with explain tree");
      const MRReply *scoreValue = MRReply_ArrayElement(score, 0);
      const MRReply *explainReply = MRReply_ArrayElement(score, 1);
      RS_LOG_ASSERT(scoreValue && MRReply_Type(scoreValue) == MR_REPLY_DOUBLE,
                    "invalid score record");
      SearchResult_SetScore(r, MRReply_Double(scoreValue));
      if (explainReply) {
        SearchResult_SetScoreExplain(r, SE_FromMRReply(explainReply));
      }
    } else {
      RS_LOG_ASSERT(MRReply_Type(score) == MR_REPLY_DOUBLE, "invalid score record");
      SearchResult_SetScore(r, MRReply_Double(score));
    }
  }

  for (size_t i = 0; i < fields_length; i += 2) {
    size_t len;
    const char *field = MRReply_String(MRReply_ArrayElement(fields, i), &len);
    MRReply *val = MRReply_TakeArrayElement(fields, i + 1);
    RSValue *v = MRReply_ToValue(val);
    RLookupRow_WriteByNameOwned(nc->lookup, field, len, SearchResult_GetRowDataMut(r), v);
  }

  return RS_RESULT_OK;
}

int rpnetNext_EOF(ResultProcessor *self, SearchResult *r) {
  return RS_RESULT_EOF;
}
