/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "hybrid_cursor_mappings.h"
#include "hybrid/hybrid_exec.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "query_error_ffi.h"
#include "shard_window_ratio.h"
#include <string.h>
#include "info/global_stats.h"
#include "aggregate/aggregate.h"

#define INTERNAL_HYBRID_RESP3_LENGTH 6
#define INTERNAL_HYBRID_RESP2_LENGTH 6

typedef struct {
    StrongRef searchMappings;
    StrongRef vsimMappings;
    arrayof(QueryError) errors;
    HybridKnnContext *knnCtx;         // KNN context for SHARD_K_RATIO optimization (may be NULL)
} processCursorMappingCallbackContext;

void CursorMapping_Release(CursorMapping *mapping) {
  rm_free(mapping->targetShard);
}

static void processHybridError(processCursorMappingCallbackContext *ctx, MRReply *rep) {
    const char *errorMessage = MRReply_String(rep, NULL);
    QueryErrorCode errCode = QueryError_GetCodeFromMessage(errorMessage);
    QueryError error = QueryError_Default();
    // Shard reply already contains the prefixed error string — set directly.
    QueryError_SetCode(&error, errCode);
    QueryError_SetDetail(&error, errorMessage);
    ctx->errors = array_ensure_append_1(ctx->errors, error);
}

// Warning strings use a different format than error strings (no prefix).
// Map warning codes to error codes for uniform handling in ProcessHybridCursorMappings.
static void processHybridWarning(processCursorMappingCallbackContext *ctx, const MRReply *rep) {
    const char *warningMessage = MRReply_String(rep, NULL);
    QueryWarningCode warningCode = QueryWarningCode_GetCodeFromMessage(warningMessage);
    // MaxTimeoutCapped is purely informational: the coordinator's own cap is
    // already surfaced to the client on the hybrid request.
    if (warningCode == QUERY_WARNING_CODE_MAX_TIMEOUT_CAPPED) {
        return;
    }
    QueryError error = QueryError_Default();
    if (warningCode == QUERY_WARNING_CODE_TIMED_OUT) {
        QueryError_SetCode(&error, QUERY_ERROR_CODE_TIMED_OUT);
    } else if (warningCode == QUERY_WARNING_CODE_OUT_OF_MEMORY_SHARD ||
               warningCode == QUERY_WARNING_CODE_OUT_OF_MEMORY_COORD) {
        QueryError_SetCode(&error, QUERY_ERROR_CODE_OUT_OF_MEMORY);
    } else {
        QueryError_SetCode(&error, QUERY_ERROR_CODE_GENERIC);
    }
    QueryError_SetDetail(&error, warningMessage);
    ctx->errors = array_ensure_append_1(ctx->errors, error);
}

static void processHybridUnknownReplyType(processCursorMappingCallbackContext *ctx, int replyType) {
    QueryError error = QueryError_Default();
    QueryError_SetWithoutUserDataFmt(&error, QUERY_ERROR_CODE_UNSUPP_TYPE, "Unsupported reply type: %d", replyType);
    ctx->errors = array_ensure_append_1(ctx->errors, error);
}

// Process cursor mappings for RESP2 protocol
static void processHybridResp2(processCursorMappingCallbackContext *ctx, MRReply *rep, MRCommand *cmd) {
    for (size_t i = 0; i < INTERNAL_HYBRID_RESP2_LENGTH; i += 2) {
        CursorMapping mapping;
        mapping.targetShard = NULL;
        mapping.targetShardIdx = 0;
        mapping.cursorId = 0;

        MRReply *key_reply = MRReply_ArrayElement(rep, i);
        MRReply *value_reply = MRReply_ArrayElement(rep, i + 1);
        const char *key = MRReply_String(key_reply, NULL);
        bool earlyBailout = false;

        // Handle warnings
        if (strcmp(key, "warnings") == 0) {
            for (size_t j = 0; j < MRReply_Length(value_reply); j++) {
                MRReply *warningReply = MRReply_ArrayElement(value_reply, j);
                processHybridWarning(ctx, warningReply);
            }
            continue;
        }

        // Handle cursor IDs
        long long value;
        MRReply_ToInteger(value_reply, &value);

        CursorMappings *vsimOrSearch = NULL;
        if (strcmp(key, "SEARCH") == 0) {

            // Check for early bailout (Cursor ID 0 means no cursor was opened)
            if (value == 0) {
                earlyBailout = true;
                // Pop the related VSIM mapping if exists
                CursorMappings *vsim = StrongRef_Get(ctx->vsimMappings);
                CursorMappings *search = StrongRef_Get(ctx->searchMappings);
                while (array_len(vsim->mappings) > array_len(search->mappings)) {
                    CursorMapping cur = array_pop(vsim->mappings);
                    CursorMapping_Release(&cur);
                }
                continue;
            }

            vsimOrSearch = StrongRef_Get(ctx->searchMappings);
            mapping.cursorId = value;
        } else if (strcmp(key, "VSIM") == 0) {
            if (earlyBailout) continue;
            vsimOrSearch = StrongRef_Get(ctx->vsimMappings);
            mapping.cursorId = value;
        }

        RS_ASSERT(vsimOrSearch);
        if (i == INTERNAL_HYBRID_RESP2_LENGTH - 2) {
            //Transferring ownership at the tail to avoid potential leak of cmd->targetShard on early bailout
            mapping.targetShard = cmd->targetShard;
            cmd->targetShard = NULL; // transfer ownership
        } else {
            mapping.targetShard = rm_strdup(cmd->targetShard);
        }
        mapping.targetShardIdx = cmd->targetShardIdx;
        vsimOrSearch->mappings = array_ensure_append_1(vsimOrSearch->mappings, mapping);
    }
}

// Process cursor mappings for RESP3 protocol
static void processHybridResp3(processCursorMappingCallbackContext *ctx, MRReply *rep, MRCommand *cmd) {
    // RESP3 uses a map structure instead of array pairs
    const char *keys[] = {"SEARCH", "VSIM"};
    const bool isSearch[] = {true, false};
    const StrongRef* mappings[] = {&ctx->searchMappings, &ctx->vsimMappings};
    for (int i = 0; i < 2; i++) {
        MRReply *cursorId = MRReply_MapElement(rep, keys[i]);
        RS_ASSERT(cursorId);
        CursorMapping mapping;
        mapping.targetShard = NULL;
        mapping.targetShardIdx = 0;
        mapping.cursorId = 0;
        long long cid;
        MRReply_ToInteger(cursorId, &cid);
        // Check for early bailout (Cursor ID 0 means no cursor was opened)
        if (cid == 0) {
            // Pop all mappings from previous subqueries
            for (int j = 0; j < i; j++) {
                CursorMappings *vsimOrSearch = StrongRef_Get(*mappings[j]);
                CursorMapping cur = array_pop(vsimOrSearch->mappings);
                CursorMapping_Release(&cur);
            }
            break;
        }
        mapping.cursorId = cid;
        CursorMappings *vsimOrSearch = StrongRef_Get(*mappings[i]);
        RS_ASSERT(vsimOrSearch);
        if (i == 1) {
            //Transferring ownership at the tail to avoid potential leak of cmd->targetShard on early bailout
            mapping.targetShard = cmd->targetShard;
            cmd->targetShard = NULL; // transfer ownership
        } else {
            mapping.targetShard = rm_strdup(cmd->targetShard);
        }
        mapping.targetShardIdx = cmd->targetShardIdx;
        vsimOrSearch->mappings = array_ensure_append_1(vsimOrSearch->mappings, mapping);
    }
    // Handle warnings
    MRReply *warnings = MRReply_MapElement(rep, "warnings");
    if (MRReply_Length(warnings) > 0) {
        for (size_t i = 0; i < MRReply_Length(warnings); i++) {
            MRReply *warningReply = MRReply_ArrayElement(warnings, i);
            processHybridWarning(ctx, warningReply);
        }
    }
}

// Callback implementation for processing cursor mappings
static void processCursorMappingCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {
    // TODO: add response validation (see netCursorCallback)
    // TODO implement error handling
    processCursorMappingCallbackContext *cb_ctx = (processCursorMappingCallbackContext *)MRIteratorCallback_GetPrivateData(ctx);
    RS_ASSERT(cb_ctx);
    MRCommand *cmd = MRIteratorCallback_GetCommand(ctx);

    const int replyType = MRReply_Type(rep);
    if (replyType == MR_REPLY_ERROR) {
        processHybridError(cb_ctx, rep);
    } else if (replyType == MR_REPLY_MAP) {
        RS_ASSERT(MRReply_Length(rep) == INTERNAL_HYBRID_RESP3_LENGTH);
        processHybridResp3(cb_ctx, rep, cmd);
    } else if (replyType == MR_REPLY_ARRAY) {
        RS_ASSERT(MRReply_Length(rep) == INTERNAL_HYBRID_RESP2_LENGTH);
        processHybridResp2(cb_ctx, rep, cmd);
    } else {
        processHybridUnknownReplyType(cb_ctx, replyType);
    }

    MRIteratorCallback_Done(ctx, 0);
    MRReply_Free(rep);
}

static void processCursorMappingErrorCallback(MRIteratorCallbackCtx *ctx) {
    processCursorMappingCallbackContext *cb_ctx = (processCursorMappingCallbackContext *)MRIteratorCallback_GetPrivateData(ctx);
    RS_ASSERT(cb_ctx);

    QueryError error = QueryError_Default();
    QueryError_SetCode(&error, QUERY_ERROR_CODE_GENERIC);
    // Shared with the MR iterator no-reply path (pre-fanout connection-validation failure).
    QueryError_SetDetail(&error, CLUSTER_QUERY_ERROR);
    cb_ctx->errors = array_ensure_append_1(cb_ctx->errors, error);
}

static void freeCursorMappingCtx(void *privateData) {
    processCursorMappingCallbackContext *ctx = (processCursorMappingCallbackContext *)privateData;
    StrongRef_Release(ctx->searchMappings);
    StrongRef_Release(ctx->vsimMappings);
    array_free_ex(ctx->errors, QueryError_ClearError((QueryError*)ptr));
    rm_free(ctx->knnCtx);
    rm_free(ctx);
}

void HybridKnnApplyShardKRatio(MRCommand *cmd, size_t numShards, const HybridKnnContext *knnCtx) {
    RS_ASSERT(cmd && knnCtx && knnCtx->kArgIndex >= 0);
    // Only apply optimization for multi-shard deployments with valid ratio
    if (numShards <= 1 || knnCtx->shardWindowRatio >= MAX_SHARD_WINDOW_RATIO) {
        return;
    }
    size_t effectiveK = calculateEffectiveK(knnCtx->originalK, knnCtx->shardWindowRatio, numShards);
    modifyVsimKNN(cmd, knnCtx->kArgIndex, effectiveK, knnCtx->originalK);
}

// Command modifier callback for SHARD_K_RATIO optimization.
// Called from iterStartCb on IO thread before commands are sent to shards.
void HybridKnnCommandModifier(MRCommand *cmd, size_t numShards, void *privateData) {
    RS_ASSERT(privateData && cmd);
    const processCursorMappingCallbackContext *ctx = (processCursorMappingCallbackContext *)privateData;
    HybridKnnApplyShardKRatio(cmd, numShards, ctx->knnCtx);
}

// Fold collected shard errors into `status` and the max-prefix flags per the OOM
// and timeout policies. Returns false on a fatal error (first one wins).
static bool resolveCursorMappingErrors(arrayof(QueryError) errors, QueryError *status,
                                       RSOomPolicy oomPolicy, RSTimeoutPolicy timeoutPolicy,
                                       bool *maxPrefixSearch, bool *maxPrefixVsim) {
    for (size_t i = 0; i < array_len(errors); i++) {
        QueryErrorCode code = QueryError_GetCode(&errors[i]);
        if (code == QUERY_ERROR_CODE_OUT_OF_MEMORY && oomPolicy == OomPolicy_Return) {
            QueryError_SetQueryOOMWarning(status);
        } else if (code == QUERY_ERROR_CODE_TIMED_OUT && timeoutPolicy != TimeoutPolicy_Fail) {
            // RETURN / RETURN-STRICT policy: acknowledge the shard timeout but don't set
            // it on qctx->err. The timeout propagates through cursor reads (RPNet detects
            // it from the depleter's last_rc), and replyWarningsWithSuffixes emits the
            // properly-suffixed warning (e.g. "(SEARCH)" / "(VSIM)").
            // Note: the _FT.DEBUG FT.HYBRID path rejects RETURN-STRICT in
            // parseHybridDebugParams, so only RETURN reaches here in debug mode.
        } else if (code == QUERY_ERROR_CODE_TIMED_OUT) {
            // FAIL policy: forward the standard timeout error directly.
            QueryError_SetCode(status, QUERY_ERROR_CODE_TIMED_OUT);
            return false;
        } else {
            const char *msg = QueryError_GetUserError(&errors[i]);
            if (msg && strncmp(msg, QUERY_WMAXPREFIXEXPANSIONS, strlen(QUERY_WMAXPREFIXEXPANSIONS)) == 0) {
                if (strstr(msg, SEARCH_SUFFIX)) {
                    *maxPrefixSearch = true;
                } else if (strstr(msg, VSIM_SUFFIX)) {
                    *maxPrefixVsim = true;
                }
            } else {
                QueryError_SetWithoutUserDataFmt(status, code,
                    "Failed to process shard responses, first error: %s, total error count: %zu",
                    msg, array_len(errors));
                return false;
            }
        }
    }
    return true;
}

bool ProcessHybridCursorMappings(const MRCommand *cmd, StrongRef searchMappingsRef, StrongRef vsimMappingsRef, HybridKnnContext *knnCtx, QueryError *status, const RSOomPolicy oomPolicy, const RSTimeoutPolicy timeoutPolicy, bool *maxPrefixSearch, bool *maxPrefixVsim, const struct timespec *deadline, RequestSyncCtx *syncCtx) {
    CursorMappings *searchMappings = StrongRef_Get(searchMappingsRef);
    CursorMappings *vsimMappings = StrongRef_Get(vsimMappingsRef);
    RS_ASSERT(array_len(searchMappings->mappings) == 0 && array_len(vsimMappings->mappings) == 0);

    // Heap-allocated because the iterator runs asynchronously; freed by
    // freeCursorMappingCtx (the iterator's destructor).
    processCursorMappingCallbackContext *ctx = rm_malloc(sizeof(processCursorMappingCallbackContext));
    *ctx = (processCursorMappingCallbackContext) {
        .searchMappings = StrongRef_Clone(searchMappingsRef),
        .vsimMappings = StrongRef_Clone(vsimMappingsRef),
        .errors = array_new(QueryError, 0),
        .knnCtx = knnCtx,  // Store KNN context for command modifier callback
      };

    // Pass HybridKnnCommandModifier if knnCtx is provided (for SHARD_K_RATIO
    // optimization)
    MRCommandModifier cmdModifier = knnCtx ? &HybridKnnCommandModifier : NULL;

    MRIterator *it = MR_IterateWithPrivateData(cmd, &(MRIteratorConfig){
        .successCB = processCursorMappingCallback,
        .errorCB = processCursorMappingErrorCallback,
        .cbPrivateData = ctx,
        .cbPrivateDataDestructor = freeCursorMappingCtx,
        .commandModifier = cmdModifier,
        .iterStartCb = iterStartCb,
    });
    if (!it) {
        // Cleanup on error
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_GENERIC, "Failed to communicate with shards");
        freeCursorMappingCtx(ctx);
        return false;
    }

    // Register the iterator's channel so an external abort - the coordinator timeout
    // callback (RequestSyncCtx_WakeAbortChannel) or a client disconnect - can wake
    // this wait promptly. Unregistered below before the iterator is released.
    RequestSyncCtx_RegisterAbortWakeChannel(syncCtx, MRIterator_GetChannel(it));

    // Wait on the channel: it unblocks when inProcess hits 0 (normal completion) or
    // when the deadline/abort fires. Both are passed because `deadline` is NULL under
    // RETURN-STRICT / disabled timeout checks, where syncCtx->timedOut is the only
    // wake; passing neither leaves the wait unbounded (chan.c asserts at least one is
    // non-NULL).
    bool timedOut = false;
    MRReply *r = MRIterator_NextWithTimeout(it, deadline, &syncCtx->timedOut, &timedOut);
    RS_ASSERT(r == NULL);  // the callbacks never AddReply; a non-NULL reply is a bug

    RequestSyncCtx_UnregisterAbortWakeChannel(syncCtx);

    if (timedOut || RS_AtomicBoolLoadRelaxed(&syncCtx->timedOut)) {
        // Terminal: a shard never replied or the request was aborted. Late callbacks
        // may still be writing mappings/errors, so do NOT read them — just release
        // (freeCursorMappingCtx frees ctx once they finish).
        QueryError_SetCode(status, QUERY_ERROR_CODE_TIMED_OUT);
        MRIterator_Release(it);
        return false;
    }

    // Normal completion: inProcess hit 0, so every callback has run. Postcondition —
    // search/vsim mappings are paired index-for-index (the early-bailout logic keeps
    // them in lockstep); catches a malformed reply that slipped past it.
    RS_ASSERT(array_len(searchMappings->mappings) == array_len(vsimMappings->mappings));

    bool success = resolveCursorMappingErrors(ctx->errors, status, oomPolicy, timeoutPolicy,
                                              maxPrefixSearch, maxPrefixVsim);
    MRIterator_Release(it);

    return success;
}
