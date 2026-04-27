/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "hybrid_cursor_mappings.h"
#include "redismodule.h"
#include "../../rmalloc.h"
#include "../../../deps/rmutil/rm_assert.h"
#include "../../query_error.h"
#include <string.h>
#include "info/global_stats.h"

#define INTERNAL_HYBRID_RESP3_LENGTH 6
#define INTERNAL_HYBRID_RESP2_LENGTH 6

typedef struct {
    StrongRef searchMappings;
    StrongRef vsimMappings;
    arrayof(QueryError) errors;
} processCursorMappingCallbackContext;

void CursorMapping_Release(CursorMapping *mapping) {
  rm_free(mapping->targetShard);
}

static void processHybridError(processCursorMappingCallbackContext *ctx, MRReply *rep) {
    const char *errorMessage = MRReply_String(rep, NULL);
    QueryErrorCode errCode = QueryError_GetCodeFromMessage(errorMessage);
    QueryError error = QueryError_Default();
    QueryError_SetError(&error, errCode, errorMessage);
    ctx->errors = array_ensure_append_1(ctx->errors, error);
}

// Warning strings use a different format than error strings (no prefix).
// Map warning codes to error codes for uniform handling in ProcessHybridCursorMappings.
static void processHybridWarning(processCursorMappingCallbackContext *ctx, const MRReply *rep) {
    const char *warningMessage = MRReply_String(rep, NULL);
    QueryWarningCode warningCode = QueryWarningCode_GetCodeFromMessage(warningMessage);
    QueryErrorCode code;
    if (warningCode == QUERY_WARNING_CODE_TIMED_OUT) {
        code = QUERY_ETIMEDOUT;
    } else if (warningCode == QUERY_WARNING_CODE_OUT_OF_MEMORY_SHARD ||
               warningCode == QUERY_WARNING_CODE_OUT_OF_MEMORY_COORD) {
        code = QUERY_EOOM;
    } else {
        code = QUERY_EGENERIC;
    }
    QueryError error = QueryError_Default();
    QueryError_SetError(&error, code, warningMessage);
    ctx->errors = array_ensure_append_1(ctx->errors, error);
}

static void processHybridUnknownReplyType(processCursorMappingCallbackContext *ctx, int replyType) {
    QueryError error = QueryError_Default();
    QueryError_SetWithoutUserDataFmt(&error, QUERY_EUNSUPPTYPE, "Unsupported reply type: %d", replyType);
    ctx->errors = array_ensure_append_1(ctx->errors, error);
}

// Process cursor mappings for RESP2 protocol
static void processHybridResp2(processCursorMappingCallbackContext *ctx, MRReply *rep, MRCommand *cmd) {
    for (size_t i = 0; i < INTERNAL_HYBRID_RESP2_LENGTH; i += 2) {
        CursorMapping mapping;
        mapping.targetShard = NULL;
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

// No-reply error callback: records a communication error. Completion is driven by
// the iterator's channel (every callback decrements inProcess), so the wait below
// unblocks once all callbacks have run regardless of this error.
static void processCursorMappingErrorCallback(MRIteratorCallbackCtx *ctx) {
    processCursorMappingCallbackContext *cb_ctx = (processCursorMappingCallbackContext *)MRIteratorCallback_GetPrivateData(ctx);
    RS_ASSERT(cb_ctx);

    QueryError error = QueryError_Default();
    // Shared with the MR iterator no-reply path (pre-fanout connection-validation failure).
    QueryError_SetError(&error, QUERY_EGENERIC, CLUSTER_QUERY_ERROR);
    cb_ctx->errors = array_ensure_append_1(cb_ctx->errors, error);
}

// Iterator destructor: frees the callback context once the iterator is released.
static void freeCursorMappingCtx(void *privateData) {
    processCursorMappingCallbackContext *ctx = (processCursorMappingCallbackContext *)privateData;
    StrongRef_Release(ctx->searchMappings);
    StrongRef_Release(ctx->vsimMappings);
    array_free_ex(ctx->errors, QueryError_ClearError((QueryError*)ptr));
    rm_free(ctx);
}

bool ProcessHybridCursorMappings(const MRCommand *cmd, StrongRef searchMappingsRef, StrongRef vsimMappingsRef, QueryError *status, const RSOomPolicy oomPolicy, const RSTimeoutPolicy timeoutPolicy, const struct timespec *deadline) {
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
      };

    MRIterator *it = MR_IterateWithPrivateData(cmd, &(MRIteratorConfig){
        .successCB = processCursorMappingCallback,
        .errorCB = processCursorMappingErrorCallback,
        .cbPrivateData = ctx,
        .cbPrivateDataDestructor = freeCursorMappingCtx,
        .iterStartCb = iterStartCb,
    });
    if (!it) {
        // Cleanup on error
        QueryError_SetWithoutUserDataFmt(status, QUERY_EGENERIC, "Failed to communicate with shards");
        freeCursorMappingCtx(ctx);
        return false;
    }

    // Wait on the iterator's channel, bounded by the request deadline.
    bool timedOut = false;
    MRReply *r = MRIterator_NextWithTimeout(it, deadline, &timedOut);
    RS_ASSERT(r == NULL);  // the callbacks never AddReply; a non-NULL reply is a bug

    if (timedOut) {
        QueryError_SetCode(status, QUERY_ETIMEDOUT);
        MRIterator_Release(it);
        return false;
    }

    // Normal completion: inProcess hit 0, so every callback has run.
    RS_ASSERT(array_len(searchMappings->mappings) == array_len(vsimMappings->mappings));

    bool success = true;
    if (array_len(ctx->errors)) {
        for (size_t i = 0; i < array_len(ctx->errors); i++) {
            if (QueryError_GetCode(&ctx->errors[i]) == QUERY_EOOM && oomPolicy == OomPolicy_Return ) {
                QueryError_SetQueryOOMWarning(status);
            } else if (QueryError_GetCode(&ctx->errors[i]) == QUERY_ETIMEDOUT && timeoutPolicy != TimeoutPolicy_Fail) {
                // RETURN policy: acknowledge the shard timeout but don't set it
                // on qctx->err. The timeout will propagate through cursor reads
                // (RPNet detects it from the depleter's last_rc), and
                // replyWarningsWithSuffixes emits the properly-suffixed warning
                // (e.g., "(SEARCH)" / "(VSIM)").
            } else if (QueryError_GetCode(&ctx->errors[i]) == QUERY_ETIMEDOUT) {
                // FAIL policy: forward the standard timeout error directly,
                // matching the standalone path which uses QueryError_Strerror().
                QueryError_SetCode(status, QUERY_ETIMEDOUT);
                success = false;
                break;
            } else {
                QueryError_SetWithoutUserDataFmt(status, QueryError_GetCode(&ctx->errors[i]), "Failed to process shard responses, first error: %s, total error count: %zu",
                    QueryError_GetUserError(&ctx->errors[i]), array_len(ctx->errors));
                success = false;
                break;
            }
        }
    }
    MRIterator_Release(it);

    return success;
}
