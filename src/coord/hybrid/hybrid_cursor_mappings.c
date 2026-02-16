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
#include "query_error.h"
#include "shard_window_ratio.h"
#include <string.h>
#include <stdatomic.h>
#include "info/global_stats.h"
#include "coord/shard_barrier.h"
#include "util/timeout.h"

#define INTERNAL_HYBRID_RESP3_LENGTH 6
#define INTERNAL_HYBRID_RESP2_LENGTH 6

typedef struct {
    // ShardCountBarrier MUST be first field so shardCountBarrier_Init can be used directly
    ShardCountBarrier shardBarrier;    // Base barrier for tracking shard responses
    StrongRef searchMappings;
    StrongRef vsimMappings;
    arrayof(QueryError) errors;
    size_t responseCount;
    pthread_mutex_t *mutex;           // Mutex for array access and completion tracking
    pthread_cond_t *completionCond;   // Condition variable for completion signaling
    HybridKnnContext *knnCtx;         // KNN context for SHARD_K_RATIO optimization (may be NULL)
    const struct timespec *timeout;   // Absolute timeout in CLOCK_MONOTONIC_RAW (may be NULL)
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
                processHybridError(ctx, warningReply);
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
            processHybridError(ctx, warningReply);
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
    pthread_mutex_lock(cb_ctx->mutex);
    // add under a lock, allows the coordinator to know when all responses have arrived
    cb_ctx->responseCount++;
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

    // we must notify the coordinator a response has arrived, even if it's an error
    pthread_cond_signal(cb_ctx->completionCond);
    pthread_mutex_unlock(cb_ctx->mutex);

    MRIteratorCallback_Done(ctx, 0);
    MRReply_Free(rep);
}

// Cleanup callback context - used as privateDataDestructor for MRIterator
// Takes void* to match MRIterator's destructor signature
static void cleanupCtx(void *ptr) {
    processCursorMappingCallbackContext *ctx = (processCursorMappingCallbackContext *)ptr;
    if (!ctx) return;
    pthread_mutex_destroy(ctx->mutex);
    pthread_cond_destroy(ctx->completionCond);
    rm_free(ctx->mutex);
    rm_free(ctx->completionCond);
    StrongRef_Release(ctx->searchMappings);
    StrongRef_Release(ctx->vsimMappings);
    array_free_ex(ctx->errors, QueryError_ClearError((QueryError*)ptr));
    rm_free(ctx);
}

// Command modifier callback for SHARD_K_RATIO optimization.
// Called from iterStartCb on IO thread before commands are sent to shards.
void HybridKnnCommandModifier(MRCommand *cmd, size_t numShards, void *privateData) {
    if (!privateData || !cmd) {
        return;
    }
    const processCursorMappingCallbackContext *ctx = (processCursorMappingCallbackContext *)privateData;
    const HybridKnnContext *knnCtx = ctx->knnCtx;
    if (!knnCtx || knnCtx->kArgIndex < 0) {
        return;
    }
    // Only apply optimization for multi-shard deployments with valid ratio
    if (numShards <= 1 || knnCtx->shardWindowRatio >= MAX_SHARD_WINDOW_RATIO) {
        return;
    }
    size_t effectiveK = calculateEffectiveK(knnCtx->originalK, knnCtx->shardWindowRatio, numShards);
    if (effectiveK == knnCtx->originalK) {
        return;
    }

    // Replace the K value argument in the command
    char effectiveK_str[32];
    int len = snprintf(effectiveK_str, sizeof(effectiveK_str), "%zu", effectiveK);
    MRCommand_ReplaceArg(cmd, knnCtx->kArgIndex, effectiveK_str, len);
}

bool ProcessHybridCursorMappings(const MRCommand *cmd,
                                 StrongRef searchMappingsRef,
                                 StrongRef vsimMappingsRef,
                                 HybridKnnContext *knnCtx,
                                 QueryError *status,
                                 const RSOomPolicy oomPolicy,
                                 const struct timespec *timeout) {
    CursorMappings *searchMappings = StrongRef_Get(searchMappingsRef);
    CursorMappings *vsimMappings = StrongRef_Get(vsimMappingsRef);
    RS_ASSERT(array_len(searchMappings->mappings) == 0 && array_len(vsimMappings->mappings) == 0);

    // Allocate callback context on heap (since MR_IterateWithPrivateData is asynchronous)
    processCursorMappingCallbackContext *ctx = rm_malloc(sizeof(processCursorMappingCallbackContext));

    // Initialize synchronization primitives on heap
    ctx->mutex = rm_malloc(sizeof(pthread_mutex_t));
    ctx->completionCond = rm_malloc(sizeof(pthread_cond_t));
    pthread_mutex_init(ctx->mutex, NULL);
    pthread_cond_init(ctx->completionCond, NULL);

    // Setup callback context
    // shardBarrier.numShards is initialized to 0 and will be set atomically by
    // shardCountBarrier_Init when called from iterStartCb on the IO thread.
    // This ensures we wait for exactly as many responses as commands were
    // actually sent (based on the IO thread's topology snapshot).
    *ctx = (processCursorMappingCallbackContext){
        .searchMappings = StrongRef_Clone(searchMappingsRef),
        .vsimMappings = StrongRef_Clone(vsimMappingsRef),
        .errors = array_new(QueryError, 0),
        .responseCount = 0,
        .mutex = ctx->mutex,
        .completionCond = ctx->completionCond,
        .knnCtx = knnCtx,  // Store KNN context for command modifier callback
        .timeout = timeout,
    };
    // We must use atomic_init here (not rely on struct initialization)
    // because the coord thread may call atomic_load on numShards before
    // shardCountBarrier_Init runs.
    atomic_init(&ctx->shardBarrier.numShards, 0);
    atomic_init(&ctx->shardBarrier.numResponded, 0);

    // Pass HybridKnnCommandModifier if knnCtx is provided (for SHARD_K_RATIO
    // optimization)
    MRCommandModifier cmdModifier = knnCtx ? &HybridKnnCommandModifier : NULL;

    // Start iteration with shardCountBarrier_Init callback to set numShards
    // from IO thread.
    // IMPORTANT: Pass cleanupCtx as the destructor so the context is freed only
    // after all callbacks have completed. This prevents use-after-free when
    // timeout occurs while IO thread callbacks are still running.
    MRIterator *it = MR_IterateWithPrivateData(cmd, processCursorMappingCallback,
                                               ctx, cleanupCtx,
                                               shardCountBarrier_Init, cmdModifier,
                                               iterStartCb, NULL);
    if (!it) {
        // Cleanup on error
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_GENERIC, "Failed to communicate with shards");
        cleanupCtx(ctx);
        return false;
    }
    // Wait for all callbacks to complete
    // numShards is re-read on each iteration because it may initially be 0
    // (in case the IO thread iterStartCb did not run yet and did not initialize
    // numShards yet).
    // Once a reply arrives, iterStartCb has finished and numShards will be set.
    pthread_mutex_lock(ctx->mutex);
    size_t numShards;
    bool timedOut = false;
    while ((numShards = atomic_load(&ctx->shardBarrier.numShards)) == 0 ||
           ctx->responseCount < numShards) {
        if (condTimedWait(ctx->completionCond, ctx->mutex, timeout)) {
            timedOut = true;
            break;
        }
    }
    pthread_mutex_unlock(ctx->mutex);

    if (timedOut) {
        QueryError_SetCode(status, QUERY_ERROR_CODE_TIMED_OUT);
        // Release iterator - cleanupCtx will be called via privateDataDestructor
        // when all callbacks complete
        MRIterator_Release(it);
        return false;
    }

    bool success = true;
    if (array_len(ctx->errors)) {
        for (size_t i = 0; i < array_len(ctx->errors); i++) {
            if (QueryError_GetCode(&ctx->errors[i]) == QUERY_ERROR_CODE_OUT_OF_MEMORY && oomPolicy == OomPolicy_Return ) {
                QueryError_SetQueryOOMWarning(status);
            } else {
                QueryError_SetWithoutUserDataFmt(status, QueryError_GetCode(&ctx->errors[i]), "Failed to process shard responses, first error: %s, total error count: %zu",
                    QueryError_GetUserError(&ctx->errors[i]), array_len(ctx->errors));
                success = false;
                break;
            }
        }
    }
    // Release iterator - cleanupCtx will be called via privateDataDestructor
    // when all callbacks complete (refcount reaches 0)
    MRIterator_Release(it);

    return success;
}
