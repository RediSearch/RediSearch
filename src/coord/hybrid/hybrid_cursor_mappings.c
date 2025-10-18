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
#include <string.h>


#define INTERNAL_HYBRID_RESP3_LENGTH 4
#define INTERNAL_HYBRID_RESP2_LENGTH 4

typedef struct {
    StrongRef searchMappings;
    StrongRef vsimMappings;
    arrayof(QueryError) errors;
    size_t responseCount;
    pthread_mutex_t *mutex;           // Mutex for array access and completion tracking
    pthread_cond_t *completionCond;   // Condition variable for completion signaling
    int numShards;                    // Total number of expected shards
} processCursorMappingCallbackContext;

static void processHybridError(processCursorMappingCallbackContext *ctx, const char *errorMessage) {
    QueryError error = QueryError_Default();
    QueryError_SetError(&error, QUERY_EGENERIC, errorMessage);
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
        CursorMapping mapping = {0};
        mapping.targetShard = cmd->targetShard;

        MRReply *key_reply = MRReply_ArrayElement(rep, i);
        MRReply *value_reply = MRReply_ArrayElement(rep, i + 1);
        const char *key = MRReply_String(key_reply, NULL);
        long long value;
        MRReply_ToInteger(value_reply, &value);

        CursorMappings *vsimOrSearch = NULL;
        if (strcmp(key, "SEARCH") == 0) {
            vsimOrSearch = StrongRef_Get(ctx->searchMappings);
            mapping.cursorId = value;
        } else if (strcmp(key, "VSIM") == 0) {
            vsimOrSearch = StrongRef_Get(ctx->vsimMappings);
            mapping.cursorId = value;
        }
        RS_ASSERT(vsimOrSearch);
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

        CursorMapping mapping = {0};
        mapping.targetShard = cmd->targetShard;
        long long cid;
        MRReply_ToInteger(cursorId, &cid);
        mapping.cursorId = cid;
        CursorMappings *vsimOrSearch = StrongRef_Get(*mappings[i]);
        RS_ASSERT(vsimOrSearch);
        vsimOrSearch->mappings = array_ensure_append_1(vsimOrSearch->mappings, mapping);
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
        const char* errorMessage = MRReply_String(rep, NULL);
        processHybridError(cb_ctx, errorMessage);
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

static inline void cleanupCtx(processCursorMappingCallbackContext *ctx) {
    pthread_mutex_destroy(ctx->mutex);
    pthread_cond_destroy(ctx->completionCond);
    rm_free(ctx->mutex);
    rm_free(ctx->completionCond);
    StrongRef_Release(ctx->searchMappings);
    StrongRef_Release(ctx->vsimMappings);
    array_free_ex(ctx->errors, QueryError_ClearError((QueryError*)ptr));
    rm_free(ctx);
}

bool ProcessHybridCursorMappings(const MRCommand *cmd, int numShards, StrongRef searchMappingsRef, StrongRef vsimMappingsRef, QueryError *status) {
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
    *ctx = (processCursorMappingCallbackContext){
        .searchMappings = StrongRef_Clone(searchMappingsRef),
        .vsimMappings = StrongRef_Clone(vsimMappingsRef),
        .errors = array_new(QueryError, numShards),
        .responseCount = 0,
        .mutex = ctx->mutex,
        .completionCond = ctx->completionCond,
        .numShards = numShards
    };

    // Start iteration
    MRIterator *it = MR_IterateWithPrivateData(cmd, processCursorMappingCallback, ctx, iterStartCb, NULL);
    if (!it) {
        // Cleanup on error
        QueryError_SetWithoutUserDataFmt(status, QUERY_EGENERIC, "Failed to communicate with shards");
        cleanupCtx(ctx);
        return false;
    }

    // Wait for all callbacks to complete
    pthread_mutex_lock(ctx->mutex);
    // initialize count with response counts in case some shards already sent a response
    for (size_t count = ctx->responseCount; count < numShards; count = ctx->responseCount) {
        pthread_cond_wait(ctx->completionCond, ctx->mutex);
    }
    pthread_mutex_unlock(ctx->mutex);
    bool success = true;
    if (array_len(ctx->errors)) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_EGENERIC, "Failed to process shard responses, first error: %s, total error count: %zu", QueryError_GetUserError(&ctx->errors[0]), array_len(ctx->errors));
        success = false;
    }

    // Cleanup
    MRIterator_Release(it);
    cleanupCtx(ctx);

    return success;
}
