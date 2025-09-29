/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "hybrid_cursor_mappings.h"
#include "../../rmalloc.h"
#include "redismodule.h"
#include "../../../deps/rmutil/rm_assert.h"
#include <string.h>

#define INTERNAL_HYBRID_RESP3_LENGTH 4
#define INTERNAL_HYBRID_RESP2_LENGTH 4

typedef struct {
    StrongRef searchMappings;
    StrongRef vsimMappings;
    pthread_mutex_t *mutex;           // Mutex for array access and completion tracking
    pthread_cond_t *completion_cond;  // Condition variable for completion signaling
    int numShards;                    // Total number of expected shards
} processCursorMappingCallbackContext;

// Process cursor mappings for RESP2 protocol
static void processHybridResp2(processCursorMappingCallbackContext *ctx, MRReply *rep, MRCommand *cmd) {
    pthread_mutex_lock(ctx->mutex);
    for (size_t i = 0; i < INTERNAL_HYBRID_RESP2_LENGTH; i += 2) {
        CursorMapping mapping = {0};
        mapping.targetSlot = cmd->targetSlot;

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
    pthread_cond_broadcast(ctx->completion_cond);
    pthread_mutex_unlock(ctx->mutex);
}

// Process cursor mappings for RESP3 protocol
static void processHybridResp3(processCursorMappingCallbackContext *ctx, MRReply *rep, MRCommand *cmd) {
    // RESP3 uses a map structure instead of array pairs
    const char *keys[] = {"SEARCH", "VSIM"};
    const bool isSearch[] = {true, false};
    const StrongRef* mappings[] = {&ctx->searchMappings, &ctx->vsimMappings};
    pthread_mutex_lock(ctx->mutex);
    for (int i = 0; i < 2; i++) {
        MRReply *cursorId = MRReply_MapElement(rep, keys[i]);
        RS_ASSERT(cursorId);

        CursorMapping mapping = {0};
        mapping.targetSlot = cmd->targetSlot;
        long long cid;
        MRReply_ToInteger(cursorId, &cid);
        mapping.cursorId = cid;
        CursorMappings *vsimOrSearch = StrongRef_Get(*mappings[i]);
        RS_ASSERT(vsimOrSearch);
        vsimOrSearch->mappings = array_ensure_append_1(vsimOrSearch->mappings, mapping);
    }
    pthread_cond_broadcast(ctx->completion_cond);
    pthread_mutex_unlock(ctx->mutex);
}

// Callback implementation for processing cursor mappings
static void processCursorMappingCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {
    // TODO: add response validation (see netCursorCallback)
    // TODO implement error handling
    processCursorMappingCallbackContext *cb_ctx = (processCursorMappingCallbackContext *)MRIteratorCallback_GetPrivateData(ctx);
    MRCommand *cmd = MRIteratorCallback_GetCommand(ctx);

    // Detect protocol version based on reply type
    bool isResp3 = MRReply_Type(rep) == MR_REPLY_MAP;

    if (isResp3) {
        RS_ASSERT(MRReply_Type(rep) == MR_REPLY_MAP && MRReply_Length(rep) == INTERNAL_HYBRID_RESP3_LENGTH);
        processHybridResp3(cb_ctx, rep, cmd);
    } else {
        RS_ASSERT(MRReply_Type(rep) == MR_REPLY_ARRAY && MRReply_Length(rep) == INTERNAL_HYBRID_RESP2_LENGTH);
        processHybridResp2(cb_ctx, rep, cmd);
    }

    MRIteratorCallback_Done(ctx, 0);
    MRReply_Free(rep);
}

int ProcessHybridCursorMappings(const MRCommand *cmd, int numShards, StrongRef searchMappingsRef, StrongRef vsimMappingsRef) {
    CursorMappings *searchMappings = StrongRef_Get(searchMappingsRef);
    CursorMappings *vsimMappings = StrongRef_Get(vsimMappingsRef);
    RS_ASSERT(array_len(searchMappings->mappings) == 0 && array_len(vsimMappings->mappings) == 0);

    // Initialize synchronization primitives
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t completion_cond = PTHREAD_COND_INITIALIZER;

    // Setup callback context
    processCursorMappingCallbackContext ctx = {
        .searchMappings = StrongRef_Clone(searchMappingsRef),
        .vsimMappings = StrongRef_Clone(vsimMappingsRef),
        .mutex = &mutex,
        .completion_cond = &completion_cond,
        .numShards = numShards
    };

    // Start iteration
    MRIterator *it = MR_IterateWithPrivateData(cmd, processCursorMappingCallback, &ctx, iterStartCb, NULL);
    if (!it) {
        // Cleanup on error
        pthread_mutex_destroy(&mutex);
        pthread_cond_destroy(&completion_cond);
        return 4; // RS_RESULT_ERROR
    }

    // Wait for all callbacks to complete
    pthread_mutex_lock(&mutex);
    int expected_total = numShards * 2; // 2 mappings per shard (search + vsim)
    for (size_t count = 0; count < expected_total;)
    {
        pthread_cond_wait(&completion_cond, &mutex);
        CursorMappings *searchMappings = StrongRef_Get(ctx.searchMappings);
        CursorMappings *vsimMappings = StrongRef_Get(ctx.vsimMappings);
        count = array_len(searchMappings->mappings) + array_len(vsimMappings->mappings);
    }
    pthread_mutex_unlock(&mutex);

    // Cleanup
    MRIterator_Release(it);
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&completion_cond);

    return 0; // RS_RESULT_OK
}
