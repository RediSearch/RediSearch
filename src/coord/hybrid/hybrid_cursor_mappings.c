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

#define INTERNAL_HYBRID_RESP3_LENGTH 2
#define INTERNAL_HYBRID_RESP2_LENGTH 4

typedef struct {
    arrayof(CursorMapping *) searchMappings;
    arrayof(CursorMapping *) vsimMappings;
    pthread_mutex_t *mutex;           // Mutex for array access and completion tracking
    pthread_cond_t *completion_cond;  // Condition variable for completion signaling
    int numShards;                    // Total number of expected shards
} processCursorMappingCallbackContext;

// Process cursor mappings for RESP2 protocol
static void processHybridResp2(processCursorMappingCallbackContext *ctx, MRReply *rep, MRCommand *cmd) {
    for (size_t i = 0; i < INTERNAL_HYBRID_RESP2_LENGTH; i += 2) {
        CursorMapping *mapping = rm_calloc(1, sizeof(CursorMapping));
        mapping->targetSlot = cmd->targetSlot;

        MRReply *key_reply = MRReply_ArrayElement(rep, i);
        MRReply *value_reply = MRReply_ArrayElement(rep, i + 1);
        const char *key = MRReply_String(key_reply, NULL);
        long long value;
        MRReply_ToInteger(value_reply, &value);

        bool isSearch = false;
        if (strcmp(key, "SEARCH") == 0) {
            mapping->cursorId = value;
            isSearch = true;
        } else if (strcmp(key, "VSIM") == 0) {
            mapping->cursorId = value;
            isSearch = false;
        } else {
            RS_ABORT("Unknown key");
        }

        // Thread-safe array append and completion tracking
        pthread_mutex_lock(ctx->mutex);
        if (isSearch) {
            ctx->searchMappings = array_ensure_append_1(ctx->searchMappings, mapping);
        } else {
            ctx->vsimMappings = array_ensure_append_1(ctx->vsimMappings, mapping);
        }

        RedisModule_Log(NULL, "warning", "processHybridResp2: key=%s, value=%lld, isSearch=%d, search_mappings_len=%d, vsim_mappings_len=%d", key, value, isSearch, array_len(ctx->searchMappings), array_len(ctx->vsimMappings));
        // Check completion using array lengths
        int total_mappings = array_len(ctx->searchMappings) + array_len(ctx->vsimMappings);
        if (total_mappings >= ctx->numShards * 2) { // 2 mappings per shard (search + vsim)
            pthread_cond_broadcast(ctx->completion_cond);
        }
        pthread_mutex_unlock(ctx->mutex);
    }
}

// Process cursor mappings for RESP3 protocol
static void processHybridResp3(processCursorMappingCallbackContext *ctx, MRReply *rep, MRCommand *cmd) {
    // RESP3 uses a map structure instead of array pairs
    const char *keys[] = {"SEARCH", "VSIM"};
    const bool isSearch[] = {true, false};

    for (int i = 0; i < INTERNAL_HYBRID_RESP3_LENGTH; i++) {
        MRReply *cursorId = MRReply_MapElement(rep, keys[i]);
        RS_ASSERT(cursorId);

        CursorMapping *mapping = rm_calloc(1, sizeof(CursorMapping));
        mapping->targetSlot = cmd->targetSlot;
        long long cid;
        MRReply_ToInteger(cursorId, &cid);
        mapping->cursorId = cid;

        // Thread-safe array append and completion tracking
        pthread_mutex_lock(ctx->mutex);
        if (isSearch[i]) {
            ctx->searchMappings = array_ensure_append_1(ctx->searchMappings, mapping);
        } else {
            ctx->vsimMappings = array_ensure_append_1(ctx->vsimMappings, mapping);
        }

        // Check completion using array lengths
        int total_mappings = array_len(ctx->searchMappings) + array_len(ctx->vsimMappings);
        if (total_mappings >= ctx->numShards * 2) { // 2 mappings per shard (search + vsim)
            pthread_cond_broadcast(ctx->completion_cond);
        }
        pthread_mutex_unlock(ctx->mutex);
    }
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

int ProcessHybridCursorMappings(
    const MRCommand *cmd,
    int numShards,
    arrayof(CursorMapping *) searchMappings,
    arrayof(CursorMapping *) vsimMappings
) {
    RS_ASSERT(array_len(searchMappings) == 0 && array_len(vsimMappings) == 0);

    // Initialize synchronization primitives
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t completion_cond = PTHREAD_COND_INITIALIZER;

    // Setup callback context
    processCursorMappingCallbackContext ctx = {
        .searchMappings = searchMappings,
        .vsimMappings = vsimMappings,
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
    while ((array_len(searchMappings) + array_len(vsimMappings)) < expected_total) {
        pthread_cond_wait(&completion_cond, &mutex);
    }
    pthread_mutex_unlock(&mutex);

    // Cleanup
    MRIterator_Release(it);
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&completion_cond);

    return 0; // RS_RESULT_OK
}
