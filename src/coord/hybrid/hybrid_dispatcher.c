/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "hybrid_dispatcher.h"
#include "../../rmalloc.h"
#include "redismodule.h"
#include "../../../deps/rmutil/rm_assert.h"
#include <string.h>
#include <unistd.h>

#define INTERNAL_HYBRID_RESP3_LENGTH 2
#define INTERNAL_HYBRID_RESP2_LENGTH 4

static void HybridDispatcher_MarkStarted(HybridDispatcher *dispatcher) {
    atomic_store(&dispatcher->started, true);
}

// Unified function for waiting until mappings are complete
void HybridDispatcher_WaitForMappingsComplete(HybridDispatcher *dispatcher) {
    pthread_mutex_lock(&dispatcher->data_mutex);
    while (array_len(dispatcher->searchMappings) != dispatcher->numShards || array_len(dispatcher->vsimMappings) != dispatcher->numShards) {
        pthread_cond_wait(&dispatcher->mapping_ready_cond, &dispatcher->data_mutex);
    }
    pthread_mutex_unlock(&dispatcher->data_mutex);
}

bool HybridDispatcher_IsStarted(const HybridDispatcher *dispatcher) {
    return atomic_load(&dispatcher->started);
}

static int HybridDispatcher_AddMapping(HybridDispatcher *dispatcher, CursorMapping *mapping, bool isSearch) {
    pthread_mutex_lock(&dispatcher->data_mutex);

    if (isSearch) {
        array_append(dispatcher->searchMappings, mapping);
    } else {
        array_append(dispatcher->vsimMappings, mapping);
    }
    // Signal that mappings have been added
    pthread_cond_broadcast(&dispatcher->mapping_ready_cond);
    pthread_mutex_unlock(&dispatcher->data_mutex);
    return 0; // RS_RESULT_OK
}


// Process cursor mappings for RESP2 protocol
static void processHybridResp2(HybridDispatcher *dispatcher, MRReply *rep, MRCommand *cmd) {
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
        RedisModule_Log(NULL, "warning", "processHybridResp2: adding mapping for shard %d, cursorId=%lld, isSearch=%d", mapping->targetSlot, mapping->cursorId, isSearch);
        HybridDispatcher_AddMapping(dispatcher, mapping, isSearch);
    }
}

// Process cursor mappings for RESP3 protocol
static void processHybridResp3(HybridDispatcher *dispatcher, MRReply *rep, MRCommand *cmd) {
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
        HybridDispatcher_AddMapping(dispatcher, mapping, isSearch[i]);
    }
}

// Callback implementation for processing cursor mappings
static void processCursorMappingCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {

    // TODO: add response validation (see netCursorCallback)
    // TODO implement error handling
    HybridDispatcher *dispatcher = (HybridDispatcher *)MRIteratorCallback_GetPrivateData(ctx);
    MRCommand *cmd = MRIteratorCallback_GetCommand(ctx);

    char *cmd_str = MRReply_String(rep, NULL);
    RedisModule_Log(NULL, "warning", "processCursorMappingCallback: cmd=%s", cmd_str);
    free(cmd_str);

    // Detect protocol version based on reply type
    bool isResp3 = MRReply_Type(rep) == MR_REPLY_MAP;

    RedisModule_Log(NULL, "warning", "processCursorMappingCallback: isResp3=%d", isResp3);
    if (isResp3) {
        RS_ASSERT(MRReply_Type(rep) == MR_REPLY_MAP && MRReply_Length(rep) == INTERNAL_HYBRID_RESP3_LENGTH);
        processHybridResp3(dispatcher, rep, cmd);
    } else {
        RS_ASSERT(MRReply_Type(rep) == MR_REPLY_ARRAY && MRReply_Length(rep) == INTERNAL_HYBRID_RESP2_LENGTH);
        processHybridResp2(dispatcher, rep, cmd);
    }

    MRIteratorCallback_Done(ctx, 0);
    MRReply_Free(rep);
}

// Free function for HybridDispatcher
static void HybridDispatcher_Free(void *obj) {
    HybridDispatcher *dispatcher = (HybridDispatcher *)obj;

    // Free command
    MRCommand_Free(&dispatcher->cmd);
    // Destroy synchronization primitives
    pthread_mutex_destroy(&dispatcher->data_mutex);
    pthread_cond_destroy(&dispatcher->mapping_ready_cond);

    rm_free(dispatcher);
}

StrongRef HybridDispatcher_New(const MRCommand *cmd,const size_t numShards) {
    HybridDispatcher *dispatcher = rm_calloc(1, sizeof(HybridDispatcher));

    dispatcher->numShards = numShards;

    // Initialize fields
    dispatcher->cmd = *cmd;
    pthread_mutex_init(&dispatcher->data_mutex, NULL);
    pthread_cond_init(&dispatcher->mapping_ready_cond, NULL);

    atomic_init(&dispatcher->started, false);

    // Create self-reference
    dispatcher->self_ref = StrongRef_New(dispatcher, HybridDispatcher_Free);

    return dispatcher->self_ref;
}

static MRIterator *HybridDispatcher_ProcessMappings(HybridDispatcher *dispatcher) {

    RedisModule_Log(NULL, "warning", "HybridDispatcher_ProcessMappings: sending _FT.HYBRID to shards");

    return MR_IterateWithPrivateData(
        &dispatcher->cmd,
        processCursorMappingCallback,  // Hardcoded callback
        dispatcher,  // Pass dispatcher as private data
        iterStartCb,  // Hardcoded callback
        NULL
    );
}

int HybridDispatcher_Dispatch(HybridDispatcher *dispatcher) {
    // Mark as started
    HybridDispatcher_MarkStarted(dispatcher);

    RedisModule_Log(NULL, "warning", "HybridDispatcher_Dispatch: Starting iterator");
    // Start the iterator
    MRIterator *it = HybridDispatcher_ProcessMappings(dispatcher);
    if (!it) {
        return -1; // RS_RESULT_ERROR
    }

    // Wait for both search and vsim mappings to be complete
    HybridDispatcher_WaitForMappingsComplete(dispatcher);

    // Release the iterator
    MRIterator_Release(it);

    // TODO: should it return rc?
    return 0; // RS_RESULT_OK
}

void HybridDispatcher_SetMappingArray(HybridDispatcher *dispatcher, arrayof(CursorMapping *) mappings, bool isSearch) {
    pthread_mutex_lock(&dispatcher->data_mutex);

    if (isSearch) {
        dispatcher->searchMappings = mappings;
    } else {
        dispatcher->vsimMappings = mappings;
    }

    pthread_mutex_unlock(&dispatcher->data_mutex);
}

