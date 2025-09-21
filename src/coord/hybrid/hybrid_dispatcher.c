/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "hybrid_dispatcher.h"
#include "rmr/rmr.h"
#include "../../rmalloc.h"
#include "redismodule.h"
#include "../../../deps/rmutil/rm_assert.h"
#include <string.h>
#include <unistd.h>

static void HybridDispatcher_MarkStarted(HybridDispatcher *dispatcher) {
    atomic_store(&dispatcher->started, true);
}

static void HybridDispatcher_MarkDone(HybridDispatcher *dispatcher) {
    atomic_store(&dispatcher->done, true);
}

bool HybridDispatcher_IsStarted(HybridDispatcher *dispatcher) {
    return atomic_load(&dispatcher->started);
}

static int HybridDispatcher_AddMapping(HybridDispatcher *dispatcher, CursorMapping *mapping, bool isSearch) {
    pthread_mutex_lock(&dispatcher->data_mutex);

    if (isSearch) {
        array_append(dispatcher->searchMappings, mapping);
    } else {
        array_append(dispatcher->vsimMappings, mapping);
    }

    pthread_mutex_unlock(&dispatcher->data_mutex);
    return 0; // RS_RESULT_OK
}


// Callback implementation for processing cursor mappings
static void processCursorMappingCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {

    HybridDispatcher *dispatcher = (HybridDispatcher *)MRIteratorCallback_GetPrivateData(ctx);

    for (size_t i = 0; i < 4; i += 2) {

        CursorMapping *mapping = rm_calloc(1, sizeof(CursorMapping));
        mapping->targetSlot = MRIteratorCallback_GetCommand(ctx)->targetSlot;;

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
        if (isSearch) {
            HybridDispatcher_AddMapping(dispatcher, mapping, isSearch);
        }
    }

    MRIteratorCallback_Done(ctx, 0);
    MRReply_Free(rep);
}

// Free function for HybridDispatcher
static void HybridDispatcher_Free(void *obj) {
    HybridDispatcher *dispatcher = (HybridDispatcher *)obj;

    // Free command
    MRCommand_Free(&dispatcher->cmd);
    // Destroy mutex
    pthread_mutex_destroy(&dispatcher->data_mutex);

    rm_free(dispatcher);
}


static arrayof(CursorMapping *) HybridDispatcher_GetSearchMappings(HybridDispatcher *dispatcher) {
    pthread_mutex_lock(&dispatcher->data_mutex);
    arrayof(CursorMapping *) result = dispatcher->searchMappings;
    pthread_mutex_unlock(&dispatcher->data_mutex);
    return result;
}

StrongRef HybridDispatcher_New(const MRCommand *cmd) {
    HybridDispatcher *dispatcher = rm_calloc(1, sizeof(HybridDispatcher));

    // Initialize fields
    dispatcher->cmd = *cmd;
    pthread_mutex_init(&dispatcher->data_mutex, NULL);

    atomic_init(&dispatcher->started, false);
    atomic_init(&dispatcher->done, false);

    // Create self-reference
    dispatcher->self_ref = StrongRef_New(dispatcher, HybridDispatcher_Free);

    return dispatcher->self_ref;
}

static MRIterator *HybridDispatcher_ProcessMappings(HybridDispatcher *dispatcher) {
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

    // Start the iterator
    MRIterator *it = HybridDispatcher_ProcessMappings(dispatcher);
    if (!it) {
        return -1; // RS_RESULT_ERROR
    }

    // 4 is the number of shards
    while (array_len(HybridDispatcher_GetSearchMappings(dispatcher)) < 4 ) {
        usleep(1000);
    }

    // Mark as done
    HybridDispatcher_MarkDone(dispatcher);

    // Release the iterator
    MRIterator_Release(it);

    // todo: should it return rc?
    return 0; // RS_RESULT_OK
}

static arrayof(CursorMapping *) HybridDispatcher_GetVsimMappings(HybridDispatcher *dispatcher) {
    pthread_mutex_lock(&dispatcher->data_mutex);
    arrayof(CursorMapping *) result = dispatcher->vsimMappings;
    pthread_mutex_unlock(&dispatcher->data_mutex);
    return result;
}


static size_t HybridDispatcher_GetTotalMappings(HybridDispatcher *dispatcher) {
    pthread_mutex_lock(&dispatcher->data_mutex);
    size_t searchCount = array_len(dispatcher->searchMappings);
    size_t vsimCount = array_len(dispatcher->vsimMappings);
    pthread_mutex_unlock(&dispatcher->data_mutex);
    return searchCount + vsimCount;
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

bool HybridDispatcher_IsDone(HybridDispatcher *dispatcher) {
    return atomic_load(&dispatcher->done);
}
