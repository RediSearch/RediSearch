/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "hybrid_dispatcher.h"
#include "coord/rmr/rmr.h"
#include "coord/rmr/command.h"
#include "hybrid/hybrid_request.h"
#include "query_error.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "util/references.h"
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include "util/misc.h"


// Create a new HybridDispatcher (simplified for single-threaded use)
HybridDispatcher *HybridDispatcher_New(RedisSearchCtx *sctx, AREQ **requests, size_t nrequests) {
    HybridDispatcher *hd = rm_calloc(1, sizeof(HybridDispatcher));
    if (!hd) return NULL;

    // Initialize flags
    hd->hybrid_dispatched = false;
    hd->setup_complete = false;

    // Initialize cursor arrays
    hd->search_cursors = array_new(long long, 0);
    hd->vsim_cursors = array_new(long long, 0);
    hd->num_shards = 0;

    // Store the first request for command building
    hd->areq = requests[0];

    return hd;
  }


// Process one response at a time
int hybridDispatcherProcessResponse(HybridDispatcher *hd) {
  if (!hd || !hd->it) {
    return RS_RESULT_ERROR;
  }
  MRReply *rep = MRIterator_Next(hd->it);

  if (rep == NULL) {
      // No response available right now
      if (MRIterator_GetPending(hd->it) == 0) {
          // No more responses coming, we're done
          RedisModule_Log(NULL, "warning", "HybridDispatcherProcessResponse: No more responses coming, done");
          return RS_RESULT_EOF;
      } else {
            //what to do here?
      }
  }

  // Parse cursor map from the response
  HybridCursorMap cursorInfo = parseHybridCursorResponse(rep);

  // Store cursor information in dispatcher
  if (cursorInfo.has_search && hd->search_cursors != NULL) {
      array_append(hd->search_cursors, cursorInfo.search_cursor);
  }
  if (cursorInfo.has_vsim && hd->vsim_cursors != NULL) {
      array_append(hd->vsim_cursors, cursorInfo.vsim_cursor);
  }

  return RS_RESULT_OK;
}


// Callback function to handle responses from shards
static void hybridCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {
  RedisModule_Log(NULL, "warning", "HybridCursorCallback: response from slot %d", (MRIteratorCallback_GetCommand(ctx)->targetSlot));

  // Store the response for later processing
  MRIteratorCallback_AddReply(ctx, rep);
  MRIteratorCallback_Done(ctx, 0);
}


// Free a HybridDispatcher (for single-threaded use)
void HybridDispatcher_Free(HybridDispatcher *hd) {
  if (!hd) return;

  // Free resources
  if (hd->it) {
    MRIterator_Release(hd->it);
  }
  MRCommand_Free(&hd->cmd);
  array_free(hd->search_cursors);
  array_free(hd->vsim_cursors);
  rm_free(hd);
}

// Dispatch functionality - handles command dispatch and cursor collection
int hybridDispatcherNext_Start(HybridDispatcher *hd) {
  // Simple dispatch check for single-threaded use
  if (hd->hybrid_dispatched) {
    return RS_RESULT_OK; // Already dispatched
  }

  // Dispatch test cursors command to all shards
  MRCommand cmd = MR_NewCommand(1, "_FT.TEST.CURSORS");
  cmd.forCursor = false;
  MRIterator *it = MR_Iterate(&cmd, hybridCursorCallback);
  RedisModule_Log(NULL, "warning", "HybridDispatcherNext_Start: it: %p", it);
  if (!it) {
    return RS_RESULT_ERROR;
  }

  hd->it = it;
  hd->cmd = cmd;
  hd->hybrid_dispatched = true;

  // Process any available responses
  hybridDispatcherProcessResponse(hd);
  hybridDispatcherProcessResponse(hd);
  hybridDispatcherProcessResponse(hd);
  //this doesnt work and prevent the coordinator shard to respond in hybridCursorCallback
//   hybridDispatcherProcessResponse(hd);
  return RS_RESULT_OK;
}


// Parse cursor array response from shards
// Expected format: ["SEARCH", s_cid, "VSIM", v_cid] (4 elements total)
HybridCursorMap parseHybridCursorResponse(MRReply *rep) {
  HybridCursorMap result = {0};

  if (!rep || MRReply_Type(rep) != MR_REPLY_ARRAY || MRReply_Length(rep) != 4) {
    return result;
  }

  for (size_t i = 0; i < 4; i += 2) {
    MRReply *key_reply = MRReply_ArrayElement(rep, i);
    MRReply *value_reply = MRReply_ArrayElement(rep, i + 1);

    if (!key_reply || !value_reply ||
        MRReply_Type(key_reply) != MR_REPLY_STRING ||
        MRReply_Type(value_reply) != MR_REPLY_INTEGER) {
      continue;
    }

    const char *key = MRReply_String(key_reply, NULL);
    long long value;
    MRReply_ToInteger(value_reply, &value);

    if (strcmp(key, "SEARCH") == 0) {
      result.search_cursor = value;
      result.has_search = true;
    } else if (strcmp(key, "VSIM") == 0) {
      result.vsim_cursor = value;
      result.has_vsim = true;
    }
  }

  return result;
}
