/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "hybrid_test_dispatcher.h"
#include "hybrid_dispatcher.h"
#include "search_ctx.h"
#include "aggregate/aggregate.h"
#include "redismodule.h"
#include "rmutil/rm_assert.h"
#include "query_error.h"
#include "coord/rmr/rmr.h"
#include <unistd.h>


// Test command: FT.TEST.DISPATCHER <index>
// This command creates a hybrid dispatcher and tests cursor parsing with a simple ping
int HybridTestDispatcherCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  RS_AutoMemory(ctx);

  // Get index name
  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  RedisSearchCtx *sctx = NewSearchCtxC(ctx, indexname, true);
  if (!sctx) {
    QueryError status = {0};
    QueryError_SetWithUserDataFmt(&status, QUERY_ENOINDEX, "No such index", " %s", indexname);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  // Create a simple dispatcher that sends a ping command
  // For testing, we'll create minimal AREQ requests
  AREQ *dummy_req = AREQ_New();
  initializeAREQ(dummy_req);
  dummy_req->sctx = sctx;

  // Create array with single dummy request
  arrayof(AREQ*) requests = array_new(AREQ*, 1);
  array_append(requests, dummy_req);

  // Create hybrid dispatcher using the factory function
  HybridDispatcher *dispatcher = HybridDispatcher_New(sctx, requests, 1);
  if (!dispatcher) {
    array_free(requests);
    AREQ_Free(dummy_req);
    return RedisModule_ReplyWithError(ctx, "Failed to create hybrid dispatcher");
  }

  // Start the dispatcher (this will send a ping-like command to shards)
  int rc = hybridDispatcherNext_Start(dispatcher);
  if (rc != REDISMODULE_OK) {
    HybridDispatcher_Free(dispatcher);
    array_free(requests);
    AREQ_Free(dummy_req);
    return RedisModule_ReplyWithError(ctx, "Failed to start hybrid dispatcher");
  }

  // Mark setup as complete after processing all responses
  dispatcher->setup_complete = true;
  RedisModule_Log(NULL, "warning", "Marked setup as complete");

  // Get cursor counts
  size_t search_cursor_count = array_len(dispatcher->search_cursors);
  size_t vsim_cursor_count = array_len(dispatcher->vsim_cursors);

  // Reply with dispatcher status
  RedisModule_ReplyWithArray(ctx, 4);
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  RedisModule_ReplyWithLongLong(ctx, search_cursor_count);
  RedisModule_ReplyWithLongLong(ctx, vsim_cursor_count);

  // Add cursor details if any
  if (search_cursor_count > 0 || vsim_cursor_count > 0) {
    RedisModule_ReplyWithArray(ctx, search_cursor_count + vsim_cursor_count);
    for (size_t i = 0; i < search_cursor_count; i++) {
      RedisModule_ReplyWithLongLong(ctx, dispatcher->search_cursors[i]);
    }
    for (size_t i = 0; i < vsim_cursor_count; i++) {
      RedisModule_ReplyWithLongLong(ctx, dispatcher->vsim_cursors[i]);
    }
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "NO_CURSORS");
  }

  // Cleanup
  HybridDispatcher_Free(dispatcher);
  array_free(requests);
  AREQ_Free(dummy_req);

  return REDISMODULE_OK;
}
