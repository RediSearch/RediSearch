#include "redismodule.h"
#include "redisearch.h"
#include "search_ctx.h"
#include "aggregate.h"
#include "cursor.h"

typedef enum { COMMAND_AGGREGATE, COMMAND_SEARCH, COMMAND_EXPLAIN } CommandType;

static int buildRequest(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int type,
                        QueryError *status, AREQ **r) {

  int rc = REDISMODULE_ERR;
  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  *r = AREQ_New();
  RedisSearchCtx *sctx = NULL;

  if (type == COMMAND_SEARCH) {
    (*r)->reqflags |= QEXEC_F_IS_SEARCH;
  }

  if (AREQ_Compile(*r, argv + 2, argc - 2, status) != REDISMODULE_OK) {
    assert(QueryError_HasError(status));
    goto done;
  }

  // Prepare the query.. this is where the context is applied.
  sctx = NewSearchCtxC(ctx, indexname);
  if (!sctx) {
    QueryError_SetErrorFmt(status, QUERY_ENOINDEX, "%s: no such index", indexname);
    goto done;
  }

  if (AREQ_ApplyContext(*r, sctx, status) != REDISMODULE_OK) {
    assert(QueryError_HasError(status));
    goto done;
  }

  rc = AREQ_BuildPipeline(*r, status);

done:
  if (rc != REDISMODULE_OK && *r) {
    AREQ_Free(*r);
    *r = NULL;
  }
  return rc;
}

static int execCommandCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                             CommandType type) {
  // Index name is argv[1]
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  AREQ *r = NULL;
  QueryError status = {0};

  if (buildRequest(ctx, argv, argc, type, &status, &r) != REDISMODULE_OK) {
    goto error;
  }

  // Execute() will call free when appropriate.
  AREQ_Execute(r, ctx);
  return REDISMODULE_OK;

error:
  if (r) {
    AREQ_Free(r);
  }

  return QueryError_ReplyAndClear(ctx, &status);
}

int RSAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return execCommandCommon(ctx, argv, argc, COMMAND_AGGREGATE);
}
int RSSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return execCommandCommon(ctx, argv, argc, COMMAND_SEARCH);
}

char *RS_GetExplainOutput(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                          QueryError *status) {
  AREQ *r = NULL;
  if (buildRequest(ctx, argv, argc, COMMAND_EXPLAIN, status, &r) != REDISMODULE_OK) {
    return NULL;
  }
  char *ret = Query_DumpExplain(&r->ast, r->sctx->spec);
  AREQ_Free(r);
  return ret;
}

void RSCursorCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
}

#if 0
void AggregateCommand_ExecAggregateEx(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                      struct ConcurrentCmdCtx *cmdCtx,
                                      const AggregateRequestSettings *settings) {

  // at least one field, and number of field/text args must be even

  RedisModule_AutoMemory(ctx);
  RedisSearchCtx *sctx;
  if (settings->flags & AGGREGATE_REQUEST_SPECLESS) {
    sctx = NewSearchCtxDefault(ctx);
  } else {
    sctx = NewSearchCtx(ctx, argv[1]);
  }
  if (sctx == NULL) {
    RedisModule_ReplyWithError(ctx, "Unknown Index name");
    return;
  }

  AggregateRequest req_s = {NULL}, *req = &req_s;
  int hasCursor = 0;
  QueryError status = {0};

  if (AggregateRequest_Start(req, sctx, settings, argv, argc, &status) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
    QueryError_ClearError(&status);
    goto done;
  }

  if (req->ap.hasCursor) {
    // Using a cursor here!
    const char *idxName = settings->cursorLookupName ? settings->cursorLookupName
                                                     : RedisModule_StringPtrLen(argv[1], NULL);

    Cursor *cursor =
        Cursors_Reserve(&RSCursors, sctx, idxName, req->ap.cursor.maxIdle, &status.detail);
    if (!cursor) {
      QueryError_MaybeSetCode(&status, QUERY_ECURSORALLOC);
      RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
      QueryError_ClearError(&status);
      goto done;
    }

    req = AggregateRequest_Persist(req);
    req->plan->opts.flags |= Search_IsCursor;
    cursor->execState = req;
    /* Don't let the context get removed from under our feet */
    if (cmdCtx) {
      ConcurrentCmdCtx_KeepRedisCtx(cmdCtx);
    } else {
      sctx->redisCtx = RedisModule_GetThreadSafeContext(NULL);
      // ctx is still the original output context - so don't change it!
    }
    runCursor(ctx, cursor, req->ap.cursor.count);
    return;
  }

  AggregateRequest_Run(req, sctx->redisCtx);

done:
  AggregateRequest_Free(req);
  SearchCtx_Free(sctx);
}

#endif

static void runCursor(RedisModuleCtx *outputCtx, Cursor *cursor, size_t num) {
  AggregateRequest *req = cursor->execState;
  if (!num) {
    num = req->cursorChunkSize;
    if (!num) {
      num = RSGlobalConfig.cursorReadSize;
    }
  }

  req->cursorChunkSize = num;
  RedisModule_ReplyWithArray(outputCtx, 2);
  AREQ_Execute(req, outputCtx);

  if (req->stateflags & QEXEC_S_ERROR) {
    RedisModule_ReplyWithLongLong(outputCtx, 0);
    goto delcursor;
  }

  if (req->stateflags & QEXEC_S_OUTPOUTDONE) {
    // Write the count!
    RedisModule_ReplyWithLongLong(outputCtx, 0);
  } else {
    RedisModule_ReplyWithLongLong(outputCtx, cursor->id);
  }

  if (req->stateflags & QEXEC_S_OUTPOUTDONE) {
    goto delcursor;
  } else {
    // Update the idle timeout
    Cursor_Pause(cursor);
    return;
  }

delcursor:
  AREQ_Free(req);
  Cursor_Free(cursor);
}

/**
 * FT.CURSOR READ {index} {CID} {ROWCOUNT} [MAXIDLE]
 * FT.CURSOR DEL {index} {CID}
 * FT.CURSOR GC {index}
 */
static void cursorRead(RedisModuleCtx *ctx, uint64_t cid, size_t count) {
  Cursor *cursor = Cursors_TakeForExecution(&RSCursors, cid);
  if (cursor == NULL) {
    RedisModule_ReplyWithError(ctx, "Cursor not found");
    return;
  }
  AggregateRequest *req = cursor->execState;
  ConcurrentSearchCtx_ReopenKeys(&req->conc);
  runCursor(ctx, cursor, count);
}

void AggregateCommand_ExecCursor(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                 struct ConcurrentCmdCtx *unused) {
  if (argc < 4) {
    RedisModule_WrongArity(ctx);
    return;
  }

  const char *cmd = RedisModule_StringPtrLen(argv[1], NULL);
  long long cid = 0;
  // argv[1] - FT.CURSOR
  // argv[1] - subcommand
  // argv[2] - index
  // argv[3] - cursor ID

  if (RedisModule_StringToLongLong(argv[3], &cid) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, "Bad cursor ID");
    return;
  }

  char cmdc = toupper(*cmd);

  if (cmdc == 'R') {
    long long count = 0;
    if (argc > 5) {
      // e.g. 'COUNT <timeout>'
      if (RedisModule_StringToLongLong(argv[5], &count) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "Bad value for COUNT");
        return;
      }
    }
    cursorRead(ctx, cid, count);

  } else if (cmdc == 'D') {
    int rc = Cursors_Purge(&RSCursors, cid);
    if (rc != REDISMODULE_OK) {
      RedisModule_ReplyWithError(ctx, "Cursor does not exist");
    } else {
      RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

  } else if (cmdc == 'G') {
    int rc = Cursors_CollectIdle(&RSCursors);
    RedisModule_ReplyWithLongLong(ctx, rc);
  } else {
    printf("Unknown command %s\n", cmd);
    RedisModule_ReplyWithError(ctx, "Unknown subcommand");
  }
}
