#include "redismodule.h"
#include "redisearch.h"
#include "search_ctx.h"
#include "aggregate.h"
#include "cursor.h"
#include "alias.h"

static void runCursor(RedisModuleCtx *outputCtx, Cursor *cursor, size_t num);

void AggregateCommand_ExecAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                    struct ConcurrentCmdCtx *cmdCtx) {
  AggregateRequestSettings settings = {.pcb = Aggregate_DefaultChainBuilder};
  if (!cmdCtx) {
    settings.flags |= AGGREGATE_REQUEST_NO_CONCURRENT;
  }
  AggregateCommand_ExecAggregateEx(ctx, argv, argc, cmdCtx, &settings);
}

/**
 * File containing top-level execution routines for aggregations
 */
/*
  FT.AGGREGATE
  {idx:string}
  {FILTER:string}
  SELECT {nargs:integer} {string} ...
  GROUPBY
    {nargs:integer} {string} ...
    [AS {AS:string}]
    REDUCE
      {FUNC:string}
      {nargs:integer} {string} ...
      [AS {AS:string}]


  [SORTBY {nargs:integer} {string} ...]
  [PROJECT
    {FUNC:string}
    {nargs:integer} {string} ...
    [AS {AS:string}]
  ] */
void AggregateCommand_ExecAggregateEx(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                      struct ConcurrentCmdCtx *cmdCtx,
                                      const AggregateRequestSettings *settings) {

  // at least one field, and number of field/text args must be even

  RedisModule_AutoMemory(ctx);
  RedisSearchCtx *sctx;

  if (settings->cursorLookupName) {
    // if we reached here we came from coordinator and its not holding the GIL
    // so we must acquire the GIL
    RedisModule_ThreadSafeContextLock(ctx);
    IndexLoadOptions lOpts = {.name = {.cstring = settings->cursorLookupName}};
    IndexSpec *sp = IndexSpec_LoadEx(ctx, &lOpts);
    if (!sp) {
      // check if its not alias
      size_t targetLen = rindex(settings->cursorLookupName, '{') - settings->cursorLookupName;
      settings->cursorLookupName[targetLen] = '\0';
      sp = IndexAlias_Get(settings->cursorLookupName);
      if (!sp) {
        RedisModule_ReplyWithError(ctx, "Unknown Index name");
        RedisModule_ThreadSafeContextUnlock(ctx);
        return;
      }

      // This is an alias, let return the cursor lookup name
      settings->cursorLookupName[targetLen] = '{';
    }
    RedisModule_ThreadSafeContextUnlock(ctx);
  }

  if (settings->flags & AGGREGATE_REQUEST_SPECLESS) {
    sctx = NewSearchCtxDefault(ctx);
  } else {
    sctx = NewSearchCtx(ctx, argv[1], true);
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

static void runCursor(RedisModuleCtx *outputCtx, Cursor *cursor, size_t num) {
  AggregateRequest *req = cursor->execState;
  if (!num) {
    num = req->ap.cursor.count;
    if (!num) {
      num = RSGlobalConfig.cursorReadSize;
    }
  }
  req->plan->opts.chunksize = num;
  clock_gettime(CLOCK_MONOTONIC_RAW, &req->plan->execCtx.startTime);

  RedisModule_ReplyWithArray(outputCtx, 2);
  AggregateRequest_Run(req, outputCtx);
  if (req->plan->outputFlags & QP_OUTPUT_FLAG_ERROR) {
    RedisModule_ReplyWithLongLong(outputCtx, 0);
    goto delcursor;
  }

  if (req->plan->outputFlags & QP_OUTPUT_FLAG_DONE) {
    // Write the count!
    RedisModule_ReplyWithLongLong(outputCtx, 0);
  } else {
    RedisModule_ReplyWithLongLong(outputCtx, cursor->id);
  }

  if (req->plan->outputFlags & QP_OUTPUT_FLAG_DONE) {
    goto delcursor;
  } else {
    // Update the idle timeout
    Cursor_Pause(cursor);
    return;
  }

delcursor:
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
  if (req->plan->conc) {
    ConcurrentSearchCtx_ReopenKeys(req->plan->conc);
  }
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
