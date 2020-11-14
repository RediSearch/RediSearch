
#include "redisearch.h"
#include "search_ctx.h"
#include "aggregate.h"
#include "cursor.h"
#include "score_explain.h"

#include "redismodule.h"
#include "rmutil/util.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// Get the sorting key of the result. This will be the sorting key of the last RLookup registry. 
// Returns NULL if there is no sorting key.

const RSValue *AREQ::getSortKey(const SearchResult *r, const PLN_ArrangeStep *astp) {
  if (!astp) {
    return NULL;
  }
  const RLookupKey *kk = astp->sortkeysLK[0];
  if ((kk->flags & RLOOKUP_F_SVSRC) && (r->rowdata.sv && r->rowdata.sv->len > kk->svidx)) {
    return r->rowdata.sv->values[kk->svidx];
  } else {
    return RLookup_GetItem(astp->sortkeysLK[0], &r->rowdata);
  }
}

//---------------------------------------------------------------------------------------------

size_t AREQ::serializeResult(RedisModuleCtx *outctx, const SearchResult *r, const CachedVars &cv) {
  const uint32_t options = reqflags;
  const RSDocumentMetadata *dmd = r->dmd;
  size_t count = 0;

  if (dmd && (options & QEXEC_F_IS_SEARCH)) {
    size_t n;
    const char *s = DMD_KeyPtrLen(dmd, &n);
    RedisModule_ReplyWithStringBuffer(outctx, s, n);
    count++;
  }

  if (options & QEXEC_F_SEND_SCORES) {
    if (!(options & QEXEC_F_SEND_SCOREEXPLAIN)) {
      RedisModule_ReplyWithDouble(outctx, r->score);
    } else {
      RedisModule_ReplyWithArray(outctx, 2);
      RedisModule_ReplyWithDouble(outctx, r->score);
      SEReply(outctx, r->scoreExplain);
    }
    count++;
  }

  if (options & QEXEC_F_SENDRAWIDS) {
    RedisModule_ReplyWithLongLong(outctx, r->docId);
    count++;
  }

  if (options & QEXEC_F_SEND_PAYLOADS) {
    count++;
    if (dmd && dmd->payload) {
      RedisModule_ReplyWithStringBuffer(outctx, dmd->payload->data, dmd->payload->len);
    } else {
      RedisModule_ReplyWithNull(outctx);
    }
  }

  if ((options & QEXEC_F_SEND_SORTKEYS)) {
    count++;
    const RSValue *sortkey = getSortKey(r, cv.lastAstp);
    RedisModuleString *rskey = NULL;
reeval_sortkey:
    if (sortkey) {
      switch (sortkey->t) {
        case RSValue_Number:
          /* Serialize double - by prepending "%" to the number, so the coordinator/client can
           * tell it's a double and not just a numeric string value */
          rskey = RedisModule_CreateStringPrintf(outctx, "#%.17g", sortkey->numval);
          break;
        case RSValue_String:
          /* Serialize string - by prepending "$" to it */
          rskey = RedisModule_CreateStringPrintf(outctx, "$%s", sortkey->strval);
          break;
        case RSValue_RedisString:
        case RSValue_OwnRstring:
          rskey = RedisModule_CreateStringPrintf(outctx, "$%s",
                                                 RedisModule_StringPtrLen(sortkey->rstrval, NULL));
          break;
        case RSValue_Null:
        case RSValue_Undef:
        case RSValue_Array:
          break;
        case RSValue_Reference:
          sortkey = RSValue_Dereference(sortkey);
          goto reeval_sortkey;
      }
      if (rskey) {
        RedisModule_ReplyWithString(outctx, rskey);
        RedisModule_FreeString(outctx, rskey);
      } else {
        RedisModule_ReplyWithNull(outctx);
      }
    } else {
      RedisModule_ReplyWithNull(outctx);
    }
  }

  if (!(options & QEXEC_F_SEND_NOFIELDS)) {
    const RLookup *lk = cv.lastLk;
    count++;
    int excludeFlags = RLOOKUP_F_HIDDEN;
    int requiredFlags = outFields.explicitReturn ? RLOOKUP_F_EXPLICITRETURN : 0;
    size_t nfields = RLookup_GetLength(lk, &r->rowdata, requiredFlags, excludeFlags);

    RedisModule_ReplyWithArray(outctx, nfields * 2);

    for (const RLookupKey *kk = lk->head; kk; kk = kk->next) {
      if (kk->flags & RLOOKUP_F_HIDDEN) {
        // printf("Skipping hidden field %s/%p\n", kk->name, kk);
        // todo: this is a dead code, no one set RLOOKUP_F_HIDDEN
        continue;
      }
      if (outFields.explicitReturn && (kk->flags & RLOOKUP_F_EXPLICITRETURN) == 0) {
        continue;
      }
      const RSValue *v = RLookup_GetItem(kk, &r->rowdata);
      if (!v) {
        continue;
      }

      RedisModule_ReplyWithStringBuffer(outctx, kk->name, strlen(kk->name));
      RSValue_SendReply(outctx, v, reqflags & QEXEC_F_TYPED);
    }
  }
  return count;
}

//---------------------------------------------------------------------------------------------

// Sends a chunk of <n> rows, optionally also sending the preamble

int AREQ::sendChunk(RedisModuleCtx *outctx, size_t limit) {
  size_t nrows = 0;
  size_t nelem = 0;
  SearchResult r;
  int rc = RS_RESULT_EOF;
  ResultProcessor *rp = RP();

  CachedVars cv;
  cv.lastLk = AGPLN_GetLookup(&ap, NULL, AGPLN_GETLOOKUP_LAST);
  cv.lastAstp = AGPLN_GetArrangeStep(&ap);

  RedisModule_ReplyWithArray(outctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  rc = rp->Next(rp, &r);
  RedisModule_ReplyWithLongLong(outctx, qiter->totalResults);
  nelem++;
  if (rc == RS_RESULT_OK && nrows++ < limit && !(reqflags & QEXEC_F_NOROWS)) {
    nelem += serializeResult(outctx, &r, cv);
  } else if (rc == RS_RESULT_ERROR) {
    RedisModule_ReplyWithArray(outctx, 1);
    QueryError_ReplyAndClear(outctx, qiter->err);
    ++nelem;
  }

  r.Clear();
  if (rc != RS_RESULT_OK) {
    goto done;
  }

  while (nrows++ < limit && (rc = rp->Next(rp, &r)) == RS_RESULT_OK) {
    if (!(reqflags & QEXEC_F_NOROWS)) {
      nelem += serializeResult(outctx, &r, cv);
    }
    // Serialize it as a search result
    r.Clear();
  }

done:
  if (rc != RS_RESULT_OK) {
    stateflags |= QEXEC_S_ITERDONE;
  }
  // Reset the total results length:
  qiter->totalResults = 0;
  RedisModule_ReplySetArrayLength(outctx, nelem);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

void AREQ::Execute(RedisModuleCtx *outctx) {
  sendChunk(outctx, -1);
}

//---------------------------------------------------------------------------------------------

AREQ::AREQ(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, CommandType type,
           QueryError *status) {
  reqflags = 0;

  RedisModuleCtx *thctx = NULL;
  try {
    int rc = REDISMODULE_ERR;

    if (type == CommandType::Search) {
      reqflags |= QEXEC_F_IS_SEARCH;
    }

    if (Compile(argv + 2, argc - 2, status) != REDISMODULE_OK) {
      RS_LOG_ASSERT(status->HasError(), "Query has error");
      throw Error(status);
    }

    // Prepare the query.. this is where the context is applied.
    if (reqflags & QEXEC_F_IS_CURSOR) {
      thctx = RedisModule_GetThreadSafeContext(NULL);
      RedisModule_SelectDb(thctx, RedisModule_GetSelectedDb(ctx));
      ctx = thctx;  // In case of error!
    }

    const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
    sctx = std::make_unique<RedisSearchCtx>(ctx, indexname, true);
#if 0
    if (!sctx) {
      status->SetErrorFmt(QUERY_ENOINDEX, "%s: no such index", indexname);
      throw Error(status);
    }
#endif // 0

    rc = ApplyContext(status);
    thctx = NULL;
    // ctx is always assigned after ApplyContext
    if (rc != REDISMODULE_OK) {
      RS_LOG_ASSERT(status->HasError(), "Query has error");
      throw Error(status);
    }

    if (BuildPipeline(0, status) != REDISMODULE_OK) {
      throw Error(status);
    }
  } catch (...) {
    if (thctx) {
      RedisModule_FreeThreadSafeContext(thctx);
    }
    throw;
  }

  if (thctx) {
    RedisModule_FreeThreadSafeContext(thctx);
  }
}

//---------------------------------------------------------------------------------------------

static int execCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, CommandType type) {
  // Index name is argv[1]
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  QueryError status;
  try {
    AREQ r(ctx, argv, argc, type, &status);
    if (r.reqflags & QEXEC_F_IS_CURSOR) {
      int rc = r.StartCursor(ctx, r.sctx->spec->name, &status);
      if (rc != REDISMODULE_OK) {
        return QueryError_ReplyAndClear(ctx, &status);
      }
    } else {
      r.Execute(ctx);
    }
    return REDISMODULE_OK;
  } catch (Error &x) {
    return QueryError_ReplyAndClear(ctx, &status);
  }
}

//---------------------------------------------------------------------------------------------

int RSAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return execCommand(ctx, argv, argc, CommandType::Aggregate);
}

int RSSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return execCommand(ctx, argv, argc, CommandType::Search);
}

//---------------------------------------------------------------------------------------------

char *RS_GetExplainOutput(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                          QueryError *status) {
  try {
    AREQ r(ctx, argv, argc, CommandType::Explain, status);
    return r.ast->DumpExplain(r.sctx->spec);
  } catch (Error &x) {
    return NULL;
  }
}

//---------------------------------------------------------------------------------------------

/**
 * Start the cursor on the current request
 * @param r the request
 * @param outctx the context used for replies (only used in current command)
 * @param lookupName the name of the index used for the cursor reservation
 * @param status if this function errors, this contains the message
 * @return REDISMODULE_OK or REDISMODULE_ERR
 *
 * If this function returns REDISMODULE_OK then the cursor might have been
 * freed. If it returns REDISMODULE_ERR, then the cursor is still valid
 * and must be freed manually.
 */

int AREQ::StartCursor(RedisModuleCtx *outctx, const char *lookupName, QueryError *err) {
  Cursor *cursor = RSCursors->Reserve(lookupName, cursorMaxIdle, err);
  if (cursor == NULL) {
    return REDISMODULE_ERR;
  }
  cursor->execState = this;
  cursor->runCursor(outctx, 0);
  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////

void Cursor::runCursor(RedisModuleCtx *outctx, size_t num) {
  AREQ *req = execState;
  if (!num) {
    num = req->cursorChunkSize;
    if (!num) {
      num = RSGlobalConfig.cursorReadSize;
    }
  }
  req->cursorChunkSize = num;
  RedisModule_ReplyWithArray(outctx, 2);
  req->sendChunk(outctx, num);

  if (req->stateflags & QEXEC_S_ITERDONE) {
    // Write the count!
    RedisModule_ReplyWithLongLong(outctx, 0);
  } else {
    RedisModule_ReplyWithLongLong(outctx, id);
  }

  if (req->stateflags & QEXEC_S_ITERDONE) {
    Free();
  } else {
    // Update the idle timeout
    Pause();
  }
}

//---------------------------------------------------------------------------------------------

/**
 * FT.CURSOR READ {index} {CID} {ROWCOUNT} [MAXIDLE]
 * FT.CURSOR DEL {index} {CID}
 * FT.CURSOR GC {index}
 */

static void cursorRead(RedisModuleCtx *ctx, CursorId cid, size_t count) {
  Cursor *cursor = RSCursors->TakeForExecution(cid);
  if (cursor == NULL) {
    RedisModule_ReplyWithError(ctx, "Cursor not found");
    return;
  }
  QueryError status;
  AREQ *req = cursor->execState;
  req->qiter->err = &status;
  req->conc->ReopenKeys();
  cursor->runCursor(ctx, count);
  req->qiter->err = NULL; //@@ TODO: review: verify correctness
}

//---------------------------------------------------------------------------------------------

int RSCursorCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }

  const char *cmd = RedisModule_StringPtrLen(argv[1], NULL);
  long long cid = 0;
  // argv[1] - FT.CURSOR
  // argv[1] - subcommand
  // argv[2] - index
  // argv[3] - cursor ID

  if (RedisModule_StringToLongLong(argv[3], &cid) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, "Bad cursor ID");
    return REDISMODULE_OK;
  }

  char cmdc = toupper(*cmd);

  if (cmdc == 'R') {
    long long count = 0;
    if (argc > 5) {
      // e.g. 'COUNT <timeout>'
      if (RedisModule_StringToLongLong(argv[5], &count) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "Bad value for COUNT");
        return REDISMODULE_OK;
      }
    }
    cursorRead(ctx, cid, count);

  } else if (cmdc == 'D') {
    int rc = RSCursors->Purge(cid);
    if (rc != REDISMODULE_OK) {
      RedisModule_ReplyWithError(ctx, "Cursor does not exist");
    } else {
      RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

  } else if (cmdc == 'G') {
    int rc = RSCursors->CollectIdle();
    RedisModule_ReplyWithLongLong(ctx, rc);

  } else {
    RedisModule_ReplyWithError(ctx, "Unknown subcommand");
  }
  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////
