#include "redismodule.h"
#include "redisearch.h"
#include "search_ctx.h"
#include "aggregate.h"
#include "cursor.h"
#include "rmutil/util.h"
#include "util/timeout.h"
#include "score_explain.h"
#include "commands.h"
#include "profile.h"

typedef enum { COMMAND_AGGREGATE, COMMAND_SEARCH, COMMAND_EXPLAIN } CommandType;
static void runCursor(RedisModuleCtx *outputCtx, Cursor *cursor, size_t num);

/**
 * Get the sorting key of the result. This will be the sorting key of the last
 * RLookup registry. Returns NULL if there is no sorting key
 */
static const RSValue *getReplyKey(const RLookupKey *kk, const SearchResult *r) {
  if ((kk->flags & RLOOKUP_F_SVSRC) && (r->rowdata.sv && r->rowdata.sv->len > kk->svidx)) {
    return r->rowdata.sv->values[kk->svidx];
  } else {
    return RLookup_GetItem(kk, &r->rowdata);
  }
}

/** Cached variables to avoid serializeResult retrieving these each time */
typedef struct {
  RLookup *lastLk;
  const PLN_ArrangeStep *lastAstp;
} cachedVars;

static void reeval_key(RedisModuleCtx *outctx, const RSValue *key) {
  RedisModuleString *rskey = NULL;
  if (!key) {
    RedisModule_ReplyWithNull(outctx); 
  }
  else {
    if(key->t == RSValue_Reference) {
      key = RSValue_Dereference(key);
    } else if (key->t == RSValue_Duo) {
      key = key->duoval.val;
    }
    switch (key->t) {
      case RSValue_Number:
        /* Serialize double - by prepending "#" to the number, so the coordinator/client can
          * tell it's a double and not just a numeric string value */
        rskey = RedisModule_CreateStringPrintf(outctx, "#%.17g", key->numval);
        break;
      case RSValue_String:
        /* Serialize string - by prepending "$" to it */
        rskey = RedisModule_CreateStringPrintf(outctx, "$%s", key->strval.str);
        break;
      case RSValue_RedisString:
      case RSValue_OwnRstring:
        rskey = RedisModule_CreateStringPrintf(outctx, "$%s",
                                                RedisModule_StringPtrLen(key->rstrval, NULL));
        break;
      case RSValue_Null:
      case RSValue_Undef:
      case RSValue_Array:
      case RSValue_Reference:
      case RSValue_Duo:
        break;
    }
    if (rskey) {
      RedisModule_ReplyWithString(outctx, rskey);
      RedisModule_FreeString(outctx, rskey);
    } else {
      RedisModule_ReplyWithNull(outctx);
    }
  }
}

static size_t serializeResult(AREQ *req, RedisModuleCtx *outctx, const SearchResult *r,
                              const cachedVars *cv) {
  const uint32_t options = req->reqflags;
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
    if (dmd && hasPayload(dmd->flags)) {
      RedisModule_ReplyWithStringBuffer(outctx, dmd->payload->data, dmd->payload->len);
    } else {
      RedisModule_ReplyWithNull(outctx);
    }
  }

  // Coordinator only - sortkey will be sent on the required fields.
  // Non Coordinator modes will require this condition.
  if ((options & QEXEC_F_SEND_SORTKEYS)) {
    count++;
    const RSValue *sortkey = NULL;
    if((cv->lastAstp) && (cv->lastAstp->sortkeysLK)) {
      const RLookupKey *kk = cv->lastAstp->sortkeysLK[0];
      sortkey = getReplyKey(kk, r);
    }
    reeval_key(outctx, sortkey);
  }

  // Coordinator only - handle required fields for coordinator request.
  if(options & QEXEC_F_REQUIRED_FIELDS) {
    // Sortkey is the first key to reply on the required fields, if the we already replied it, continue to the next one.
    size_t currentField = options & QEXEC_F_SEND_SORTKEYS ? 1 : 0;
    size_t requiredFieldsCount = array_len(req->requiredFields);
      for(; currentField < requiredFieldsCount; currentField++) {
        count++;
        const RLookupKey *rlk = RLookup_GetKey(cv->lastLk, req->requiredFields[currentField], 0);
        RSValue *v = (RSValue*)getReplyKey(rlk, r);
        if (v->t == RSValue_Duo) {
          // For duo value, we use `val` as the value
          v = v->duoval.val;
        }
        RSValue rsv;
        if (rlk && rlk->fieldtype == RLOOKUP_C_DBL && v && v->t != RSVALTYPE_DOUBLE) {
          double d;
          RSValue_ToNumber(v, &d);
          RSValue_SetNumber(&rsv, d);
          v = &rsv;
        }
        reeval_key(outctx, v);
      }
  }

  if (!(options & QEXEC_F_SEND_NOFIELDS)) {
    const RLookup *lk = cv->lastLk;
    count++;

    if (dmd && dmd->flags & Document_Deleted) {
      RedisModule_ReplyWithNull(outctx);
      return count;
    }

    // Get the number of fields in the reply. 
    // Excludes hidden fields, fields not included in RETURN and, score and language fields.
    SchemaRule *rule = req->sctx ? req->sctx->spec->rule : NULL;
    int excludeFlags = RLOOKUP_F_HIDDEN;
    int requiredFlags = (req->outFields.explicitReturn ? RLOOKUP_F_EXPLICITRETURN : 0);
    int skipFieldIndex[lk->rowlen]; // Array has `0` for fields which will be skipped
    memset(skipFieldIndex, 0, lk->rowlen * sizeof(*skipFieldIndex));
    size_t nfields = RLookup_GetLength(lk, &r->rowdata, skipFieldIndex, requiredFlags, excludeFlags, rule);

    RedisModule_ReplyWithArray(outctx, nfields * 2);
    int i = 0;
    for (const RLookupKey *kk = lk->head; kk; kk = kk->next) {
      if (!skipFieldIndex[i++]) {
        continue;
      }
      const RSValue *v = RLookup_GetItem(kk, &r->rowdata);
      RS_LOG_ASSERT(v, "v was found in RLookup_GetLength iteration")

      RedisModule_ReplyWithStringBuffer(outctx, kk->name, kk->name_len);
      RSValue_SendReply(outctx, v, req->reqflags & QEXEC_F_TYPED);
    }
  }
  return count;
}

static size_t getResultsFactor(AREQ *req) {
  size_t count = 0;

  if (req->reqflags & QEXEC_F_IS_SEARCH) {
    count++;
  }

  if (req->reqflags & QEXEC_F_SEND_SCORES) {
    count++;
  }

  if (req->reqflags & QEXEC_F_SENDRAWIDS) {
    count++;
  }

  if (req->reqflags & QEXEC_F_SEND_PAYLOADS) {
    count++;
  }

  if (req->reqflags & QEXEC_F_SEND_SORTKEYS) {
    count++;
  }

  if(req->reqflags & QEXEC_F_REQUIRED_FIELDS) {
    count+= array_len(req->requiredFields);
    if (req->reqflags & QEXEC_F_SEND_SORTKEYS) {
      count--;
    }
  }

  if (!(req->reqflags & QEXEC_F_SEND_NOFIELDS)) {
    count++;
  }
  return count;
}

/**
 * Sends a chunk of <n> rows, optionally also sending the preamble
 */
void sendChunk(AREQ *req, RedisModuleCtx *outctx, size_t limit) {
  size_t nrows = 0;
  size_t nelem = 0;
  SearchResult r = {0};
  int rc = RS_RESULT_EOF;
  ResultProcessor *rp = req->qiter.endProc;

  if (!(req->reqflags & QEXEC_F_IS_CURSOR) && 
      !(req->reqflags & QEXEC_F_IS_SEARCH)) {
    limit = RSGlobalConfig.maxAggregateResults;
  }

  cachedVars cv = {0};
  cv.lastLk = AGPLN_GetLookup(&req->ap, NULL, AGPLN_GETLOOKUP_LAST);
  cv.lastAstp = AGPLN_GetArrangeStep(&req->ap);

  rc = rp->Next(rp, &r);
  long resultsLen = REDISMODULE_POSTPONED_ARRAY_LEN;
  if (rc == RS_RESULT_TIMEDOUT && !(req->reqflags & QEXEC_F_IS_CURSOR) && !IsProfile(req) &&
      RSGlobalConfig.timeoutPolicy == TimeoutPolicy_Fail) {
    resultsLen = 1;
  } else if (rc == RS_RESULT_ERROR) {
    resultsLen = 2;
  } else if (req->reqflags & QEXEC_F_IS_SEARCH && rc != RS_RESULT_TIMEDOUT) {
    PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(&req->ap);
    size_t reqLimit = arng && arng->isLimited? arng->limit : DEFAULT_LIMIT;
    size_t reqOffset = arng && arng->isLimited? arng->offset : 0;
    size_t resultFactor = getResultsFactor(req);
    
    size_t reqResults;
    if (reqLimit + reqOffset <= RSGlobalConfig.maxSearchResults) {
    	reqResults = req->qiter.totalResults > reqOffset ?
                   req->qiter.totalResults - reqOffset : 0;
    } else {
    	reqResults = RSGlobalConfig.maxSearchResults > reqOffset ?
                   RSGlobalConfig.maxSearchResults - reqOffset : 0;
    }

    resultsLen = 1 + MIN(limit, MIN(reqLimit, reqResults)) * resultFactor;
  }

  RedisModule_ReplyWithArray(outctx, resultsLen);

  if (rc == RS_RESULT_TIMEDOUT) {
    if (!(req->reqflags & QEXEC_F_IS_CURSOR) && !IsProfile(req) &&
        RSGlobalConfig.timeoutPolicy == TimeoutPolicy_Fail) {
      RedisModule_ReplyWithSimpleString(outctx, "Timeout limit was reached");
    } else {
      rc = RS_RESULT_OK;
      RedisModule_ReplyWithLongLong(outctx, req->qiter.totalResults);
    }
  } else if (rc == RS_RESULT_ERROR) {
    RedisModule_ReplyWithLongLong(outctx, req->qiter.totalResults);
    RedisModule_ReplyWithArray(outctx, 1);
    QueryError_ReplyAndClear(outctx, req->qiter.err);
    nelem++;
  } else {
    RedisModule_ReplyWithLongLong(outctx, req->qiter.totalResults);
  }
  nelem++;

  if (rc == RS_RESULT_OK && nrows++ < limit && !(req->reqflags & QEXEC_F_NOROWS)) {
    nelem += serializeResult(req, outctx, &r, &cv);
  }

  SearchResult_Clear(&r);
  if (rc != RS_RESULT_OK) {
    goto done;
  }

  while (nrows++ < limit && (rc = rp->Next(rp, &r)) == RS_RESULT_OK) {
    if (!(req->reqflags & QEXEC_F_NOROWS)) {
      nelem += serializeResult(req, outctx, &r, &cv);
    }
    // Serialize it as a search result
    SearchResult_Clear(&r);
  }

done:
  SearchResult_Destroy(&r);
  if (rc != RS_RESULT_OK) {
    req->stateflags |= QEXEC_S_ITERDONE;
  }

  // Reset the total results length:
  req->qiter.totalResults = 0;
  if (resultsLen == REDISMODULE_POSTPONED_ARRAY_LEN) {
    RedisModule_ReplySetArrayLength(outctx, nelem);
  } else {
    RS_LOG_ASSERT(resultsLen == nelem, "Precalculated number of replies must be equal to actual number");
  }
}

void AREQ_Execute(AREQ *req, RedisModuleCtx *outctx) {
  sendChunk(req, outctx, -1);
  if (IsProfile(req)) {
    Profile_Print(outctx, req);
  }
  AREQ_Free(req);
}

static int buildRequest(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int type,
                        QueryError *status, AREQ **r) {

  int rc = REDISMODULE_ERR;
  hires_clock_t parseClock;
  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  RedisSearchCtx *sctx = NULL;
  RedisModuleCtx *thctx = NULL;

  if (type == COMMAND_SEARCH) {
    (*r)->reqflags |= QEXEC_F_IS_SEARCH;
  }
  else if (type == COMMAND_AGGREGATE) {
    (*r)->reqflags |= QEXEC_F_IS_EXTENDED;
  }

  if (AREQ_Compile(*r, argv + 2, argc - 2, status) != REDISMODULE_OK) {
    RS_LOG_ASSERT(QueryError_HasError(status), "Query has error");
    goto done;
  }

  // Prepare the query.. this is where the context is applied.
  if ((*r)->reqflags & QEXEC_F_IS_CURSOR) {
    RedisModuleCtx *newctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_SelectDb(newctx, RedisModule_GetSelectedDb(ctx));
    ctx = thctx = newctx;  // In case of error!
  }

  sctx = NewSearchCtxC(ctx, indexname, true);
  if (!sctx) {
    QueryError_SetErrorFmt(status, QUERY_ENOINDEX, "%s: no such index", indexname);
    goto done;
  }

  rc = AREQ_ApplyContext(*r, sctx, status);
  thctx = NULL;
  // ctx is always assigned after ApplyContext
  if (rc != REDISMODULE_OK) {
    RS_LOG_ASSERT(QueryError_HasError(status), "Query has error");
    goto done;
  }

  bool is_profile = IsProfile(*r);
  if (is_profile) {
    hires_clock_get(&parseClock);
    (*r)->parseTime += hires_clock_diff_msec(&parseClock, &(*r)->initClock);
  }

  rc = AREQ_BuildPipeline(*r, 0, status);

  if (is_profile) {
    (*r)->pipelineBuildTime = hires_clock_since_msec(&parseClock);
  }

done:
  if (rc != REDISMODULE_OK && *r) {
    AREQ_Free(*r);
    *r = NULL;
    if (thctx) {
      RedisModule_FreeThreadSafeContext(thctx);
    }
  }
  return rc;
}

#define NO_PROFILE 0
#define PROFILE_FULL 1
#define PROFILE_LIMITED 2

static int parseProfile(AREQ *r, int withProfile, RedisModuleString **argv, int argc, QueryError *status) {
  if (withProfile != NO_PROFILE) {

    // WithCursor is disabled on the shards for external use but is available internally to the coordinator
    #ifndef RS_COORDINATOR
    if (RMUtil_ArgExists("WITHCURSOR", argv, argc, 3)) {
      QueryError_SetError(status, QUERY_EGENERIC, "FT.PROFILE does not support cursor");
      return REDISMODULE_ERR;
    }
    #endif

    r->reqflags |= QEXEC_F_PROFILE;
    if (withProfile == PROFILE_LIMITED) {
      r->reqflags |= QEXEC_F_PROFILE_LIMITED;
    }
    hires_clock_get(&r->initClock);
  }
  return REDISMODULE_OK;
}

static int execCommandCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                             CommandType type, int withProfile) {
  // Index name is argv[1]
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  AREQ *r = AREQ_New();
  QueryError status = {0};
  if (parseProfile(r, withProfile, argv, argc, &status) != REDISMODULE_OK) {
    goto error;
  }

  if (buildRequest(ctx, argv, argc, type, &status, &r) != REDISMODULE_OK) {
    goto error;
  }

  if (r->reqflags & QEXEC_F_IS_CURSOR) {
    int rc = AREQ_StartCursor(r, ctx, r->sctx->spec->name, &status);
    if (rc != REDISMODULE_OK) {
      goto error;
    }
  } else {
    if (IsProfile(r)) {
      RedisModule_ReplyWithArray(ctx, 2);
    }
    AREQ_Execute(r, ctx);
  }
  return REDISMODULE_OK;

error:
  if (r) {
    AREQ_Free(r);
  }
  return QueryError_ReplyAndClear(ctx, &status);
}

int RSAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return execCommandCommon(ctx, argv, argc, COMMAND_AGGREGATE, NO_PROFILE);
}
int RSSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return execCommandCommon(ctx, argv, argc, COMMAND_SEARCH, NO_PROFILE);
}

#define PROFILE_1ST_PARAM 2

RedisModuleString **_profileArgsDup(RedisModuleString **argv, int argc, int params) {
  RedisModuleString **newArgv = rm_malloc(sizeof(*newArgv) * (argc- params));
  // copy cmd & index
  memcpy(newArgv, argv, PROFILE_1ST_PARAM * sizeof(*newArgv));
  // copy non-profile commands
  memcpy(newArgv + PROFILE_1ST_PARAM, argv + PROFILE_1ST_PARAM + params,
          (argc - PROFILE_1ST_PARAM - params) * sizeof(*newArgv));
  return newArgv;
}

int RSProfileCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }

  CommandType cmdType;
  int curArg = PROFILE_1ST_PARAM;
  int withProfile = PROFILE_FULL;

  // Check the command type
  const char *cmd = RedisModule_StringPtrLen(argv[curArg++], NULL);
  if (strcasecmp(cmd, "SEARCH") == 0) {
    cmdType = COMMAND_SEARCH;
  } else if (strcasecmp(cmd, "AGGREGATE") == 0) {
    cmdType = COMMAND_AGGREGATE;
  } else {
    RedisModule_ReplyWithError(ctx, "No `SEARCH` or `AGGREGATE` provided");
    return REDISMODULE_OK;
  }
  
  cmd = RedisModule_StringPtrLen(argv[curArg++], NULL);
  if (strcasecmp(cmd, "LIMITED") == 0) {
    withProfile = PROFILE_LIMITED;
    cmd = RedisModule_StringPtrLen(argv[curArg++], NULL);
  }

  if (strcasecmp(cmd, "QUERY") != 0) {
    RedisModule_ReplyWithError(ctx, "The QUERY keyword is expected");
    return REDISMODULE_OK;
  }

  int newArgc = argc - curArg + PROFILE_1ST_PARAM; 
  RedisModuleString **newArgv = _profileArgsDup(argv, argc, curArg - PROFILE_1ST_PARAM);
  execCommandCommon(ctx, newArgv, newArgc, cmdType, withProfile);
  rm_free(newArgv);
  return REDISMODULE_OK;
}

char *RS_GetExplainOutput(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                          QueryError *status) {
  AREQ *r = AREQ_New();
  if (buildRequest(ctx, argv, argc, COMMAND_EXPLAIN, status, &r) != REDISMODULE_OK) {
    return NULL;
  }
  char *ret = QAST_DumpExplain(&r->ast, r->sctx->spec);
  AREQ_Free(r);
  return ret;
}

static void runCursor(RedisModuleCtx *outputCtx, Cursor *cursor, size_t num);

int AREQ_StartCursor(AREQ *r, RedisModuleCtx *outctx, const char *lookupName, QueryError *err) {
  Cursor *cursor = Cursors_Reserve(&RSCursors, lookupName, r->cursorMaxIdle, err);
  if (cursor == NULL) {
    return REDISMODULE_ERR;
  }
  cursor->execState = r;
  runCursor(outctx, cursor, 0);
  return REDISMODULE_OK;
}

static void runCursor(RedisModuleCtx *outputCtx, Cursor *cursor, size_t num) {
  AREQ *req = cursor->execState;
  
  // reset profile clock for cursor reads except for 1st 
  if (IsProfile(req) && req->totalTime != 0) {
    hires_clock_get(&req->initClock);
  }

  // update timeout for current cursor read
  if (req->qiter.rootProc->type != RP_NETWORK) {
    updateTimeout(&req->timeoutTime, req->reqTimeout);
    updateRPIndexTimeout(req->qiter.rootProc, req->timeoutTime);
  }
  if (!num) {
    num = req->cursorChunkSize;
    if (!num) {
      num = RSGlobalConfig.cursorReadSize;
    }
  }
  req->cursorChunkSize = num;
  int arrayLen = 2;
  if (IsProfile(req)) {
    arrayLen = 3;
  }
  // return array of [results, cursorID]. (the typical result reply is in the first reply)
  // for profile, we return array of [results, cursorID, profile]
  RedisModule_ReplyWithArray(outputCtx, arrayLen);
  sendChunk(req, outputCtx, num);

  if (req->stateflags & QEXEC_S_ITERDONE) {
    // Write the count!
    RedisModule_ReplyWithLongLong(outputCtx, 0);
    if (IsProfile(req)) {
      Profile_Print(outputCtx, req);
    }
  } else {
    RedisModule_ReplyWithLongLong(outputCtx, cursor->id);
    if (IsProfile(req)) {
      // If the cursor is still alive, don't print profile info to save bandwidth
      RedisModule_ReplyWithNull(outputCtx);
    }
  }

  if (req->stateflags & QEXEC_S_ITERDONE) {
    goto delcursor;
  } else {
    // Update the idle timeout
    Cursor_Pause(cursor);
    return;
  }

delcursor:
  AREQ_Free(req);
  if (cursor) {
    cursor->execState = NULL;
  }
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
  QueryError status = {0};
  AREQ *req = cursor->execState;
  req->qiter.err = &status;
  ConcurrentSearchCtx_ReopenKeys(&req->conc);
  runCursor(ctx, cursor, count);
}

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
    RedisModule_ReplyWithError(ctx, "Unknown subcommand");
  }
  return REDISMODULE_OK;
}

void Cursor_FreeExecState(void *p) {
  AREQ *r = p;
  AREQ_Free(p);
}
