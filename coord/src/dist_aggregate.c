/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "result_processor.h"
#include "rmr/rmr.h"
#include "rmutil/util.h"
#include "search_cluster.h"
#include "commands.h"
#include "aggregate/aggregate.h"
#include "dist_plan.h"
#include "coord_module.h"
#include "profile.h"
#include "util/timeout.h"
#include "resp3.h"

#include <err.h>

// Get cursor command using a cursor id and an existing aggregate command

static int getCursorCommand(MRReply *res, MRCommand *cmd) {
  long long cursorId;
  if (!MRReply_ToInteger(MRReply_ArrayElement(res, 1), &cursorId)) {
    // Invalid format?!
    return 0;
  }

  if (cursorId == 0) {
    // Cursor was set to 0, end of reply chain.
    return 0;
  }
  if (cmd->num < 2) {
    return 0;  // Invalid command!??
  }

  char buf[128];
  sprintf(buf, "%lld", cursorId);
  int shardingKey = MRCommand_GetShardingKey(cmd);
  const char *idx = MRCommand_ArgStringPtrLen(cmd, shardingKey, NULL);
  MRCommand newCmd = MR_NewCommand(4, "_FT.CURSOR", "READ", idx, buf);
  newCmd.targetSlot = cmd->targetSlot;
  newCmd.protocol = cmd->protocol;
  MRCommand_Free(cmd);
  *cmd = newCmd;

  return 1;
}

static int netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep, MRCommand *cmd) {
  // Should we assert this??
  bool bail_out = !rep || MRReply_Type(rep) != MR_REPLY_ARRAY;
  if (!bail_out) {
    size_t len = MRReply_Length(rep);
    if (cmd->protocol == 3) {
      bail_out = len != 2; // (map, cursor)
    } else {
      bail_out = len != 2 && len != 3; // (results, cursor) or (results, cursor, profile)
    }
  }

  if (bail_out) {
    MRReply_Free(rep);
    MRIteratorCallback_Done(ctx, 1);
    RedisModule_Log(NULL, "warning", "An empty reply was received from a shard");
    return REDIS_ERR;
  }

  // rewrite and resend the cursor command if needed
  int rc = REDIS_OK;
  bool done = !getCursorCommand(rep, cmd);

  // Push the reply down the chain
  if (cmd->protocol == 3) // RESP3
  {
    MRReply *map = MRReply_ArrayElement(rep, 0);
    MRReply *results = NULL;
    if (map && MRReply_Type(map) == MR_REPLY_MAP) {
      results = MRReply_MapElement(map, "results");
      if (results && MRReply_Type(results) == MR_REPLY_ARRAY && MRReply_Length(results) > 0) {
        MRIteratorCallback_AddReply(ctx, rep); // to be picked up by getNextReply
        // User code now owns the reply, so we can't free it here ourselves!
        rep = NULL;
      } else {
        done = true;
      }
    } else {
      done = true;
    }
  }
  else // RESP2
  {
    MRReply *results = MRReply_ArrayElement(rep, 0);
    if (results && MRReply_Type(results) == MR_REPLY_ARRAY && MRReply_Length(results) > 1) {
      MRIteratorCallback_AddReply(ctx, rep); // to be picked up by getNextReply
      // User code now owns the reply, so we can't free it here ourselves!
      rep = NULL;
    } else {
      done = true;
    }
  }

  if (done) {
    MRIteratorCallback_Done(ctx, 0);
  } else {
    // resend command
    if (REDIS_ERR == MRIteratorCallback_ResendCommand(ctx, cmd)) {
      MRIteratorCallback_Done(ctx, 1);
      rc = REDIS_ERR;
    }
  }

  if (rep != NULL) {
    // If rep has been set to NULL, it means the callback has been invoked
    MRReply_Free(rep);
  }
  return rc;
}

RSValue *MRReply_ToValue(MRReply *r) {
  if (!r) return RS_NullVal();
  RSValue *v = NULL;
  switch (MRReply_Type(r)) {
    case MR_REPLY_STATUS:
    case MR_REPLY_STRING: {
      size_t l;
      char *s = MRReply_String(r, &l);
      v = RS_NewCopiedString(s, l);
      // v = RS_StringValT(s, l, RSString_Volatile);
      break;
    }
    case MR_REPLY_ERROR: {
      double d = 42;
      MRReply_ToDouble(r, &d);
      v = RS_NumVal(d);
      break;
    }
    case MR_REPLY_INTEGER:
      v = RS_NumVal((double)MRReply_Integer(r));
      break;
    case MR_REPLY_DOUBLE:
      v = RS_NumVal(MRReply_Double(r));
      break;
    case MR_REPLY_MAP: {
      size_t n = MRReply_Length(r);
      RS_LOG_ASSERT(n % 2 == 0, "map of odd length");
      RSValue **map = rm_malloc(n * sizeof(*map));
      for (size_t i = 0; i < n; ++i) {
        MRReply *e = MRReply_ArrayElement(r, i);
        if (i % 2 == 0) {
          RS_LOG_ASSERT(MRReply_Type(e) == MR_REPLY_STRING, "non-string map key");
        }
        map[i] = MRReply_ToValue(e);
      }
      v = RSValue_NewMap(map, n / 2);
      break;
    }
    case MR_REPLY_ARRAY: {
      size_t n = MRReply_Length(r);
      RSValue **arr = rm_malloc(n * sizeof(*arr));
      for (size_t i = 0; i < n; ++i) {
        arr[i] = MRReply_ToValue(MRReply_ArrayElement(r, i));
      }
      v = RSValue_NewArrayEx(arr, n, RSVAL_ARRAY_ALLOC | RSVAL_ARRAY_NOINCREF);
      break;
    }
    case MR_REPLY_NIL:
      v = RS_NullVal();
      break;
    default:
      v = RS_NullVal();
      break;
  }
  return v;
}

typedef struct {
  ResultProcessor base;
  struct {
    MRReply *root;  // Root reply. We need to free this when done with the rows
    MRReply *rows;  // Array containing reply rows for quick access
  } current;
  // Lookup - the rows are written in here
  RLookup *lookup;
  size_t curIdx;
  MRIterator *it;
  MRCommand cmd;
  MRCommandGenerator cg;
  AREQ *areq;

  // profile vars
  MRReply **shardsProfile;
  int shardsProfileIdx;
} RPNet;

static int getNextReply(RPNet *nc) {
  while (1) {
    MRReply *root = MRIterator_Next(nc->it);
    if (root == MRITERATOR_DONE) {
      // No more replies
      nc->current.root = NULL;
      nc->current.rows = NULL;
      return 0;
    }

    MRReply *rows = MRReply_ArrayElement(root, 0);
    if (   rows == NULL
        || (MRReply_Type(rows) != MR_REPLY_ARRAY && MRReply_Type(rows) != MR_REPLY_MAP)
        || MRReply_Length(rows) == 0) {
      MRReply_Free(root);
      root = NULL;
      rows = NULL;
      RedisModule_Log(NULL, "warning", "An empty reply was received from a shard");
    }

    // invariant: either rows == NULL or least one row exists

    nc->current.root = root;
    nc->current.rows = rows;

    assert(   !nc->current.rows
           || MRReply_Type(nc->current.rows) == MR_REPLY_ARRAY
           || MRReply_Type(nc->current.rows) == MR_REPLY_MAP);
    return 1;
  }
}

static const RLookupKey *keyForField(RPNet *nc, const char *s) {
  for (const RLookupKey *kk = nc->lookup->head; kk; kk = kk->next) {
    if (!strcmp(kk->name, s)) {
      return kk;
    }
  }
  return NULL;
}

void processResultFormat(uint32_t *flags, MRReply *map) {
  // Logic of which format to use is done by the shards
  MRReply *format = MRReply_MapElement(map, "format");
  RS_LOG_ASSERT(format, "missing format specification");
  if (MRReply_StringEquals(format, "EXPAND", false)) {
    *flags |= QEXEC_FORMAT_EXPAND;
  } else {
    *flags &= ~QEXEC_FORMAT_EXPAND;
  }
  *flags &= ~QEXEC_FORMAT_DEFAULT;
}

static int rpnetNext(ResultProcessor *self, SearchResult *r) {
  RPNet *nc = (RPNet *)self;
  MRReply *root = nc->current.root, *rows = nc->current.rows;

  // root (array) has similar structure for RESP2/3:
  // [0] array of results (rows) described right below
  // [1] cursor (int)

  // rows:
  // RESP2: [ num_results, [ field, value, ... ], ... ]
  // RESP3: { ..., "results": [ { field: value, ... }, ... ], ... }

  // can also get an empty row:
  // RESP2: [] or [ 0 ]
  // RESP3: {}

  if (rows) {
      bool resp3 = MRReply_Type(rows) == MR_REPLY_MAP;
      size_t len;
      if (resp3) {
        MRReply *results = MRReply_MapElement(rows, "results");
        RS_LOG_ASSERT(results, "invalid results record: missing 'results' key");
        len = MRReply_Length(results);
      } else {
        len = MRReply_Length(rows);
      }

      if (nc->curIdx == len) {
        long long cursorId = MRReply_Integer(MRReply_ArrayElement(root, 1));

        // in profile mode, save shard's profile info to be returned later
        if (cursorId == 0 && nc->shardsProfile) {
          nc->shardsProfile[nc->shardsProfileIdx++] = root;
        } else {
          MRReply_Free(root);
        }
        nc->current.root = nc->current.rows = root = rows = NULL;
      }
  }

  int new_reply = !root;

  // get the next reply from the channel
  while (!root || !rows || MRReply_Length(rows) == 0) {
      if (!getNextReply(nc)) {
        return RS_RESULT_EOF;
      }

      root = nc->current.root;
      rows = nc->current.rows;
  }

  // invariant: at least one row exists

  bool resp3 = MRReply_Type(rows) == MR_REPLY_MAP;
  if (new_reply) {
    if (resp3) { // RESP3
      nc->curIdx = 0;
      MRReply *results = MRReply_MapElement(rows, "results");
      RS_LOG_ASSERT(results, "invalid results record: missing 'results' key");
      nc->base.parent->totalResults += MRReply_Length(results);
    } else { // RESP2
      // Get the index from the first
      nc->base.parent->totalResults += MRReply_Integer(MRReply_ArrayElement(rows, 0));
      nc->curIdx = 1;
    }
  }

  if (resp3) // RESP3
  {
    MRReply *results = MRReply_MapElement(rows, "results");
    RS_LOG_ASSERT(results && MRReply_Type(results) == MR_REPLY_ARRAY, "invalid results record");
    MRReply *result = MRReply_ArrayElement(results, nc->curIdx++);
    RS_LOG_ASSERT(result && MRReply_Type(result) == MR_REPLY_MAP, "invalid result record");
    MRReply *fields = MRReply_MapElement(result, "extra_attributes");
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_MAP, "invalid fields record");

    processResultFormat(&nc->areq->reqflags, rows);

    for (size_t i = 0; i < MRReply_Length(fields); i += 2) {
      size_t len;
      const char *field = MRReply_String(MRReply_ArrayElement(fields, i), &len);
      MRReply *val = MRReply_ArrayElement(fields, i + 1);
      RSValue *v = MRReply_ToValue(val);
      RLookup_WriteOwnKeyByName(nc->lookup, field, len, &r->rowdata, v);
    }
  }
  else // RESP2
  {
    MRReply *rep = MRReply_ArrayElement(rows, nc->curIdx++);
    for (size_t i = 0; i < MRReply_Length(rep); i += 2) {
      size_t len;
      const char *field = MRReply_String(MRReply_ArrayElement(rep, i), &len);
      RSValue *v = RS_NullVal();
      if (i + 1 < MRReply_Length(rep)) {
        MRReply *val = MRReply_ArrayElement(rep, i + 1);
        v = MRReply_ToValue(val);
      }
      RLookup_WriteOwnKeyByName(nc->lookup, field, len, &r->rowdata, v);
    }
  }
  return RS_RESULT_OK;
}

static int rpnetNext_Start(ResultProcessor *rp, SearchResult *r) {
  RPNet *nc = (RPNet *)rp;
  MRIterator *it = MR_Iterate(nc->cg, netCursorCallback, NULL);
  if (!it) {
    return RS_RESULT_ERROR;
  }
  nc->it = it;
  nc->base.Next = rpnetNext;
  return rpnetNext(rp, r);
}

static void rpnetFree(ResultProcessor *rp) {
  RPNet *nc = (RPNet *)rp;

  // the iterator might not be done - some producers might still be sending data, let's wait for
  // them...
  if (nc->it) {
    MRIterator_WaitDone(nc->it);
  }

  nc->cg.Free(nc->cg.ctx);

  if (nc->shardsProfile) {
    for (size_t i = 0; i < nc->shardsProfileIdx; ++i) {
      if (nc->shardsProfile[i] != nc->current.root) {
        MRReply_Free(nc->shardsProfile[i]);
      }
    }
    rm_free(nc->shardsProfile);
  }

  if (nc->current.root) {
    MRReply_Free(nc->current.root);
  }

  if (nc->it) MRIterator_Free(nc->it);
  rm_free(rp);
}

static RPNet *RPNet_New(const MRCommand *cmd, SearchCluster *sc) {
  //  MRCommand_FPrint(stderr, &cmd);
  RPNet *nc = rm_calloc(1, sizeof(*nc));
  nc->cmd = *cmd;
  nc->cg = SearchCluster_MultiplexCommand(sc, &nc->cmd);
  nc->areq = NULL;
  nc->shardsProfileIdx = 0;
  nc->shardsProfile = NULL;
  nc->base.Free = rpnetFree;
  nc->base.Next = rpnetNext_Start;
  nc->base.type = RP_NETWORK;
  return nc;
}

static void buildMRCommand(RedisModuleString **argv, int argc, int profileArgs,
                           AREQDIST_UpstreamInfo *us, MRCommand *xcmd) {
  // We need to prepend the array with the command, index, and query that
  // we want to use.
  const char **tmparr = array_new(const char *, us->nserialized);

  if (profileArgs == 0) {
    tmparr = array_append(tmparr, RS_AGGREGATE_CMD);                         // Command
    tmparr = array_append(tmparr, RedisModule_StringPtrLen(argv[1], NULL));  // Index name
  } else {
    tmparr = array_append(tmparr, RS_PROFILE_CMD);
    tmparr = array_append(tmparr, RedisModule_StringPtrLen(argv[1], NULL));  // Index name
    tmparr = array_append(tmparr, "AGGREGATE");
    if (profileArgs == 3) {
      tmparr = array_append(tmparr, "LIMITED");
    }
    tmparr = array_append(tmparr, "QUERY");
  }

  tmparr = array_append(tmparr, RedisModule_StringPtrLen(argv[2 + profileArgs], NULL));  // Query
  tmparr = array_append(tmparr, "WITHCURSOR");
  // Numeric responses are encoded as simple strings.
  tmparr = array_append(tmparr, "_NUM_SSTRING");

  int argOffset = RMUtil_ArgIndex("DIALECT", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (argOffset != -1 && argOffset + 3 + 1 + profileArgs < argc) {
    tmparr = array_append(tmparr, "DIALECT");
    tmparr = array_append(tmparr, RedisModule_StringPtrLen(argv[argOffset + 3 + 1 + profileArgs], NULL));  // the dialect
  }

  argOffset = RMUtil_ArgIndex("FORMAT", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (argOffset != -1 && argOffset + 3 + 1 + profileArgs < argc) {
    tmparr = array_append(tmparr, "FORMAT");
    tmparr = array_append(tmparr, RedisModule_StringPtrLen(argv[argOffset + 3 + 1 + profileArgs], NULL));  // the format
  }

  for (size_t ii = 0; ii < us->nserialized; ++ii) {
    tmparr = array_append(tmparr, us->serialized[ii]);
  }

  *xcmd = MR_NewCommandArgv(array_len(tmparr), tmparr);

  // PARAMS was already validated at AREQ_Compile
  int loc = RMUtil_ArgIndex("PARAMS", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (loc != -1) {
    long long nargs;
    int rc = RedisModule_StringToLongLong(argv[loc + 3 + 1 + profileArgs], &nargs);

    // append params string including PARAMS keyword and nargs
    for (int i = 0; i < nargs + 2; ++i) {
      MRCommand_AppendRstr(xcmd, argv[loc + 3 + i + profileArgs]);
    }
  }

  // check for timeout argument and append it to the command.
  // If TIMEOUT exists, it was already validated at AREQ_Compile.
  int timeout_index = RMUtil_ArgIndex("TIMEOUT", argv + 3 + profileArgs, argc - 4 - profileArgs);
  if (timeout_index != -1) {
    MRCommand_AppendRstr(xcmd, argv[timeout_index + 3 + profileArgs]);
    MRCommand_AppendRstr(xcmd, argv[timeout_index + 4 + profileArgs]);
  }

  MRCommand_SetPrefix(xcmd, "_FT");

  array_free(tmparr);
}

static void buildDistRPChain(AREQ *r, MRCommand *xcmd, SearchCluster *sc,
                             AREQDIST_UpstreamInfo *us) {
  // Establish our root processor, which is the distributed processor
  RPNet *rpRoot = RPNet_New(xcmd, sc);
  rpRoot->base.parent = &r->qiter;
  rpRoot->lookup = us->lookup;
  rpRoot->areq = r;

  ResultProcessor *rpProfile = NULL;
  if (IsProfile(r)) {
    rpProfile = RPProfile_New(&rpRoot->base, &r->qiter);
  }

  assert(!r->qiter.rootProc);
  // Get the deepest-most root:
  int found = 0;
  for (ResultProcessor *rp = r->qiter.endProc; rp; rp = rp->upstream) {
    if (!rp->upstream) {
      rp->upstream = IsProfile(r) ? rpProfile : &rpRoot->base;
      found = 1;
      break;
    }
  }

  // update root and end with RPNet
  r->qiter.rootProc = &rpRoot->base;
  if (!found) {
    r->qiter.endProc = &rpRoot->base;
  }

  // allocate memory for replies and update endProc if necessary
  if (IsProfile(r)) {
    rpRoot->shardsProfile = rm_malloc(sizeof(*rpRoot->shardsProfile) * sc->size);
    if (!found) {
      r->qiter.endProc = rpProfile;
    }
  }
}

size_t PrintShardProfile_resp2(RedisModule_Reply *reply, int count, MRReply **replies, int isSearch);
size_t PrintShardProfile_resp3(RedisModule_Reply *reply, int count, MRReply **replies);

void printAggProfile(RedisModule_Reply *reply, AREQ *req) {
  clock_t finishTime = clock();

  RedisModule_Reply_Map(reply); // root

    // profileRP replace netRP as end PR
    RPNet *rpnet = (RPNet *)req->qiter.rootProc;

    // Print shards profile
    if (reply->resp3) {
      PrintShardProfile_resp3(reply, rpnet->shardsProfileIdx, rpnet->shardsProfile);
    } else {
      PrintShardProfile_resp2(reply, rpnet->shardsProfileIdx, rpnet->shardsProfile, 0);
    }

    // Print coordinator profile

    RedisModule_ReplyKV_Map(reply, "Coordinator"); // >coordinator

      RedisModule_ReplyKV_Map(reply, "Result processors profile");
      Profile_Print(reply, req);
      RedisModule_Reply_MapEnd(reply);

      RedisModule_ReplyKV_Double(reply, "Total Coordinator time", (double)(clock() - req->initClock) / CLOCKS_PER_MILLISEC);

    RedisModule_Reply_MapEnd(reply); // >coordinator

  RedisModule_Reply_MapEnd(reply); // root
}

static int parseProfile(RedisModuleString **argv, int argc, AREQ *r) {
  // Profile args
  int profileArgs = 0;
  if (RMUtil_ArgIndex("FT.PROFILE", argv, 1) != -1) {
    profileArgs += 2;     // SEARCH/AGGREGATE + QUERY
    r->initClock = clock();
    r->reqflags |= QEXEC_F_PROFILE;
    if (RMUtil_ArgIndex("LIMITED", argv + 3, 1) != -1) {
      profileArgs++;
      r->reqflags |= QEXEC_F_PROFILE_LIMITED;
    }
    if (RMUtil_ArgIndex("QUERY", argv + 3, 2) == -1) {
      QueryError_SetError(r->qiter.err, QUERY_EPARSEARGS, "No QUERY keyword provided");
      return -1;
    }
  }
  return profileArgs;
}

void RSExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         struct ConcurrentCmdCtx *cmdCtx) {
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  bool has_map = RedisModule_HasMap(reply);

  // CMD, index, expr, args...
  AREQ *r = AREQ_New();
  QueryError status = {0};
  specialCaseCtx *knnCtx = NULL;

  r->qiter.err = &status;
  r->reqflags |= QEXEC_F_IS_EXTENDED | QEXEC_F_BUILDPIPELINE_NO_ROOT;

  int profileArgs = parseProfile(argv, argc, r);
  if (profileArgs == -1) goto err;
  int rc = AREQ_Compile(r, argv + 2 + profileArgs, argc - 2 - profileArgs, &status);
  if (rc != REDISMODULE_OK) goto err;

  unsigned int dialect = r->reqConfig.dialectVersion;
  if(dialect >= 2) {
    // Check if we have KNN in the query string, and if so, parse the query string to see if it is
    // a KNN section in the query. IN that case, we treat this as a SORTBY+LIMIT step.
    if(strcasestr(r->query, "KNN")) {
      knnCtx = prepareOptionalTopKCase(r->query, argv, argc, &status);
      if (QueryError_HasError(&status)) {
        goto err;
      }
      if (knnCtx != NULL) {
        // If we found KNN, add an arange step, so it will be the first step after
        // the root (which is first plan step to be executed after the root).
        AGPLN_AddKNNArrangeStep(&r->ap, knnCtx->knn.k, knnCtx->knn.fieldName);
      }
    }
  }
  rc = AGGPLN_Distribute(&r->ap, &status);
  if (rc != REDISMODULE_OK) goto err;

  AREQDIST_UpstreamInfo us = {NULL};
  rc = AREQ_BuildDistributedPipeline(r, &us, &status);
  if (rc != REDISMODULE_OK) goto err;

  SearchCluster *sc = GetSearchCluster();

  // Construct the command string
  MRCommand xcmd;
  buildMRCommand(argv , argc, profileArgs, &us, &xcmd);
  xcmd.protocol = is_resp3(ctx) ? 3 : 2;

  // Build the result processor chain
  buildDistRPChain(r, &xcmd, sc, &us);

  if (IsProfile(r)) r->parseTime = clock() - r->initClock;

  // Create the Search context
  // (notice with cursor, we rely on the existing mechanism of AREQ to free the ctx object when the cursor is exhausted)
  r->sctx = rm_new(RedisSearchCtx);
  *r->sctx = SEARCH_CTX_STATIC(ctx, NULL);
  r->sctx->apiVersion = dialect;
  // r->sctx->expanded should be recieved from shards

  if (r->reqflags & QEXEC_F_IS_CURSOR) {
    // Keep the original concurrent context
    ConcurrentCmdCtx_KeepRedisCtx(cmdCtx);

    StrongRef dummy_spec_ref = {.rm = NULL};
    rc = AREQ_StartCursor(r, reply, dummy_spec_ref, &status, true);

    if (rc != REDISMODULE_OK) {
      goto err;
    }
  } else {
    if (reply->resp3 || IsProfile(r)) {
      RedisModule_Reply_Map(reply);
    }
    sendChunk(r, reply, -1);
    if (IsProfile(r)) {
      printAggProfile(reply, r);
    }
    if (reply->resp3 || IsProfile(r)) {
      RedisModule_Reply_MapEnd(reply);
    }
    AREQ_Free(r);
  }
  SpecialCaseCtx_Free(knnCtx);
  RedisModule_EndReply(reply);
  return;

// See if we can distribute the plan...
err:
  assert(QueryError_HasError(&status));
  QueryError_ReplyAndClear(ctx, &status);
  SpecialCaseCtx_Free(knnCtx);
  AREQ_Free(r);
  RedisModule_EndReply(reply);
  return;
}
