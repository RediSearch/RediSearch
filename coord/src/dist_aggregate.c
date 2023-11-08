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
#include "profile.h"
#include "util/timeout.h"
#include "coord/src/config.h"

#include <err.h>

/* Get cursor command using a cursor id and an existing aggregate command */
static int getCursorCommand(MRReply *prev, MRCommand *cmd) {
  long long cursorId;
  if (!MRReply_ToInteger(MRReply_ArrayElement(prev, 1), &cursorId)) {
    // Invalid format?!
    return 0;
  }
  if (cursorId == 0) {
    // Cursor was set to 0, end of reply chain.
    cmd->depleted = true;
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
  newCmd.forCursor = cmd->forCursor;
  MRCommand_Free(cmd);
  *cmd = newCmd;

  return 1;
}

static int netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep, MRCommand *cmd) {
  // Should we assert this??
  if (!rep || MRReply_Type(rep) != MR_REPLY_ARRAY ||
             (MRReply_Length(rep) != 2 && MRReply_Length(rep) != 3)) {
    if (MRReply_Type(rep) == MR_REPLY_ERROR) {
      //      printf("Error is '%s'\n", MRReply_String(rep, NULL));
    }
    MRReply_Free(rep);
    MRIteratorCallback_Done(ctx, 1);
    RedisModule_Log(NULL, "warning", "An empty reply was received from a shard");
    return REDIS_ERR;
  }

  // rewrite and resend the cursor command if needed
  int rc = REDIS_OK;
  int isDone = !getCursorCommand(rep, cmd);

  // Push the reply down the chain
  MRReply *arr = MRReply_ArrayElement(rep, 0);
  if (arr && MRReply_Type(arr) == MR_REPLY_ARRAY && MRReply_Length(arr) > 1) {
    MRIteratorCallback_AddReply(ctx, rep);
    // User code now owns the reply, so we can't free it here ourselves!
    rep = NULL;
  } else {
    isDone = 1;
  }

  if (isDone) {
    MRIteratorCallback_Done(ctx, 0);
  } else if (cmd->forCursor) {
    MRIteratorCallback_ProcessDone(ctx);
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
    case MR_REPLY_ARRAY: {
      int n = MRReply_Length(r);
      RSValue **arr = rm_malloc(n * sizeof(*arr));
      for (size_t i = 0; i < n; ++i) {
        arr[i] = MRReply_ToValue(MRReply_ArrayElement(r, i));
      }
      v = RSValue_NewArrayFromValues(arr, n);
      break;
    }
    case MR_REPLY_NIL:
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

  // profile vars
  MRReply **shardsProfile;
  int shardsProfileIdx;
} RPNet;

static int getNextReply(RPNet *nc) {
  if (nc->cmd.forCursor) {
    // if there are no more than `clusterConfig.cursorReplyThreshold` replies, trigger READs at the shards.
    // TODO: could be replaced with a query specific configuration
    if (!MR_ManuallyTriggerNextIfNeeded(nc->it, clusterConfig.cursorReplyThreshold)) {
      // No more replies
      nc->current.root = NULL;
      nc->current.rows = NULL;
      return 0;
    }
  }

  MRReply *root = MRIterator_Next(nc->it);
  if (root == MRITERATOR_DONE) {
    // No more replies
    nc->current.root = NULL;
    nc->current.rows = NULL;
    return 0;
  }

  MRReply *rows = MRReply_ArrayElement(root, 0);
  if (rows == NULL || MRReply_Type(rows) != MR_REPLY_ARRAY || MRReply_Length(rows) == 0) {
    MRReply_Free(root);
    RedisModule_Log(NULL, "warning", "An empty reply was received from a shard");
    ;
  }
  nc->current.root = root;
  nc->current.rows = rows;
  return 1;
}

static const RLookupKey *keyForField(RPNet *nc, const char *s) {
  for (const RLookupKey *kk = nc->lookup->head; kk; kk = kk->next) {
    if (!strcmp(kk->name, s)) {
      return kk;
    }
  }
  return NULL;
}

static int rpnetNext(ResultProcessor *self, SearchResult *r) {
  RPNet *nc = (RPNet *)self;
  // if we've consumed the last reply - free it
  if (nc->current.rows && nc->curIdx == MRReply_Length(nc->current.rows)) {
    long long cursorId = MRReply_Integer(MRReply_ArrayElement(nc->current.root, 1));
    // in profile mode, save shard's profile info to be returned later
    if (cursorId == 0 && nc->shardsProfile) {
      nc->shardsProfile[nc->shardsProfileIdx++] = nc->current.root;
    } else {
      MRReply_Free(nc->current.root);
    }
    nc->current.root = nc->current.rows = NULL;
  }

  // get the next reply from the channel
  if (!nc->current.root) {
    if (!getNextReply(nc)) {
      return RS_RESULT_EOF;
    }
    // Get the index from the first
    nc->base.parent->totalResults += MRReply_Integer(MRReply_ArrayElement(nc->current.rows, 0));
    nc->curIdx = 1;
  }

  MRReply *rep = MRReply_ArrayElement(nc->current.rows, nc->curIdx++);
  for (size_t i = 0; i < MRReply_Length(rep); i += 2) {
    const char *c = MRReply_String(MRReply_ArrayElement(rep, i), NULL);
    RSValue *v = RS_NullVal();
    if (i + 1 < MRReply_Length(rep)) {
      MRReply *val = MRReply_ArrayElement(rep, i + 1);
      v = MRReply_ToValue(val);
    }
    RLookup_WriteOwnKeyByName(nc->lookup, c, &r->rowdata, v);
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
    MRIterator_WaitDone(nc->it, nc->cmd.forCursor);
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

  int dialectOffset = RMUtil_ArgIndex("DIALECT", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (dialectOffset != -1 && dialectOffset + 3 + 1 + profileArgs < argc) {
    tmparr = array_append(tmparr, "DIALECT");
    tmparr = array_append(tmparr, RedisModule_StringPtrLen(argv[dialectOffset + 3 + 1 + profileArgs], NULL));  // the dialect
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

  // check for timeout argument and append it to the command
  int timeout_index = RMUtil_ArgIndex("TIMEOUT", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (timeout_index != -1) {
    MRCommand_AppendRstr(xcmd, argv[timeout_index]);
    MRCommand_AppendRstr(xcmd, argv[timeout_index + 1]);
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

size_t PrintShardProfile(RedisModuleCtx *ctx, int count, MRReply **replies, int isSearch);

void printAggProfile(RedisModuleCtx *ctx, AREQ *req) {
  size_t nelem = 0;
  clock_t finishTime = clock();
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  // profileRP replace netRP as end PR
  RPNet *rpnet = (RPNet *)req->qiter.rootProc;

  // Print shards profile
  nelem += PrintShardProfile(ctx, rpnet->shardsProfileIdx, rpnet->shardsProfile, 0);

  // Print coordinator profile
  RedisModule_ReplyWithSimpleString(ctx, "Coordinator");
  nelem++;

  RedisModule_ReplyWithSimpleString(ctx, "Result processors profile");
  Profile_Print(ctx, req);
  nelem += 2;

  RedisModule_ReplyWithSimpleString(ctx, "Total Coordinator time");
  RedisModule_ReplyWithDouble(ctx, (double)(clock() - req->initClock) / CLOCKS_PER_MILLISEC);
  nelem += 2;

  RedisModule_ReplySetArrayLength(ctx, nelem);
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
  // CMD, index, expr, args...
  AREQ *r = AREQ_New();
  QueryError status = {0};
  r->qiter.err = &status;
  r->reqflags |= QEXEC_F_IS_EXTENDED;

  int profileArgs = parseProfile(argv, argc, r);
  if (profileArgs == -1) goto err;

  int rc = AREQ_Compile(r, argv + 2 + profileArgs, argc - 2 - profileArgs, &status);
  if (rc != REDISMODULE_OK) goto err;

  rc = AGGPLN_Distribute(&r->ap, &status);
  if (rc != REDISMODULE_OK) goto err;

  AREQDIST_UpstreamInfo us = {NULL};
  rc = AREQ_BuildDistributedPipeline(r, &us, &status);
  if (rc != REDISMODULE_OK) goto err;

  SearchCluster *sc = GetSearchCluster();

  // Construct the command string
  MRCommand xcmd;
  buildMRCommand(argv , argc, profileArgs, &us, &xcmd);
  xcmd.forCursor = r->reqflags & QEXEC_F_IS_CURSOR;

  // Build the result processor chain
  buildDistRPChain(r, &xcmd, sc, &us);

  if (IsProfile(r)) r->parseTime = clock() - r->initClock;

  if (r->reqflags & QEXEC_F_IS_CURSOR) {
    const char *ixname = RedisModule_StringPtrLen(argv[1 + profileArgs], NULL);
//    const char *partTag = PartitionTag(&sc->part, sc->myPartition);
//    size_t dummy;
//    char *tagged = writeTaggedId(ixname, strlen(ixname), partTag, strlen(partTag), &dummy);

    // Keep the original concurrent context
    ConcurrentCmdCtx_KeepRedisCtx(cmdCtx);

    /**
     * The next three lines are a hack to ensure that the cursor retains a valid
     * RedisModuleCtx object. We rely on the existing mechanism of AREQ to free
     * the Ctx object used by NewSearchCtx. We don't actually read the spec
     * at all.
     */
    RedisModule_ThreadSafeContextLock(ctx);
    r->sctx = NewSearchCtxC(ctx, ixname, true);
    RedisModule_ThreadSafeContextUnlock(ctx);

    rc = AREQ_StartCursor(r, ctx, ixname, &status, true);

    if (rc != REDISMODULE_OK) {
      goto err;
    }
  } else if (IsProfile(r)) {
    RedisModule_ReplyWithArray(ctx, 2);
    sendChunk(r, ctx, -1);
    printAggProfile(ctx, r);
    AREQ_Free(r);
  } else {
    sendChunk(r, ctx, -1);
    AREQ_Free(r);
  }
  return;

// See if we can distribute the plan...
err:
  assert(QueryError_HasError(&status));
  QueryError_ReplyAndClear(ctx, &status);
  AREQ_Free(r);
  return;
}
