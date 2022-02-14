#include "result_processor.h"
#include "rmr/rmr.h"
#include "rmutil/util.h"
#include "search_cluster.h"
#include "commands.h"
#include "aggregate/aggregate.h"
#include "dist_plan.h"
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
  MRCommand_Free(cmd);
  *cmd = newCmd;

  return 1;
}

static int netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep, MRCommand *cmd) {
  if (!rep || MRReply_Type(rep) != MR_REPLY_ARRAY || MRReply_Length(rep) != 2) {
    if (MRReply_Type(rep) == MR_REPLY_ERROR) {
      //      printf("Error is '%s'\n", MRReply_String(rep, NULL));
    }
    MRReply_Free(rep);
    MRIteratorCallback_Done(ctx, 1);
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
      RSValue **arr = rm_calloc(MRReply_Length(r), sizeof(*arr));
      for (size_t i = 0; i < MRReply_Length(r); i++) {
        arr[i] = MRReply_ToValue(MRReply_ArrayElement(r, i));
      }
      v = RSValue_NewArrayEx(arr, MRReply_Length(r), RSVAL_ARRAY_ALLOC | RSVAL_ARRAY_NOINCREF);
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
    if (rows == NULL || MRReply_Type(rows) != MR_REPLY_ARRAY || MRReply_Length(rows) == 0) {
      MRReply_Free(root);
      continue;
    }
    nc->current.root = root;
    nc->current.rows = rows;
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

static int rpnetNext(ResultProcessor *self, SearchResult *r) {
  RPNet *nc = (RPNet *)self;
  // if we've consumed the last reply - free it
  if (nc->current.rows && nc->curIdx == MRReply_Length(nc->current.rows)) {
    MRReply_Free(nc->current.root);
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
    MRIterator_WaitDone(nc->it);
  }

  nc->cg.Free(nc->cg.ctx);

  if (nc->current.root) {
    MRReply_Free(nc->current.root);
  }

  if (nc->it) MRIterator_Free(nc->it);
  free(rp);
}

static RPNet *RPNet_New(const MRCommand *cmd, SearchCluster *sc) {
  //  MRCommand_FPrint(stderr, &cmd);
  RPNet *nc = calloc(1, sizeof(*nc));
  nc->cmd = *cmd;
  nc->cg = SearchCluster_MultiplexCommand(sc, &nc->cmd);
  nc->base.Free = rpnetFree;
  nc->base.Next = rpnetNext_Start;
  nc->base.name = "Network";
  return nc;
}

static void buildMRCommand(RedisModuleString **argv, int argc, AREQDIST_UpstreamInfo *us,
                           MRCommand *xcmd) {
  // We need to prepend the array with the command, index, and query that
  // we want to use.
  const char **tmparr = array_new(const char *, us->nserialized);
  tmparr = array_append(tmparr, RS_AGGREGATE_CMD);                         // Command
  tmparr = array_append(tmparr, RedisModule_StringPtrLen(argv[1], NULL));  // Query
  tmparr = array_append(tmparr, RedisModule_StringPtrLen(argv[2], NULL));
  tmparr = array_append(tmparr, "WITHCURSOR");
  // Numeric responses are encoded as simple strings.
  tmparr = array_append(tmparr, "_NUM_SSTRING");

  for (size_t ii = 0; ii < us->nserialized; ++ii) {
    tmparr = array_append(tmparr, us->serialized[ii]);
  }

  *xcmd = MR_NewCommandArgv(array_len(tmparr), tmparr);
  MRCommand_SetPrefix(xcmd, "_FT");

  array_free(tmparr);
}

static void buildDistRPChain(AREQ *r, MRCommand *xcmd, SearchCluster *sc,
                             AREQDIST_UpstreamInfo *us) {
  // Establish our root processor, which is the distributed processor
  RPNet *rpRoot = RPNet_New(xcmd, sc);
  rpRoot->lookup = us->lookup;

  assert(!r->qiter.rootProc);
  // Get the deepest-most root:
  int found = 0;
  for (ResultProcessor *rp = r->qiter.endProc; rp; rp = rp->upstream) {
    if (!rp->upstream) {
      rp->upstream = &rpRoot->base;
      found = 1;
      break;
    }
  }

  // assert(found);
  r->qiter.rootProc = &rpRoot->base;
  if (!found) {
    r->qiter.endProc = &rpRoot->base;
  }
  rpRoot->base.parent = &r->qiter;
}

void RSExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         struct ConcurrentCmdCtx *cmdCtx) {
  // CMD, index, expr, args...
  AREQ *r = AREQ_New();
  QueryError status = {0};
  r->qiter.err = &status;
  int rc = AREQ_Compile(r, argv + 2, argc - 2, &status);
  if (rc != REDISMODULE_OK) {
    assert(QueryError_HasError(&status));
    goto err;
  }
  rc = AGGPLN_Distribute(&r->ap, &status);
  if (rc != REDISMODULE_OK) {
    assert(QueryError_HasError(&status));
    goto err;
  }
  AREQDIST_UpstreamInfo us = {NULL};
  rc = AREQ_BuildDistributedPipeline(r, &us, &status);
  if (rc != REDISMODULE_OK) {
    assert(QueryError_HasError(&status));
    goto err;
  }

  SearchCluster *sc = GetSearchCluster();

  // Construct the command string
  MRCommand xcmd;
  buildMRCommand(argv, argc, &us, &xcmd);

  // Build the result processor chain
  buildDistRPChain(r, &xcmd, sc, &us);

  if (r->reqflags & QEXEC_F_IS_CURSOR) {
    const char *ixname = RedisModule_StringPtrLen(argv[1], NULL);
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

    rc = AREQ_StartCursor(r, ctx, ixname, &status);

//    free(tagged);
    if (rc != REDISMODULE_OK) {
      assert(QueryError_HasError(&status));
      goto err;
    }
  } else {
    AREQ_Execute(r, ctx);
  }
  return;

// See if we can distribute the plan...
err:
  assert(QueryError_HasError(&status));
  QueryError_ReplyAndClear(ctx, &status);
  AREQ_Free(r);
  return;
}
