/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "result_processor.h"
#include "rmr/rmr.h"
#include "rmutil/util.h"
#include "commands.h"
#include "aggregate/aggregate.h"
#include "dist_plan.h"
#include "coord_module.h"
#include "profile.h"
#include "util/timeout.h"
#include "resp3.h"
#include "coord/src/config.h"

#include <err.h>

// Get cursor command using a cursor id and an existing aggregate command
// Returns true if the cursor is not done (i.e., not depleted)
static bool getCursorCommand(MRReply *res, MRCommand *cmd, MRIteratorCtx *ctx) {
  long long cursorId;
  if (!MRReply_ToInteger(MRReply_ArrayElement(res, 1), &cursorId)) {
    // Invalid format?!
    return false;
  }

  if (cursorId == 0) {
    // Cursor was set to 0, end of reply chain. cmd->depleted will be set in `MRIteratorCallback_Done`.
    return false;
  }

  RS_LOG_ASSERT(cmd->num >= 2, "Invalid command?!");

  // Check if the coordinator experienced a timeout or not
  bool timedout = MRIteratorCallback_GetTimedOut(ctx);

  MRCommand newCmd;
  char buf[128];
  sprintf(buf, "%lld", cursorId);
  // AGGREGATE commands has the index name at position 1
  // while CURSOR READ/DEL commands has it at position 2
  const char *idx = MRCommand_ArgStringPtrLen(cmd, cmd->rootCommand == C_AGG ? 1 : 2, NULL);
  // If we timed out and not in cursor mode, we want to send the shard a DEL
  // command instead of a READ command (here we know it has more results)
  if (timedout && !cmd->forCursor) {
    newCmd = MR_NewCommand(4, "_FT.CURSOR", "DEL", idx, buf);
    newCmd.depleted = true;
    // Mark that the last command was a DEL command
    newCmd.rootCommand = C_DEL;
  } else {
    newCmd = MR_NewCommand(4, "_FT.CURSOR", "READ", idx, buf);
    newCmd.rootCommand = C_READ;
  }

  if(timedout && cmd->forCursor) {
    // Reset the `timedOut` value in case it was set (for next iterations, as
    // we're in cursor mode)
    MRIteratorCallback_ResetTimedOut(ctx);
  }

  newCmd.targetSlot = cmd->targetSlot;
  newCmd.protocol = cmd->protocol;
  newCmd.forCursor = cmd->forCursor;
  MRCommand_Free(cmd);
  *cmd = newCmd;

  return true;
}


static int netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {
  MRCommand *cmd = MRIteratorCallback_GetCommand(ctx);

  // If the root command of this reply is a DEL command, we don't want to
  // propagate it up the chain to the client
  if (cmd->rootCommand == C_DEL) {
    if (MRReply_Type(rep) == MR_REPLY_ERROR) {
      RedisModule_Log(RSDummyContext, "warning", "Error returned for CURSOR.DEL command from shard");
    }
    // Discard the response, and return REDIS_OK
    MRIteratorCallback_Done(ctx, MRReply_Type(rep) == MR_REPLY_ERROR);
    MRReply_Free(rep);
    return REDIS_OK;
  }

  // Check if an error returned from the shard
  if (MRReply_Type(rep) == MR_REPLY_ERROR) {
    RedisModule_Log(RSDummyContext, "notice", "Coordinator got an error from a shard");
    RedisModule_Log(RSDummyContext, "verbose", "Shard error: %s", MRReply_String(rep, NULL));
    MRIteratorCallback_AddReply(ctx, rep); // to be picked up by getNextReply
    MRIteratorCallback_Done(ctx, 1);
    return REDIS_ERR;
  }

  bool bail_out = MRReply_Type(rep) != MR_REPLY_ARRAY;

  if (!bail_out) {
    size_t len = MRReply_Length(rep);
    if (cmd->protocol == 3) {
      bail_out = len != 2; // (map, cursor)
      if (bail_out) {
        RedisModule_Log(RSDummyContext, "warning", "Expected reply of length 2, got %ld", len);
      }
    } else {
      bail_out = len != 2 && len != 3; // (results, cursor) or (results, cursor, profile)
      if (bail_out) {
        RedisModule_Log(RSDummyContext, "warning", "Expected reply of length 2 or 3, got %ld", len);
      }
    }
  }

  if (bail_out) {
    RedisModule_Log(RSDummyContext, "warning", "An unexpected reply was received from a shard");
    MRReply_Free(rep);
    MRIteratorCallback_Done(ctx, 1);
    return REDIS_ERR;
  }

  // rewrite and resend the cursor command if needed
  int rc = REDIS_OK;
  bool done = !getCursorCommand(rep, cmd, MRIteratorCallback_GetCtx(ctx));

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
  } else if (cmd->forCursor) {
    MRIteratorCallback_ProcessDone(ctx);
  } else {
    // resend command
    if (MRIteratorCallback_ResendCommand(ctx) == REDIS_ERR) {
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
      const char *s = MRReply_String(r, &l);
      v = RS_NewCopiedString(s, l);
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
      RSValue **arr = RSValue_AllocateArray(n);
      for (size_t i = 0; i < n; ++i) {
        arr[i] = MRReply_ToValue(MRReply_ArrayElement(r, i));
      }
      v = RSValue_NewArray(arr, n);
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
  AREQ *areq;

  // profile vars
  arrayof(MRReply *) shardsProfile;
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

  // Check if an error was returned
  if(MRReply_Type(root) == MR_REPLY_ERROR) {
    nc->current.root = root;
    return 1;
  }

  MRReply *rows = MRReply_ArrayElement(root, 0);
  if (   rows == NULL
      || (MRReply_Type(rows) != MR_REPLY_ARRAY && MRReply_Type(rows) != MR_REPLY_MAP)
      || MRReply_Length(rows) == 0) {
    MRReply_Free(root);
    root = NULL;
    rows = NULL;
    RedisModule_Log(RSDummyContext, "warning", "An empty reply was received from a shard");
  }

  // invariant: either rows == NULL or least one row exists

  nc->current.root = root;
  nc->current.rows = rows;

  assert(   !nc->current.rows
         || MRReply_Type(nc->current.rows) == MR_REPLY_ARRAY
         || MRReply_Type(nc->current.rows) == MR_REPLY_MAP);
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
  // Or
  // Simple error

  // If root isn't a simple error:
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
        bool timed_out = false;
        // Check for a warning (resp3 only)
        MRReply *warning = MRReply_MapElement(rows, "warning");
        if (resp3 && MRReply_Length(warning) > 0) {
          const char *warning_str = MRReply_String(MRReply_ArrayElement(warning, 0), NULL);
          // Set an error to be later picked up and sent as a warning
          if (!strcmp(warning_str, QueryError_Strerror(QUERY_ETIMEDOUT))) {
            timed_out = true;
          } else if (!strcmp(warning_str, QUERY_WMAXPREFIXEXPANSIONS)) {
            nc->areq->qiter.err->reachedMaxPrefixExpansions = true;
          }
        }

        long long cursorId = MRReply_Integer(MRReply_ArrayElement(root, 1));

        // in profile mode, save shard's profile info to be returned later
        if (cursorId == 0 && nc->shardsProfile) {
          array_ensure_append_1(nc->shardsProfile, root);
        } else {

          MRReply_Free(root);
        }
        nc->current.root = nc->current.rows = root = rows = NULL;

        if (timed_out) {
          return RS_RESULT_TIMEDOUT;
        }
      }
  }

  int new_reply = !root;

  // get the next reply from the channel
  while (!root || !rows || MRReply_Length(rows) == 0) {
    if (TimedOut(&self->parent->sctx->time.timeout)) {
      // Set the `timedOut` flag in the MRIteratorCtx, later to be read by the
      // callback so that a `CURSOR DEL` command will be dispatched instead of
      // a `CURSOR READ` command.
      MRIteratorCallback_SetTimedOut(MRIterator_GetCtx(nc->it));

      return RS_RESULT_TIMEDOUT;
    } else if (MRIteratorCallback_GetTimedOut(MRIterator_GetCtx(nc->it))) {
      // if timeout was set in previous reads, reset it
      MRIteratorCallback_ResetTimedOut(MRIterator_GetCtx(nc->it));
    }

    if (!getNextReply(nc)) {
      return RS_RESULT_EOF;
    }

    // If an error was returned, propagate it
    if (nc->current.root && MRReply_Type(nc->current.root) == MR_REPLY_ERROR) {
      const char *strErr = MRReply_String(nc->current.root, NULL);
      if (!strErr
          || strcmp(strErr, "Timeout limit was reached")
          || nc->areq->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) {
        QueryError_SetError(nc->areq->qiter.err, QUERY_EGENERIC, strErr);
        return RS_RESULT_ERROR;
      }
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
  MRIterator *it = MR_Iterate(&nc->cmd, netCursorCallback);
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
    MRIterator_Free(nc->it);
  }

  if (nc->shardsProfile) {
    array_foreach(nc->shardsProfile, reply, {
      if (reply != nc->current.root) {
        MRReply_Free(reply);
      }
    });
    array_free(nc->shardsProfile);
  }

  MRReply_Free(nc->current.root);
  MRCommand_Free(&nc->cmd);

  rm_free(rp);
}

static RPNet *RPNet_New(const MRCommand *cmd) {
  RPNet *nc = rm_calloc(1, sizeof(*nc));
  nc->cmd = *cmd; // Take ownership of the command's internal allocations
  nc->areq = NULL;
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
    array_append(tmparr, RS_AGGREGATE_CMD);                         // Command
    array_append(tmparr, RedisModule_StringPtrLen(argv[1], NULL));  // Index name
  } else {
    array_append(tmparr, RS_PROFILE_CMD);
    array_append(tmparr, RedisModule_StringPtrLen(argv[1], NULL));  // Index name
    array_append(tmparr, "AGGREGATE");
    if (profileArgs == 3) {
      array_append(tmparr, "LIMITED");
    }
    array_append(tmparr, "QUERY");
  }

  array_append(tmparr, RedisModule_StringPtrLen(argv[2 + profileArgs], NULL));  // Query
  array_append(tmparr, "WITHCURSOR");
  // Numeric responses are encoded as simple strings.
  array_append(tmparr, "_NUM_SSTRING");

  int argOffset = RMUtil_ArgIndex("DIALECT", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (argOffset != -1 && argOffset + 3 + 1 + profileArgs < argc) {
    array_append(tmparr, "DIALECT");
    array_append(tmparr, RedisModule_StringPtrLen(argv[argOffset + 3 + 1 + profileArgs], NULL));  // the dialect
  }

  argOffset = RMUtil_ArgIndex("FORMAT", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (argOffset != -1 && argOffset + 3 + 1 + profileArgs < argc) {
    array_append(tmparr, "FORMAT");
    array_append(tmparr, RedisModule_StringPtrLen(argv[argOffset + 3 + 1 + profileArgs], NULL));  // the format
  }

  if (RMUtil_ArgIndex("ADDSCORES", argv + 3 + profileArgs, argc - 3 - profileArgs) != -1) {
    array_append(tmparr, "ADDSCORES");
  }

  if (RMUtil_ArgIndex("VERBATIM", argv + 3 + profileArgs, argc - 3 - profileArgs) != -1) {
    array_append(tmparr, "VERBATIM");
  }

  for (size_t ii = 0; ii < us->nserialized; ++ii) {
    array_append(tmparr, us->serialized[ii]);
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

static void buildDistRPChain(AREQ *r, MRCommand *xcmd, AREQDIST_UpstreamInfo *us) {
  // Establish our root processor, which is the distributed processor
  RPNet *rpRoot = RPNet_New(xcmd); // This will take ownership of the command
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
    // 2 is just a starting size, as we most likely have more than 1 shard
    rpRoot->shardsProfile = array_new(MRReply*, 2);
    if (!found) {
      r->qiter.endProc = rpProfile;
    }
  }
}

void PrintShardProfile_resp2(RedisModule_Reply *reply, int count, MRReply **replies, bool isSearch);
void PrintShardProfile_resp3(RedisModule_Reply *reply, int count, MRReply **replies, bool isSearch);

void printAggProfile(RedisModule_Reply *reply, AREQ *req, bool timedout, bool reachedMaxPrefixExpansions) {
  clock_t finishTime = clock();

  RedisModule_ReplyKV_Map(reply, "Shards"); // >Shards

  // profileRP replace netRP as end PR
  RPNet *rpnet = (RPNet *)req->qiter.rootProc;

  // Print shards profile
  if (reply->resp3) {
    PrintShardProfile_resp3(reply, array_len(rpnet->shardsProfile), rpnet->shardsProfile, false);
  } else {
    PrintShardProfile_resp2(reply, array_len(rpnet->shardsProfile), rpnet->shardsProfile, false);
  }

  RedisModule_Reply_MapEnd(reply); // Shards
  // Print coordinator profile

  RedisModule_ReplyKV_Map(reply, "Coordinator"); // >coordinator

  RedisModule_ReplyKV_Map(reply, "Result processors profile");
  Profile_Print(reply, req, timedout, reachedMaxPrefixExpansions);
  RedisModule_Reply_MapEnd(reply);

  RedisModule_ReplyKV_Double(reply, "Total Coordinator time", (double)(clock() - req->initClock) / CLOCKS_PER_MILLISEC);

  RedisModule_Reply_MapEnd(reply); // >coordinator
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
  r->profile = printAggProfile;

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

  // Construct the command string
  MRCommand xcmd;
  buildMRCommand(argv , argc, profileArgs, &us, &xcmd);
  xcmd.protocol = is_resp3(ctx) ? 3 : 2;
  xcmd.forCursor = r->reqflags & QEXEC_F_IS_CURSOR;
  xcmd.rootCommand = C_AGG;  // Response is equivalent to a `CURSOR READ` response

  // Build the result processor chain
  buildDistRPChain(r, &xcmd, &us);

  if (IsProfile(r)) r->parseTime = clock() - r->initClock;

  // Create the Search context
  // (notice with cursor, we rely on the existing mechanism of AREQ to free the ctx object when the cursor is exhausted)
  r->sctx = rm_new(RedisSearchCtx);
  *r->sctx = SEARCH_CTX_STATIC(ctx, NULL);
  r->sctx->apiVersion = dialect;
  SearchCtx_UpdateTime(r->sctx, r->reqConfig.queryTimeoutMS);
  r->qiter.sctx = r->sctx;
  // r->sctx->expanded should be received from shards

  if (r->reqflags & QEXEC_F_IS_CURSOR) {
    // Keep the original concurrent context
    ConcurrentCmdCtx_KeepRedisCtx(cmdCtx);

    StrongRef dummy_spec_ref = {.rm = NULL};
    rc = AREQ_StartCursor(r, reply, dummy_spec_ref, &status, true);

    if (rc != REDISMODULE_OK) {
      goto err;
    }
  } else {
    sendChunk(r, reply, -1);
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
