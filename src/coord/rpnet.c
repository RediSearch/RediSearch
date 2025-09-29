/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "rpnet.h"
#include "rmr/reply.h"
#include "rmr/rmr.h"
#include "hiredis/sds.h"


#define CURSOR_EOF 0


// Get cursor command using a cursor id and an existing aggregate command
// Returns true if the cursor is not done (i.e., not depleted)
static bool getCursorCommand(long long cursorId, MRCommand *cmd, MRIteratorCtx *ctx) {
  if (cursorId == CURSOR_EOF) {
    // Cursor was set to 0, end of reply chain. cmd->depleted will be set in `MRIteratorCallback_Done`.
    return false;
  }

  RS_LOG_ASSERT(cmd->num >= 2, "Invalid command?!");

  // Check if the coordinator experienced a timeout or not
  bool timedout = MRIteratorCallback_GetTimedOut(ctx);
  // The previous command was a _FT.CURSOR READ command, so we may not need to change anything.
  RS_LOG_ASSERT(cmd->rootCommand == C_READ, "calling `getCursorCommand` after a DEL command");
  RS_ASSERT(cmd->num == 4);
  RS_ASSERT(STR_EQ(cmd->strs[0], cmd->lens[0], "_FT.CURSOR"));
  RS_ASSERT(STR_EQ(cmd->strs[1], cmd->lens[1], "READ"));
  RS_ASSERT(atoll(cmd->strs[3]) == cursorId);

  // If we timed out and not in cursor mode, we want to send the shard a DEL
  // command instead of a READ command (here we know it has more results)
  if (timedout && !cmd->forCursor) {
    MRCommand_ReplaceArg(cmd, 1, "DEL", 3);
    cmd->rootCommand = C_DEL;
  }

  if (timedout && cmd->forCursor) {
    // Reset the `timedOut` value in case it was set (for next iterations, as
    // we're in cursor mode)
    MRIteratorCallback_ResetTimedOut(ctx);
  }

  return true;
}


static void netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {
  MRCommand *cmd = MRIteratorCallback_GetCommand(ctx);

  // If the root command of this reply is a DEL command, we don't want to
  // propagate it up the chain to the client
  if (cmd->rootCommand == C_DEL) {
    // Discard the response, and return REDIS_OK
    MRIteratorCallback_Done(ctx, MRReply_Type(rep) == MR_REPLY_ERROR);
    MRReply_Free(rep);
    return;
  }

  // Check if an error returned from the shard
  if (MRReply_Type(rep) == MR_REPLY_ERROR) {
    const char* error = MRReply_String(rep, NULL);
    // RedisModule_Log(RSDummyContext, "notice", "Coordinator got an error '%.*s' from a shard", GetRedisErrorCodeLength(error), error);
    // RedisModule_Log(RSDummyContext, "verbose", "Shard error: %s", error);
    MRIteratorCallback_AddReply(ctx, rep); // to be picked up by getNextReply
    MRIteratorCallback_Done(ctx, 1);
    return;
  }

  // Normal reply from the shard.
  // In any case, the cursor id is the second element in the reply
  RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 1)) == MR_REPLY_INTEGER);
  long long cursorId = MRReply_Integer(MRReply_ArrayElement(rep, 1));
  RedisModule_Log(NULL, "warning", "netCursorCallback: cmd is %s", MRCommand_SafeToString(cmd));
  RedisModule_Log(NULL, "warning", "netCursorCallback: cursorId is %lld", cursorId);
  RedisModule_Log(NULL, "warning", "netCursorCallback: printMRReplyRecursive: ");
  printMRReplyRecursive(rep, 0);

  // Push the reply down the chain, to be picked up by getNextReply
  MRIteratorCallback_AddReply(ctx, rep); // take ownership of the reply

  // rewrite and resend the cursor command if needed
  // should only be determined based on the cursor and not on the set of results we get
  if (!getCursorCommand(cursorId, cmd, MRIteratorCallback_GetCtx(ctx))) {
    MRIteratorCallback_Done(ctx, 0);
  } else if (cmd->forCursor) {
    MRIteratorCallback_ProcessDone(ctx);
  } else if (MRIteratorCallback_ResendCommand(ctx) == REDIS_ERR) {
    MRIteratorCallback_Done(ctx, 1);
  }
}


static RSValue *MRReply_ToValue(MRReply *r) {
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

static int getNextReply(RPNet *nc) {
  if (nc->cmd.forCursor) {
    // if there are no more than `clusterConfig.cursorReplyThreshold` replies, trigger READs at the shards.
    // TODO: could be replaced with a query specific configuration
    if (!MR_ManuallyTriggerNextIfNeeded(nc->it, clusterConfig.cursorReplyThreshold)) {
      // No more replies
      RPNet_resetCurrent(nc);
      return 0;
    }
  }
  MRReply *root = MRIterator_Next(nc->it);
  RedisModule_Log(NULL, "warning", "getNextReply: root is %p", root);
  RedisModule_Log(NULL, "warning", "getNextReply: printMRReplyRecursive: ");
  printMRReplyRecursive(root, 0);
  if (root == NULL) {
    // No more replies
    RPNet_resetCurrent(nc);
    return MRIterator_GetPending(nc->it);
  }

  // Check if an error was returned
  if(MRReply_Type(root) == MR_REPLY_ERROR) {
    nc->current.root = root;
    return 1;
  }

  // For profile command, extract the profile data from the reply
  if (nc->cmd.forProfiling) {
    // if the cursor id is 0, this is the last reply from this shard, and it has the profile data
    if (CURSOR_EOF == MRReply_Integer(MRReply_ArrayElement(root, 1))) {
      MRReply *profile_data;
      if (nc->cmd.protocol == 3) {
        // [
        //   {
        //     "Results": { <FT.AGGREGATE reply> },
        //     "Profile": { <profile data> }
        //   },
        //   cursor_id
        // ]
        MRReply *data = MRReply_ArrayElement(root, 0);
        profile_data = MRReply_TakeMapElement(data, "profile");
      } else {
        // RESP2
        RS_ASSERT(nc->cmd.protocol == 2);
        // [
        //   <FT.AGGREGATE reply>,
        //   cursor_id,
        //   <profile data>
        // ]
        RS_ASSERT(MRReply_Length(root) == 3);
        profile_data = MRReply_TakeArrayElement(root, 2);
      }
      array_append(nc->shardsProfile, profile_data);
    }
  }

  MRReply *rows = NULL, *meta = NULL;
  if (nc->cmd.protocol == 3) { // RESP3
    meta = MRReply_ArrayElement(root, 0);
    if (nc->cmd.forProfiling) {
      meta = MRReply_MapElement(meta, "results"); // profile has an extra level
    }
    rows = MRReply_MapElement(meta, "results");
  } else { // RESP2
    rows = MRReply_ArrayElement(root, 0);
  }

  const size_t empty_rows_len = nc->cmd.protocol == 3 ? 0 : 1; // RESP2 has the first element as the number of results.
  RS_ASSERT(rows && MRReply_Type(rows) == MR_REPLY_ARRAY);
  if (MRReply_Length(rows) <= empty_rows_len) {
    RedisModule_Log(RSDummyContext, "verbose", "An empty reply was received from a shard");
    MRReply_Free(root);
    root = NULL;
    rows = NULL;
    meta = NULL;
  }

  nc->current.root = root;
  nc->current.rows = rows;
  nc->current.meta = meta;
  return 1;
}


static void nopCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {
//debug
  printMRReplyRecursive(rep, 0);

  MRIteratorCallback_AddReply(ctx, rep);
  MRIteratorCallback_Done(ctx, 0);
}

/**
 * Start function for RPNet with cursor mappings
 * Replaces rpnetNext_StartDispatcher
 */
int rpnetNext_StartWithMappings(ResultProcessor *rp, SearchResult *r) {
    RPNet *nc = (RPNet *)rp;

    CursorMappings *vsimOrSearch = StrongRef_Get(nc->mappings);
    // Mappings should already be populated by HybridRequest_executePlan
    if (!vsimOrSearch || array_len(vsimOrSearch->mappings) == 0) {
        RedisModule_Log(NULL, "error", "No cursor mappings available for RPNet");
        return REDISMODULE_ERR;
    }

    // Get index name from command
    const char *idx = MRCommand_ArgStringPtrLen(&nc->cmd, 1, NULL);

    RedisModule_Log(NULL, "warning", "rpnetNext_StartWithMappings: vsimOrSearch is %p, array_len(vsimOrSearch->mappings) is %zu", vsimOrSearch, array_len(vsimOrSearch->mappings));

    // Create cursor read command
    nc->cmd = MR_NewCommand(3, "_FT.CURSOR", "READ", idx);
    nc->cmd.protocol = 3;
    nc->it = MR_IterateWithPrivateData(&nc->cmd, netCursorCallback, NULL, iterCursorMappingCb, vsimOrSearch);
    nc->base.Next = rpnetNext;

    return rpnetNext(rp, r);
}

void rpnetFree(ResultProcessor *rp) {
  RPNet *nc = (RPNet *)rp;

  if (nc->it) {
    RS_DEBUG_LOG("rpnetFree: calling MRIterator_Release");
    MRIterator_Release(nc->it);
  }

  if (nc->shardsProfile) {
    array_foreach(nc->shardsProfile, reply, MRReply_Free(reply));
    array_free(nc->shardsProfile);
  }

  // NEW: Free cursor mappings
  if (nc->mappings.rm) {
    StrongRef_Release(nc->mappings);
  }
  MRReply_Free(nc->current.root);
  MRCommand_Free(&nc->cmd);

  rm_free(rp);
}


RPNet *RPNet_New(const MRCommand *cmd, int (*nextFunc)(ResultProcessor *, SearchResult *)) {
  RPNet *nc = rm_calloc(1, sizeof(*nc));
  nc->cmd = *cmd; // Take ownership of the command's internal allocations
  nc->areq = NULL;
  nc->shardsProfile = NULL;
  nc->base.Free = rpnetFree;
  nc->base.Next = nextFunc;
  nc->base.type = RP_NETWORK;
  return nc;
}

void RPNet_resetCurrent(RPNet *nc) {
    nc->current.root = NULL;
    nc->current.rows = NULL;
    nc->current.meta = NULL;
}

int rpnetNext(ResultProcessor *self, SearchResult *r) {
  RedisModule_Log(NULL, "warning", "rpnetNext: beginning", self);
  RPNet *nc = (RPNet *)self;
  MRReply *root = nc->current.root, *rows = nc->current.rows;
  const bool resp3 = nc->cmd.protocol == 3;


  // root (array) has similar structure for RESP2/3:
  // [0] array of results (rows) described right below
  // [1] cursor (int)
  // Or
  // Simple error

  // If root isn't a simple error:
  // rows:
  // RESP2: [ num_results, [ field, value, ... ], ... ]
  // RESP3: [ { field: value, ... }, ... ]

  // can also get an empty row:
  // RESP2: [] or [ 0 ]
  // RESP3: {}

  RedisModule_Log(NULL, "warning", "rpnetNext: rows is %p", rows);
  if (rows) {
      size_t len = MRReply_Length(rows);

      if (nc->curIdx == len) {
        bool timed_out = false;
        // Check for a warning (resp3 only)
        if (resp3) {
          MRReply *warning = MRReply_MapElement(nc->current.meta, "warning");
          if (MRReply_Length(warning) > 0) {
            const char *warning_str = MRReply_String(MRReply_ArrayElement(warning, 0), NULL);
            // Set an error to be later picked up and sent as a warning
            if (!strcmp(warning_str, QueryError_Strerror(QUERY_ETIMEDOUT))) {
              timed_out = true;
            } else if (!strcmp(warning_str, QUERY_WMAXPREFIXEXPANSIONS)) {
              AREQ_QueryProcessingCtx(nc->areq)->err->reachedMaxPrefixExpansions = true;
            }
          }
        }

        MRReply_Free(root);
        root = rows = NULL;
        RPNet_resetCurrent(nc);

        if (timed_out) {
          return RS_RESULT_TIMEDOUT;
        }
      }
  }

  bool new_reply = !root;

  // get the next reply from the channel
  while (!root) {
    // if (TimedOut(&nc->areq->sctx->time.timeout)) {
    //   // Set the `timedOut` flag in the MRIteratorCtx, later to be read by the
    //   // callback so that a `CURSOR DEL` command will be dispatched instead of
    //   // a `CURSOR READ` command.
    //   MRIteratorCallback_SetTimedOut(MRIterator_GetCtx(nc->it));

    //   return RS_RESULT_TIMEDOUT;
    // } else if (MRIteratorCallback_GetTimedOut(MRIterator_GetCtx(nc->it))) {
    //   // if timeout was set in previous reads, reset it
    //   MRIteratorCallback_ResetTimedOut(MRIterator_GetCtx(nc->it));
    // }

    if (!getNextReply(nc)) {
      return RS_RESULT_EOF;
    }

    // If an error was returned, propagate it
    if (nc->current.root && MRReply_Type(nc->current.root) == MR_REPLY_ERROR) {
      const char *strErr = MRReply_String(nc->current.root, NULL);
      if (!strErr
          || strcmp(strErr, "Timeout limit was reached")
          || nc->areq->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) {
        QueryError_SetError(AREQ_QueryProcessingCtx(nc->areq)->err, QUERY_EGENERIC, strErr);
        return RS_RESULT_ERROR;
      }
    }

    root = nc->current.root;
    rows = nc->current.rows;
  }

  // invariant: at least one row exists
  if (new_reply) {
    if (resp3) { // RESP3
      nc->curIdx = 0;
      nc->base.parent->totalResults += MRReply_Length(rows);
      processResultFormat(&nc->areq->reqflags, nc->current.meta);
    } else { // RESP2
      // Get the index from the first
      nc->base.parent->totalResults += MRReply_Integer(MRReply_ArrayElement(rows, 0));
      nc->curIdx = 1;
    }
  }

  RedisModule_Log(NULL, "warning", "rpnetNext: rows is %s", MRReply_String(rows, NULL));

  MRReply *fields = MRReply_ArrayElement(rows, nc->curIdx++);
  if (resp3) {
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_MAP, "invalid result record");
    fields = MRReply_MapElement(fields, "extra_attributes");
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_MAP, "invalid fields record");
  } else {
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_ARRAY, "invalid result record");
    RS_LOG_ASSERT(MRReply_Length(fields) % 2 == 0, "invalid fields record");
  }

  for (size_t i = 0; i < MRReply_Length(fields); i += 2) {
    size_t len;
    const char *field = MRReply_String(MRReply_ArrayElement(fields, i), &len);
    if (strcmp(field, "__key") == 0) {
      r->dmd = rm_calloc(1, sizeof(RSDocumentMetadata));
      r->dmd->keyPtr = sdsnewlen(MRReply_String(MRReply_ArrayElement(fields, i + 1), &len), len);
    }
    MRReply *val = MRReply_ArrayElement(fields, i + 1);
    RSValue *v = MRReply_ToValue(val);
    RLookup_WriteOwnKeyByName(nc->lookup, field, len, &r->rowdata, v);
  }
  return RS_RESULT_OK;
}



