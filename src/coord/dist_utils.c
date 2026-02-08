/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "dist_utils.h"
#include "util/misc.h"
#include "util/strconv.h"
#include "rpnet.h"

static bool getCursorCommand(long long cursorId, MRCommand *cmd, MRIteratorCtx *ctx, bool shardTimedOut);

// Helper function to extract total_results from a shard reply
// Returns true if total_results was found, false otherwise
static bool extractTotalResults(MRReply *rep, MRCommand *cmd, long long *out_total) {
  if (cmd->protocol == 3) {
    // RESP3: [map, cursor]
    MRReply *meta = MRReply_ArrayElement(rep, 0);

    // Handle profiling: results are nested under "results" key
    if (cmd->forProfiling) {
      meta = MRReply_MapElement(meta, "results");
    }

    // Extract total_results from metadata
    if (meta) {
      MRReply *totalReply = MRReply_MapElement(meta, "total_results");
      if (totalReply && MRReply_Type(totalReply) == MR_REPLY_INTEGER) {
        *out_total = MRReply_Integer(totalReply);
        return true;
      }
    }
  } else {
    // RESP2: [results, cursor] or [results, cursor, profile]
    MRReply *results = MRReply_ArrayElement(rep, 0);
    if (results && MRReply_Type(results) == MR_REPLY_ARRAY && MRReply_Length(results) > 0) {
      // First element is total_results
      MRReply *totalReply = MRReply_ArrayElement(results, 0);
      if (totalReply && MRReply_ToInteger(totalReply, out_total)) {
        return true;
      }
    }
  }

  return false;
}

void netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {
  MRCommand *cmd = MRIteratorCallback_GetCommand(ctx);
  ShardResponseBarrier *barrier = (ShardResponseBarrier *)MRIteratorCallback_GetPrivateData(ctx);

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
    RedisModule_Log(RSDummyContext, "notice", "Coordinator got an error '%.*s' from a shard", GetRedisErrorCodeLength(error), error);
    RedisModule_Log(RSDummyContext, "verbose", "Shard error: %s", error);
    if (barrier && barrier->notifyCallback) {
      // Notify an error was received
      barrier->notifyCallback(cmd->targetShard, 0, true, barrier);
    }
    MRIteratorCallback_AddReply(ctx, rep); // to be picked up by getNextReply
    MRIteratorCallback_Done(ctx, 1);
    return;
  }

  const bool isResp3 = cmd->protocol == 3;
  bool bail_out = MRReply_Type(rep) != MR_REPLY_ARRAY;

  if (!bail_out) {
    size_t len = MRReply_Length(rep);
    if (isResp3) {
      bail_out = len != 2; // (map, cursor)
      if (bail_out) {
        RedisModule_Log(RSDummyContext, "warning", "Expected reply of length 2, got %ld", len);
      }
      if (!bail_out) bail_out = MRReply_Type(MRReply_ArrayElement(rep, 0)) != MR_REPLY_MAP;
      if (bail_out) {
        RedisModule_Log(RSDummyContext, "warning", "Expected reply of type map, got %d", MRReply_Type(MRReply_ArrayElement(rep, 0)));
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
    return;
  }

  long long cursorId;
  MRReply* cursor = MRReply_ArrayElement(rep, 1);
  if (!MRReply_ToInteger(cursor, &cursorId)) {
    cursorId = CURSOR_EOF;
  }

  // Extract total_results and notify barrier via callback (if registered)
  if (barrier && barrier->notifyCallback) {
    long long shardTotal;
    if (!extractTotalResults(rep, cmd, &shardTotal)) {
      // If no error was detected earlier, and still we failed to extract total_results,
      // Response is malformed: log a warning and set total to 0.
      // Notice: must still call the notify callback since a response was received
      shardTotal = 0;
      RedisModule_Log(RSDummyContext, "notice", "Coordinator could not extract total_results from shard %d reply", cmd->targetShard);
    }
    barrier->notifyCallback(cmd->targetShard, shardTotal, false, barrier);
  }

  // Check if the shard returned a timeout warning (for profiling commands with RESP3)
  bool shardTimedOut = false;
  if (cmd->forProfiling && cmd->protocol == 3) {
    RS_LOG_ASSERT(!cmd->forCursor, "Profiling is not supported on a cursor command");
    MRReply *meta = MRReply_ArrayElement(rep, 0);
    meta = MRReply_MapElement(meta, "results");  // profile has an extra level

    // Check if we got timeout
    MRReply *warning = MRReply_MapElement(meta, "warning");
    // Iterate over all warnings in the array and check for timeout
    for (size_t i = 0; i < MRReply_Length(warning); i++) {
      const char *warning_str = MRReply_String(MRReply_ArrayElement(warning, i), NULL);
      if (!strcmp(warning_str, QueryError_Strerror(QUERY_ETIMEDOUT))) {
        // When a shard returns timeout on RETURN policy, the profile is not returned.
        // We capture this locally and pass it to getCursorCommand to avoid a race
        // condition with the coordinator thread that might reset the shared timedOut flag.
        shardTimedOut = true;
      }
    }
  }

  // Push the reply down the chain, to be picked up by getNextReply
  MRIteratorCallback_AddReply(ctx, rep); // take ownership of the reply

  // rewrite and resend the cursor command if needed
  // should only be determined based on the cursor and not on the set of results we get
  if (!getCursorCommand(cursorId, cmd, MRIteratorCallback_GetCtx(ctx), shardTimedOut)) {
    MRIteratorCallback_Done(ctx, 0);
  } else if (cmd->forCursor) {
    MRIteratorCallback_ProcessDone(ctx);
  } else if (MRIteratorCallback_ResendCommand(ctx) == REDIS_ERR) {
    MRIteratorCallback_Done(ctx, 1);
  }
}

// Get cursor command using a cursor id and an existing aggregate command
// Returns true if the cursor is not done (i.e., not depleted)
static bool getCursorCommand(long long cursorId, MRCommand *cmd, MRIteratorCtx *ctx, bool shardTimedOut) {
  if (cursorId == CURSOR_EOF) {
    // Cursor was set to 0, end of reply chain. cmd->depleted will be set in `MRIteratorCallback_Done`.
    return false;
  }

  RS_LOG_ASSERT(cmd->num >= 2, "Invalid command?!");

  // Check if the coordinator experienced a timeout or not
  bool timedout = MRIteratorCallback_GetTimedOut(ctx) || shardTimedOut;

  MRCommand newCmd;
  char buf[128];
  sprintf(buf, "%lld", cursorId);
  // AGGREGATE commands has the index name at position 1
  // while CURSOR READ/DEL commands has it at position 2
  const char *idx = MRCommand_ArgStringPtrLen(cmd, cmd->rootCommand == C_AGG ? 1 : 2, NULL);
  // If we timed out and it's a profile command, we want to get the profile data
  if (timedout && cmd->forProfiling) {
    RS_LOG_ASSERT(!cmd->forCursor, "profile is not supported on a cursor command");
    newCmd = MR_NewCommand(4, "_FT.CURSOR", "PROFILE", idx, buf);
    // Internally we delete the cursor
    newCmd.rootCommand = C_PROFILE;
  // If we timed out and not in cursor mode, we want to send the shard a DEL
  // command instead of a READ command (here we know it has more results)
  } else if (timedout && !cmd->forCursor) {
    newCmd = MR_NewCommand(4, "_FT.CURSOR", "DEL", idx, buf);
    // Mark that the last command was a DEL command
    newCmd.rootCommand = C_DEL;
  } else {
    newCmd = MR_NewCommand(4, "_FT.CURSOR", "READ", idx, buf);
    newCmd.rootCommand = C_READ;
  }

  newCmd.targetShard = cmd->targetShard;
  newCmd.targetSlot = cmd->targetSlot;
  newCmd.protocol = cmd->protocol;
  newCmd.forCursor = cmd->forCursor;
  newCmd.forProfiling = cmd->forProfiling;
  MRCommand_Free(cmd);
  *cmd = newCmd;

  return true;
}
