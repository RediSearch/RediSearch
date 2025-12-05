/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "coord/src/dist_utils.h"
#include "util/misc.h"
#include "util/strconv.h"
#include "rpnet.h"

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
    cursorId = 0;
  }

  if (cmd->forProfiling && cmd->protocol == 3) {
    RS_LOG_ASSERT(!cmd->forCursor, "Profiling is not supported on a cursor command");
    MRReply *rows = NULL, *meta = NULL;
    rows = MRReply_ArrayElement(rep, 0);

    // Check if we got timeout
    MRReply *warning = MRReply_MapElement(rows, "warning");
    if (MRReply_Length(warning) > 0) {
      const char *warning_str = MRReply_String(MRReply_ArrayElement(warning, 0), NULL);
      // Set an error to be later picked up by `getCursorCommand`
      if (!strcmp(warning_str, QueryError_Strerror(QUERY_ETIMEDOUT))) {
        // When a shard returns timeout on RETURN policy, the profile is not returned.
        // We set the timeout here so in the next getCursorCommand we will send CURSOR PROFILE
        MRIteratorCallback_SetTimedOut(MRIteratorCallback_GetCtx(ctx));
      }
    }
  }

  // Push the reply down the chain
  if (isResp3) // RESP3
  {
    MRReply *map = MRReply_ArrayElement(rep, 0);
    MRReply *results = NULL;
    if (map && MRReply_Type(map) == MR_REPLY_MAP) {
      results = MRReply_MapElement(map, "results");
      if (results && MRReply_Type(results) == MR_REPLY_ARRAY) {
        MRIteratorCallback_AddReply(ctx, rep); // to be picked up by getNextReply
        // User code now owns the reply, so we can't free it here ourselves!
        rep = NULL;
      }
    }
  }
  else // RESP2
  {
    MRReply *results = MRReply_ArrayElement(rep, 0);
    if (results && MRReply_Type(results) == MR_REPLY_ARRAY && MRReply_Length(results) >= 1) {
      MRIteratorCallback_AddReply(ctx, rep); // to be picked up by getNextReply
      // User code now owns the reply, so we can't free it here ourselves!
      rep = NULL;
    }
  }

  // rewrite and resend the cursor command if needed
  // should only be determined based on the cursor and not on the set of results we get
  if (!getCursorCommand(cursorId, cmd, MRIteratorCallback_GetCtx(ctx))) {
    MRIteratorCallback_Done(ctx, 0);
  } else if (cmd->forCursor) {
    MRIteratorCallback_ProcessDone(ctx);
  } else if (MRIteratorCallback_ResendCommand(ctx) == REDIS_ERR) {
    MRIteratorCallback_Done(ctx, 1);
  }

  if (rep != NULL) {
    // If rep has been set to NULL, it means the callback has been invoked
    MRReply_Free(rep);
  }
}

// Get cursor command using a cursor id and an existing aggregate command
// Returns true if the cursor is not done (i.e., not depleted)
bool getCursorCommand(long long cursorId, MRCommand *cmd, MRIteratorCtx *ctx) {
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

  if(timedout && cmd->forCursor) {
    // Reset the `timedOut` value in case it was set (for next iterations, as
    // we're in cursor mode)
    MRIteratorCallback_ResetTimedOut(ctx);
  }

  newCmd.targetSlot = cmd->targetSlot;
  newCmd.protocol = cmd->protocol;
  newCmd.forCursor = cmd->forCursor;
  newCmd.forProfiling = cmd->forProfiling;
  MRCommand_Free(cmd);
  *cmd = newCmd;

  return true;
}
