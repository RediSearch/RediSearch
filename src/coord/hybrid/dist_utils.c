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

  // Normal reply from the shard.
  // In any case, the cursor id is the second element in the reply
  RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 1)) == MR_REPLY_INTEGER);
  long long cursorId = MRReply_Integer(MRReply_ArrayElement(rep, 1));

  // Assert that the reply is in the expected format.
#ifdef ENABLE_ASSERT
  if (cmd->protocol == 3) {
    // RESP3 reply structure:
    // [map, cursor] - map contains the results, cursor is the next cursor id
    RS_ASSERT(MRReply_Type(rep) == MR_REPLY_ARRAY);
    RS_ASSERT(MRReply_Length(rep) == 2);
    RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 0)) == MR_REPLY_MAP);
    RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 1)) == MR_REPLY_INTEGER);
    MRReply *map = MRReply_ArrayElement(rep, 0);
    MRReply *Results = MRReply_MapElement(map, "Results");

    if (cmd->forProfiling) {
      // If the command is for profiling, the map at index 0 contains 2 elements:
      // 1. "results" - the results of the command
      // 2. "Profile" - the profile reply, if this is the last reply from this shard
      // If this is the last reply from this shard, the profile reply should set, otherwise it should be NULL
      RS_ASSERT(Results != NULL); // Query reply, nested
      RS_ASSERT(MRReply_Type(Results) == MR_REPLY_MAP);
      RS_ASSERT(MRReply_MapElement(Results, "results") != NULL); // Actual reply results
      if (cursorId == CURSOR_EOF) {
        RS_ASSERT(MRReply_Length(map) == 4); // 2 elements in the map, key and value
        RS_ASSERT(MRReply_MapElement(map, "Profile") != NULL);
        RS_ASSERT(MRReply_Type(MRReply_MapElement(map, "Profile")) == MR_REPLY_MAP);
      } else {
        RS_ASSERT(MRReply_Length(map) == 2); // 1 element in the map, key and value
        RS_ASSERT(MRReply_MapElement(map, "Profile") == NULL); // No profile reply, as this is not the last reply from this shard
      }
    } else {
      // If the command is not for profiling, the map at index 0 is the query reply
      // and contains the results of the command, and additional metadata.
      RS_ASSERT(Results != NULL);
    }
  } else {
    // RESP2 reply structure:
    // [results, cursor] or [results, cursor, profile]
    // results is an array of results, cursor is the next cursor id, and profile is
    // an optional profile reply (if the command was for profiling).
    if (cmd->forProfiling) {
      // If the command is for profiling, the reply should contain 3 elements:
      // [results, cursor, profile]
      RS_ASSERT(MRReply_Length(rep) == 3);
      RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 0)) == MR_REPLY_ARRAY);
      RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 1)) == MR_REPLY_INTEGER);
      // If this is the last reply from this shard, the profile reply should be set, otherwise it should be NULL
      if (cursorId == CURSOR_EOF) {
        RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 2)) == MR_REPLY_ARRAY);
      } else {
        RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 2)) == MR_REPLY_NIL);
      }
    } else {
      // If the command is not for profiling, the reply should contain 2 elements:
      // [results, cursor]
      RS_ASSERT(MRReply_Length(rep) == 2);
      RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 0)) == MR_REPLY_ARRAY);
      RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 1)) == MR_REPLY_INTEGER);
    }
  }
#endif // Reply structure assertions

  if (cmd->forProfiling && cmd->protocol == 3) {
    RS_LOG_ASSERT(!cmd->forCursor, "Profiling is not supported on a cursor command");
    MRReply *rows = NULL, *meta = NULL;
    meta = MRReply_ArrayElement(rep, 0);
    meta = MRReply_MapElement(meta, "results");  // profile has an extra level

    // Check if we got timeout
    MRReply *warning = MRReply_MapElement(meta, "warning");
    if (MRReply_Length(warning) > 0) {
      const char *warning_str = MRReply_String(MRReply_ArrayElement(warning, 0), NULL);
      // Set an error to be later picked up by `getCursorCommand`
      if (!strcmp(warning_str, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT))) {
        // When a shard returns timeout on RETURN policy, the profile is not returned.
        // We set the timeout here so in the next getCursorCommand we will send CURSOR PROFILE
        MRIteratorCallback_SetTimedOut(MRIteratorCallback_GetCtx(ctx));
      }
    }
  }

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

// Get cursor command using a cursor id and an existing aggregate command
// Returns true if the cursor is not done (i.e., not depleted)
bool getCursorCommand(long long cursorId, MRCommand *cmd, MRIteratorCtx *ctx) {
  if (cursorId == CURSOR_EOF) {
    // Cursor was set to 0, end of reply chain. cmd->depleted will be set in `MRIteratorCallback_Done`.
    return false;
  }

  RS_LOG_ASSERT(cmd->num >= 2, "Invalid command?!");

  // Check if the coordinator experienced a timeout or not
  bool timedout = MRIteratorCallback_GetTimedOut(ctx);

  if (cmd->rootCommand == C_AGG) {
    MRCommand newCmd;
    char buf[24]; // enough digits for a long long
    sprintf(buf, "%lld", cursorId);
    // AGGREGATE commands has the index name at position 1
    const char *idx = MRCommand_ArgStringPtrLen(cmd, 1, NULL);
    // If we timed out and not in cursor mode, we want to send the shard a DEL
    // command instead of a READ command (here we know it has more results)
    if (timedout && cmd->forProfiling) {
      newCmd = MR_NewCommand(4, "_FT.CURSOR", "PROFILE", idx, buf);
      // Internally we delete the cursor
      newCmd.rootCommand = C_PROFILE;
    } else if (timedout && !cmd->forCursor) {
      newCmd = MR_NewCommand(4, "_FT.CURSOR", "DEL", idx, buf);
      // Mark that the last command was a DEL command
      newCmd.rootCommand = C_DEL;
    } else {
      newCmd = MR_NewCommand(4, "_FT.CURSOR", "READ", idx, buf);
      newCmd.rootCommand = C_READ;
    }

    newCmd.targetShard = cmd->targetShard;
    newCmd.protocol = cmd->protocol;
    newCmd.forCursor = cmd->forCursor;
    newCmd.forProfiling = cmd->forProfiling;
    MRCommand_Free(cmd);
    *cmd = newCmd;

  } else {
    // The previous command was a _FT.CURSOR READ command, so we may not need to change anything.
    RS_LOG_ASSERT(cmd->rootCommand == C_READ, "calling `getCursorCommand` after a DEL command");
    RS_ASSERT(cmd->num == 4);
    RS_ASSERT(STR_EQ(cmd->strs[0], cmd->lens[0], "_FT.CURSOR"));
    RS_ASSERT(STR_EQ(cmd->strs[1], cmd->lens[1], "READ"));
    RS_ASSERT(atoll(cmd->strs[3]) == cursorId);

    if (timedout) {
      // We are going to modify the command, so we need to free the cached sds
      if (cmd->cmd) {
        sdsfree(cmd->cmd);
        cmd->cmd = NULL;
      }
      // If we timed out and it's a profile command, we want to get the profile data
      if (cmd->forProfiling) {
        RS_LOG_ASSERT(!cmd->forCursor, "profile is not supported on a cursor command");
        MRCommand_ReplaceArg(cmd, 1, "PROFILE", strlen("PROFILE"));
        cmd->rootCommand = C_PROFILE;
      } else if (!cmd->forCursor) {
        // If we timed out and not in cursor mode, we want to send the shard a DEL
        // command instead of a READ command (here we know it has more results)
        MRCommand_ReplaceArg(cmd, 1, "DEL", 3);
        cmd->rootCommand = C_DEL;
      }
    }
  }

  if (timedout && cmd->forCursor) {
    // Reset the `timedOut` value in case it was set (for next iterations, as
    // we're in cursor mode)
    MRIteratorCallback_ResetTimedOut(ctx);
  }

  return true;
}
