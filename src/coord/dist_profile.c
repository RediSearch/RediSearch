#include "dist_profile.h"
#include "rmutil/util.h"
#include "../profile/profile.h"

int ParseProfile(ArgsCursor *ac, QueryError *status, ProfileOptions *options) {
  // Profile args
  *options = EXEC_NO_FLAGS;
  if (AC_AdvanceIfMatch(ac, "FT.PROFILE")) {
    *options |= EXEC_WITH_PROFILE;
    // advance past index name and command type
    if (AC_AdvanceBy(ac, 2) != AC_OK) {
      QueryError_SetError(status, QUERY_EPARSEARGS, "No index name and/or command type provided");
      return REDISMODULE_ERR;
    } 
    if (AC_AdvanceIfMatch(ac, "LIMITED")) {
        *options |= EXEC_WITH_PROFILE_LIMITED;
    }
    if (!AC_AdvanceIfMatch(ac, "QUERY")) {
      QueryError_SetError(status, QUERY_EPARSEARGS, "No QUERY keyword provided");
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;
}

/**
 * This function is used to print profiles received from the shards.
 * It is used by both SEARCH and AGGREGATE.
 */
static void PrintShardProfile_resp2(RedisModule_Reply *reply, int count, MRReply **replies, bool isSearch) {
  // On FT.SEARCH, `replies` is an array of replies from the shards.
  // On FT.AGGREGATE, `replies` is already the profile part only
  for (int i = 0; i < count; ++i) {
    MRReply *current = replies[i];
    // Check if reply is error
    if (MRReply_Type(current) == MR_REPLY_ERROR) {
      MR_ReplyWithMRReply(reply, current);
      continue;
    }
    if (isSearch) {
      // On FT.SEARCH, extract the profile information from the reply. (should be the second element)
      current = MRReply_ArrayElement(current, 1);
    }
    MRReply *shards_array_profile = MRReply_ArrayElement(current, 1);
    MRReply *shard_profile = MRReply_ArrayElement(shards_array_profile, 0);
    MR_ReplyWithMRReply(reply, shard_profile);
  }
}

static void PrintShardProfile_resp3(RedisModule_Reply *reply, int count, MRReply **replies, bool isSearch) {
  for (int i = 0; i < count; ++i) {
    MRReply *current = replies[i];
    // Check if reply is error
    if (MRReply_Type(current) == MR_REPLY_ERROR) {
      MR_ReplyWithMRReply(reply, current);
      continue;
    }
    if (isSearch) { // On aggregate commands, we get the profile info directly.
      current = MRReply_MapElement(current, PROFILE_STR);
    }
    MRReply *shards = MRReply_MapElement(current, PROFILE_SHARDS_STR);
    MRReply *shard = MRReply_ArrayElement(shards, 0);

    MR_ReplyWithMRReply(reply, shard);
  }
}

void PrintShardProfile(RedisModule_Reply *reply, void *ctx) {
  PrintShardProfile_ctx *pCtx = ctx;
  if (reply->resp3) {
    PrintShardProfile_resp3(reply, pCtx->count, pCtx->replies, pCtx->isSearch);
  } else {
    PrintShardProfile_resp2(reply, pCtx->count, pCtx->replies, pCtx->isSearch);
  }
}
