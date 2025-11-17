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
#include "coord/hybrid/dist_utils.h"


#define CURSOR_EOF 0


static RSValue *MRReply_ToValue(MRReply *r) {
  if (!r) return RSValue_NullStatic();
  RSValue *v = NULL;
  switch (MRReply_Type(r)) {
    case MR_REPLY_STATUS:
    case MR_REPLY_STRING: {
      size_t l;
      const char *s = MRReply_String(r, &l);
      v = RSValue_NewCopiedString(s, l);
      break;
    }
    case MR_REPLY_ERROR: {
      double d = 42;
      MRReply_ToDouble(r, &d);
      v = RSValue_NewNumber(d);
      break;
    }
    case MR_REPLY_INTEGER:
      v = RSValue_NewNumber((double)MRReply_Integer(r));
      break;
    case MR_REPLY_DOUBLE:
      v = RSValue_NewNumber(MRReply_Double(r));
      break;
    case MR_REPLY_MAP: {
      size_t n = MRReply_Length(r);
      RS_LOG_ASSERT(n % 2 == 0, "map of odd length");
      size_t map_len = n / 2;
      RSValueMap map = RSValueMap_AllocUninit(map_len);
      for (size_t i = 0; i < map_len; i++) {
        MRReply *e_k = MRReply_ArrayElement(r, i * 2);
        RS_LOG_ASSERT(MRReply_Type(e_k) == MR_REPLY_STRING, "non-string map key");
        MRReply *e_v = MRReply_ArrayElement(r, (i * 2) + 1);
        RSValueMap_SetEntry(&map, i,  MRReply_ToValue(e_k), MRReply_ToValue(e_v));
      }
      v = RSValue_NewMap(map);
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
      v = RSValue_NullStatic();
      break;
    default:
      v = RSValue_NullStatic();
      break;
  }
  return v;
}


int getNextReply(RPNet *nc) {
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

  if (root == NULL) {
    // No more replies
    RPNet_resetCurrent(nc);
    return MRIterator_GetPending(nc->it);
  }

  // Check if an error was returned
  if(MRReply_Type(root) == MR_REPLY_ERROR) {
    nc->current.root = root;
    // If for profiling, clone and append the error
    if (nc->cmd.forProfiling) {
      // Clone the error and append it to the profile
      MRReply *error = MRReply_Clone(root);
      array_append(nc->shardsProfile, error);
    }
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

    size_t idx_len;
    const char *idx = MRCommand_ArgStringPtrLen(&nc->cmd, 1, &idx_len);
    char *idx_copy = rm_strndup(idx, idx_len);
    MRCommand_Free(&nc->cmd);

    // Create cursor read command using the copied index name
    nc->cmd = MR_NewCommand(3, "_FT.CURSOR", "READ", idx_copy);
    nc->cmd.rootCommand = C_READ;
    nc->cmd.protocol = 3;
    rm_free(idx_copy);

    nc->it = MR_IterateWithPrivateData(&nc->cmd, netCursorCallback, NULL, iterCursorMappingCb, &nc->mappings);
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
            if (!strcmp(warning_str, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT))) {
              timed_out = true;
            } else if (!strcmp(warning_str, QUERY_WMAXPREFIXEXPANSIONS)) {
              QueryError_SetReachedMaxPrefixExpansionsWarning(AREQ_QueryProcessingCtx(nc->areq)->err);
            } else if (!strcmp(warning_str, QUERY_WOOM_CLUSTER)) {
              QueryError_SetQueryOOMWarning(AREQ_QueryProcessingCtx(nc->areq)->err);
            }
            if (!strcmp(warning_str, QUERY_WINDEXING_FAILURE)) {
              AREQ_QueryProcessingCtx(nc->areq)->bgScanOOM = true;
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
    if (TimedOut(&nc->areq->sctx->time.timeout)) {
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
      QueryErrorCode errCode = QueryError_GetCodeFromMessage(MRReply_String(nc->current.root, NULL));
      // TODO - use should_return_error after it is changed to support RequestConfig ptr
      if (errCode == QUERY_ERROR_CODE_GENERIC ||
          ((errCode == QUERY_ERROR_CODE_TIMED_OUT) && nc -> areq -> reqConfig.timeoutPolicy == TimeoutPolicy_Fail) ||
          ((errCode == QUERY_ERROR_CODE_OUT_OF_MEMORY) && nc -> areq -> reqConfig.oomPolicy == OomPolicy_Fail)) {
        // We need to pass the reply string as the error message, since the error code might be generic
        QueryError_SetError(AREQ_QueryProcessingCtx(nc->areq)->err, errCode,  MRReply_String(nc->current.root, NULL));
        return RS_RESULT_ERROR;
      } else {
        // Handle shards returning error unexpectedly
        // Might be from different Timeout/OOM policy (See MOD-10774)
        // Free the error reply before we override it and continue
        MRReply_Free(nc->current.root);
        // Set it as NULL avoid another free
        nc->current.root = NULL;
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

  MRReply *score = NULL;
  MRReply *fields = MRReply_ArrayElement(rows, nc->curIdx++);
  if (resp3) {
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_MAP, "invalid result record");
    // extract score if it exists, WITHSCORES was specified
    score = MRReply_MapElement(fields, "score");
    fields = MRReply_MapElement(fields, "extra_attributes");
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_MAP, "invalid fields record");
  } else {
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_ARRAY, "invalid result record");
    RS_LOG_ASSERT(MRReply_Length(fields) % 2 == 0, "invalid fields record");
  }

  // The score is optional, in hybrid we need the score for the sorter and hybrid merger
  // We expect for it to exist in hybrid since we send WITHSCORES to the shard and we should use resp3
  // when opening shard connections
  if (score) {
    RS_LOG_ASSERT(MRReply_Type(score) == MR_REPLY_DOUBLE, "invalid score record");
    SearchResult_SetScore(r, MRReply_Double(score));
  }

  for (size_t i = 0; i < MRReply_Length(fields); i += 2) {
    size_t len;
    const char *field = MRReply_String(MRReply_ArrayElement(fields, i), &len);
    MRReply *val = MRReply_ArrayElement(fields, i + 1);
    RSValue *v = MRReply_ToValue(val);
    RLookup_WriteOwnKeyByName(nc->lookup, field, len, SearchResult_GetRowDataMut(r), v);
  }
  return RS_RESULT_OK;
}

int rpnetNext_EOF(ResultProcessor *self, SearchResult *r) {
  return RS_RESULT_EOF;
}
