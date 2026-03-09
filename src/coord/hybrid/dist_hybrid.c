/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "dist_hybrid.h"
#include "hybrid/hybrid_request.h"
#include "hybrid/hybrid_exec.h"
#include "hybrid/dist_hybrid_plan.h"
#include "hybrid/parse_hybrid.h"
#include "dist_plan.h"
#include "rmr/rmr.h"
#include "rmutil/util.h"
#include "commands.h"
#include "rpnet.h"
#include "hybrid_cursor_mappings.h"
#include "info/global_stats.h"
#include "profile/profile.h"
#include "dist_profile.h"
#include "shard_window_ratio.h"
#include "config.h"
#include "coord/coord_request_ctx.h"

// We mainly need the resp protocol to be three in order to easily extract the "score" key from the response
#define HYBRID_RESP_PROTOCOL_VERSION 3

/**
 * Appends all SEARCH-related arguments to MR command.
 * This includes SEARCH keyword, query, and optional SCORER and YIELD_SCORE_AS parameters
 * that come immediately after the query in sequence.
 *
 * @param xcmd - destination MR command to append arguments to
 * @param argv - source command arguments array
 * @param argc - total argument count
 * @param searchOffset - offset where SEARCH keyword appears
 */
static void MRCommand_appendSearch(MRCommand *xcmd, RedisModuleString **argv, int argc, int searchOffset) {
  // Add SEARCH keyword and query
  MRCommand_AppendRstr(xcmd, argv[searchOffset]);     // SEARCH
  MRCommand_AppendRstr(xcmd, argv[searchOffset + 1]); // query

  // Process optional parameters sequentially right after the query
  int currentOffset = searchOffset + 2; // Start after SEARCH "query"

  // Process SCORER and YIELD_SCORE_AS in any order, but they must be sequential
  while (currentOffset < argc) {
    const char *argStr = RedisModule_StringPtrLen(argv[currentOffset], NULL);

    if (strcmp(argStr, "SCORER") == 0 && currentOffset < argc - 1) {
      // Found SCORER - append it and its argument
      MRCommand_AppendRstr(xcmd, argv[currentOffset]);     // SCORER
      MRCommand_AppendRstr(xcmd, argv[currentOffset + 1]); // scorer name
      currentOffset += 2;
    } else if (strcmp(argStr, "YIELD_SCORE_AS") == 0 && currentOffset < argc - 1) {
      // Found YIELD_SCORE_AS - append it and its argument
      MRCommand_AppendRstr(xcmd, argv[currentOffset]);     // YIELD_SCORE_AS
      MRCommand_AppendRstr(xcmd, argv[currentOffset + 1]); // score alias
      currentOffset += 2;
    } else {
      // Not a SEARCH parameter - we've reached the end of SEARCH section
      break;
    }
  }
}

/**
 * Appends VSIM FILTER arguments to MR command.
 * This includes FILTER keyword, filter expression, and optional POLICY and BATCH_SIZE parameters.
 *
 * @param xcmd - destination MR command to append arguments to
 * @param argv - source command arguments array
 * @param argc - total argument count
 * @param actualFilterOffset - offset where FILTER keyword appears
 * @return number of tokens parsed/appended
 */
static int MRCommand_appendVsimFilter(MRCommand *xcmd, RedisModuleString **argv, int argc,
                                      int actualFilterOffset) {
  // This is a VSIM FILTER - append it to the command
  // Format: FILTER [count] <expression>...
  // If count is present, append FILTER, count, and the next count tokens
  // If count is not present, append FILTER and the filter-expression

  MRCommand_AppendRstr(xcmd, argv[actualFilterOffset]);     // FILTER keyword

  // Check if the next token is an unsigned integer (count)
  if (actualFilterOffset + 1 >= argc) {
    return 1; // Only FILTER keyword, no more tokens
  }

  unsigned long long count = 0;
  int isCount = (RedisModule_StringToULongLong(argv[actualFilterOffset + 1], &count) == REDISMODULE_OK);

  if (isCount) {
    // Format: FILTER count <expression>... (count tokens)
    MRCommand_AppendRstr(xcmd, argv[actualFilterOffset + 1]); // count
    int tokensAppended = 2; // FILTER + count

    // Append the next count tokens
    for (unsigned long long i = 0; i < count && actualFilterOffset + tokensAppended < argc; i++) {
      MRCommand_AppendRstr(xcmd, argv[actualFilterOffset + tokensAppended]);
      tokensAppended++;
    }
    return tokensAppended;
  } else {
    // Format: FILTER <filter-expression>, for backward compatibility
    MRCommand_AppendRstr(xcmd, argv[actualFilterOffset + 1]); // filter expression
    return 2; // FILTER + filter-expression
  }
}

/**
 * Appends all VSIM-related arguments to MR command.
 * This includes VSIM keyword, field, vector, KNN/RANGE method, and VSIM FILTER
 * if present.
 *
 * SHARD_K_RATIO is only valid for KNN queries, but this is validated during
 * parsing - no validation is done here.
 *
 * @param xcmd - destination MR command to append arguments to
 * @param argv - source command arguments array
 * @param argc - total argument count
 * @param vsimOffset - offset where VSIM keyword appears
 * @param kArgIndex - output parameter for the index of the K value argument in
 *        the built command (set to -1 if no KNN K argument found). Can be NULL.
 */
static void MRCommand_appendVsim(MRCommand *xcmd, RedisModuleString **argv,
                                 int argc, int vsimOffset, int *kArgIndex) {
  RS_LOG_ASSERT(kArgIndex, "kArgIndex must not be NULL");

  // Initialize output parameter
  *kArgIndex = -1;

  // Add VSIM keyword and field
  MRCommand_AppendRstr(xcmd, argv[vsimOffset]);     // VSIM
  MRCommand_AppendRstr(xcmd, argv[vsimOffset + 1]); // field

  // Add vector data (handle parameter placeholders vs raw data)
  size_t paramLen;
  const char *paramStr = RedisModule_StringPtrLen(argv[vsimOffset + 2], &paramLen);
  if (paramLen > 0 && paramStr[0] == '$') {
    // It's a parameter placeholder - forward as is
    MRCommand_AppendRstr(xcmd, argv[vsimOffset + 2]);
  } else {
    // It's raw data - forward as binary
    MRCommand_Append(xcmd, paramStr, paramLen);
  }

  // Find and add KNN/RANGE method and its arguments
  long long methodNargs = 0;
  int vectorMethodOffset = -1;

  int argOffset = RMUtil_ArgIndex("KNN", argv + vsimOffset, argc - vsimOffset);
  if (argOffset == -1) {
    argOffset = RMUtil_ArgIndex("RANGE", argv + vsimOffset, argc - vsimOffset);
  }

  if (argOffset != -1) {
    vectorMethodOffset = argOffset + vsimOffset;
    if (vectorMethodOffset < argc - 2) {
      RedisModule_StringToLongLong(argv[vectorMethodOffset + 1], &methodNargs);

      // Append method name (KNN/RANGE) and argument count
      MRCommand_AppendRstr(xcmd, argv[vectorMethodOffset]);     // KNN or RANGE
      MRCommand_AppendRstr(xcmd, argv[vectorMethodOffset + 1]); // argument count

      // Append method arguments
      // Format for KNN: KNN <count> K <value> [EF_RUNTIME <value>]...
      for (int i = 2; i < methodNargs + 2; ++i) {
        size_t argLen;
        const char *argStr = RedisModule_StringPtrLen(argv[vectorMethodOffset + i], &argLen);
        bool kFound = (argLen == 1 && strncasecmp(argStr, "K", 1) == 0);

        if (*kArgIndex == -1 && kFound && i + 1 < methodNargs + 2) {
          // Found K keyword - append it and record position of the K value
          MRCommand_AppendRstr(xcmd, argv[vectorMethodOffset + i]);  // K keyword
          ++i;  // Move to K value
          *kArgIndex = xcmd->num;  // Record position where K value will be appended
          MRCommand_AppendRstr(xcmd, argv[vectorMethodOffset + i]);  // K value
        } else {
          // Regular argument - append as-is
          MRCommand_AppendRstr(xcmd, argv[vectorMethodOffset + i]);
        }
      }
    }
  }

  // Add VSIM FILTER if present at expected position
  // Format: VSIM <field> <vector> [KNN/RANGE <count> <args...>] [FILTER <expression> [[POLICY ADHOC/BATCHES] [BATCH_SIZE <value>]]]
  int expectedFilterOffset = vsimOffset + 3; // VSIM + field + vector
  if (vectorMethodOffset != -1) {
    expectedFilterOffset += 2 + methodNargs; // method + count + args
  }

  int actualFilterOffset = RMUtil_ArgIndex("FILTER", argv + vsimOffset, argc - vsimOffset);
  actualFilterOffset = actualFilterOffset != -1 ? actualFilterOffset + vsimOffset : -1;
  int tokensAppended = 0;

  if (actualFilterOffset == expectedFilterOffset && actualFilterOffset < argc - 1) {
    tokensAppended = MRCommand_appendVsimFilter(xcmd, argv, argc, actualFilterOffset);
  }

  // Add YIELD_SCORE_AS if present
  // Format: ... [FILTER count <expression> [[POLICY ADHOC/BATCHES] [BATCH_SIZE <value>]]] YIELD_SCORE_AS <alias>
  int yieldScoreOffset = RMUtil_ArgIndex("YIELD_SCORE_AS", argv + vsimOffset, argc - vsimOffset);
  yieldScoreOffset = yieldScoreOffset != -1 ? yieldScoreOffset + vsimOffset : -1;

  // Calculate expected position: base it on actualFilterOffset (zero-based from FILTER) if present, otherwise expectedFilterOffset
  int expectedYieldScoreOffset = (actualFilterOffset == expectedFilterOffset)
                                  ? actualFilterOffset + tokensAppended
                                  : expectedFilterOffset;

  if (yieldScoreOffset == expectedYieldScoreOffset && yieldScoreOffset < argc - 1) {
    // This is a VSIM YIELD_SCORE_AS - append it to the command
    MRCommand_AppendRstr(xcmd, argv[yieldScoreOffset]);     // YIELD_SCORE_AS keyword
    MRCommand_AppendRstr(xcmd, argv[yieldScoreOffset + 1]); // score alias
  }
}

// The function transforms FT.HYBRID index SEARCH query VSIM field vector
// into _FT.HYBRID index SEARCH query VSIM field vector WITHCURSOR
// _NUM_SSTRING _INDEX_PREFIXES ...
void HybridRequest_buildMRCommand(RedisModuleString **argv, int argc,
                            ProfileOptions profileOptions,
                            MRCommand *xcmd, arrayof(char*) serialized,
                            IndexSpec *sp, const VectorQuery *vq,
                            size_t numShards) {
  int argOffset;
  const char *index_name = RedisModule_StringPtrLen(argv[1], NULL);

  int cmdArgCount = 2;
  const char *cmdArgs[5] = {"_FT.HYBRID", index_name};

  if (profileOptions != EXEC_NO_FLAGS) {
    cmdArgs[0] = "_FT.PROFILE";
    cmdArgs[cmdArgCount++] = "HYBRID";
    if (profileOptions & EXEC_WITH_PROFILE_LIMITED) {
      cmdArgs[cmdArgCount++] = "LIMITED";
    }
    cmdArgs[cmdArgCount++] = "QUERY";
  }
  *xcmd = MR_NewCommandArgv(cmdArgCount, cmdArgs);

  // Add all SEARCH-related arguments (SEARCH, query, optional SCORER, YIELD_SCORE_AS)
  int searchOffset = RMUtil_ArgIndex("SEARCH", argv, argc);
  MRCommand_appendSearch(xcmd, argv, argc, searchOffset);

  // Add all VSIM-related arguments (VSIM, field, vector, methods, filter)
  int vsimOffset = RMUtil_ArgIndex("VSIM", argv, argc);
  int kArgIndex = -1;
  MRCommand_appendVsim(xcmd, argv, argc, vsimOffset, &kArgIndex);

  // Calculate and apply effective K for KNN queries if SHARD_K_RATIO is set
  if (vq && vq->type == VECSIM_QT_KNN) {
    double shardWindowRatio = vq->knn.shardWindowRatio;
    if (shardWindowRatio < MAX_SHARD_WINDOW_RATIO && numShards > 1) {
      size_t effectiveK = calculateEffectiveK(vq->knn.k, shardWindowRatio, numShards);
      modifyVsimKNN(xcmd, kArgIndex, effectiveK, vq->knn.k);
    }
  }

  int combineOffset = RMUtil_ArgIndex("COMBINE", argv + vsimOffset, argc - vsimOffset);
  combineOffset = combineOffset != -1 ? combineOffset + vsimOffset : -1;
  bool hasCombine = combineOffset != -1;

  // Add COMBINE
  if (combineOffset != -1) {
    MRCommand_AppendRstr(xcmd, argv[combineOffset]);
    // Add RRF/LINEAR and its arguments
    argOffset = RMUtil_ArgIndex("RRF", argv + vsimOffset, argc - vsimOffset);
    if (argOffset == -1) {
      argOffset = RMUtil_ArgIndex("LINEAR", argv + vsimOffset, argc - vsimOffset);
    }
    argOffset = argOffset != -1 ? argOffset + vsimOffset : -1;
    if (argOffset != -1 && argOffset < argc - 2) {
      long long nargs;
      RedisModule_StringToLongLong(argv[argOffset + 1], &nargs);

      for (int i = 0; i < nargs + 2; ++i) {
        MRCommand_AppendRstr(xcmd, argv[argOffset + i]);
      }
    }
  }

  if (serialized) {
    for (size_t ii = 0; ii < array_len(serialized); ++ii) {
      const char *token = serialized[ii];
      MRCommand_Append(xcmd, token, strlen(token));
    }
  }

  // Add PARAMS arguments if present
  argOffset = RMUtil_ArgIndex("PARAMS", argv + vsimOffset, argc - vsimOffset);
  argOffset = argOffset != -1 ? argOffset + vsimOffset : -1;
  if (argOffset != -1) {
    long long nargs;
    int rc = RedisModule_StringToLongLong(argv[argOffset + 1], &nargs);

    // PARAMS keyword and count - treat as string
    MRCommand_AppendRstr(xcmd, argv[argOffset]);
    MRCommand_AppendRstr(xcmd, argv[argOffset + 1]);

    // append params string including PARAMS keyword and nargs
    for (int i = 2; i < nargs + 2; ++i) {
      if (i % 2 == 0) {
        // Parameter name - treat as string
        MRCommand_AppendRstr(xcmd, argv[argOffset + i]);
      } else {
        // Parameter value - could be binary, treat as binary
        size_t valueLen;
        const char *valueData = RedisModule_StringPtrLen(argv[argOffset + i], &valueLen);
        MRCommand_Append(xcmd, valueData, valueLen);
      }
    }
  }

  // check for timeout argument and append it to the command.
  // If TIMEOUT exists, it was already validated at AREQ_Compile.
  argOffset = RMUtil_ArgIndex("TIMEOUT", argv, argc);
  if (argOffset != -1) {
    MRCommand_AppendRstr(xcmd, argv[argOffset]);
    MRCommand_AppendRstr(xcmd, argv[argOffset + 1]);
  }

  // Add DIALECT arguments if present
  argOffset = RMUtil_ArgIndex("DIALECT", argv, argc);
  if (argOffset != -1) {
    MRCommand_AppendRstr(xcmd, argv[argOffset]);
    MRCommand_AppendRstr(xcmd, argv[argOffset + 1]);
  }

  // Add WITHCURSOR
  MRCommand_Append(xcmd, "WITHCURSOR", strlen("WITHCURSOR"));

  MRCommand_Append(xcmd, "WITHSCORES", strlen("WITHSCORES"));
  // Numeric responses are encoded as simple strings.
  MRCommand_Append(xcmd, "_NUM_SSTRING", strlen("_NUM_SSTRING"));

  // Prepare command for slot info (Cluster mode)
  MRCommand_PrepareForSlotInfo(xcmd, xcmd->num);

  // Prepare placeholder for dispatch time (will be filled in when sending to shards)
  MRCommand_PrepareForDispatchTime(xcmd, xcmd->num);

  if (sp && sp->rule && sp->rule->prefixes && array_len(sp->rule->prefixes) > 0) {
    MRCommand_Append(xcmd, "_INDEX_PREFIXES", strlen("_INDEX_PREFIXES"));
    arrayof(HiddenUnicodeString*) prefixes = sp->rule->prefixes;
    char *n_prefixes;
    rm_asprintf(&n_prefixes, "%u", array_len(prefixes));
    MRCommand_Append(xcmd, n_prefixes, strlen(n_prefixes));
    rm_free(n_prefixes);

    for (uint32_t i = 0; i < array_len(prefixes); i++) {
      size_t len;
      const char* prefix = HiddenUnicodeString_GetUnsafe(prefixes[i], &len);
      MRCommand_Append(xcmd, prefix, len);
    }
  }
}

// UPDATED: Set RPNet types when creating them
// NOTE: Caller should clone the dispatcher_ref before calling this function
static void HybridRequest_buildDistRPChain(AREQ *r, MRCommand *xcmd,
                          RLookup *lookup,
                          int (*nextFunc)(ResultProcessor *, SearchResult *)) {
  // Establish our root processor, which is the distributed processor
  MRCommand cmd = MRCommand_Copy(xcmd);

  RPNet *rpRoot = RPNet_New(&cmd, nextFunc);

  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(r);
  rpRoot->base.parent = qctx;
  rpRoot->lookup = lookup;
  rpRoot->areq = r;

  ResultProcessor *rpProfile = NULL;
  if (IsProfile(r)) {
    rpProfile = RPProfile_New(&rpRoot->base, qctx);
  }

  // RS_ASSERT(!AREQ_QueryProcessingCtx(r)->rootProc);
  // Get the deepest-most root:
  int found = 0;
  for (ResultProcessor *rp = AREQ_QueryProcessingCtx(r)->endProc; rp; rp = rp->upstream) {
    if (!rp->upstream) {
      rp->upstream = IsProfile(r) ? rpProfile : &rpRoot->base;
      found = 1;
      break;
    }
  }

  // update root and end with RPNet
  qctx->rootProc = &rpRoot->base;
  if (!found) {
    qctx->endProc = &rpRoot->base;
  }

  // allocate memory for replies and update endProc if necessary
  if (IsProfile(r)) {
    // 2 is just a starting size, as we most likely have more than 1 shard
    rpRoot->shardsProfile = array_new(MRReply*, 2);
    if (!found) {
      qctx->endProc = rpProfile;
    }
  }
}

static void setupCoordinatorArrangeSteps(AREQ *searchRequest, AREQ *vectorRequest, HybridPipelineParams *hybridParams) {
  const size_t window = hybridParams->scoringCtx->scoringType == HYBRID_SCORING_RRF ? hybridParams->scoringCtx->rrfCtx.window : hybridParams->scoringCtx->linearCtx.window;

  // TODO: would be better to look for a vector node (recursive search on the ast) and decide according to its query type (knn/range)
  const bool isKNN = vectorRequest->ast.root->type == QN_VECTOR;
  const size_t K = isKNN ? vectorRequest->ast.root->vn.vq->knn.k : 0;

  PLN_ArrangeStep *searchArrangeStep = AGPLN_GetOrCreateArrangeStep(AREQ_AGGPlan(searchRequest));
  searchArrangeStep->limit = window;

  PLN_ArrangeStep *vectorArrangeStep = AGPLN_GetOrCreateArrangeStep(AREQ_AGGPlan(vectorRequest));
  if (isKNN) {
    // Vector subquery is a KNN query
    // Heapsize should be min(window, KNN K)
    // ast structure is: root = vector node <- filter node <- ... rest
    vectorArrangeStep->limit = MIN(window, K);
  } else {
    // its range, limit = window
    vectorArrangeStep->limit = window;
  }
}

// Helper function to extract shard profile from a reply based on protocol version
static MRReply *extractShardProfile(MRReply *current, bool resp3) {
  if (MRReply_Type(current) == MR_REPLY_ERROR) {
    return current;
  }
  if (resp3) {
    // RESP3: profile -> Shards -> [shard_profile]
    MRReply *shards = MRReply_MapElement(current, PROFILE_SHARDS_STR);
    return MRReply_ArrayElement(shards, 0);
  } else {
    // RESP2: [results, shards_array] -> shards_array[0] is the shard profile
    MRReply *shards_array_profile = MRReply_ArrayElement(current, 1);
    MRReply *profile = MRReply_ArrayElement(shards_array_profile, 0);
    // Convert to map for easier access (modifies in place)
    MRReply_ArrayToMap(profile);
    return profile;
  }
}

// Helper function to extract Shard ID MRReply from a shard profile
// Returns the MRReply for the Shard ID value (for use with MR_ReplyWithMRReply)
static MRReply *extractShardIdReply(MRReply *shardProfile) {
  if (!shardProfile || MRReply_Type(shardProfile) == MR_REPLY_ERROR) {
    return NULL;
  }
  return MRReply_MapElement(shardProfile, "Shard ID");
}

// Helper function to print a profile map excluding the "Shard ID" field
static void printProfileExcludingShardId(RedisModule_Reply *reply, MRReply *profile) {
  if (!profile || MRReply_Type(profile) == MR_REPLY_ERROR) {
    MR_ReplyWithMRReply(reply, profile);
    return;
  }

  // Profile should be a map at this point
  RedisModule_Reply_Map(reply);
  size_t len = MRReply_Length(profile);
  // Iterate through key-value pairs (len is total elements, so len/2 pairs)
  for (size_t i = 0; i < len; i += 2) {
    MRReply *key = MRReply_ArrayElement(profile, i);
    MRReply *value = MRReply_ArrayElement(profile, i + 1);

    // Skip "Shard ID" field
    if (MRReply_StringEquals(key, "Shard ID", 1)) {
      continue;
    }

    // Print key and value
    MR_ReplyWithMRReply(reply, key);
    MR_ReplyWithMRReply(reply, value);
  }
  RedisModule_Reply_MapEnd(reply);
}

void printShardsHybridProfile(RedisModule_Reply *reply, void *ctx) {
  HybridRequest *hreq = ctx;
  // New format: group by shard with Shard ID printed once per shard
  // [{"Shard ID": "id", "SEARCH": profile (without Shard ID), "VSIM": profile (without Shard ID)}, ...]

  // Get RPNets for SEARCH and VSIM requests
  AREQ *searchAreq = hreq->requests[SEARCH_INDEX];
  AREQ *vsimAreq = hreq->requests[VECTOR_INDEX];
  RPNet *searchRpnet = (RPNet *)AREQ_QueryProcessingCtx(searchAreq)->rootProc;
  RPNet *vsimRpnet = (RPNet *)AREQ_QueryProcessingCtx(vsimAreq)->rootProc;

  size_t searchCount = array_len(searchRpnet->shardsProfile);

  bool resp3 = reply->resp3;

  // Iterate over shards and print both SEARCH and VSIM profiles for each shard
  for (size_t i = 0; i < searchCount; i++) {
    RedisModule_Reply_Map(reply);  // Start shard map

    // Extract shard profiles
    MRReply *searchProfile = extractShardProfile(searchRpnet->shardsProfile[i], resp3);
    MRReply *vsimProfile = extractShardProfile(vsimRpnet->shardsProfile[i], resp3);

    // Extract and print Shard ID from SEARCH profile
    MRReply *shardIdReply = NULL;
    if (searchProfile) {
      shardIdReply = extractShardIdReply(searchProfile);
      RedisModule_Reply_SimpleString(reply, "Shard ID");
      MR_ReplyWithMRReply(reply, shardIdReply);
    }

    // Print SEARCH profile for this shard (excluding Shard ID)
    if (searchProfile) {
      RedisModule_Reply_SimpleString(reply, "SEARCH");
      printProfileExcludingShardId(reply, searchProfile);
    }

    // Print VSIM profile for this shard (excluding Shard ID)
    if (vsimProfile) {
      RedisModule_Reply_SimpleString(reply, "VSIM");
      printProfileExcludingShardId(reply, vsimProfile);
    }

    RedisModule_Reply_MapEnd(reply);  // End shard map
  }
}

// Callback to print subquery result processors for the coordinator profile
static void printDistHybridSubqueryRPs(RedisModule_Reply *reply, void *ctx) {
  HybridRequest *hreq = ctx;
  bool profile_verbose = hreq->reqConfig.printProfileClock;

  // Print subqueries result processors
  // (SEARCH and VSIM pipelines in coordinator)
  RedisModule_ReplyKV_Map(reply, "Subqueries result processors profile");

  for (size_t i = 0; i < hreq->nrequests; i++) {
    AREQ *areq = hreq->requests[i];
    const char *subqueryType = "N/A";
    if (AREQ_RequestFlags(areq) & QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY) {
      subqueryType = "SEARCH";
    } else if (AREQ_RequestFlags(areq) & QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY) {
      subqueryType = "VSIM";
    }

    ResultProcessor *rp = AREQ_QueryProcessingCtx(areq)->endProc;
    RedisModule_ReplyKV_Array(reply, subqueryType);
    Profile_PrintResultProcessors(reply, rp, profile_verbose);
    RedisModule_Reply_ArrayEnd(reply);
  }

  RedisModule_Reply_MapEnd(reply);
}

// Coordinator profile printer that includes subquery result processors
static void printDistHybridCoordinatorProfile(RedisModule_Reply *reply,
                                              void *ctx) {
  Profile_PrintHybridExtra(reply, ctx, printDistHybridSubqueryRPs, ctx);
}

void printDistHybridProfile(RedisModule_Reply *reply, void *ctx) {
  Profile_PrintInFormat(reply, printShardsHybridProfile, ctx,
                        printDistHybridCoordinatorProfile, ctx);
}

static bool shouldCheckInPipelineTimeoutCoord(HybridRequest *req) {
  // We should check for timeout in pipeline if policy is return and timeout > 0
  return req->reqConfig.queryTimeoutMS > 0 &&
         (req->reqConfig.timeoutPolicy == TimeoutPolicy_Return);
}

static int HybridRequest_prepareForExecution(HybridRequest *hreq,
        RedisModuleCtx *ctx, RedisModuleString **argv, int argc, IndexSpec *sp,
        size_t numShards, QueryError *status) {

    hreq->tailPipeline->qctx.err = status;
    hreq->profile = printDistHybridProfile;

    // Parse the hybrid command (equivalent to AREQ_Compile)
    HybridPipelineParams hybridParams = {0};
    ParseHybridCommandCtx cmd = {0};
    cmd.search = hreq->requests[SEARCH_INDEX];
    cmd.vector = hreq->requests[VECTOR_INDEX];
    cmd.cursorConfig = &hreq->cursorConfig;
    cmd.hybridParams = &hybridParams;
    cmd.tailPlan = &hreq->tailPipeline->ap;
    cmd.reqConfig = &hreq->reqConfig;
    cmd.coordDispatchTime = &hreq->profileClocks.coordDispatchTime;

    ArgsCursor ac = {0};
    ArgsCursor_InitRString(&ac, argv, argc);
    ProfileOptions profileOptions = EXEC_NO_FLAGS;
    int rc = ParseProfile(&ac, status, &profileOptions);
    if (rc == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (profileOptions == EXEC_NO_FLAGS) {
      // No profile args, we can use the original args cursor to skip past the command name and index
      HybridRequest_InitArgsCursor(hreq, &ac, argv, argc);
    }

    hreq->tailPipeline->qctx.isProfile = profileOptions != EXEC_NO_FLAGS;
    rc = parseHybridCommand(ctx, &ac, hreq->sctx, &cmd, status, false, profileOptions);
    // we only need parse the combine and what comes after it
    // we can manually create the subqueries pipelines (depleter -> sorter(window)-> RPNet(shared dispatcher ))
    if (rc != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }

    // Set skip timeout
    HybridRequest_SetSkipTimeoutChecks(hreq, !shouldCheckInPipelineTimeoutCoord(hreq));

    rs_wall_clock parseClock;
    if (profileOptions != EXEC_NO_FLAGS) {
      // Initialize parseClock after parsing is done, we want that to be accounted in the parsing timing
      rs_wall_clock_init(&parseClock);
      // Calculate the time elapsed for profileParseTime by using the initialized parseClock
      hreq->profileClocks.profileParseTime = rs_wall_clock_diff_ns(&hreq->profileClocks.initClock, &parseClock);
    }

    // Initialize timeout for all subqueries BEFORE building pipelines
    // but after the parsing to know the timeout values
    for (int i = 0; i < hreq->nrequests; i++) {
        AREQ *subquery = hreq->requests[i];
        SearchCtx_UpdateTime(AREQ_SearchCtx(subquery), hreq->reqConfig.queryTimeoutMS);
    }
    SearchCtx_UpdateTime(hreq->sctx, hreq->reqConfig.queryTimeoutMS);

    // Set request flags from hybridParams
    hreq->reqflags = (QEFlags)hybridParams.aggregationParams.common.reqflags;

    for (size_t i = 0; i < hreq->nrequests; i++) {
        AREQ *areq = hreq->requests[i];
        if (AGGPLN_Distribute(AREQ_AGGPlan(areq), status) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }
    }
    // apply the sorting changes after the distribute phase
    setupCoordinatorArrangeSteps(hreq->requests[SEARCH_INDEX], hreq->requests[VECTOR_INDEX], &hybridParams);
    RLookup *lookups[HYBRID_REQUEST_NUM_SUBQUERIES] = {0};

    arrayof(char*) serialized = HybridRequest_BuildDistributedPipeline(hreq, &hybridParams, lookups, status);
    if (!serialized) {
      return REDISMODULE_ERR;
    }

    // Construct the command string
    MRCommand xcmd;
    // Get the VectorQuery from the vector request's AST for SHARD_K_RATIO
    // optimization
    // Note: parsedVectorData is only set on shards, not on coordinator
    // The coordinator has the VectorQuery in the AST after parsing
    const AREQ *vectorRequest = hreq->requests[VECTOR_INDEX];
    const VectorQuery *vq = (vectorRequest->ast.root && vectorRequest->ast.root->type == QN_VECTOR)
                      ? vectorRequest->ast.root->vn.vq : NULL;
    HybridRequest_buildMRCommand(argv, argc, profileOptions, &xcmd, serialized,
                                 sp, vq, numShards);

    xcmd.protocol = HYBRID_RESP_PROTOCOL_VERSION;
    xcmd.forCursor = hreq->reqflags & QEXEC_F_IS_CURSOR;
    xcmd.forProfiling = profileOptions != EXEC_NO_FLAGS;
    xcmd.rootCommand = C_READ;
    xcmd.coordStartTime = hreq->profileClocks.coordStartTime;

    // UPDATED: Use new start function with mappings (no dispatcher needed)
    HybridRequest_buildDistRPChain(hreq->requests[0], &xcmd, lookups[0], rpnetNext_StartWithMappings);
    HybridRequest_buildDistRPChain(hreq->requests[1], &xcmd, lookups[1], rpnetNext_StartWithMappings);

    if (profileOptions != EXEC_NO_FLAGS) {
      rs_wall_clock pipelineClock;
      rs_wall_clock_init(&pipelineClock);
      // Calculate the time elapsed for profileParseTime by using the initialized parseClock
      hreq->profileClocks.profilePipelineBuildTime = rs_wall_clock_diff_ns(&parseClock, &pipelineClock);
    }

    // Free the command
    MRCommand_Free(&xcmd);
    return REDISMODULE_OK;
}

static void FreeCursorMappings(void *mappings) {
  CursorMappings *vsimOrSearch = (CursorMappings *)mappings;
  for (size_t i = 0; i < array_len(vsimOrSearch->mappings); i++) {
    CursorMapping_Release(&vsimOrSearch->mappings[i]);
  }
  array_free(vsimOrSearch->mappings);
  rm_free(mappings);
}

static int HybridRequest_executePlan(HybridRequest *hreq, struct ConcurrentCmdCtx *cmdCtx,
                        RedisModule_Reply *reply, QueryError *status) {

    // Get RPNet structures from query context
    QueryProcessingCtx *searchQctx = AREQ_QueryProcessingCtx(hreq->requests[0]);
    QueryProcessingCtx *vsimQctx = AREQ_QueryProcessingCtx(hreq->requests[1]);
    RPNet *searchRPNet = (RPNet *)searchQctx->rootProc;
    RPNet *vsimRPNet = (RPNet *)vsimQctx->rootProc;

    CursorMappings *search = rm_calloc(1, sizeof(CursorMappings));
    CursorMappings *vsim = rm_calloc(1, sizeof(CursorMappings));
    search->type = TYPE_SEARCH;
    vsim->type = TYPE_VSIM;

    StrongRef searchMappingsRef = StrongRef_New(search, FreeCursorMappings);
    StrongRef vsimMappingsRef = StrongRef_New(vsim, FreeCursorMappings);

    // Get the command from the RPNet (it was set during prepareForExecution)
    MRCommand *cmd = &searchRPNet->cmd;
    int numShards = ConcurrentCmdCtx_GetNumShards(cmdCtx);
    cmd->coordStartTime = hreq->profileClocks.coordStartTime;

    const RSOomPolicy oomPolicy = hreq->reqConfig.oomPolicy;
    if (!ProcessHybridCursorMappings(cmd, numShards, searchMappingsRef, vsimMappingsRef, hreq->tailPipeline->qctx.err, oomPolicy)) {
        // Handle error
        StrongRef_Release(searchMappingsRef);
        StrongRef_Release(vsimMappingsRef);
        return REDISMODULE_ERR;
    }

    RS_ASSERT(array_len(search->mappings) == array_len(vsim->mappings));
    if (array_len(search->mappings) == 0) {
      // No mappings available - set next function to EOF.
      // Error handling relies on QueryError status and return codes, not on mapping availability.
      searchRPNet->base.Next = rpnetNext_EOF;
      vsimRPNet->base.Next = rpnetNext_EOF;
    }

    // Store mappings in RPNet structures
    searchRPNet->mappings = StrongRef_Clone(searchMappingsRef);
    vsimRPNet->mappings = StrongRef_Clone(vsimMappingsRef);
    StrongRef_Release(searchMappingsRef);
    StrongRef_Release(vsimMappingsRef);

    bool isCursor = hreq->reqflags & QEXEC_F_IS_CURSOR;
    if (isCursor) {
        // // TODO:
        // // Keep the original concurrent context
        // ConcurrentCmdCtx_KeepRedisCtx(cmdCtx);

        // StrongRef dummy_spec_ref = {.rm = NULL};

        // if (HybridRequest_StartCursor(hreq, reply, &dummy_spec_ref, status, true) != REDISMODULE_OK) {
        //     return REDISMODULE_ERR;
        // }
    } else {
        // TODO: Validate cv use
        AGGPlan *plan = &hreq->tailPipeline->ap;
        cachedVars cv = {
            .lastLookup = AGPLN_GetLookup(plan, NULL, AGPLN_GETLOOKUP_LAST),
            .lastAstp = AGPLN_GetArrangeStep(plan)
        };
        sendChunk_hybrid(hreq, reply, UINT64_MAX, cv);
        HybridRequest_DecrRef(hreq);
    }
    return REDISMODULE_OK;
}

static void DistHybridCleanups(RedisModuleCtx *ctx,
    struct ConcurrentCmdCtx *cmdCtx, IndexSpec *sp, StrongRef *strong_ref,
    HybridRequest *hreq, RedisModule_Reply *reply,
    QueryError *status) {

    // Try to claim reply ownership. If we get it, we can safely write to the reply.
    CoordRequestCtx *reqCtx = RedisModule_BlockClientGetPrivateData(ConcurrentCmdCtx_GetBlockedClient(cmdCtx));

    if (!QueryError_HasError(status)) {
      // No error - just cleanup
      // This can happen if during request creation we realized the query timed out
      goto cleanup;
    }

    // Check that we can claim the reply. If not, the timeout callback owns the reply and we should not write to it.
    // If we can't claim the reply, we should just clear the error and return.
    // If hreq is NULL, we don't have a request to claim. This can happen if the cleanup was called before the request was created.
    if (hreq && !CoordRequestCtx_TryClaimReply(reqCtx)) {
        // Timeout callback owns reply - just clear the error
        QueryError_ClearError(status);
        goto cleanup;
    }

    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(status), 1, COORD_ERR_WARN);

    QueryError_ReplyAndClear(ctx, status);
    CoordRequestCtx_MarkReplied(reqCtx);

    cleanup:
    WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
    if (sp) {
      IndexSpecRef_Release(*strong_ref);
    }
    if (hreq) {
      HybridRequest_DecrRef(hreq);
    }

    RedisModule_EndReply(reply);
}

void RSExecDistHybrid(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                        struct ConcurrentCmdCtx *cmdCtx) {

    CoordRequestCtx *reqCtx = RedisModule_BlockClientGetPrivateData(ConcurrentCmdCtx_GetBlockedClient(cmdCtx));

    // Lock before creating request to prevent race with timeout callback
    CoordRequestCtx_LockSetRequest(reqCtx);
    if(CoordRequestCtx_TimedOut(reqCtx)) {
      // Query timed out before request creation - unlock and return
      CoordRequestCtx_UnlockSetRequest(reqCtx);
      return;
    }

    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
    QueryError status = QueryError_Default();

    // CMD, index, expr, args...
    const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
    RedisSearchCtx *sctx = NewSearchCtxC(ctx, indexname, true);
    if (!sctx) {
        QueryError_SetWithUserDataFmt(&status, QUERY_ERROR_CODE_NO_INDEX, "Index not found", ": %s", indexname);
        // return QueryError_ReplyAndClear(ctx, &status);
        DistHybridCleanups(ctx, cmdCtx, NULL, NULL, NULL, reply, &status);
        CoordRequestCtx_UnlockSetRequest(reqCtx);
        return;
    }

    // Check if the index still exists, and promote the ref accordingly
    StrongRef strong_ref = IndexSpecRef_Promote(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
    IndexSpec *sp = StrongRef_Get(strong_ref);
    if (!sp) {
        QueryError_SetCode(&status, QUERY_ERROR_CODE_DROPPED_BACKGROUND);
        DistHybridCleanups(ctx, cmdCtx, sp, &strong_ref, NULL, reply, &status);
        CoordRequestCtx_UnlockSetRequest(reqCtx);
        return;
    }


    // Check if already timed out
    if (CoordRequestCtx_TimedOut(reqCtx)) {
        CoordRequestCtx_UnlockSetRequest(reqCtx);
        SearchCtx_Free(sctx);
        DistHybridCleanups(ctx, cmdCtx, sp, &strong_ref, NULL, reply, &status);
        return;
    }

    // Create and set request atomically while holding lock
    HybridRequest *hreq = MakeDefaultHybridRequest(sctx);
    CoordRequestCtx_SetRequest(reqCtx, hreq);

    // Store coordinator start time for dispatch time tracking
    hreq->profileClocks.coordStartTime = ConcurrentCmdCtx_GetCoordStartTime(cmdCtx);

    CoordRequestCtx_UnlockSetRequest(reqCtx);

    // Get numShards captured from main thread for thread-safe access
    size_t numShards = ConcurrentCmdCtx_GetNumShards(cmdCtx);

    if (HybridRequest_prepareForExecution(hreq, ctx, argv, argc, sp, numShards, &status) != REDISMODULE_OK) {
      DistHybridCleanups(ctx, cmdCtx, sp, &strong_ref, hreq, reply, &status);
      return;
    }


    if (HybridRequest_executePlan(hreq, cmdCtx, reply, &status) != REDISMODULE_OK) {
        DistHybridCleanups(ctx, cmdCtx, sp, &strong_ref, hreq, reply, &status);
        return;
    }
    WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
    IndexSpecRef_Release(strong_ref);
    RedisModule_EndReply(reply);
}

// Timeout callback for Coordinator HybridRequest execution
// Called on the main thread when the blocking client times out (FAIL policy only).
int DistHybridTimeoutFailClient(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UNUSED(argv);
  UNUSED(argc);

  CoordRequestCtx *CoordReqCtx = RedisModule_GetBlockedClientPrivateData(ctx);
  if (!CoordReqCtx) {
    // This shouldn't happen but handle gracefully
    return RedisModule_ReplyWithError(ctx, "ERR timeout with no context");
  }

  RS_ASSERT(CoordReqCtx->type == COMMAND_HYBRID);

  // Lock to coordinate with request creation in the background thread.
  // If the request was not yet created, it won't be - background thread will see the timeout flag.
  // If the request was already created, we can safely try to claim reply ownership.
  CoordRequestCtx_LockSetRequest(CoordReqCtx);

  // Signal timeout to the background thread
  CoordRequestCtx_SetTimedOut(CoordReqCtx);


  if(!CoordRequestCtx_HasRequest(CoordReqCtx)) {
    // Request not created yet
    // In this case no need to claim reply ownership - just reply with timeout error
    // Background thread will notice the timeout and abort execution.
    QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, 1, COORD_ERR_WARN);
    RedisModule_ReplyWithError(ctx, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT));
    CoordRequestCtx_MarkReplied(CoordReqCtx);
    CoordRequestCtx_UnlockSetRequest(CoordReqCtx);
    return REDISMODULE_OK;
  }

  CoordRequestCtx_UnlockSetRequest(CoordReqCtx);

  if (CoordRequestCtx_TryClaimReply(CoordReqCtx)) {
    // We claimed it - reply with timeout error
    QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, 1, COORD_ERR_WARN);
    RedisModule_ReplyWithError(ctx, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT));
    CoordRequestCtx_MarkReplied(CoordReqCtx);
  } else {
    // Background thread owns reply - wait for it to finish if still in progress
    while (CoordRequestCtx_GetReplyState(CoordReqCtx) == ReplyState_Replying) {
      // Busy wait until background thread transitions to REPLIED
    }
  }

  return REDISMODULE_OK;
}
