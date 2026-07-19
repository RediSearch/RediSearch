/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "profile.h"
#include "types_ffi.h"
#include "iterators/iterator_api.h"
#include "iterators_ffi.h"
#include "query_term_ffi.h"
#include "reply_macros.h"
#include "util/units.h"
#include "coord/rmr/rmr.h"
#include "hybrid/hybrid_request.h"

static double _recursiveProfilePrint(RedisModule_Reply *reply, ResultProcessor *rp, int printProfileClock) {
  if (rp == NULL) {
    return 0;
  }
  double upstreamTime = _recursiveProfilePrint(reply, rp->upstream, printProfileClock);

  if (rp->type > RP_MAX) {
    RS_LOG_ASSERT_FMT(rp->type < RP_MAX_DEBUG, "RPType error, type: %d", rp->type);
    return upstreamTime;
  }

  // Array is filled backward in pair of [common, profile] result processors
  if (rp->type != RP_PROFILE) {
    RedisModule_Reply_Map(reply); // start of recursive map

    switch (rp->type) {
      case RP_INDEX:
      case RP_METRICS:
      case RP_LOADER:
      case RP_KEY_NAME_LOADER:
      case RP_SCORER:
      case RP_SORTER:
      case RP_COUNTER:
      case RP_PAGER_LIMITER:
      case RP_HIGHLIGHTER:
      case RP_GROUP:
      case RP_MAX_SCORE_NORMALIZER:
      case RP_NETWORK:
      case RP_SAFE_DEPLETER:
      case RP_VECTOR_NORMALIZER:
      case RP_HYBRID_MERGER:
      case RP_DEPLETER:
      case RP_DISK_ASYNC_LOADER:
        printProfileType(RPTypeToString(rp->type));
        break;

      case RP_PROJECTOR:
      case RP_FILTER:
        RPEvaluator_Reply(reply, "Type", rp);
        break;

      case RP_SAFE_LOADER:
        printProfileType(RPTypeToString(rp->type));
        printProfileGILTime(rs_wall_clock_convert_ns_to_ms_d(rp->rpGILTime));
        break;

      default: // LCOV_EXCL_START — defensive: all valid RPType values are handled above
        RS_ABORT("RPType error");
        break;
      // LCOV_EXCL_STOP
    }

    return upstreamTime;
  }
  double totalRPTime = rs_wall_clock_convert_ns_to_ms_d(RPProfile_GetTime(rp));

  if (printProfileClock) {
    double deltaTime = totalRPTime - upstreamTime;
    printProfileTime(deltaTime);
  }
  printProfileRPCounter(RPProfile_GetCount(rp) - 1);
  RedisModule_Reply_MapEnd(reply); // end of recursive map
  return totalRPTime;
}

static double printProfileRP(RedisModule_Reply *reply, ResultProcessor *rp, int printProfileClock) {
  return _recursiveProfilePrint(reply, rp, printProfileClock);
}

void Profile_PrintResultProcessors(RedisModule_Reply *reply, ResultProcessor *rp, bool verbose) {
  printProfileRP(reply, rp, verbose);
}

// Internal implementation that supports an optional callback to print extra
// content before the result processors section.
// Used in hybrid search profile to print the hybrid search subqueries profile.
static void Profile_PrintCommon(RedisModule_Reply *reply,
                                ProfileRequest *request,
                                ProfilePrinterCB printbeforeRPSectionCB,
                                void *beforeRPSectionCtx) {
  ProfilePrinterCtx *profileCtx = NULL;
  ProfileClocks *clocks = NULL;
  QueryProcessingCtx *qctx = NULL;
  QEFlags reqFlags = 0;
  bool profile_verbose = false;
  AREQ *req = NULL;  // Keep for iterator access

  switch (request->type) {
    case PROFILE_REQUEST_TYPE_AREQ:
      req = request->req;
      profileCtx = AREQ_ProfilePrinterCtx(req);
      profile_verbose = req->reqConfig.printProfileClock;
      clocks = &(req->profileClocks);
      qctx = AREQ_QueryProcessingCtx(req);
      reqFlags = AREQ_RequestFlags(req);
      break;

    case PROFILE_REQUEST_TYPE_HYBRID: {
      HybridRequest *hreq = request->hreq;
      profileCtx = &(hreq->profileCtx);
      profile_verbose = hreq->reqConfig.printProfileClock;
      clocks = &(hreq->profileClocks);
      qctx = &hreq->tailPipeline->qctx;
      reqFlags = (QEFlags)hreq->reqflags;
      break;
    }
  }

  bool timedout = ProfileWarnings_Has(&profileCtx->warnings, PROFILE_WARNING_TYPE_TIMEOUT);
  bool reachedMaxPrefixExpansions = ProfileWarnings_Has(&profileCtx->warnings, PROFILE_WARNING_TYPE_MAX_PREFIX_EXPANSIONS);
  bool bgScanOOM = ProfileWarnings_Has(&profileCtx->warnings, PROFILE_WARNING_TYPE_BG_SCAN_OOM);
  bool queryOOM = ProfileWarnings_Has(&profileCtx->warnings, PROFILE_WARNING_TYPE_QUERY_OOM);
  bool asmTrimmingDelayTimeout = ProfileWarnings_Has(&profileCtx->warnings, PROFILE_WARNING_TYPE_ASM_INACCURATE_RESULTS);

  clocks->profileTotalTime += rs_wall_clock_elapsed_ns(&clocks->initClock);

  RedisModule_Reply_Map(reply);

  // Get and add the Shard ID string to the profile reply (guarded by a ref count).
  const char *node_id = MR_GetLocalNodeId();
  if (node_id) {
    RedisModule_ReplyKV_SimpleString(reply, "Shard ID", node_id);
  }
  MR_ReleaseLocalNodeIdReadLock();

  // Print total time
  if (profile_verbose) {
    RedisModule_ReplyKV_Double(reply, "Total profile time",
                               rs_wall_clock_convert_ns_to_ms_d(clocks->profileTotalTime));
  }

  // Print query parsing time
  if (profile_verbose) {
    RedisModule_ReplyKV_Double(reply, "Parsing time",
                               rs_wall_clock_convert_ns_to_ms_d(clocks->profileParseTime));
  }

  if (profile_verbose) {
    RedisModule_ReplyKV_Double(reply, "Workers queue time",
                               rs_wall_clock_convert_ns_to_ms_d(clocks->profileQueueTime));
  }

  // Print iterators creation time
  if (profile_verbose) {
    RedisModule_ReplyKV_Double(reply, "Pipeline creation time",
                               rs_wall_clock_convert_ns_to_ms_d(clocks->profilePipelineBuildTime));
  }

  // Print total GIL time
  if (profile_verbose) {
    if (reqFlags & QEXEC_F_RUN_IN_BACKGROUND) {
      RedisModule_ReplyKV_Double(reply, "Total GIL time",
                                 rs_wall_clock_convert_ns_to_ms_d(qctx->queryGILTime));
    }
  }

  bool isInternal = reqFlags & QEXEC_F_INTERNAL;
  // Print coord dispatch time if this is a shard handling a coordinator request.
  if (profile_verbose && isInternal && req) {
    RedisModule_ReplyKV_Double(reply, "Coordinator dispatch time [ms]",
                               rs_wall_clock_convert_ns_to_ms_d(req->profileClocks.coordDispatchTime));
  }

  // Print whether a warning was raised throughout command execution
  bool warningRaised = bgScanOOM || queryOOM || timedout || reachedMaxPrefixExpansions || asmTrimmingDelayTimeout;
  RedisModule_ReplyKV_Array(reply, "Warning");
  if (!warningRaised) {
    RedisModule_Reply_SimpleString(reply, "None");
  } else {
    if (bgScanOOM) {
      RedisModule_Reply_SimpleString(reply, QUERY_WINDEXING_FAILURE);
    }
    if (queryOOM) {
      // This function is called by Shard or SA, so always return SHARD warning.
      RedisModule_Reply_SimpleString(reply, QUERY_WOOM_SHARD);
    }
    if (timedout) {
      RedisModule_Reply_SimpleString(reply, QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT));
    }
    if (reachedMaxPrefixExpansions) {
      RedisModule_Reply_SimpleString(reply, QUERY_WMAXPREFIXEXPANSIONS);
    }
    if (asmTrimmingDelayTimeout) {
      RedisModule_Reply_SimpleString(reply, QUERY_ASM_INACCURATE_RESULTS);
    }
  }
  RedisModule_Reply_ArrayEnd(reply); // >warnings

  // Print cursor reads count if this is a cursor request.
  if (req && IsCursor(req)) {
    // Only internal requests can use profile with cursor.
    RS_ASSERT(IsInternal(req));
    RedisModule_ReplyKV_LongLong(reply, "Internal cursor reads", profileCtx->cursor_reads);
  }

  // Print profile of iterators
  QueryIterator *root = QITR_GetRootFilter(qctx);
  // Coordinator does not have iterators
  if (req && root) {
    RedisModule_Reply_SimpleString(reply, "Iterators profile");
    Profile_PrintIterators(reply->ctx, root,
                           AREQ_RequestFlags(req) & QEXEC_F_PROFILE_LIMITED,
                           profile_verbose);
    // The Rust function emits directly through ctx, bypassing the reply
    // wrapper's count tracking. Notify the wrapper about the emitted element.
    RedisModule_Reply_TrackExternalElement(reply);
  }

  // Call printbeforeRPSectionCB if provided (before printing main result processors)
  if (printbeforeRPSectionCB) {
    printbeforeRPSectionCB(reply, beforeRPSectionCtx);
  }

  // Print profile of result processors
  ResultProcessor *rp = qctx->endProc;
  RedisModule_ReplyKV_Array(reply, "Result processors profile");
  printProfileRP(reply, rp, profile_verbose);
  RedisModule_Reply_ArrayEnd(reply);
  RedisModule_Reply_MapEnd(reply);
}

void Profile_PrintHybrid(RedisModule_Reply *reply, void *ctx) {
  HybridRequest *hreq = ctx;
  ProfileRequest request = {
    .type = PROFILE_REQUEST_TYPE_HYBRID,
    .hreq = hreq
  };
  Profile_PrintCommon(reply, &request, NULL, NULL);
}

void Profile_PrintHybridExtra(RedisModule_Reply *reply, void *ctx,
                              ProfilePrinterCB printbeforeRPSectionCB,
                              void *beforeRPSectionCtx) {
  HybridRequest *hreq = ctx;
  ProfileRequest request = {
    .type = PROFILE_REQUEST_TYPE_HYBRID,
    .hreq = hreq
  };
  Profile_PrintCommon(reply, &request, printbeforeRPSectionCB, beforeRPSectionCtx);
}

void Profile_Print(RedisModule_Reply *reply, void *ctx) {
  AREQ *req = ctx;
  ProfileRequest request = {
    .type = PROFILE_REQUEST_TYPE_AREQ,
    .req = req
  };
  Profile_PrintCommon(reply, &request, NULL, NULL);
}

void Profile_PrepareMapForReply(RedisModule_Reply *reply) {
  if (reply->resp3) {
    RedisModule_ReplyKV_Map(reply, "Results");
  } else {
    RedisModule_Reply_Map(reply);
  }
}

void Profile_PrintInFormat(RedisModule_Reply *reply,
                           ProfilePrinterCB shards_cb, void *shards_ctx,
                           ProfilePrinterCB coordinator_cb, void *coordinator_ctx) {
  if (reply->resp3) {
    RedisModule_ReplyKV_Map(reply, PROFILE_STR); /* >profile */
  } else {
    RedisModule_Reply_Map(reply); /* >profile */
  }
  /* Print shards profile */
  RedisModule_ReplyKV_Array(reply, PROFILE_SHARDS_STR); /* >Shards */
  if (shards_cb) shards_cb(reply, shards_ctx);
  RedisModule_Reply_ArrayEnd(reply); /* Shards */
  /* Print coordinator profile */
  RedisModule_Reply_SimpleString(reply, PROFILE_COORDINATOR_STR); /* >coordinator */
  if (coordinator_cb) {
    coordinator_cb(reply, coordinator_ctx); /* reply is already a map */
  } else {
    RedisModule_Reply_EmptyMap(reply);
  }
  RedisModule_Reply_MapEnd(reply); /* >profile */
}

// Receives context as void*
// Will be used as ProfilePrinterCtx*
void Profile_PrintDefault(RedisModule_Reply *reply, void *ctx) {
  Profile_PrintInFormat(reply, Profile_Print, ctx, NULL, NULL);
}
