/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "profile.h"
#include "reply_macros.h"
#include "util/units.h"
#include "rs_wall_clock.h"

void printReadIt(RedisModule_Reply *reply, IndexIterator *root, size_t counter, double cpuTime, PrintProfileConfig *config) {
  IndexReader *ir = root->ctx;

  RedisModule_Reply_Map(reply);

  if (ir->idx->flags == Index_DocIdsOnly) {
    printProfileType("TAG");
    REPLY_KVSTR_SAFE("Term", ir->record->term.term->str);
  } else if (ir->idx->flags & Index_StoreNumeric) {
    const NumericFilter *flt = ir->decoderCtx.filter;
    if (!flt || flt->geoFilter == NULL) {
      printProfileType("NUMERIC");
      RedisModule_Reply_SimpleString(reply, "Term");
      RedisModule_Reply_SimpleStringf(reply, "%g - %g", ir->profileCtx.numeric.rangeMin, ir->profileCtx.numeric.rangeMax);
    } else {
      printProfileType("GEO");
      RedisModule_Reply_SimpleString(reply, "Term");
      double se[2];
      double nw[2];
      decodeGeo(ir->profileCtx.numeric.rangeMin, se);
      decodeGeo(ir->profileCtx.numeric.rangeMax, nw);
      RedisModule_Reply_SimpleStringf(reply, "%g,%g - %g,%g", se[0], se[1], nw[0], nw[1]);
    }
  } else {
    printProfileType("TEXT");
    REPLY_KVSTR_SAFE("Term", ir->record->term.term->str);
  }

  // print counter and clock
  if (config->printProfileClock) {
    printProfileTime(cpuTime);
  }

  printProfileIteratorCounter(counter);

  RedisModule_ReplyKV_LongLong(reply, "Estimated number of matches", root->NumEstimated(ir));

  RedisModule_Reply_MapEnd(reply);
}

static double _recursiveProfilePrint(RedisModule_Reply *reply, ResultProcessor *rp, int printProfileClock) {
  if (rp == NULL) {
    return 0;
  }
  double upstreamTime = _recursiveProfilePrint(reply, rp->upstream, printProfileClock);

  // Array is filled backward in pair of [common, profile] result processors
  if (rp->type != RP_PROFILE) {
    RedisModule_Reply_Map(reply); // start of resursive map

    switch (rp->type) {
      case RP_INDEX:
      case RP_METRICS:
      case RP_LOADER:
      case RP_SAFE_LOADER:
      case RP_SCORER:
      case RP_SORTER:
      case RP_COUNTER:
      case RP_PAGER_LIMITER:
      case RP_HIGHLIGHTER:
      case RP_GROUP:
      case RP_NETWORK:
        printProfileType(RPTypeToString(rp->type));
        break;

      case RP_PROJECTOR:
      case RP_FILTER:
        RPEvaluator_Reply(reply, "Type", rp);
        break;

      case RP_PROFILE:
      case RP_MAX:
        RS_ABORT("RPType error");
        break;
    }

    return upstreamTime;
  }

  double totalRPTime = rs_wall_clock_convert_ns_to_ms_d(RPProfile_GetClock(rp));
  if (printProfileClock) {
    printProfileTime(totalRPTime - upstreamTime);
  }
  printProfileRPCounter(RPProfile_GetCount(rp) - 1);
  RedisModule_Reply_MapEnd(reply); // end of recursive map
  return totalRPTime;
}

static double printProfileRP(RedisModule_Reply *reply, ResultProcessor *rp, int printProfileClock) {
  return _recursiveProfilePrint(reply, rp, printProfileClock);
}

void Profile_Print(RedisModule_Reply *reply, ProfilePrinterCtx *ctx) {
  bool has_map = RedisModule_HasMap(reply);
  AREQ *req = ctx->req;
  req->profileTotalTime += rs_wall_clock_elapsed_ns(&req->initClock);

  //-------------------------------------------------------------------------------------------
  if (has_map) { // RESP3 variant
    RedisModule_ReplyKV_Map(reply, "profile"); // profile

      int profile_verbose = req->reqConfig.printProfileClock;
      // Print total time
      if (profile_verbose)
        RedisModule_ReplyKV_Double(reply, "Total profile time",
          rs_wall_clock_convert_ns_to_ms_d(req->profileTotalTime));

      // Print query parsing time
      if (profile_verbose)
        RedisModule_ReplyKV_Double(reply, "Parsing time",
          rs_wall_clock_convert_ns_to_ms_d(req->profileParseTime));

      // Print iterators creation time
        if (profile_verbose)
          RedisModule_ReplyKV_Double(reply, "Pipeline creation time",
            rs_wall_clock_convert_ns_to_ms_d(req->profilePipelineBuildTime));

      // Print whether a warning was raised throughout command execution
      if (ctx->bgScanOOM) {
        RedisModule_ReplyKV_SimpleString(reply, "Warning", QUERY_WINDEXING_FAILURE);
      }
      if (ctx->timedout) {
        RedisModule_ReplyKV_SimpleString(reply, "Warning", QueryError_Strerror(QUERY_ETIMEDOUT));
      } else if (ctx->reachedMaxPrefixExpansions) {
        RedisModule_ReplyKV_SimpleString(reply, "Warning", QUERY_WMAXPREFIXEXPANSIONS);
      } else {
        RedisModule_ReplyKV_SimpleString(reply, "Warning", "None");
      }

      // print into array with a recursive function over result processors

      // Print profile of iterators
      IndexIterator *root = QITR_GetRootFilter(&req->qiter);
      // Coordinator does not have iterators
      if (root) {
        RedisModule_ReplyKV_Array(reply, "Iterators profile");
          PrintProfileConfig config = {.iteratorsConfig = &req->ast.config,
                                       .printProfileClock = profile_verbose};
          printIteratorProfile(reply, root, 0, 0, 2, req->reqflags & QEXEC_F_PROFILE_LIMITED, &config);
        RedisModule_Reply_ArrayEnd(reply);
      }

      // Print profile of result processors
      ResultProcessor *rp = req->qiter.endProc;
      RedisModule_ReplyKV_Array(reply, "Result processors profile");
        printProfileRP(reply, rp, req->reqConfig.printProfileClock);
      RedisModule_Reply_ArrayEnd(reply);

      RedisModule_Reply_MapEnd(reply); // profile
  }
  //-------------------------------------------------------------------------------------------
  else // ! has_map (RESP2 variant)
  {
    RedisModule_Reply_Array(reply);

    int profile_verbose = req->reqConfig.printProfileClock;
    // Print total time
    RedisModule_Reply_Array(reply);
      RedisModule_Reply_SimpleString(reply, "Total profile time");
      if (profile_verbose)
        RedisModule_Reply_Double(reply, rs_wall_clock_convert_ns_to_ms_d(req->profileTotalTime));
    RedisModule_Reply_ArrayEnd(reply);

    // Print query parsing time
    RedisModule_Reply_Array(reply);
      RedisModule_Reply_SimpleString(reply, "Parsing time");
      if (profile_verbose)
        RedisModule_Reply_Double(reply, rs_wall_clock_convert_ns_to_ms_d(req->profileParseTime));
    RedisModule_Reply_ArrayEnd(reply);

    // Print iterators creation time
    RedisModule_Reply_Array(reply);
    RedisModule_Reply_SimpleString(reply, "Pipeline creation time");
    if (profile_verbose)
      RedisModule_Reply_Double(reply, rs_wall_clock_convert_ns_to_ms_d(req->profilePipelineBuildTime));
    RedisModule_Reply_ArrayEnd(reply);

    // Print whether a warning was raised throughout command execution
    RedisModule_Reply_Array(reply);
    RedisModule_Reply_SimpleString(reply, "Warning");
    if (ctx->bgScanOOM) {
      RedisModule_Reply_SimpleString(reply, QUERY_WINDEXING_FAILURE);
    } else if (ctx->timedout) {
      RedisModule_Reply_SimpleString(reply, QueryError_Strerror(QUERY_ETIMEDOUT));
    } else if (ctx->reachedMaxPrefixExpansions) {
      RedisModule_Reply_SimpleString(reply, QUERY_WMAXPREFIXEXPANSIONS);
    }
    RedisModule_Reply_ArrayEnd(reply);

    // print into array with a recursive function over result processors

    // Print profile of iterators
    IndexIterator *root = QITR_GetRootFilter(&req->qiter);
    // Coordinator does not have iterators
    if (root) {
      RedisModule_Reply_Array(reply);
        RedisModule_Reply_SimpleString(reply, "Iterators profile");
        PrintProfileConfig config = {.iteratorsConfig = &req->ast.config,
                                     .printProfileClock = profile_verbose};
        printIteratorProfile(reply, root, 0 ,0, 2, (req->reqflags & QEXEC_F_PROFILE_LIMITED), &config);
      RedisModule_Reply_ArrayEnd(reply);
    }

    // Print profile of result processors
    ResultProcessor *rp = req->qiter.endProc;
    RedisModule_Reply_Array(reply);
      RedisModule_Reply_SimpleString(reply, "Result processors profile");
      printProfileRP(reply, rp, req->reqConfig.printProfileClock);
    RedisModule_Reply_ArrayEnd(reply);

    RedisModule_Reply_ArrayEnd(reply);
  }
}
