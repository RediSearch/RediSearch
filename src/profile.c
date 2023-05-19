/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "profile.h"

void printReadIt(RedisModule_Reply *reply, IndexIterator *root, size_t counter, double cpuTime, PrintProfileConfig *config) {
  IndexReader *ir = root->ctx;

  RedisModule_Reply_Array(reply);

  if (ir->idx->flags == Index_DocIdsOnly) {
    printProfileType("TAG");
    RedisModule_ReplyKV_SimpleString(reply, "Term", ir->record->term.term->str);

  } else if (ir->idx->flags & Index_StoreNumeric) {
    NumericFilter *flt = ir->decoderCtx.ptr;
    if (!flt || flt->geoFilter == NULL) {
      printProfileType("NUMERIC");
      RedisModule_Reply_SimpleString(reply, "Term");
      RedisModule_Reply_Stringf(reply, "%g - %g", ir->decoderCtx.rangeMin, ir->decoderCtx.rangeMax);
    } else {
      printProfileType("GEO");
      RedisModule_Reply_SimpleString(reply, "Term");
      double se[2];
      double nw[2];
      decodeGeo(ir->decoderCtx.rangeMin, se);
      decodeGeo(ir->decoderCtx.rangeMax, nw);
      RedisModule_Reply_Stringf(reply, "%g,%g - %g,%g", se[0], se[1], nw[0], nw[1]);
    }
  } else {
    printProfileType("TEXT");
    RedisModule_ReplyKV_SimpleString(reply, "Term", ir->record->term.term->str);
  }

  // print counter and clock
  if (config->printProfileClock) {
    printProfileTime(cpuTime);
  }

  printProfileCounter(counter);

  RedisModule_ReplyKV_SimpleString(reply, "Size", root->NumEstimated(ir));

  RedisModule_Reply_ArrayEnd(reply);
}

static double _recursiveProfilePrint(RedisModule_Reply *reply, ResultProcessor *rp, int printProfileClock) {
  if (rp == NULL) {
    return 0;
  }
  double upstreamTime = _recursiveProfilePrint(reply, rp->upstream, arrlen, printProfileClock);

  // Array is filled backward in pair of [common, profile] result processors
  if (rp->type != RP_PROFILE) {
    RedisModule_Reply_Array(reply);

    switch (rp->type) {
      case RP_INDEX:
      case RP_METRICS:
      case RP_LOADER:
      case RP_BUFFER_AND_LOCKER:
      case RP_UNLOCKER:
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
        RS_LOG_ASSERT(0, "RPType error");
        break;
    }

    RedisModule_Reply_ArrayEnd(reply);
    return upstreamTime;
  }

  double totalRPTime = RPProfile_GetDurationMSec(rp);
  if (printProfileClock) { printProfileTime(totalRPTime - upstreamTime); }
  printProfileCounter(RPProfile_GetCount(rp) - 1);
  ++(*arrlen);
  return totalRPTime;
}

static double printProfileRP(RedisModule_Reply *reply, ResultProcessor *rp, int printProfileClock) {
  return _recursiveProfilePrint(reply, rp, printProfileClock);
}

int Profile_Print(RedisModule_Reply *reply, AREQ *req){
  hires_clock_t now;

  req->totalTime += hires_clock_since_msec(&req->initClock);
  RedisModule_ReplyKV_Map(reply, "profile");

    int profile_verbose = req->reqConfig.printProfileClock;
    // Print total time
    if (profile_verbose)
      RedisModule_ReplyKV_WithDouble(reply, "Total profile time", (double)req->totalTime);
    else
      RedisModule_ReplyKV_Null(reply, "Total profile time");

    // Print query parsing time
    if (profile_verbose)
      RedisModule_ReplyKV_Double(reply, "Parsing time", (double)req->parseTime);
    else
      RedisModule_ReplyKV_Null(reply, "Parsing time");

    // Print iterators creation time
    if (profile_verbose)
      RedisModule_ReplyKV_Double(reply, "Pipeline creation time", (double)req->pipelineBuildTime);
    else
      RedisModule_ReplyKV_Null(reply, "Pipeline creation time");

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
      size_t alen = 1;
      printProfileRP(reply, rp, &alen, req->reqConfig.printProfileClock);
    RedisModule_Reply_ArrayEnd(reply);

  RedisModule_Reply_MapEnd(reply); // profile

  return REDISMODULE_OK;
}
