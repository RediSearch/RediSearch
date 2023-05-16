/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "profile.h"

void printReadIt(RedisModuleCtx *ctx, IndexIterator *root, size_t counter, double cpuTime, PrintProfileConfig *config) {
  IndexReader *ir = root->ctx;

  size_t nlen = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  if (ir->idx->flags == Index_DocIdsOnly) {
    printProfileType("TAG");
    RedisModule_ReplyWithSimpleString(ctx, "Term");
    RedisModule_ReplyWithSimpleString(ctx, ir->record->term.term->str);

  } else if (ir->idx->flags & Index_StoreNumeric) {
    NumericFilter *flt = ir->decoderCtx.ptr;
    if (!flt || flt->geoFilter == NULL) {
      printProfileType("NUMERIC");
      RedisModule_ReplyWithSimpleString(ctx, "Term");
      RedisModule_ReplyWithPrintf(ctx, "%g - %g", ir->decoderCtx.rangeMin, ir->decoderCtx.rangeMax);

    } else {
      printProfileType("GEO");
      RedisModule_ReplyWithSimpleString(ctx, "Term");
      double se[2];
      double nw[2];
      decodeGeo(ir->decoderCtx.rangeMin, se);
      decodeGeo(ir->decoderCtx.rangeMax, nw);
      RedisModule_ReplyWithPrintf(ctx, "%g,%g - %g,%g", se[0], se[1], nw[0], nw[1]);
    }
  } else {
    printProfileType("TEXT");
    RedisModule_ReplyWithSimpleString(ctx, "Term");
    RedisModule_ReplyWithSimpleString(ctx, ir->record->term.term->str);
  }
  // We have added both Type and Term fields
  nlen += 4;

  // print counter and clock
  if (config->printProfileClock) {
    printProfileTime(cpuTime);
    nlen += 2;
  }

  printProfileCounter(counter);
  nlen += 2;

  RedisModule_ReplyWithSimpleString(ctx, "Size");
  RedisModule_ReplyWithLongLong(ctx, root->NumEstimated(ir));
  nlen += 2;

  RedisModule_ReplySetArrayLength(ctx, nlen);
}

static double _recursiveProfilePrint(RedisModuleCtx *ctx, ResultProcessor *rp, size_t *arrlen, int printProfileClock) {
  if (rp == NULL) {
    return 0;
  }
  double upstreamTime = _recursiveProfilePrint(ctx, rp->upstream, arrlen, printProfileClock);

  // Array is filled backward in pair of [common, profile] result processors
  if (rp->type != RP_PROFILE) {
    RedisModule_ReplyWithArray(ctx, (2 + printProfileClock) * 2);
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
        RedisModule_ReplyWithSimpleString(ctx, "Type");
        RPEvaluator_Reply(ctx, rp);
        break;

      case RP_PROFILE:
      case RP_MAX:
        RS_LOG_ASSERT(0, "RPType error");
        break;
    }
    return upstreamTime;
  }

  double totalRPTime = RPProfile_GetDurationMSec(rp);
  if (printProfileClock) { printProfileTime(totalRPTime - upstreamTime); }
  printProfileCounter(RPProfile_GetCount(rp) - 1);
  ++(*arrlen);
  return totalRPTime;
}

static double printProfileRP(RedisModuleCtx *ctx, ResultProcessor *rp, size_t *arrlen, int printProfileClock) {
  return _recursiveProfilePrint(ctx, rp, arrlen, printProfileClock);
}

int Profile_Print(RedisModuleCtx *ctx, AREQ *req){
  size_t nelem = 0;

  hires_clock_t now;
  req->totalTime += hires_clock_since_msec(&req->initClock);
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  int profile_verbose = req->reqConfig.printProfileClock;
  // Print total time
  RedisModule_ReplyWithArray(ctx, 1 + profile_verbose);
  RedisModule_ReplyWithSimpleString(ctx, "Total profile time");
  if (profile_verbose)
      RedisModule_ReplyWithDouble(ctx, (double)req->totalTime);
  nelem++;

  // Print query parsing time
  RedisModule_ReplyWithArray(ctx, 1 + profile_verbose);
  RedisModule_ReplyWithSimpleString(ctx, "Parsing time");
  if (profile_verbose)
      RedisModule_ReplyWithDouble(ctx, (double)req->parseTime);
  nelem++;

  // Print iterators creation time
  RedisModule_ReplyWithArray(ctx, 1 + profile_verbose);
  RedisModule_ReplyWithSimpleString(ctx, "Pipeline creation time");
  if (profile_verbose)
      RedisModule_ReplyWithDouble(ctx, (double)req->pipelineBuildTime);
  nelem++;

  // print into array with a recursive function over result processors

  // Print profile of iterators
  IndexIterator *root = QITR_GetRootFilter(&req->qiter);
  if (root) {     // Coordinator does not have iterators
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithSimpleString(ctx, "Iterators profile");
    PrintProfileConfig config = {.iteratorsConfig = &req->ast.config,
                                 .printProfileClock = profile_verbose};
    printIteratorProfile(ctx, root, 0 ,0, 2, (req->reqflags & QEXEC_F_PROFILE_LIMITED), &config);
    nelem++;
  }

  // Print profile of result processors
  ResultProcessor *rp = req->qiter.endProc;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithSimpleString(ctx, "Result processors profile");
  size_t alen = 1;
  printProfileRP(ctx, rp, &alen, req->reqConfig.printProfileClock);
  RedisModule_ReplySetArrayLength(ctx, alen);
  nelem++;

  RedisModule_ReplySetArrayLength(ctx, nelem);

  return REDISMODULE_OK;
}
