#include "profile.h"

void printReadIt(RedisModuleCtx *ctx, IndexIterator *root, size_t counter, double cpuTime) {
  IndexReader *ir = root->ctx;

  RedisModule_ReplyWithArray(ctx, 3 + PROFILE_VERBOSE);

  if (ir->idx->flags == Index_DocIdsOnly) {
    RedisModule_ReplyWithSimpleString(ctx, "Tag reader");
    RedisModule_ReplyWithSimpleString(ctx, ir->record->term.term->str);
  } else if (ir->idx->flags & Index_StoreNumeric) {
    NumericFilter *flt = ir->decoderCtx.ptr;
    if (!flt || flt->geoFilter == NULL) {
      RedisModule_ReplyWithSimpleString(ctx, "Numeric reader");
      RedisModule_ReplyWithPrintf(ctx, "%g - %g", ir->decoderCtx.rangeMin, ir->decoderCtx.rangeMax);
    } else {
      RedisModule_ReplyWithSimpleString(ctx, "Geo reader");
      double se[2];
      double nw[2];
      decodeGeo(ir->decoderCtx.rangeMin, se);
      decodeGeo(ir->decoderCtx.rangeMax, nw);
      RedisModule_ReplyWithPrintf(ctx, "%g,%g - %g,%g", se[0], se[1], nw[0], nw[1]);
    }
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "Term reader");
    RedisModule_ReplyWithSimpleString(ctx, ir->record->term.term->str);
  }

  // print counter and clock
  RedisModule_ReplyWithLongLong(ctx, counter);
  if (PROFILE_VERBOSE) {
      RedisModule_ReplyWithLongDouble(ctx, cpuTime);
  }
}

static double _recursiveProfilePrint(RedisModuleCtx *ctx, ResultProcessor *rp, size_t *arrlen) {
  if (rp == NULL) {
    return 0;
  }
  double upstreamTime = _recursiveProfilePrint(ctx, rp->upstream, arrlen);

  // Array is filled backward in pair of [common, profile] result processors
  if (rp->type != RP_PROFILE) {
    RedisModule_ReplyWithArray(ctx, 2 + PROFILE_VERBOSE);
    switch (rp->type) {
      case RP_INDEX:
      case RP_LOADER:
      case RP_SCORER:
      case RP_SORTER:
      case RP_PAGER_LIMITER:
      case RP_GROUP:
      case RP_NETWORK:
        RedisModule_ReplyWithSimpleString(ctx, RPTypeToString(rp->type));
        break;

      case RP_PROJECTOR:
      case RP_FILTER:
        RPEvaluator_Reply(ctx, rp);
        break;

      default:
        RS_LOG_ASSERT(0, "RPType error");
        break;
    }
    return upstreamTime;
  }
  double totalTime = (double)RPProfile_GetClock(rp) / CLOCKS_PER_MILLISEC;
  RedisModule_ReplyWithLongLong(ctx, RPProfile_GetCount(rp));
  if (PROFILE_VERBOSE)
      RedisModule_ReplyWithDouble(ctx, totalTime - upstreamTime);
  ++(*arrlen);
  return totalTime;
}

static double printProfileRP(RedisModuleCtx *ctx, ResultProcessor *rp, size_t *arrlen) {
  return _recursiveProfilePrint(ctx, rp, arrlen);
}

int Profile_Print(RedisModuleCtx *ctx, AREQ *req, size_t *nelem){
  // Print total time
  RedisModule_ReplyWithArray(ctx, 1 + PROFILE_VERBOSE);
  RedisModule_ReplyWithSimpleString(ctx, "Total profile time");
  if (PROFILE_VERBOSE) 
      RedisModule_ReplyWithDouble(ctx, (double)(clock() - req->initTime) / CLOCKS_PER_MILLISEC);
  (*nelem)++;

  // Print query parsing and creation time
  RedisModule_ReplyWithArray(ctx, 1 + PROFILE_VERBOSE);
  RedisModule_ReplyWithSimpleString(ctx, "Parsing and iterator creation time");
  if (PROFILE_VERBOSE)
      RedisModule_ReplyWithDouble(ctx, (double)req->parseTime / CLOCKS_PER_MILLISEC);
  (*nelem)++;

  // print into array with a recursive function over result processors
  ResultProcessor *rp = req->qiter.endProc;
  IndexIterator *root = QITR_GetRootFilter(&req->qiter);

  // Print profile of iterators
  RedisModule_ReplyWithArray(ctx, 2);
  RedisModule_ReplyWithSimpleString(ctx, "Iterators profile");
  printIteratorProfile(ctx, root, 0 ,0, 1, (req->reqflags & QEXEC_F_PROFILE_LIMITED));
  (*nelem)++;

  // Print profile of result processors
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithSimpleString(ctx, "Result processors profile");
  size_t alen = 1;
  printProfileRP(ctx, rp, &alen);
  RedisModule_ReplySetArrayLength(ctx, alen);
  (*nelem)++;

  // Print header for results
  /*if (!(req->reqflags & QEXEC_F_NOROWS)) {
    RedisModule_ReplyWithSimpleString(ctx, "Results");
    (*nelem)++;
  }*/
  
  return REDISMODULE_OK;
}