#include "profile.h"

void printReadIt(RedisModuleCtx *ctx, IndexIterator *root, size_t counter, double cpuTime) {
  IndexReader *ir = root->ctx;
  int verbose = PROFILE_VERBOSE;

  RedisModule_ReplyWithArray(ctx, 3 + verbose);
  RedisModule_ReplyWithSimpleString(ctx, "Reader");
  const char *term = NULL;
  switch (ir->record->type) {
  case RSResultType_Term:
    RedisModule_ReplyWithSimpleString(ctx, ir->record->term.term->str);
    break;
  case RSResultType_Numeric:
    RedisModule_ReplyWithDouble(ctx, ir->record->num.value);
    break;
  case RSResultType_Union:
  case RSResultType_Intersection:
  case RSResultType_Virtual:
    RS_LOG_ASSERT(0, "debug");
    break;
  }
  RedisModule_ReplyWithLongLong(ctx, counter);
  if (verbose) {
      RedisModule_ReplyWithLongDouble(ctx, cpuTime);
  }
}

static double _recursiveProfilePrint(RedisModuleCtx *ctx, ResultProcessor *rp, size_t *arrlen) {
  if (rp == NULL) {
    return 0;
  }
  double upstreamTime = _recursiveProfilePrint(ctx, rp->upstream, arrlen);

  if (strcmp(rp->name, "Profile") != 0) {
    RedisModule_ReplyWithSimpleString(ctx, rp->name);
    ++*arrlen;
    return upstreamTime;
  }
  double totalTime = (double)RPProfile_GetClock(rp) / CLOCKS_PER_MILLISEC;
  RedisModule_ReplyWithLongLong(ctx, RPProfile_GetCount(rp));
  RedisModule_ReplyWithDouble(ctx, totalTime - upstreamTime);
  *arrlen += 2;
  return totalTime;
}

static double printProfileRP(RedisModuleCtx *ctx, ResultProcessor *rp, size_t *arrlen) {
  RedisModule_ReplyWithSimpleString(ctx, "Result processors profile");
  (*arrlen)++;
  return _recursiveProfilePrint(ctx, rp, arrlen);
}

int Profile_Print(RedisModuleCtx *ctx, AREQ *req, size_t *nelem){
  // Print query parsing and creation time
  RedisModule_ReplyWithArray(ctx, 2);
  RedisModule_ReplyWithSimpleString(ctx, "Parsing and iterator creation time");
  RedisModule_ReplyWithDouble(ctx, (double)req->parseTime / CLOCKS_PER_MILLISEC);
  (*nelem)++;

  // print into array with a recursive function over result processors
  size_t alen = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  ResultProcessor *rp = req->qiter.endProc;
  IndexIterator *root = QITR_GetRootFilter(&req->qiter);

  // Print profile of iterators
  RedisModule_ReplyWithSimpleString(ctx, "Iterators profile");
  printIteratorProfile(ctx, root, 0 ,0);
  alen += 2; // function does not add a line internally.

  // Print profile of result processors
  printProfileRP(ctx, rp, &alen);
  RedisModule_ReplySetArrayLength(ctx, alen);
  (*nelem)++;

  return REDISMODULE_OK;
}