#include "profile.h"

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
  RedisModule_ReplyWithDouble(ctx, totalTime - upstreamTime);
  RedisModule_ReplyWithLongLong(ctx, RPProfile_GetCount(rp));
  *arrlen += 2;
  return totalTime;
}

int Profile_Print(RedisModuleCtx *ctx, AREQ *req){
  size_t alen = 0;
  ResultProcessor *rp = req->qiter.endProc;

  // print into array with a recursive function over result processors
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  _recursiveProfilePrint(ctx, rp, &alen);
  RedisModule_ReplySetArrayLength(ctx, alen);

  return REDISMODULE_OK;
}