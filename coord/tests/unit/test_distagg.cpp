
#include "redismodule.h"
#include "dist_plan.h"
#include "aggregate/aggregate.h"
#include "tests/cpptests/redismock/util.h"

#include <vector>

extern "C" {
static int my_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (RedisModule_Init(ctx, "dummy", 0, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}
}

// Declare the main query

// cmd = ['ft.aggregate', 'games', 'sony',
//        'GROUPBY', '1', '@brand',
//        'REDUCE', 'avg', '1', '@price', 'AS', 'avg_price',
//        'REDUCE', 'count', '0',
//        'SORTBY', '2', '@avg_price', 'DESC']

static void testAverage() {
  AREQ *r = AREQ_New();
  RMCK::Context ctx{};
  RMCK::ArgvList vv(ctx, "sony",                                        // nl
                    "GROUPBY", "1", "@brand",                           // nl
                    "REDUCE", "avg", "1", "@price", "as", "avg_price",  // nl
                    "REDUCE", "count", "0",                             // nl
                    "sortby", "2", "@avg_price", "DESC"                 // nl
  );
  QueryError status{QueryErrorCode(0)};
  int rc = AREQ_Compile(r, vv, vv.size(), &status);
  if (rc != REDISMODULE_OK) {
    printf("Couldn't compile: %s\n", QueryError_GetError(&status));
    abort();
  }

  // so far, so good, eh?
  rc = AGGPLN_Distribute(&r->ap, &status);
  assert(rc == REDISMODULE_OK);

  PLN_DistributeStep *dstp =
      (PLN_DistributeStep *)AGPLN_FindStep(&r->ap, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  dstp = (PLN_DistributeStep *)AGPLN_FindStep(&r->ap, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  r->reqflags |= QEXEC_F_BUILDPIPELINE_NO_ROOT; // mark for coordinator pipeline

  dstp->lk.options |= RLOOKUP_OPT_UNRESOLVED_OK;
  rc = AREQ_BuildPipeline(r, &status);
  dstp->lk.options &= ~RLOOKUP_OPT_UNRESOLVED_OK;
  if (rc != REDISMODULE_OK) {
    printf("ERROR!!!: %s\n", QueryError_GetError(&status));
  }
  AREQ_Free(r);
}

/**
 *         cmd = ['FT.AGGREGATE', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'COUNT_DISTINCT', '1', '@title', 'AS', 'count_distinct(title)',
               'REDUCE', 'COUNT', '0'
               ]
 */
static void testCountDistinct() {
  AREQ *r = AREQ_New();
  r->reqflags |= QEXEC_F_BUILDPIPELINE_NO_ROOT; // mark for coordinator pipeline
  RMCK::Context ctx{};
  RMCK::ArgvList vv(ctx, "*",                                                                  // nl
                    "GROUPBY", "1", "@brand",                                                  // nl
                    "REDUCE", "COUNT_DISTINCT", "1", "@title", "AS", "count_distinct(title)",  // nl
                    "REDUCE", "COUNT", "0"                                                     // nl
  );
  QueryError status{QueryErrorCode(0)};
  int rc = AREQ_Compile(r, vv, vv.size(), &status);
  if (rc != REDISMODULE_OK) {
    printf("Couldn't compile: %s\n", QueryError_GetError(&status));
    abort();
  }

  rc = AGGPLN_Distribute(&r->ap, &status);
  assert(rc == REDISMODULE_OK);

  PLN_DistributeStep *dstp =
      (PLN_DistributeStep *)AGPLN_FindStep(&r->ap, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  AREQDIST_UpstreamInfo us = {0};
  rc = AREQ_BuildDistributedPipeline(r, &us, &status);
  if (rc != REDISMODULE_OK) {
    printf("Couldn't build distributed pipeline: %s\n", QueryError_GetError(&status));
  }
  assert(rc == REDISMODULE_OK);
  AREQ_Free(r);
}

static void testSplit() {
  AREQ *r = AREQ_New();
  r->reqflags |= QEXEC_F_BUILDPIPELINE_NO_ROOT; // mark for coordinator pipeline
  RMCK::Context ctx{};
  RMCK::ArgvList vv(ctx, "*",                                                                  // nl
                    "GROUPBY", "1", "@brand",                                                  // nl
                    "REDUCE", "COUNT_DISTINCT", "1", "@title", "AS", "count_distinct(title)",  // nl
                    "REDUCE", "COUNT", "0"                                                     // nl
  );
  QueryError status{QueryErrorCode(0)};
  int rc = AREQ_Compile(r, vv, vv.size(), &status);
  if (rc != REDISMODULE_OK) {
    printf("Couldn't compile: %s\n", QueryError_GetError(&status));
    abort();
  }

  rc = AGGPLN_Distribute(&r->ap, &status);
  assert(rc == REDISMODULE_OK);

  PLN_DistributeStep *dstp =
      (PLN_DistributeStep *)AGPLN_FindStep(&r->ap, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  AREQDIST_UpstreamInfo us = {0};
  rc = AREQ_BuildDistributedPipeline(r, &us, &status);
  if (rc != REDISMODULE_OK) {
    printf("Couldn't build distributed pipeline: %s\n", QueryError_GetError(&status));
  }
  assert(rc == REDISMODULE_OK);
  AREQ_Free(r);
}

int main(int, char **) {
  RMCK_Bootstrap(my_OnLoad, NULL, 0);
  RMCK::init();
  testAverage();
  testCountDistinct();
  // RMCK_Shutdown() is causing a segfault, but I need to remove the scorer before exiting to avoid sanitizer errors
  rm_free((void *)RSGlobalConfig.defaultScorer);
  RSGlobalConfig.defaultScorer = NULL;
}

//REDISMODULE_INIT_SYMBOLS();
