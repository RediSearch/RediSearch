
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
  printf("Dumping %p\n", &r->ap);
  AGPLN_Dump(&r->ap);

  PLN_DistributeStep *dstp =
      (PLN_DistributeStep *)AGPLN_FindStep(&r->ap, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  // Serialize it!
  // printf("Printing serialized plan..\n");
  // AGPLN_Dump(dstp->plan);
  auto &v = *dstp->serialized;
  for (size_t ii = 0; ii < v.size(); ++ii) {
    printf("Serialized[%lu]: %s\n", ii, v[ii]);
  }

  dstp = (PLN_DistributeStep *)AGPLN_FindStep(&r->ap, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  printf("Printing local plan\n");
  AGPLN_Dump(&r->ap);

  dstp->lk.options |= RLOOKUP_OPT_UNRESOLVED_OK;
  rc = AREQ_BuildPipeline(r, AREQ_BUILDPIPELINE_NO_ROOT, &status);
  dstp->lk.options &= ~RLOOKUP_OPT_UNRESOLVED_OK;
  printf("Built pipeline.. rc=%d\n", rc);
  if (rc != REDISMODULE_OK) {
    printf("ERROR!!!: %s\n", QueryError_GetError(&status));
    AGPLN_Dump(&r->ap);
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
  printf("Dumping %p\n", &r->ap);
  AGPLN_Dump(&r->ap);

  PLN_DistributeStep *dstp =
      (PLN_DistributeStep *)AGPLN_FindStep(&r->ap, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  AREQDIST_UpstreamInfo us = {0};
  rc = AREQ_BuildDistributedPipeline(r, &us, &status);
  if (rc != REDISMODULE_OK) {
    printf("Couldn't build distributed pipeline: %s\n", QueryError_GetError(&status));
  }
  assert(rc == REDISMODULE_OK);
  AGPLN_Dump(&r->ap);
  for (size_t ii = 0; ii < us.nserialized; ++ii) {
    printf("Serialized[%lu]: %s\n", ii, us.serialized[ii]);
  }
  AREQ_Free(r);
}

static void testSplit() {
  AREQ *r = AREQ_New();
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
  printf("Dumping %p\n", &r->ap);
  AGPLN_Dump(&r->ap);

  PLN_DistributeStep *dstp =
      (PLN_DistributeStep *)AGPLN_FindStep(&r->ap, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  AREQDIST_UpstreamInfo us = {0};
  rc = AREQ_BuildDistributedPipeline(r, &us, &status);
  if (rc != REDISMODULE_OK) {
    printf("Couldn't build distributed pipeline: %s\n", QueryError_GetError(&status));
  }
  assert(rc == REDISMODULE_OK);
  AGPLN_Dump(&r->ap);
  for (size_t ii = 0; ii < us.nserialized; ++ii) {
    printf("Serialized[%lu]: %s\n", ii, us.serialized[ii]);
  }
  AREQ_Free(r);
}

int main(int, char **) {
  RMCK_Bootstrap(my_OnLoad, NULL, 0);
  RMCK::init();
  testAverage();
  testCountDistinct();
}

//REDISMODULE_INIT_SYMBOLS();
