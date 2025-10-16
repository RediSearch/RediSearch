/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

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
    printf("Couldn't compile: %s\n", QueryError_GetUserError(&status));
    abort();
  }

  // so far, so good, eh?
  AGGPlan *plan = AREQ_AGGPlan(r);
  rc = AGGPLN_Distribute(plan, &status);
  assert(rc == REDISMODULE_OK);
  printf("Dumping %p\n", plan);
  AGPLN_Dump(plan);

  PLN_DistributeStep *dstp =
      (PLN_DistributeStep *)AGPLN_FindStep(plan, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  // Serialize it!
  // printf("Printing serialized plan..\n");
  // AGPLN_Dump(dstp->plan)
  for (size_t ii = 0; ii < array_len(dstp->serialized); ++ii) {
    printf("Serialized[%lu]: %s\n", ii, dstp->serialized[ii]);
  }

  dstp = (PLN_DistributeStep *)AGPLN_FindStep(plan, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  printf("Printing local plan\n");
  AGPLN_Dump(plan);

  AREQ_AddRequestFlags(r, QEXEC_F_BUILDPIPELINE_NO_ROOT); // mark for coordinator pipeline

  dstp->lk.options |= RLOOKUP_OPT_UNRESOLVED_OK;
  rc = AREQ_BuildPipeline(r, &status);
  dstp->lk.options &= ~RLOOKUP_OPT_UNRESOLVED_OK;
  if (rc != REDISMODULE_OK) {
    printf("ERROR!!!: %s\n", QueryError_GetUserError(&status));
    AGPLN_Dump(plan);
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
  AREQ_AddRequestFlags(r, QEXEC_F_BUILDPIPELINE_NO_ROOT); // mark for coordinator pipeline
  RMCK::Context ctx{};
  RMCK::ArgvList vv(ctx, "*",                                                                  // nl
                    "GROUPBY", "1", "@brand",                                                  // nl
                    "REDUCE", "COUNT_DISTINCT", "1", "@title", "AS", "count_distinct(title)",  // nl
                    "REDUCE", "COUNT", "0"                                                     // nl
  );
  QueryError status{QueryErrorCode(0)};
  int rc = AREQ_Compile(r, vv, vv.size(), &status);
  if (rc != REDISMODULE_OK) {
    printf("Couldn't compile: %s\n", QueryError_GetUserError(&status));
    abort();
  }

  AGGPlan *plan2 = AREQ_AGGPlan(r);
  rc = AGGPLN_Distribute(plan2, &status);
  assert(rc == REDISMODULE_OK);
  printf("Dumping %p\n", plan2);
  AGPLN_Dump(plan2);

  PLN_DistributeStep *dstp =
      (PLN_DistributeStep *)AGPLN_FindStep(plan2, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  AREQDIST_UpstreamInfo us = {0};
  rc = AREQ_BuildDistributedPipeline(r, &us, &status);
  if (rc != REDISMODULE_OK) {
    printf("Couldn't build distributed pipeline: %s\n", QueryError_GetUserError(&status));
  }
  assert(rc == REDISMODULE_OK);
  AGPLN_Dump(plan2);
  for (size_t ii = 0; ii < array_len(us.serialized); ++ii) {
    printf("Serialized[%lu]: %s\n", ii, us.serialized[ii]);
  }
  AREQ_Free(r);
}
static void testSplit() {
  AREQ *r = AREQ_New();
  AREQ_AddRequestFlags(r, QEXEC_F_BUILDPIPELINE_NO_ROOT); // mark for coordinator pipeline
  RMCK::Context ctx{};
  RMCK::ArgvList vv(ctx, "*",                                                                  // nl
                    "GROUPBY", "1", "@brand",                                                  // nl
                    "REDUCE", "COUNT_DISTINCT", "1", "@title", "AS", "count_distinct(title)",  // nl
                    "REDUCE", "COUNT", "0"                                                     // nl
  );
  QueryError status{QueryErrorCode(0)};
  int rc = AREQ_Compile(r, vv, vv.size(), &status);
  if (rc != REDISMODULE_OK) {
    printf("Couldn't compile: %s\n", QueryError_GetUserError(&status));
    abort();
  }

  AGGPlan *plan3 = AREQ_AGGPlan(r);
  rc = AGGPLN_Distribute(plan3, &status);
  assert(rc == REDISMODULE_OK);
  printf("Dumping %p\n", plan3);
  AGPLN_Dump(plan3);

  PLN_DistributeStep *dstp =
      (PLN_DistributeStep *)AGPLN_FindStep(plan3, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  AREQDIST_UpstreamInfo us = {0};
  rc = AREQ_BuildDistributedPipeline(r, &us, &status);
  if (rc != REDISMODULE_OK) {
    printf("Couldn't build distributed pipeline: %s\n", QueryError_GetUserError(&status));
  }
  assert(rc == REDISMODULE_OK);
  AGPLN_Dump(plan3);
  for (size_t ii = 0; ii < array_len(us.serialized); ++ii) {
    printf("Serialized[%lu]: %s\n", ii, us.serialized[ii]);
  }
  AREQ_Free(r);
}

int main(int, char **) {
  RMCK::init();
  testAverage();
  testCountDistinct();
  RMCK_Shutdown();
}

//REDISMODULE_INIT_SYMBOLS();
