/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Argv-level tests for `distributeQuantile` in `src/coord/dist_plan.cpp`: the
// remote RANDOM_SAMPLE / local QUANTILE split, with and without the optional
// resolution argument.

#include "gtest/gtest.h"

#include "redismock/redismock.h"
#include "redismock/util.h"
#include "aggregate/aggregate.h"
#include "aggregate/aggregate_plan.h"
#include "dist_plan.h"
#include "util/arr.h"

#include <strings.h>
#include <string>
#include <vector>

namespace {

using Argv = std::vector<std::string>;

class DistributeQuantileTest : public ::testing::Test {
private:
  RedisModuleCtx *ctx = nullptr;

protected:
  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
  }

  void TearDown() override {
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = nullptr;
    }
  }

  // Compiles `* GROUPBY 1 @g REDUCE QUANTILE <n> <tokens...> AS q` and distributes it.
  // Compilation must succeed; the distribution result is returned. `*outPlan` is owned
  // by the returned AREQ, which the caller frees with `AREQ_Free`.
  int distribute(const Argv &quantileTokens, AREQ **outReq, AGGPlan **outPlan,
                 QueryError *qerr) {
    Argv argv = {"*", "GROUPBY", "1", "@g", "REDUCE", "QUANTILE",
                 std::to_string(quantileTokens.size())};
    argv.insert(argv.end(), quantileTokens.begin(), quantileTokens.end());
    argv.emplace_back("AS");
    argv.emplace_back("q");
    RMCK::ArgvList rmArgs(ctx, argv);

    AREQ *r = AREQ_New(rmArgs, rmArgs.size());
    AREQ_AddRequestFlags(r, QEXEC_F_IS_COORDINATOR);
    EXPECT_EQ(AREQ_Compile(r, ctx, 0, false, qerr), REDISMODULE_OK)
        << QueryError_GetUserError(qerr);
    *outReq = r;
    *outPlan = AREQ_AGGPlan(r);
    return AGGPLN_Distribute(*outPlan, qerr);
  }

  static const PLN_Reducer *findReducer(const PLN_GroupStep *gstp, const char *name) {
    for (size_t i = 0; i < array_len(gstp->reducers); i++) {
      const PLN_Reducer *r = gstp->reducers + i;
      if (r->name && strcasecmp(r->name, name) == 0) return r;
    }
    return nullptr;
  }

  static Argv argsOf(const PLN_Reducer *r) {
    Argv out;
    for (size_t i = 0; i < r->args.argc; i++) {
      out.emplace_back(static_cast<const char *>(r->args.objs[i]));
    }
    return out;
  }

  struct Split {
    const PLN_Reducer *remoteSample = nullptr;
    const PLN_Reducer *localQuantile = nullptr;
  };

  static Split locateSplit(const AGGPlan *plan) {
    Split sp;
    const PLN_BaseStep *distStep = AGPLN_FindStep(plan, nullptr, nullptr, PLN_T_DISTRIBUTE);
    EXPECT_NE(distStep, nullptr);
    if (!distStep) return sp;
    auto local = (const PLN_GroupStep *)AGPLN_FindStep(plan, distStep, nullptr, PLN_T_GROUP);
    auto dstp = (const PLN_DistributeStep *)distStep;
    auto remote = (const PLN_GroupStep *)AGPLN_FindStep(dstp->plan, nullptr, nullptr, PLN_T_GROUP);
    EXPECT_NE(local, nullptr);
    EXPECT_NE(remote, nullptr);
    if (local) sp.localQuantile = findReducer(local, "QUANTILE");
    if (remote) sp.remoteSample = findReducer(remote, "RANDOM_SAMPLE");
    return sp;
  }
};

TEST_F(DistributeQuantileTest, WithoutResolution) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distribute({"@price", "0.5"}, &r, &plan, &qerr), REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  Split sp = locateSplit(plan);
  ASSERT_NE(sp.remoteSample, nullptr);
  ASSERT_NE(sp.localQuantile, nullptr);
  EXPECT_EQ(argsOf(sp.remoteSample), (Argv{"price", "500"}));
  EXPECT_EQ(argsOf(sp.localQuantile), (Argv{sp.remoteSample->alias, "0.5"}));
  EXPECT_STREQ(sp.localQuantile->alias, "q");

  AREQ_Free(r);
}

TEST_F(DistributeQuantileTest, WithResolution_ForwardsItToTheLocalReducer) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distribute({"@price", "0.5", "100"}, &r, &plan, &qerr), REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  Split sp = locateSplit(plan);
  ASSERT_NE(sp.remoteSample, nullptr);
  ASSERT_NE(sp.localQuantile, nullptr);
  // The sample size is fixed; the resolution shapes the estimate over the samples.
  EXPECT_EQ(argsOf(sp.remoteSample), (Argv{"price", "500"}));
  EXPECT_EQ(argsOf(sp.localQuantile), (Argv{sp.remoteSample->alias, "0.5", "100"}));

  AREQ_Free(r);
}

TEST_F(DistributeQuantileTest, RejectsMalformedArgs) {
  for (const Argv &tokens : std::vector<Argv>{{"@price"}, {"@price", "0.5", "100", "extra"}}) {
    AREQ *r = nullptr;
    AGGPlan *plan = nullptr;
    QueryError qerr = QueryError_Default();
    EXPECT_EQ(distribute(tokens, &r, &plan, &qerr), REDISMODULE_ERR);
    EXPECT_TRUE(QueryError_HasError(&qerr));
    QueryError_ClearError(&qerr);
    AREQ_Free(r);
  }
}

}  // namespace
