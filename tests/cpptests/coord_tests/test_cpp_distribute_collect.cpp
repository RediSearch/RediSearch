/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Argv-level tests for `distributeCollect` in `src/coord/dist_plan.cpp`.
//
// Each test compiles an `FT.AGGREGATE ... REDUCE COLLECT ...` query into an
// AGGPlan, runs `AGGPLN_Distribute`, and inspects the resulting local /
// remote `PLN_Reducer.args` slices. The matrix exercises:
//
//   FIELDS (list | *)  ×  SORTBY (absent | present)  ×  LIMIT (absent | present)
//
// The key invariants asserted:
//   - LIMIT on the remote is always rewritten to `0 (offset + count)`.
//   - LIMIT on the local keeps the user's original offset/count.
//   - All other tokens (FIELDS, SORTBY, `FIELDS *`) are forwarded verbatim
//     from the original argv, with no normalization.
//   - The local reducer's `inputAlias` matches the remote reducer's `alias`,
//     wiring the merge step to its shard payload source.

#include "gtest/gtest.h"

#include "redismock/redismock.h"
#include "redismock/util.h"
#include "aggregate/aggregate.h"
#include "aggregate/aggregate_plan.h"
#include "dist_plan.h"
#include "config.h"
#include "util/arr.h"

#include <string>
#include <vector>

namespace {

class DistributeCollectTest : public ::testing::Test {
private:
  RedisModuleCtx *ctx = nullptr;

protected:
  // `AREQ_Compile` + `AGGPLN_Distribute` are purely syntactic — neither
  // resolves keys against an `IndexSpec`, so no `FT.CREATE` is needed here.
  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
  }

  void TearDown() override {
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = nullptr;
    }
  }

  // Build the FT.AGGREGATE argv from a vector of COLLECT-clause tokens.
  //
  // Wraps the tokens in:
  //   * GROUPBY 1 @color REDUCE COLLECT <nargs> <tokens...> AS payload
  static std::vector<std::string> buildAggregateArgv(
      const std::vector<std::string> &collectTokens) {
    std::vector<std::string> argv = {"*", "GROUPBY", "1", "@color",
                                     "REDUCE", "COLLECT",
                                     std::to_string(collectTokens.size())};
    argv.insert(argv.end(), collectTokens.begin(), collectTokens.end());
    argv.emplace_back("AS");
    argv.emplace_back("payload");
    return argv;
  }

  // Compile + distribute. Sets *outPlan and returns the AREQ; caller frees
  // the AREQ via AREQ_DecrRef when done. *outPlan is owned by the AREQ.
  AREQ *compileAndDistribute(const std::vector<std::string> &collectTokens,
                             AGGPlan **outPlan) {
    auto argv = buildAggregateArgv(collectTokens);
    RMCK::ArgvList rmArgs(ctx, argv);

    QueryError qerr = QueryError_Default();
    AREQ *r = AREQ_New();
    AREQ_AddRequestFlags(r, QEXEC_F_IS_COORDINATOR);
    int rc = AREQ_Compile(r, ctx, rmArgs, rmArgs.size(), false, &qerr);
    EXPECT_EQ(rc, REDISMODULE_OK) << QueryError_GetUserError(&qerr);
    if (rc != REDISMODULE_OK) {
      AREQ_DecrRef(r);
      return nullptr;
    }

    AGGPlan *plan = AREQ_AGGPlan(r);
    rc = AGGPLN_Distribute(plan, &qerr);
    EXPECT_EQ(rc, REDISMODULE_OK) << QueryError_GetUserError(&qerr);
    if (rc != REDISMODULE_OK) {
      AREQ_DecrRef(r);
      return nullptr;
    }

    *outPlan = plan;
    return r;
  }

  // Locate the COLLECT reducer pair the rewrite produced. Asserts the
  // distribute step exists and that both sides contain exactly one COLLECT.
  struct CollectPair {
    const PLN_Reducer *local = nullptr;
    const PLN_Reducer *remote = nullptr;
  };

  static const PLN_Reducer *findCollectReducer(const PLN_GroupStep *gstp) {
    for (size_t i = 0; i < array_len(gstp->reducers); i++) {
      const PLN_Reducer *r = gstp->reducers + i;
      if (r->name && strcasecmp(r->name, "COLLECT") == 0) {
        return r;
      }
    }
    return nullptr;
  }

  static CollectPair locateCollectPair(const AGGPlan *plan) {
    CollectPair p;
    const PLN_BaseStep *distStep =
        AGPLN_FindStep(plan, nullptr, nullptr, PLN_T_DISTRIBUTE);
    EXPECT_NE(distStep, nullptr) << "distribute step missing";
    if (!distStep) return p;

    const PLN_BaseStep *localGroup =
        AGPLN_FindStep(plan, distStep, nullptr, PLN_T_GROUP);
    EXPECT_NE(localGroup, nullptr) << "local GROUPBY missing";
    if (localGroup) {
      p.local = findCollectReducer((const PLN_GroupStep *)localGroup);
    }

    auto dstp = (const PLN_DistributeStep *)distStep;
    const PLN_BaseStep *remoteGroup =
        AGPLN_FindStep(dstp->plan, nullptr, nullptr, PLN_T_GROUP);
    EXPECT_NE(remoteGroup, nullptr) << "remote GROUPBY missing";
    if (remoteGroup) {
      p.remote = findCollectReducer((const PLN_GroupStep *)remoteGroup);
    }
    return p;
  }

  // Extract the reducer's args slice as a list of strings. The distribute
  // layer constructs args via `ArgsCursor_InitCString`, so `objs[]` entries
  // are C-string pointers.
  static std::vector<std::string> argsAsStrings(const PLN_Reducer *r) {
    std::vector<std::string> out;
    out.reserve(r->args.argc);
    for (size_t i = 0; i < r->args.argc; i++) {
      out.emplace_back(static_cast<const char *>(r->args.objs[i]));
    }
    return out;
  }
};

// ----------------------------------------------------------------------------
// FIELDS list
// ----------------------------------------------------------------------------

TEST_F(DistributeCollectTest, FieldsList_NoSortBy_NoLimit) {
  AGGPlan *plan = nullptr;
  AREQ *r = compileAndDistribute({"FIELDS", "1", "@name"}, &plan);
  ASSERT_NE(r, nullptr);

  auto pair = locateCollectPair(plan);
  ASSERT_NE(pair.remote, nullptr);
  ASSERT_NE(pair.local, nullptr);

  EXPECT_EQ(argsAsStrings(pair.remote),
            (std::vector<std::string>{"fields", "1", "@name"}));
  EXPECT_EQ(argsAsStrings(pair.local),
            (std::vector<std::string>{"fields", "1", "@name"}));

  ASSERT_NE(pair.local->inputAlias, nullptr);
  ASSERT_NE(pair.remote->alias, nullptr);
  EXPECT_STREQ(pair.local->inputAlias, pair.remote->alias);

  AREQ_DecrRef(r);
}

TEST_F(DistributeCollectTest, FieldsList_SortByOnly_NoLimit) {
  AGGPlan *plan = nullptr;
  // The user omits the per-key direction (`SORTBY 1 @price`, not
  // `SORTBY 2 @price ASC`). Aside from keyword-case canonicalization, SORTBY is
  // forwarded as-is, so the omitted direction stays omitted on both sides.
  AREQ *r = compileAndDistribute(
      {"FIELDS", "1", "@name", "SORTBY", "1", "@price"}, &plan);
  ASSERT_NE(r, nullptr);

  auto pair = locateCollectPair(plan);
  ASSERT_NE(pair.remote, nullptr);
  ASSERT_NE(pair.local, nullptr);

  std::vector<std::string> expected = {"fields", "1", "@name",
                                       "sortby", "1", "@price"};
  EXPECT_EQ(argsAsStrings(pair.remote), expected);
  EXPECT_EQ(argsAsStrings(pair.local), expected);

  AREQ_DecrRef(r);
}

TEST_F(DistributeCollectTest, FieldsList_NoSortBy_Limit_RewritesRemoteLimit) {
  AGGPlan *plan = nullptr;
  AREQ *r = compileAndDistribute(
      {"FIELDS", "1", "@name", "LIMIT", "2", "3"}, &plan);
  ASSERT_NE(r, nullptr);

  auto pair = locateCollectPair(plan);
  ASSERT_NE(pair.remote, nullptr);
  ASSERT_NE(pair.local, nullptr);

  // Remote: offset=0, count=2+3=5
  EXPECT_EQ(argsAsStrings(pair.remote),
            (std::vector<std::string>{"fields", "1", "@name",
                                      "limit", "0", "5"}));
  // Local: original offset=2, count=3
  EXPECT_EQ(argsAsStrings(pair.local),
            (std::vector<std::string>{"fields", "1", "@name",
                                      "limit", "2", "3"}));

  AREQ_DecrRef(r);
}

TEST_F(DistributeCollectTest, FieldsList_SortBy_Limit_RewritesRemoteLimit) {
  AGGPlan *plan = nullptr;
  AREQ *r = compileAndDistribute(
      {"FIELDS", "1", "@name",
       "SORTBY", "2", "@price", "ASC",
       "LIMIT", "2", "3"}, &plan);
  ASSERT_NE(r, nullptr);

  auto pair = locateCollectPair(plan);
  ASSERT_NE(pair.remote, nullptr);
  ASSERT_NE(pair.local, nullptr);

  EXPECT_EQ(argsAsStrings(pair.remote),
            (std::vector<std::string>{"fields", "1", "@name",
                                      "sortby", "2", "@price", "asc",
                                      "limit", "0", "5"}));
  EXPECT_EQ(argsAsStrings(pair.local),
            (std::vector<std::string>{"fields", "1", "@name",
                                      "sortby", "2", "@price", "asc",
                                      "limit", "2", "3"}));

  AREQ_DecrRef(r);
}

// ----------------------------------------------------------------------------
// FIELDS *
// ----------------------------------------------------------------------------

TEST_F(DistributeCollectTest, FieldsStar_NoSortBy_NoLimit) {
  AGGPlan *plan = nullptr;
  AREQ *r = compileAndDistribute({"FIELDS", "*"}, &plan);
  ASSERT_NE(r, nullptr);

  auto pair = locateCollectPair(plan);
  ASSERT_NE(pair.remote, nullptr);
  ASSERT_NE(pair.local, nullptr);

  EXPECT_EQ(argsAsStrings(pair.remote),
            (std::vector<std::string>{"fields", "*"}));
  EXPECT_EQ(argsAsStrings(pair.local),
            (std::vector<std::string>{"fields", "*"}));

  AREQ_DecrRef(r);
}

TEST_F(DistributeCollectTest, FieldsStar_SortByOnly_NoLimit) {
  AGGPlan *plan = nullptr;
  AREQ *r = compileAndDistribute(
      {"FIELDS", "*", "SORTBY", "2", "@price", "ASC"}, &plan);
  ASSERT_NE(r, nullptr);

  auto pair = locateCollectPair(plan);
  ASSERT_NE(pair.remote, nullptr);
  ASSERT_NE(pair.local, nullptr);

  std::vector<std::string> expected = {"fields", "*",
                                       "sortby", "2", "@price", "asc"};
  EXPECT_EQ(argsAsStrings(pair.remote), expected);
  EXPECT_EQ(argsAsStrings(pair.local), expected);

  AREQ_DecrRef(r);
}

TEST_F(DistributeCollectTest, FieldsStar_NoSortBy_Limit_RewritesRemoteLimit) {
  AGGPlan *plan = nullptr;
  AREQ *r = compileAndDistribute({"FIELDS", "*", "LIMIT", "1", "4"}, &plan);
  ASSERT_NE(r, nullptr);

  auto pair = locateCollectPair(plan);
  ASSERT_NE(pair.remote, nullptr);
  ASSERT_NE(pair.local, nullptr);

  EXPECT_EQ(argsAsStrings(pair.remote),
            (std::vector<std::string>{"fields", "*", "limit", "0", "5"}));
  EXPECT_EQ(argsAsStrings(pair.local),
            (std::vector<std::string>{"fields", "*", "limit", "1", "4"}));

  AREQ_DecrRef(r);
}

TEST_F(DistributeCollectTest, FieldsStar_SortBy_Limit_RewritesRemoteLimit) {
  AGGPlan *plan = nullptr;
  AREQ *r = compileAndDistribute(
      {"FIELDS", "*",
       "SORTBY", "2", "@price", "DESC",
       "LIMIT", "5", "10"}, &plan);
  ASSERT_NE(r, nullptr);

  auto pair = locateCollectPair(plan);
  ASSERT_NE(pair.remote, nullptr);
  ASSERT_NE(pair.local, nullptr);

  EXPECT_EQ(argsAsStrings(pair.remote),
            (std::vector<std::string>{"fields", "*",
                                      "sortby", "2", "@price", "desc",
                                      "limit", "0", "15"}));
  EXPECT_EQ(argsAsStrings(pair.local),
            (std::vector<std::string>{"fields", "*",
                                      "sortby", "2", "@price", "desc",
                                      "limit", "5", "10"}));

  AREQ_DecrRef(r);
}

}  // namespace
