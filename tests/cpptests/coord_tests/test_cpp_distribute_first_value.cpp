/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Argv-level tests for `distributeFirstValue` in `src/coord/dist_plan.cpp`.
//
// Each test compiles an `FT.AGGREGATE ... REDUCE FIRST_VALUE ...` query into an
// AGGPlan, runs `AGGPLN_Distribute`, and inspects the local / remote
// `PLN_Reducer`s the rewrite produced: which reducers each side runs, their
// argv, how the local reducer's argv names the remote reducers' aliases, and
// which properties the rewrite has the shards LOAD.

#include "gtest/gtest.h"

#include "redismock/redismock.h"
#include "redismock/util.h"
#include "aggregate/aggregate.h"
#include "aggregate/aggregate_plan.h"
#include "dist_plan.h"
#include "util/arr.h"
#include "util/dllist.h"

#include <strings.h>
#include <set>
#include <string>
#include <vector>

namespace {

using Argv = std::vector<std::string>;

class DistributeFirstValueTest : public ::testing::Test {
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

  // Compiles `* <prefix...> GROUPBY 1 @g REDUCE FIRST_VALUE <n> <tokens...> AS fv
  // <extraReducers...>` and distributes it. Compilation must succeed; the
  // distribution result is returned so the caller can assert either way.
  // `*outPlan` is owned by the returned AREQ, which the caller frees with `AREQ_Free`.
  int distributeWithPrefix(const Argv &prefix, const Argv &firstValueTokens,
                           const Argv &extraReducers, AREQ **outReq, AGGPlan **outPlan,
                           QueryError *qerr) {
    Argv argv = {"*"};
    argv.insert(argv.end(), prefix.begin(), prefix.end());
    argv.insert(argv.end(), {"GROUPBY", "1", "@g", "REDUCE", "FIRST_VALUE",
                             std::to_string(firstValueTokens.size())});
    argv.insert(argv.end(), firstValueTokens.begin(), firstValueTokens.end());
    argv.emplace_back("AS");
    argv.emplace_back("fv");
    argv.insert(argv.end(), extraReducers.begin(), extraReducers.end());
    RMCK::ArgvList rmArgs(ctx, argv);

    AREQ *r = AREQ_New(rmArgs, rmArgs.size());
    AREQ_AddRequestFlags(r, QEXEC_F_IS_COORDINATOR);
    EXPECT_EQ(AREQ_Compile(r, ctx, 0, false, qerr), REDISMODULE_OK)
        << QueryError_GetUserError(qerr);
    *outReq = r;
    *outPlan = AREQ_AGGPlan(r);
    return AGGPLN_Distribute(*outPlan, qerr);
  }

  int distribute(const Argv &firstValueTokens, const Argv &extraReducers, AREQ **outReq,
                 AGGPlan **outPlan, QueryError *qerr) {
    return distributeWithPrefix({}, firstValueTokens, extraReducers, outReq, outPlan, qerr);
  }

  // The argv of each LOAD step in the remote plan, in plan order. A LOAD after the
  // remote GROUPBY would be rejected by the shard, so that fails the test.
  static std::vector<Argv> remoteLoads(const AGGPlan *plan) {
    std::vector<Argv> out;
    auto dstp = (const PLN_DistributeStep *)AGPLN_FindStep(plan, nullptr, nullptr,
                                                            PLN_T_DISTRIBUTE);
    EXPECT_NE(dstp, nullptr) << "distribute step missing";
    if (!dstp) return out;
    bool pastGroup = false;
    for (DLLIST_node *nn = dstp->plan->steps.next; nn != &dstp->plan->steps; nn = nn->next) {
      const PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
      if (stp->type == PLN_T_GROUP) {
        pastGroup = true;
      } else if (stp->type == PLN_T_LOAD) {
        EXPECT_FALSE(pastGroup) << "LOAD after the remote GROUPBY";
        auto lstp = (const PLN_LoadStep *)stp;
        Argv args;
        for (size_t i = 0; i < lstp->args.argc; i++) {
          args.emplace_back(AC_StringArg(&lstp->args, i, NULL));
        }
        out.push_back(args);
      }
    }
    return out;
  }

  // The split GROUPBY: the local group follows the distribute step, the remote
  // group is the one inside it.
  struct Groups {
    const PLN_GroupStep *local = nullptr;
    const PLN_GroupStep *remote = nullptr;
  };

  static Groups locateGroups(const AGGPlan *plan) {
    Groups g;
    const PLN_BaseStep *distStep = AGPLN_FindStep(plan, nullptr, nullptr, PLN_T_DISTRIBUTE);
    EXPECT_NE(distStep, nullptr) << "distribute step missing";
    if (!distStep) return g;
    g.local = (const PLN_GroupStep *)AGPLN_FindStep(plan, distStep, nullptr, PLN_T_GROUP);
    EXPECT_NE(g.local, nullptr) << "local GROUPBY missing";
    auto dstp = (const PLN_DistributeStep *)distStep;
    g.remote = (const PLN_GroupStep *)AGPLN_FindStep(dstp->plan, nullptr, nullptr, PLN_T_GROUP);
    EXPECT_NE(g.remote, nullptr) << "remote GROUPBY missing";
    return g;
  }

  static std::vector<const PLN_Reducer *> reducersNamed(const PLN_GroupStep *gstp,
                                                        const char *name) {
    std::vector<const PLN_Reducer *> out;
    for (size_t i = 0; i < array_len(gstp->reducers); i++) {
      const PLN_Reducer *r = gstp->reducers + i;
      if (r->name && strcasecmp(r->name, name) == 0) {
        out.push_back(r);
      }
    }
    return out;
  }

  // The distribute layer builds reducer args from C strings, so `objs[]`
  // entries are C-string pointers.
  static Argv argsOf(const PLN_Reducer *r) {
    Argv out;
    out.reserve(r->args.argc);
    for (size_t i = 0; i < r->args.argc; i++) {
      out.emplace_back(static_cast<const char *>(r->args.objs[i]));
    }
    return out;
  }
};

TEST_F(DistributeFirstValueTest, WithoutBy_ForwardsAsItself) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distribute({"@title"}, {}, &r, &plan, &qerr), REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  Groups g = locateGroups(plan);
  auto remote = reducersNamed(g.remote, "FIRST_VALUE");
  auto local = reducersNamed(g.local, "FIRST_VALUE");
  ASSERT_EQ(remote.size(), 1u);
  ASSERT_EQ(local.size(), 1u);

  EXPECT_EQ(argsOf(remote[0]), (Argv{"title"}));
  EXPECT_EQ(argsOf(local[0]), (Argv{remote[0]->alias}));
  EXPECT_STREQ(local[0]->alias, "fv");

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, WithBy_ShipsSortValueAndDefaultsToAsc) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distribute({"@title", "BY", "@price"}, {}, &r, &plan, &qerr), REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  Groups g = locateGroups(plan);
  auto remote = reducersNamed(g.remote, "FIRST_VALUE");
  auto local = reducersNamed(g.local, "FIRST_VALUE");
  ASSERT_EQ(remote.size(), 2u);
  ASSERT_EQ(local.size(), 1u);

  // The pick, then the sort value it won with, both under the same ordering.
  EXPECT_EQ(argsOf(remote[0]), (Argv{"title", "BY", "price", "ASC"}));
  EXPECT_EQ(argsOf(remote[1]), (Argv{"price", "BY", "price", "ASC"}));
  EXPECT_EQ(argsOf(local[0]), (Argv{remote[0]->alias, "BY", remote[1]->alias, "ASC"}));
  EXPECT_STREQ(remote[0]->alias, "__first_value_0");
  EXPECT_STREQ(remote[1]->alias, "__first_value_1");
  EXPECT_STREQ(local[0]->alias, "fv");

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, WithBy_NormalizesKeywordCase) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distribute({"@title", "by", "@price", "desc"}, {}, &r, &plan, &qerr),
            REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  Groups g = locateGroups(plan);
  auto remote = reducersNamed(g.remote, "FIRST_VALUE");
  auto local = reducersNamed(g.local, "FIRST_VALUE");
  ASSERT_EQ(remote.size(), 2u);
  ASSERT_EQ(local.size(), 1u);

  EXPECT_EQ(argsOf(remote[0]), (Argv{"title", "BY", "price", "DESC"}));
  EXPECT_EQ(argsOf(remote[1]), (Argv{"price", "BY", "price", "DESC"}));
  EXPECT_EQ(argsOf(local[0]), (Argv{remote[0]->alias, "BY", remote[1]->alias, "DESC"}));

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, SortKeyIsReturnKey_SingleRemoteReducer) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distribute({"@price", "BY", "@price", "DESC"}, {}, &r, &plan, &qerr),
            REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  Groups g = locateGroups(plan);
  auto remote = reducersNamed(g.remote, "FIRST_VALUE");
  auto local = reducersNamed(g.local, "FIRST_VALUE");
  // The pick and its sort value are the same reducer, so the remote dedups them.
  ASSERT_EQ(remote.size(), 1u);
  ASSERT_EQ(local.size(), 1u);

  EXPECT_EQ(argsOf(remote[0]), (Argv{"price", "BY", "price", "DESC"}));
  EXPECT_EQ(argsOf(local[0]), (Argv{remote[0]->alias, "BY", remote[0]->alias, "DESC"}));
  EXPECT_EQ(remoteLoads(plan), (std::vector<Argv>{{"@price"}}));

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, CaseDistinctFields_GetDistinctRemoteAliases) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distribute({"@Price", "BY", "@price", "ASC"}, {}, &r, &plan, &qerr),
            REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  // Field names are case-sensitive, so these are two different reducers and must not
  // share an output name on the shard.
  Groups g = locateGroups(plan);
  auto remote = reducersNamed(g.remote, "FIRST_VALUE");
  ASSERT_EQ(remote.size(), 2u);
  EXPECT_EQ(argsOf(remote[0]), (Argv{"Price", "BY", "price", "ASC"}));
  EXPECT_EQ(argsOf(remote[1]), (Argv{"price", "BY", "price", "ASC"}));
  EXPECT_STRNE(remote[0]->alias, remote[1]->alias);

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, SeparatorInFieldNames_GetDistinctRemoteAliases) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  // `a,b BY c` and `a BY b,c` read the same once joined with commas; their four remote
  // reducers must still get four different output names.
  ASSERT_EQ(distribute({"@a,b", "BY", "@c", "ASC"},
                       {"REDUCE", "FIRST_VALUE", "4", "@a", "BY", "@b,c", "ASC", "AS", "fv2"},
                       &r, &plan, &qerr),
            REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  Groups g = locateGroups(plan);
  auto remote = reducersNamed(g.remote, "FIRST_VALUE");
  ASSERT_EQ(remote.size(), 4u);
  std::set<std::string> aliases;
  for (const PLN_Reducer *rr : remote) aliases.insert(rr->alias);
  EXPECT_EQ(aliases.size(), remote.size());

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, SharedSortKey_DedupsAcrossReducers) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distribute({"@a", "BY", "@p", "DESC"},
                       {"REDUCE", "FIRST_VALUE", "4", "@b", "BY", "@p", "DESC", "AS", "fv2"},
                       &r, &plan, &qerr),
            REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  // Two user reducers over the same sort key share one remote sort-value reducer.
  Groups g = locateGroups(plan);
  auto remote = reducersNamed(g.remote, "FIRST_VALUE");
  auto local = reducersNamed(g.local, "FIRST_VALUE");
  ASSERT_EQ(remote.size(), 3u);
  ASSERT_EQ(local.size(), 2u);
  const PLN_Reducer *sortReducer = nullptr;
  for (const PLN_Reducer *rr : remote) {
    if (argsOf(rr) == (Argv{"p", "BY", "p", "DESC"})) sortReducer = rr;
  }
  ASSERT_NE(sortReducer, nullptr);
  EXPECT_EQ(argsOf(local[0])[2], std::string(sortReducer->alias));
  EXPECT_EQ(argsOf(local[1])[2], std::string(sortReducer->alias));

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, DistributesAlongsideOtherReducers) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distribute({"@title", "BY", "@price", "DESC"},
                       {"REDUCE", "COUNT", "0", "AS", "cnt"}, &r, &plan, &qerr),
            REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  // A distributable FIRST_VALUE no longer pins the whole GROUPBY to the coordinator:
  // the COUNT next to it is pushed down too.
  Groups g = locateGroups(plan);
  EXPECT_EQ(reducersNamed(g.remote, "FIRST_VALUE").size(), 2u);
  EXPECT_EQ(reducersNamed(g.remote, "COUNT").size(), 1u);
  EXPECT_EQ(reducersNamed(g.local, "FIRST_VALUE").size(), 1u);

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, LoadsItsPropertiesOnTheShards) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distribute({"@name", "BY", "@price"}, {}, &r, &plan, &qerr), REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  // The coordinator-side plan had the shards LOAD what the GROUPBY named; the shard-side
  // reducer gets the same LOAD, so `@__key`, fields outside the schema and SORTABLE fields
  // keep yielding the values they did.
  EXPECT_EQ(remoteLoads(plan), (std::vector<Argv>{{"@name", "@price"}}));

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, SkipsPropertiesAnEarlierRemoteStepProduces) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distributeWithPrefix({"LOAD", "1", "@name", "APPLY", "strlen(@name)", "AS", "len"},
                                 {"@name", "BY", "@len"}, {}, &r, &plan, &qerr),
            REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  // `name` is loaded already and `len` is an APPLY output: the user's LOAD stays the only one.
  EXPECT_EQ(remoteLoads(plan), (std::vector<Argv>{{"@name"}}));

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, LoadAllDoesNotReplaceTheExplicitLoad) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distributeWithPrefix({"LOAD", "*"}, {"@__key", "BY", "@price"}, {}, &r, &plan, &qerr),
            REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  // `LOAD *` (empty argv) does not provide `__key`, so the properties are still loaded by name.
  EXPECT_EQ(remoteLoads(plan), (std::vector<Argv>{{}, {"@__key", "@price"}}));

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, LoadAliasIsWhatAnEarlierLoadProduces) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distributeWithPrefix({"LOAD", "3", "@name", "AS", "n"}, {"@name", "BY", "@n"}, {},
                                 &r, &plan, &qerr),
            REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  // The shard's row has `n`, not `name`, so only `name` is loaded again.
  EXPECT_EQ(remoteLoads(plan), (std::vector<Argv>{{"@name", "AS", "n"}, {"@name"}}));

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, SharedPropertiesAreLoadedOnce) {
  AREQ *r = nullptr;
  AGGPlan *plan = nullptr;
  QueryError qerr = QueryError_Default();
  ASSERT_EQ(distribute({"@a", "BY", "@p"},
                       {"REDUCE", "FIRST_VALUE", "4", "@b", "BY", "@p", "DESC", "AS", "fv2"},
                       &r, &plan, &qerr),
            REDISMODULE_OK)
      << QueryError_GetUserError(&qerr);

  // The second reducer's sort key was loaded by the first one's step.
  EXPECT_EQ(remoteLoads(plan), (std::vector<Argv>{{"@a", "@p"}, {"@b"}}));

  AREQ_Free(r);
}

TEST_F(DistributeFirstValueTest, RejectsMalformedArgs) {
  const std::vector<Argv> malformed = {
      {"@a", "@b"},
      {"@a", "BY"},
      {"@a", "BY", "@b", "SIDEWAYS"},
      {"@a", "BY", "@b", "ASC", "extra"},
  };
  for (const Argv &tokens : malformed) {
    AREQ *r = nullptr;
    AGGPlan *plan = nullptr;
    QueryError qerr = QueryError_Default();
    EXPECT_EQ(distribute(tokens, {}, &r, &plan, &qerr), REDISMODULE_ERR);
    EXPECT_TRUE(QueryError_HasError(&qerr));
    QueryError_ClearError(&qerr);
    AREQ_Free(r);
  }
}

}  // namespace
