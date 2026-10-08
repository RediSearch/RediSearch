/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Argv-level tests for LOAD-step handling in `src/coord/dist_plan.cpp`.
//
// `LOAD` parsing captures a counted slice of raw tokens, and the distributed
// planner walks that slice — interpreting `AS` as alias syntax — to decide
// which fields the shards must return. That walk happens before the
// aggregation pipeline validates the step, so these tests pin both halves of
// the contract: a client slice with a dangling `AS` is rejected while parsing,
// and a token that merely spells `AS` stays an ordinary field name.

#include "gtest/gtest.h"

#include "redismock/redismock.h"
#include "redismock/util.h"
#include "aggregate/aggregate.h"
#include "aggregate/aggregate_plan.h"
#include "dist_plan.h"
#include "rlookup.h"

#include <algorithm>
#include <string>
#include <vector>

namespace {

class DistributeLoadTest : public ::testing::Test {
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

  // Compile the FT.AGGREGATE argv as a coordinator would. Returns nullptr and
  // fills `qerr` when parsing rejects the command; the caller owns the AREQ.
  AREQ *compile(const std::vector<std::string> &argv, QueryError *qerr) {
    RMCK::ArgvList rmArgs(ctx, argv);
    AREQ *r = AREQ_New(rmArgs, rmArgs.size());
    AREQ_AddRequestFlags(r, QEXEC_F_IS_COORDINATOR);
    if (AREQ_Compile(r, ctx, 0, false, qerr) != REDISMODULE_OK) {
      AREQ_Free(r);
      return nullptr;
    }
    return r;
  }

  // Names the distributed step expects to receive back from the shards.
  static std::vector<std::string> distributeLookupNames(const AGGPlan *plan) {
    std::vector<std::string> names;
    auto dstp = (PLN_DistributeStep *)AGPLN_FindStep(plan, nullptr, nullptr, PLN_T_DISTRIBUTE);
    EXPECT_NE(dstp, nullptr) << "distribute step missing";
    if (!dstp) return names;
    RLOOKUP_FOREACH(kk, &dstp->lk, {
      names.emplace_back(RLookupKey_GetName(kk));
    });
    return names;
  }

  static bool contains(const std::vector<std::string> &names, const char *name) {
    return std::find(names.begin(), names.end(), std::string(name)) != names.end();
  }
};

TEST_F(DistributeLoadTest, TrailingAs_RejectedWhileParsing) {
  QueryError qerr = QueryError_Default();
  AREQ *r = compile({"*", "LOAD", "2", "@field", "AS"}, &qerr);
  ASSERT_EQ(r, nullptr) << "LOAD slice with no alias after `AS` was accepted into the plan";
  EXPECT_EQ(QueryError_GetCode(&qerr), QUERY_ERROR_CODE_PARSE_ARGS);
  const char *msg = QueryError_GetUserError(&qerr);
  ASSERT_NE(msg, nullptr);
  EXPECT_NE(std::string(msg).find("LOAD path AS name - must be accompanied with NAME"),
            std::string::npos)
      << "unexpected error: " << msg;
  QueryError_ClearError(&qerr);
}

// The planner's walk matches `AS` case-insensitively, so the check that feeds
// it has to as well.
TEST_F(DistributeLoadTest, TrailingLowercaseAs_RejectedWhileParsing) {
  QueryError qerr = QueryError_Default();
  AREQ *r = compile({"*", "LOAD", "2", "@field", "as"}, &qerr);
  ASSERT_EQ(r, nullptr) << "LOAD slice with no alias after `as` was accepted into the plan";
  EXPECT_EQ(QueryError_GetCode(&qerr), QUERY_ERROR_CODE_PARSE_ARGS);
  QueryError_ClearError(&qerr);
}

TEST_F(DistributeLoadTest, TrailingAsAfterAliasedPair_RejectedWhileParsing) {
  QueryError qerr = QueryError_Default();
  AREQ *r = compile({"*", "LOAD", "5", "@a", "AS", "b", "@c", "AS"}, &qerr);
  ASSERT_EQ(r, nullptr) << "LOAD slice with no alias after `AS` was accepted into the plan";
  EXPECT_EQ(QueryError_GetCode(&qerr), QUERY_ERROR_CODE_PARSE_ARGS);
  QueryError_ClearError(&qerr);
}

TEST_F(DistributeLoadTest, AliasedLoad_AliasRegisteredForShardReply) {
  QueryError qerr = QueryError_Default();
  AREQ *r = compile({"*", "LOAD", "3", "@field", "AS", "alias"}, &qerr);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&qerr);

  AGGPlan *plan = AREQ_AGGPlan(r);
  ASSERT_EQ(AGGPLN_Distribute(plan, &qerr), REDISMODULE_OK) << QueryError_GetUserError(&qerr);

  auto names = distributeLookupNames(plan);
  EXPECT_TRUE(contains(names, "alias"));
  AREQ_Free(r);
}

TEST_F(DistributeLoadTest, PlainLoad_FieldsRegisteredForShardReply) {
  QueryError qerr = QueryError_Default();
  AREQ *r = compile({"*", "LOAD", "2", "@first", "@second"}, &qerr);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&qerr);

  AGGPlan *plan = AREQ_AGGPlan(r);
  ASSERT_EQ(AGGPLN_Distribute(plan, &qerr), REDISMODULE_OK) << QueryError_GetUserError(&qerr);

  auto names = distributeLookupNames(plan);
  EXPECT_TRUE(contains(names, "first"));
  EXPECT_TRUE(contains(names, "second"));
  AREQ_Free(r);
}

// `AS` as the only LOAD token cannot be alias syntax, so it names a field.
TEST_F(DistributeLoadTest, LoneAsToken_IsAFieldName) {
  QueryError qerr = QueryError_Default();
  AREQ *r = compile({"*", "LOAD", "1", "AS"}, &qerr);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&qerr);

  AGGPlan *plan = AREQ_AGGPlan(r);
  ASSERT_EQ(AGGPLN_Distribute(plan, &qerr), REDISMODULE_OK) << QueryError_GetUserError(&qerr);

  EXPECT_TRUE(contains(distributeLookupNames(plan), "AS"));
  AREQ_Free(r);
}

// FILTER makes the planner generate its own LOAD step from the expression's
// field names, with no alias syntax involved. A field named `AS` last in that
// list must not be consumed as the keyword — which is what used to read past
// the end of the list. (A generated list whose `AS` is not last is still
// misparsed; see the note at the walk in `finalize_distribution`.)
TEST_F(DistributeLoadTest, FilterOverFieldNamedAsLast_RegistersBothFields) {
  QueryError qerr = QueryError_Default();
  AREQ *r = compile({"*", "FILTER", "@x == 1 || @AS == 1"}, &qerr);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&qerr);

  AGGPlan *plan = AREQ_AGGPlan(r);
  ASSERT_EQ(AGGPLN_Distribute(plan, &qerr), REDISMODULE_OK) << QueryError_GetUserError(&qerr);

  auto names = distributeLookupNames(plan);
  EXPECT_TRUE(contains(names, "x"));
  EXPECT_TRUE(contains(names, "AS"));
  AREQ_Free(r);
}

}  // namespace
