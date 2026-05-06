/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "dist_plan_utils.h"

#include <initializer_list>
#include <string>
#include <vector>

static ArgsCursor makeArgs(const char **argv, size_t argc) {
  ArgsCursor ac;
  ac.objs = (void **)argv;
  ac.argc = argc;
  ac.offset = 0;
  ac.type = AC_TYPE_CHAR;
  return ac;
}

static void assertArgs(const ArgsCursor &out, std::initializer_list<const char *> expected) {
  ASSERT_EQ(out.argc, expected.size());
  size_t i = 0;
  for (const char *arg : expected) {
    ASSERT_STREQ((const char *)out.objs[i], arg) << "objs[" << i << "]";
    i++;
  }
}

static ArgsCursor callRemote(std::vector<void *> &objs, std::string &countStr,
                              const ArgsCursor &src, const char *shard_count) {
  countStr = std::to_string(src.argc);
  objs.assign(collectObjsBufLen(src.argc, /*has_alias=*/false), nullptr);
  return buildRemoteCollectArgs(objs.data(), countStr.c_str(), &src, shard_count);
}

static ArgsCursor callLocal(std::vector<void *> &objs, std::string &countStr,
                             const ArgsCursor &src, const char *alias) {
  countStr = std::to_string(src.argc);
  objs.assign(collectObjsBufLen(src.argc, /*has_alias=*/true), nullptr);
  return buildLocalCollectArgs(objs.data(), countStr.c_str(), &src, alias);
}

// --- Full distribution cycle ---

TEST(DistPlanUtils, Collect_DistributeWithLimit) {
  // Full cycle: parse → rewrite → build shard args.
  // offset=5, count=10 → shard gets LIMIT 0 15.
  const char *src[] = {"FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "5", "10"};
  ArgsCursor srcArgs = makeArgs(src, 10);

  bool hasLimit;
  CollectLimit limit{};
  QueryError status = QueryError_Default();
  ASSERT_TRUE(parseCollectLimit(&srcArgs, 1000, &hasLimit, &limit, &status));
  ASSERT_TRUE(hasLimit);

  std::string shardCountStr = std::to_string(limit.offset + limit.count);
  std::vector<void *> objs;
  std::string countStr;
  ArgsCursor out = callRemote(objs, countStr, srcArgs, shardCountStr.c_str());

  assertArgs(out,
             {"10", "FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "0", "15"});
}

TEST(DistPlanUtils, Collect_DistributeNoLimit) {
  // No LIMIT in src args → parseCollectLimit returns false, shard args pass through unchanged.
  const char *src[] = {"FIELDS", "2", "@a", "@b"};
  ArgsCursor srcArgs = makeArgs(src, 4);

  bool hasLimit;
  CollectLimit limit{};
  QueryError status = QueryError_Default();
  ASSERT_TRUE(parseCollectLimit(&srcArgs, 1000, &hasLimit, &limit, &status));
  ASSERT_FALSE(hasLimit);

  std::vector<void *> objs;
  std::string countStr;
  ArgsCursor out = callRemote(objs, countStr, srcArgs, nullptr);

  assertArgs(out, {"4", "FIELDS", "2", "@a", "@b"});
  // Forwarded args point directly into srcArgs (no copy).
  for (size_t i = 0; i < 4; i++) {
    ASSERT_EQ(out.objs[i + 1], srcArgs.objs[i]);
  }
}

// --- buildLocalCollectArgs ---

TEST(DistPlanUtils, LocalCollectArgs_Basic) {
  const char *src[] = {"FIELDS", "2", "@a", "@b"};
  ArgsCursor srcArgs = makeArgs(src, 4);
  std::vector<void *> objs;
  std::string countStr;
  ArgsCursor out = callLocal(objs, countStr, srcArgs, "my_collect");

  ASSERT_EQ(out.offset, 0u);
  ASSERT_EQ(out.type, AC_TYPE_CHAR);
  assertArgs(out, {"4", "FIELDS", "2", "@a", "@b", "AS", "my_collect"});
}

TEST(DistPlanUtils, LocalCollectArgs_EmptyArgs) {
  ArgsCursor srcArgs = makeArgs(nullptr, 0);
  std::vector<void *> objs;
  std::string countStr;
  ArgsCursor out = callLocal(objs, countStr, srcArgs, "ua");

  assertArgs(out, {"0", "AS", "ua"});
}

// --- parseCollectLimit error paths ---

TEST(DistPlanUtils, ParseCollectLimit_ExceedsMax) {
  const char *src[] = {"FIELDS", "1", "@x", "LIMIT", "0", "200"};
  ArgsCursor srcArgs = makeArgs(src, 6);
  bool present = false;
  CollectLimit limit{};
  QueryError status = QueryError_Default();

  ASSERT_FALSE(parseCollectLimit(&srcArgs, 100, &present, &limit, &status));
  ASSERT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_LIMIT);
  ASSERT_NE(strstr(QueryError_GetUserError(&status), "LIMIT exceeds maximum of 100"), nullptr);
  QueryError_ClearError(&status);
}

TEST(DistPlanUtils, ParseCollectLimit_MalformedNumber) {
  const char *src[] = {"FIELDS", "1", "@x", "LIMIT", "abc", "10"};
  ArgsCursor srcArgs = makeArgs(src, 6);
  bool present = false;
  CollectLimit limit{};
  QueryError status = QueryError_Default();

  ASSERT_FALSE(parseCollectLimit(&srcArgs, 100, &present, &limit, &status));
  ASSERT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_PARSE_ARGS);
  QueryError_ClearError(&status);
}

TEST(DistPlanUtils, ParseCollectLimit_CountZeroRejected) {
  const char *src[] = {"FIELDS", "1", "@x", "LIMIT", "0", "0"};
  ArgsCursor srcArgs = makeArgs(src, 6);
  bool present = false;
  CollectLimit limit{};
  QueryError status = QueryError_Default();

  ASSERT_FALSE(parseCollectLimit(&srcArgs, 100, &present, &limit, &status));
  ASSERT_TRUE(QueryError_HasError(&status));
  QueryError_ClearError(&status);
}

TEST(DistPlanUtils, ParseCollectLimit_Truncated) {
  const char *src[] = {"FIELDS", "1", "@x", "LIMIT", "5"};
  ArgsCursor srcArgs = makeArgs(src, 5);
  bool present = false;
  CollectLimit limit{};
  QueryError status = QueryError_Default();

  ASSERT_FALSE(parseCollectLimit(&srcArgs, 100, &present, &limit, &status));
  ASSERT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_PARSE_ARGS);
  QueryError_ClearError(&status);
}
