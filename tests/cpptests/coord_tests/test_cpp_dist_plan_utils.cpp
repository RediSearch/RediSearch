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

#include <array>
#include <initializer_list>
#include <string>

static ArgsCursor makeArgs(const char **argv, size_t argc) {
  ArgsCursor ac;
  ac.objs = (void **)argv;
  ac.argc = argc;
  ac.offset = 0;
  ac.type = AC_TYPE_CHAR;
  return ac;
}

static void assertArgs(const ArgsCursor& out, std::initializer_list<const char*> expected) {
  ASSERT_EQ(out.argc, expected.size());

  size_t i = 0;
  for (const char* arg : expected) {
    ASSERT_STREQ((const char*)out.objs[i], arg) << "objs[" << i << "]";
    i++;
  }
}

// --- buildRemoteCollectArgs (no rewrite) ---

TEST(DistPlanUtils, RemoteCollectArgs_FieldsOnly) {
  const char *src[] = {"FIELDS", "2", "@a", "@b"};
  ArgsCursor srcArgs = makeArgs(src, 4);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 5> objs;  // collectObjsBufLen(4, /*has_alias=*/false)
  ArgsCursor out = buildRemoteCollectArgs(objs.data(), countStr.c_str(), &srcArgs, nullptr);

  ASSERT_EQ(out.offset, 0u);
  ASSERT_EQ(out.type, AC_TYPE_CHAR);
  assertArgs(out, {"4", "FIELDS", "2", "@a", "@b"});
}

TEST(DistPlanUtils, RemoteCollectArgs_FieldsSortbyLimit) {
  const char *src[] = {"FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "0", "10"};
  ArgsCursor srcArgs = makeArgs(src, 10);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 11> objs;  // collectObjsBufLen(10, /*has_alias=*/false)
  ArgsCursor out = buildRemoteCollectArgs(objs.data(), countStr.c_str(), &srcArgs, nullptr);

  assertArgs(out, {"10", "FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "0",
                   "10"});
  for (size_t i = 0; i < 10; i++) {
    ASSERT_EQ(out.objs[i + 1], srcArgs.objs[i])
        << "objs[" << i + 1 << "] should point to src_args element " << i;
  }
}

TEST(DistPlanUtils, RemoteCollectArgs_EmptyArgs) {
  ArgsCursor srcArgs = makeArgs(nullptr, 0);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 1> objs;  // collectObjsBufLen(0, /*has_alias=*/false)
  ArgsCursor out = buildRemoteCollectArgs(objs.data(), countStr.c_str(), &srcArgs, nullptr);

  assertArgs(out, {"0"});
}

// --- buildLocalCollectArgs ---

TEST(DistPlanUtils, LocalCollectArgs_FieldsOnly) {
  const char *src[] = {"FIELDS", "2", "@a", "@b"};
  ArgsCursor srcArgs = makeArgs(src, 4);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 7> objs;  // collectObjsBufLen(4, /*has_alias=*/true)
  ArgsCursor out = buildLocalCollectArgs(objs.data(), countStr.c_str(), &srcArgs, "my_collect");

  ASSERT_EQ(out.offset, 0u);
  ASSERT_EQ(out.type, AC_TYPE_CHAR);
  assertArgs(out, {"4", "FIELDS", "2", "@a", "@b", "AS", "my_collect"});
}

TEST(DistPlanUtils, LocalCollectArgs_FieldsSortbyLimit) {
  const char *src[] = {"FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "0", "10"};
  ArgsCursor srcArgs = makeArgs(src, 10);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 13> objs;  // collectObjsBufLen(10, /*has_alias=*/true)
  ArgsCursor out = buildLocalCollectArgs(objs.data(), countStr.c_str(), &srcArgs, "user_alias");

  assertArgs(out, {"10", "FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "0",
                   "10", "AS", "user_alias"});

  // Original args forwarded in order
  for (size_t i = 0; i < 10; i++) {
    ASSERT_EQ(out.objs[i + 1], srcArgs.objs[i])
        << "objs[" << i + 1 << "] should point to src_args element " << i;
  }
}

TEST(DistPlanUtils, LocalCollectArgs_EmptyOriginalArgs) {
  ArgsCursor srcArgs = makeArgs(nullptr, 0);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 3> objs;  // collectObjsBufLen(0, /*has_alias=*/true)
  ArgsCursor out = buildLocalCollectArgs(objs.data(), countStr.c_str(), &srcArgs, "ua");

  assertArgs(out, {"0", "AS", "ua"});
}

// --- parseCollectLimit ---

TEST(DistPlanUtils, ParseCollectLimit_NoLimitKeyword) {
  const char *src[] = {"FIELDS", "1", "@x"};
  ArgsCursor srcArgs = makeArgs(src, 3);
  bool present = true;
  CollectLimit limit{};
  QueryError status = QueryError_Default();

  ASSERT_TRUE(parseCollectLimit(&srcArgs, 100, &present, &limit, &status));
  ASSERT_FALSE(present);
  ASSERT_FALSE(QueryError_HasError(&status));
}

TEST(DistPlanUtils, ParseCollectLimit_Valid) {
  const char *src[] = {"FIELDS", "1", "@x", "LIMIT", "5", "10"};
  ArgsCursor srcArgs = makeArgs(src, 6);
  bool present = false;
  CollectLimit limit{};
  QueryError status = QueryError_Default();

  ASSERT_TRUE(parseCollectLimit(&srcArgs, 100, &present, &limit, &status));
  ASSERT_TRUE(present);
  ASSERT_EQ(limit.offset, 5u);
  ASSERT_EQ(limit.count, 10u);
  ASSERT_FALSE(QueryError_HasError(&status));
}

TEST(DistPlanUtils, ParseCollectLimit_ValidOffsetZero) {
  const char *src[] = {"FIELDS", "1", "@x", "LIMIT", "0", "10"};
  ArgsCursor srcArgs = makeArgs(src, 6);
  bool present = false;
  CollectLimit limit{};
  QueryError status = QueryError_Default();

  ASSERT_TRUE(parseCollectLimit(&srcArgs, 100, &present, &limit, &status));
  ASSERT_TRUE(present);
  ASSERT_EQ(limit.offset, 0u);
  ASSERT_EQ(limit.count, 10u);
}

TEST(DistPlanUtils, ParseCollectLimit_ExceedsMax) {
  const char *src[] = {"FIELDS", "1", "@x", "LIMIT", "0", "200"};
  ArgsCursor srcArgs = makeArgs(src, 6);
  bool present = false;
  CollectLimit limit{};
  QueryError status = QueryError_Default();

  ASSERT_FALSE(parseCollectLimit(&srcArgs, 100, &present, &limit, &status));
  ASSERT_TRUE(QueryError_HasError(&status));
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
  ASSERT_TRUE(QueryError_HasError(&status));
  ASSERT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_PARSE_ARGS);
  QueryError_ClearError(&status);
}

TEST(DistPlanUtils, ParseCollectLimit_CountZeroRejected) {
  // AC_F_GE1 on count means 0 is invalid.
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
  // Only one token after LIMIT — AC_GetSlice returns AC_ERR_NOARG.
  const char *src[] = {"FIELDS", "1", "@x", "LIMIT", "5"};
  ArgsCursor srcArgs = makeArgs(src, 5);
  bool present = false;
  CollectLimit limit{};
  QueryError status = QueryError_Default();

  ASSERT_FALSE(parseCollectLimit(&srcArgs, 100, &present, &limit, &status));
  ASSERT_TRUE(QueryError_HasError(&status));
  ASSERT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_PARSE_ARGS);
  QueryError_ClearError(&status);
}

// --- buildRemoteCollectArgs ---

TEST(DistPlanUtils, BuildRemoteCollectArgs_RewritesLimitAtStart) {
  // LIMIT is the very first token — scan must start at i=0 to find it.
  const char *src[] = {"LIMIT", "5", "10"};
  ArgsCursor srcArgs = makeArgs(src, 3);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 4> objs;  // collectObjsBufLen(3, /*has_alias=*/false)
  ShardCollectLimit rewrite{"0", "15"};

  ArgsCursor out = buildRemoteCollectArgs(objs.data(), countStr.c_str(), &srcArgs, &rewrite);
  assertArgs(out, {"3", "LIMIT", "0", "15"});
}

TEST(DistPlanUtils, BuildRemoteCollectArgs_RewritesLimit) {
  const char *src[] = {"FIELDS", "1", "@x", "LIMIT", "5", "10"};
  ArgsCursor srcArgs = makeArgs(src, 6);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 7> objs;  // collectObjsBufLen(6, /*has_alias=*/false)
  ShardCollectLimit rewrite{"0", "15"};

  ArgsCursor out = buildRemoteCollectArgs(objs.data(), countStr.c_str(), &srcArgs, &rewrite);
  assertArgs(out, {"6", "FIELDS", "1", "@x", "LIMIT", "0", "15"});
}

TEST(DistPlanUtils, BuildRemoteCollectArgs_RewritesLimitOffsetZero) {
  // offset=0 + count=10 -> "0" + "10" (no-op rewrite, but path still exercised)
  const char *src[] = {"FIELDS", "1", "@x", "LIMIT", "0", "10"};
  ArgsCursor srcArgs = makeArgs(src, 6);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 7> objs;
  ShardCollectLimit rewrite{"0", "10"};

  ArgsCursor out = buildRemoteCollectArgs(objs.data(), countStr.c_str(), &srcArgs, &rewrite);
  assertArgs(out, {"6", "FIELDS", "1", "@x", "LIMIT", "0", "10"});
}

TEST(DistPlanUtils, BuildRemoteCollectArgs_RewritesLimitWithSortby) {
  // LIMIT appears after SORTBY; the scan must still find and patch it.
  const char *src[] = {"FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "5", "10"};
  ArgsCursor srcArgs = makeArgs(src, 10);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 11> objs;  // collectObjsBufLen(10, /*has_alias=*/false)
  ShardCollectLimit rewrite{"0", "15"};

  ArgsCursor out = buildRemoteCollectArgs(objs.data(), countStr.c_str(), &srcArgs, &rewrite);
  assertArgs(out, {"10", "FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "0", "15"});
}

TEST(DistPlanUtils, BuildRemoteCollectArgs_NullRewriteNoChange) {
  // nullptr rewrite -> tokens forwarded verbatim, original LIMIT values preserved.
  const char *src[] = {"FIELDS", "1", "@x", "LIMIT", "5", "10"};
  ArgsCursor srcArgs = makeArgs(src, 6);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 7> objs;

  ArgsCursor out = buildRemoteCollectArgs(objs.data(), countStr.c_str(), &srcArgs, nullptr);
  // Tokens are forwarded verbatim; original LIMIT values preserved.
  assertArgs(out, {"6", "FIELDS", "1", "@x", "LIMIT", "5", "10"});
  // Pointers alias src_args.
  for (size_t i = 0; i < 6; i++) {
    ASSERT_EQ(out.objs[i + 1], srcArgs.objs[i])
        << "objs[" << i + 1 << "] should alias src_args.objs[" << i << "]";
  }
}

