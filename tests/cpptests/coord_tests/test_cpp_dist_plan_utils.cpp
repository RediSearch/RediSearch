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

// --- buildCollectArgs remote ---

TEST(DistPlanUtils, ShardCollectArgs_FieldsOnly) {
  const char *src[] = {"FIELDS", "2", "@a", "@b"};
  ArgsCursor srcArgs = makeArgs(src, 4);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 5> objs;  // collectObjsBufLen(4, /*has_alias=*/false)
  ArgsCursor out = buildCollectArgs(objs.data(), countStr.c_str(), &srcArgs, nullptr);

  ASSERT_EQ(out.offset, 0u);
  ASSERT_EQ(out.type, AC_TYPE_CHAR);
  assertArgs(out, {"4", "FIELDS", "2", "@a", "@b"});
}

TEST(DistPlanUtils, ShardCollectArgs_FieldsSortbyLimit) {
  const char *src[] = {"FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "0", "10"};
  ArgsCursor srcArgs = makeArgs(src, 10);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 11> objs;  // collectObjsBufLen(10, /*has_alias=*/false)
  ArgsCursor out = buildCollectArgs(objs.data(), countStr.c_str(), &srcArgs, nullptr);

  assertArgs(out, {"10", "FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "0",
                   "10"});
  for (size_t i = 0; i < 10; i++) {
    ASSERT_EQ(out.objs[i + 1], srcArgs.objs[i])
        << "objs[" << i + 1 << "] should point to src_args element " << i;
  }
}

TEST(DistPlanUtils, ShardCollectArgs_EmptyArgs) {
  ArgsCursor srcArgs = makeArgs(nullptr, 0);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 1> objs;  // collectObjsBufLen(0, /*has_alias=*/false)
  ArgsCursor out = buildCollectArgs(objs.data(), countStr.c_str(), &srcArgs, nullptr);

  assertArgs(out, {"0"});
}

// --- buildCollectArgs local ---

TEST(DistPlanUtils, CoordCollectArgs_FieldsOnly) {
  const char *src[] = {"FIELDS", "2", "@a", "@b"};
  ArgsCursor srcArgs = makeArgs(src, 4);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 7> objs;  // collectObjsBufLen(4, /*has_alias=*/true)
  ArgsCursor out = buildCollectArgs(objs.data(), countStr.c_str(), &srcArgs, "my_collect");

  ASSERT_EQ(out.offset, 0u);
  ASSERT_EQ(out.type, AC_TYPE_CHAR);
  assertArgs(out, {"4", "FIELDS", "2", "@a", "@b", "AS", "my_collect"});
}

TEST(DistPlanUtils, CoordCollectArgs_FieldsSortbyLimit) {
  const char *src[] = {"FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "0", "10"};
  ArgsCursor srcArgs = makeArgs(src, 10);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 13> objs;  // collectObjsBufLen(10, /*has_alias=*/true)
  ArgsCursor out = buildCollectArgs(objs.data(), countStr.c_str(), &srcArgs, "user_alias");

  assertArgs(out, {"10", "FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "0",
                   "10", "AS", "user_alias"});

  // Original args forwarded in order
  for (size_t i = 0; i < 10; i++) {
    ASSERT_EQ(out.objs[i + 1], srcArgs.objs[i])
        << "objs[" << i + 1 << "] should point to src_args element " << i;
  }
}

TEST(DistPlanUtils, CoordCollectArgs_EmptyOriginalArgs) {
  ArgsCursor srcArgs = makeArgs(nullptr, 0);

  std::string countStr = std::to_string(srcArgs.argc);
  std::array<void *, 3> objs;  // collectObjsBufLen(0, /*has_alias=*/true)
  ArgsCursor out = buildCollectArgs(objs.data(), countStr.c_str(), &srcArgs, "ua");

  assertArgs(out, {"0", "AS", "ua"});
}
