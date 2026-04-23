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

static ArgsCursor makeArgs(const char **argv, size_t argc) {
  ArgsCursor ac;
  ac.objs = (void **)argv;
  ac.argc = argc;
  ac.offset = 0;
  ac.type = AC_TYPE_CHAR;
  return ac;
}

// --- buildShardCollectArgs ---

TEST(DistPlanUtils, ShardCollectArgs_FieldsOnly) {
  const char *src[] = {"FIELDS", "2", "@a", "@b"};
  ArgsCursor srcArgs = makeArgs(src, 4);

  char countBuf[16];
  void *objs[5];  // argc + 1
  ArgsCursor out;
  buildShardCollectArgs(&out, objs, countBuf, &srcArgs);

  ASSERT_EQ(out.argc, 5u);
  ASSERT_EQ(out.offset, 0u);
  ASSERT_EQ(out.type, AC_TYPE_CHAR);
  ASSERT_STREQ((const char *)out.objs[0], "4");
  ASSERT_STREQ((const char *)out.objs[1], "FIELDS");
  ASSERT_STREQ((const char *)out.objs[2], "2");
  ASSERT_STREQ((const char *)out.objs[3], "@a");
  ASSERT_STREQ((const char *)out.objs[4], "@b");
}

TEST(DistPlanUtils, ShardCollectArgs_FieldsSortbyLimit) {
  const char *src[] = {"FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "0", "10"};
  ArgsCursor srcArgs = makeArgs(src, 10);

  char countBuf[16];
  void *objs[11];  // argc + 1
  ArgsCursor out;
  buildShardCollectArgs(&out, objs, countBuf, &srcArgs);

  ASSERT_EQ(out.argc, 11u);
  ASSERT_STREQ((const char *)out.objs[0], "10");
  for (size_t i = 0; i < 10; i++) {
    ASSERT_EQ(out.objs[i + 1], srcArgs.objs[i])
        << "objs[" << i + 1 << "] should point to src_args element " << i;
  }
}

TEST(DistPlanUtils, ShardCollectArgs_EmptyArgs) {
  ArgsCursor srcArgs = makeArgs(nullptr, 0);

  char countBuf[16];
  void *objs[1];  // argc + 1
  ArgsCursor out;
  buildShardCollectArgs(&out, objs, countBuf, &srcArgs);

  ASSERT_EQ(out.argc, 1u);
  ASSERT_STREQ((const char *)out.objs[0], "0");
}

// --- buildCoordCollectArgs ---

TEST(DistPlanUtils, CoordCollectArgs_FieldsOnly) {
  const char *src[] = {"FIELDS", "2", "@a", "@b"};
  ArgsCursor srcArgs = makeArgs(src, 4);

  char countBuf[16];
  void *objs[9];  // argc + 5
  ArgsCursor out;
  buildCoordCollectArgs(&out, objs, countBuf, &srcArgs, "__collect_ab", "my_collect");

  ASSERT_EQ(out.argc, 9u);
  ASSERT_EQ(out.offset, 0u);
  ASSERT_EQ(out.type, AC_TYPE_CHAR);
  ASSERT_STREQ((const char *)out.objs[0], "6");       // argc + 2
  ASSERT_STREQ((const char *)out.objs[1], "FIELDS");
  ASSERT_STREQ((const char *)out.objs[2], "2");
  ASSERT_STREQ((const char *)out.objs[3], "@a");
  ASSERT_STREQ((const char *)out.objs[4], "@b");
  ASSERT_STREQ((const char *)out.objs[5], "__SOURCE__");
  ASSERT_STREQ((const char *)out.objs[6], "__collect_ab");
  ASSERT_STREQ((const char *)out.objs[7], "AS");
  ASSERT_STREQ((const char *)out.objs[8], "my_collect");
}

TEST(DistPlanUtils, CoordCollectArgs_FieldsSortbyLimit) {
  const char *src[] = {"FIELDS", "1", "@x", "SORTBY", "2", "@x", "ASC", "LIMIT", "0", "10"};
  ArgsCursor srcArgs = makeArgs(src, 10);

  char countBuf[16];
  void *objs[15];  // argc + 5
  ArgsCursor out;
  buildCoordCollectArgs(&out, objs, countBuf, &srcArgs, "shard_alias", "user_alias");

  ASSERT_EQ(out.argc, 15u);
  ASSERT_STREQ((const char *)out.objs[0], "12");  // argc + 2

  // Original args forwarded in order
  for (size_t i = 0; i < 10; i++) {
    ASSERT_EQ(out.objs[i + 1], srcArgs.objs[i])
        << "objs[" << i + 1 << "] should point to src_args element " << i;
  }

  // Appended __SOURCE__ block (no explicit count because __SOURCE__ is
  // registered as fixed-arity in the reducer parser)
  ASSERT_STREQ((const char *)out.objs[11], "__SOURCE__");
  ASSERT_STREQ((const char *)out.objs[12], "shard_alias");

  // AS + alias outside the counted block
  ASSERT_STREQ((const char *)out.objs[13], "AS");
  ASSERT_STREQ((const char *)out.objs[14], "user_alias");
}

TEST(DistPlanUtils, CoordCollectArgs_EmptyOriginalArgs) {
  ArgsCursor srcArgs = makeArgs(nullptr, 0);

  char countBuf[16];
  void *objs[5];  // argc + 5
  ArgsCursor out;
  buildCoordCollectArgs(&out, objs, countBuf, &srcArgs, "sa", "ua");

  ASSERT_EQ(out.argc, 5u);
  ASSERT_STREQ((const char *)out.objs[0], "2");  // 0 + 2
  ASSERT_STREQ((const char *)out.objs[1], "__SOURCE__");
  ASSERT_STREQ((const char *)out.objs[2], "sa");
  ASSERT_STREQ((const char *)out.objs[3], "AS");
  ASSERT_STREQ((const char *)out.objs[4], "ua");
}
