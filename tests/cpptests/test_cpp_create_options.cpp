/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "spec.h"
#include <vector>

class CreateOptionsTest : public ::testing::Test {
 protected:
  QueryError error = QueryError_Default();
  IndexSpecOptions options = {};
  ArgsCursor cursor = {};
  std::vector<const char*> args;

  int parse(std::initializer_list<const char*> input, bool flex = false) {
    args = input;
    ArgsCursor_InitCString(&cursor, args.data(), args.size());
    QueryError_ClearError(&error);
    return IndexSpec_ParseOptions(&cursor, flex, &options, &error);
  }

  void TearDown() override {
    QueryError_ClearError(&error);
  }
};

TEST_F(CreateOptionsTest, DefaultsAndSchemaBoundary) {
  for (bool flex : {false, true}) {
    ASSERT_EQ(REDISMODULE_OK, parse({"SCHEMA", "TEMPORARY", "TEXT"}, flex));
    EXPECT_EQ(INDEX_DEFAULT_FLAGS, options.flags);
    EXPECT_EQ(-1000, options.timeout);
    EXPECT_FALSE(AC_IsInitialized(&options.prefixes));
    EXPECT_FALSE(AC_IsInitialized(&options.stopwords));
    EXPECT_EQ(3, AC_NumRemaining(&cursor));
    EXPECT_STREQ("SCHEMA", AC_GetStringNC(&cursor, nullptr));
    ASSERT_EQ(REDISMODULE_OK, parse({}, flex));
    EXPECT_EQ(0, AC_NumRemaining(&cursor));
  }
}

TEST_F(CreateOptionsTest, BorrowedListsAndOptionLookingValues) {
  ASSERT_EQ(REDISMODULE_OK, parse({"PREFIX", "2", "TEMPORARY", "SCHEMA", "STOPWORDS", "2", "PREFIX",
                                   "TEMPORARY", "SCHEMA", "TEMPORARY", "TEXT"}));
  EXPECT_FALSE(options.flags & Index_Temporary);
  EXPECT_EQ(2, AC_NumRemaining(&options.prefixes));
  EXPECT_EQ(2, AC_NumRemaining(&options.stopwords));
  ArgsCursor prefixes = options.prefixes;
  EXPECT_STREQ("TEMPORARY", AC_GetStringNC(&prefixes, nullptr));
  EXPECT_STREQ("SCHEMA", AC_GetStringNC(&prefixes, nullptr));
  EXPECT_EQ(2, AC_NumRemaining(&options.prefixes));
  EXPECT_STREQ("PREFIX", AC_GetStringNC(&options.stopwords, nullptr));
  EXPECT_EQ(3, AC_NumRemaining(&cursor));
}

TEST_F(CreateOptionsTest, MalformedOptionsAndFailureCursor) {
  ASSERT_EQ(REDISMODULE_ERR, parse({"TEMPORARY", "bad", "SCHEMA"}));
  EXPECT_TRUE(QueryError_HasError(&error));
  EXPECT_EQ(2, AC_NumRemaining(&cursor));
  EXPECT_STREQ("bad", AC_GetStringNC(&cursor, nullptr));
}

TEST_F(CreateOptionsTest, DeferredUnknownAndUnsupportedOptions) {
  ASSERT_EQ(REDISMODULE_OK, parse({"TEMPORARY", "1", "SCHEMA"}, true));
  EXPECT_TRUE(options.unsupportedOption);
  EXPECT_EQ(3, AC_NumRemaining(&cursor));
  EXPECT_FALSE(QueryError_HasError(&error));
  for (bool flex : {false, true}) {
    ASSERT_EQ(REDISMODULE_OK, parse({"UNKNOWN", "SCHEMA"}, flex));
    EXPECT_FALSE(options.unsupportedOption);
    EXPECT_EQ(2, AC_NumRemaining(&cursor));
    EXPECT_FALSE(QueryError_HasError(&error));
  }
}
