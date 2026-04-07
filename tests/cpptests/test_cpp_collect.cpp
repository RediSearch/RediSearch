/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "aggregate/reducer.h"
#include "spec.h"
#include "config.h"
#include "result_processor.h"
#include "redismock/redismock.h"

#include <vector>
#include <string>
#include <initializer_list>

class CollectParserTest : public ::testing::Test {
protected:
  RLookup lk;

  void SetUp() override {
    lk = RLookup_New();
  }

  void TearDown() override {
    RLookup_Cleanup(&lk);
  }

  void registerKeys(std::initializer_list<const char *> names) {
    for (const char *name : names) {
      RLookup_GetKey_Write(&lk, name, RLOOKUP_F_NOFLAGS);
    }
  }

  Reducer *parseCollect(std::vector<const char *> &args, QueryError *status) {
    ArgsCursor ac;
    ArgsCursor_InitCString(&ac, args.data(), args.size());
    ReducerOptions opts = REDUCEROPTS_INIT("COLLECT", &ac, &lk, NULL, status, true);
    return RDCRCollect_New(&opts);
  }
};

// ====== Happy path tests ======

TEST_F(CollectParserTest, FieldsOnly) {
  registerKeys({"price", "name"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {"FIELDS", "2", "@price", "@name"};
  Reducer *r = parseCollect(args, &status);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&status);
  r->Free(r);
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, FieldsWildcard) {
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {"FIELDS", "1", "*"};
  Reducer *r = parseCollect(args, &status);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&status);
  r->Free(r);
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, WildcardAmongFields) {
  registerKeys({"price", "name"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {"FIELDS", "3", "@price", "*", "@name"};
  Reducer *r = parseCollect(args, &status);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&status);
  r->Free(r);
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, FieldsAndSortBy) {
  registerKeys({"price", "name"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "2", "@price", "@name",
    "SORTBY", "3", "@price", "DESC", "@name"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&status);
  r->Free(r);
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, FieldsSortByAndLimit) {
  registerKeys({"price"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "@price",
    "SORTBY", "2", "@price", "ASC",
    "LIMIT", "0", "10"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&status);
  r->Free(r);
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, FieldsAndLimitWithoutSortBy) {
  registerKeys({"price"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "@price",
    "LIMIT", "5", "100"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&status);
  r->Free(r);
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, MultipleSortKeysWithDirections) {
  registerKeys({"a", "b", "c"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "3", "@a", "@b", "@c",
    "SORTBY", "5", "@a", "ASC", "@b", "DESC", "@c"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&status);
  r->Free(r);
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, SortByDefaultsToAscending) {
  registerKeys({"price"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "@price",
    "SORTBY", "1", "@price"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&status);
  r->Free(r);
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, SortByMaxFields) {
  registerKeys({"x", "a", "b", "c", "d", "e", "f", "g", "h"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "@x",
    "SORTBY", "8", "@a", "@b", "@c", "@d", "@e", "@f", "@g", "@h"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&status);
  r->Free(r);
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, LimitZeroOffset) {
  registerKeys({"x"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "@x",
    "LIMIT", "0", "50"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&status);
  r->Free(r);
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, JsonPathField) {
  registerKeys({"$..price"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {"FIELDS", "1", "$..price"};
  Reducer *r = parseCollect(args, &status);
  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&status);
  r->Free(r);
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, SortByJsonPathRejected) {
  registerKeys({"$..price"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "$..price",
    "SORTBY", "1", "$..price"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status),
    "MISSING ASC or DESC after sort field");
  QueryError_ClearError(&status);
}

// ====== Validation / error tests ======

TEST_F(CollectParserTest, EmptyArgs) {
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {};
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status), "Bad arguments for COLLECT");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, MissingFieldsRequired) {
  registerKeys({"price"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {"SORTBY", "1", "@price"};
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status), "Bad arguments for COLLECT");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, FieldWithoutAtPrefix) {
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {"FIELDS", "1", "price"};
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status),
    "Missing prefix: name requires '@' prefix, JSON path require '$' prefix");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, FieldEmptyAfterAt) {
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {"FIELDS", "1", "@"};
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status), "Property not loaded nor in pipeline");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, FieldNotInPipeline) {
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {"FIELDS", "1", "@nonexistent"};
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status), "Property not loaded nor in pipeline");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, DuplicateWildcard) {
  registerKeys({"price"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {"FIELDS", "3", "*", "@price", "*"};
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status),
    "Wildcard `*` can only appear once in FIELDS");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, UnknownSubcommand) {
  registerKeys({"x"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "@x",
    "FOO", "1", "2"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status), "Bad arguments for COLLECT");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, SortByFieldWithoutAtPrefix) {
  registerKeys({"x"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "@x",
    "SORTBY", "1", "bad_field"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status),
    "MISSING ASC or DESC after sort field");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, SortByTooManyFields) {
  registerKeys({"x", "a", "b", "c", "d", "e", "f", "g", "h", "i"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "@x",
    "SORTBY", "9", "@a", "@b", "@c", "@d", "@e", "@f", "@g", "@h", "@i"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status),
    "SORTBY exceeds maximum of 8 fields");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, SortByExceedsMaxTokens) {
  registerKeys({"x", "a", "b", "c", "d", "e", "f", "g", "h", "i"});
  QueryError status = QueryError_Default();
  // 2 * SORTASCMAP_MAXFIELDS + 1 = 17 tokens — exceeds parser max_args
  std::vector<const char *> args = {
    "FIELDS", "1", "@x",
    "SORTBY", "17",
    "@a", "ASC", "@b", "ASC", "@c", "ASC", "@d", "ASC",
    "@e", "ASC", "@f", "ASC", "@g", "ASC", "@h", "ASC",
    "@i"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status), "Bad arguments for COLLECT");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, FieldsExceedsMax) {
  QueryError status = QueryError_Default();
  // Build FIELDS with SPEC_MAX_FIELDS + 1 = 1025 entries
  std::vector<const char *> args;
  args.push_back("FIELDS");
  args.push_back("1025");
  std::vector<std::string> field_strs(1025);
  for (int i = 0; i < 1025; i++) {
    field_strs[i] = "@f" + std::to_string(i);
    args.push_back(field_strs[i].c_str());
  }
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status),
    "FIELDS count exceeds maximum of 1024");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, SortByInvalidTokenBetweenFields) {
  registerKeys({"x", "a", "b"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "@x",
    "SORTBY", "3", "@a", "INVALID", "@b"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status),
    "MISSING ASC or DESC after sort field");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, LimitNegativeOffset) {
  registerKeys({"x"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "@x",
    "LIMIT", "-1", "10"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status),
    "LIMIT offset must be a non-negative integer");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, LimitNegativeCount) {
  registerKeys({"x"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "@x",
    "LIMIT", "0", "-5"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status),
    "LIMIT count must be a non-negative integer");
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, LimitNonNumericOffset) {
  registerKeys({"x"});
  QueryError status = QueryError_Default();
  std::vector<const char *> args = {
    "FIELDS", "1", "@x",
    "LIMIT", "abc", "10"
  };
  Reducer *r = parseCollect(args, &status);
  ASSERT_EQ(r, nullptr);
  EXPECT_STREQ(QueryError_GetUserError(&status),
    "LIMIT offset must be a non-negative integer");
  QueryError_ClearError(&status);
}
