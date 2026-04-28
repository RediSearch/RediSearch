/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// TODO: This file is temporary. Migrate these tests to Python flow tests
// (`test_groupby_collect.py`) and delete this file along with the FFI
// accessors in `reducers_ffi/src/collect.rs` and `reducers/src/collect.rs`.

#include "gtest/gtest.h"

#include "aggregate/reducer.h"
#include "reducers_rs.h"
#include "spec.h"
#include "config.h"
#include "result_processor.h"
#include "redismock/redismock.h"

#include <initializer_list>
#include <string>
#include <vector>

class CollectParserTest : public ::testing::Test {
protected:
  RLookup lk;

  void SetUp() override {
    RSGlobalConfig.enableUnstableFeatures = true;
    lk = RLookup_New();
  }

  void TearDown() override {
    RLookup_Cleanup(&lk);
    RSGlobalConfig.enableUnstableFeatures = false;
  }

  void registerKeys(std::initializer_list<const char *> names) {
    for (const char *name : names) {
      RLookup_GetKey_Write(&lk, name, RLOOKUP_F_NOFLAGS);
    }
  }

  Reducer *parseCollect(std::vector<const char *> &args, QueryError *status,
                        bool isLocal = false, const RLookupKey *inputKey = NULL) {
    ArgsCursor ac;
    ArgsCursor_InitCString(&ac, args.data(), args.size());
    ReducerOptions opts = REDUCEROPTS_INIT("COLLECT", &ac, &lk, NULL, status, true, isLocal,
                                           inputKey, 0);
    return RDCRCollect_New(&opts);
  }

  void expectError(std::vector<const char *> args, const char *expected_detail) {
    QueryError status = QueryError_Default();
    Reducer *r = parseCollect(args, &status);
    ASSERT_EQ(r, nullptr) << "Expected parse failure but got success";
    const char *user_error = QueryError_GetUserError(&status);
    ASSERT_NE(user_error, nullptr);
    EXPECT_TRUE(std::string(user_error).find(expected_detail) != std::string::npos)
        << "Expected error containing: " << expected_detail << ", got: " << user_error;
    QueryError_ClearError(&status);
  }

  Reducer *parseCollectOk(std::vector<const char *> args) {
    QueryError status = QueryError_Default();
    Reducer *r = parseCollect(args, &status);
    EXPECT_NE(r, nullptr) << QueryError_GetUserError(&status);
    QueryError_ClearError(&status);
    return r;
  }
};

// ====== Happy path tests ======

TEST_F(CollectParserTest, WildcardOnlyRejected) {
  expectError({"FIELDS", "1", "*"},
      "COLLECT does not yet support `*` in FIELDS");
}

TEST_F(CollectParserTest, WildcardAmongFieldsRejected) {
  registerKeys({"price", "name"});
  expectError({"FIELDS", "3", "@price", "*", "@name"},
      "COLLECT does not yet support `*` in FIELDS");
}

TEST_F(CollectParserTest, FieldsAndSortBy) {
  registerKeys({"price", "name"});
  Reducer *r = parseCollectOk({
      "FIELDS", "2", "@price", "@name",
      "SORTBY", "3", "@price", "DESC", "@name",
  });
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(CollectReducer_GetFieldKeysLen(r), 2u);
  EXPECT_EQ(CollectReducer_GetSortKeysLen(r), 2u);
  EXPECT_FALSE(SORTASCMAP_GETASC(CollectReducer_GetSortAscMap(r), 0));
  EXPECT_TRUE(SORTASCMAP_GETASC(CollectReducer_GetSortAscMap(r), 1));
  r->Free(r);
}

TEST_F(CollectParserTest, LocalFieldsUsePlannerInputKey) {
  const RLookupKey *inputKey = RLookup_GetKey_Write(&lk, "remote_collect", RLOOKUP_F_NOFLAGS);
  std::vector<const char *> args = {"FIELDS", "2", "@price", "@name"};
  QueryError status = QueryError_Default();

  Reducer *r = parseCollect(args, &status, true, inputKey);

  ASSERT_NE(r, nullptr) << QueryError_GetUserError(&status);
  r->Free(r);
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, LocalCollectRequiresPlannerInputKey) {
  std::vector<const char *> args = {"FIELDS", "1", "@price"};
  QueryError status = QueryError_Default();

  Reducer *r = parseCollect(args, &status, true, NULL);

  ASSERT_EQ(r, nullptr);
  const char *user_error = QueryError_GetUserError(&status);
  ASSERT_NE(user_error, nullptr);
  EXPECT_TRUE(std::string(user_error).find("COLLECT input key was not provided") !=
              std::string::npos)
      << user_error;
  QueryError_ClearError(&status);
}

TEST_F(CollectParserTest, FieldsSortByAndLimit) {
  registerKeys({"price"});
  Reducer *r = parseCollectOk({
      "FIELDS", "1", "@price",
      "SORTBY", "2", "@price", "ASC",
      "LIMIT", "0", "10",
  });
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(CollectReducer_GetSortKeysLen(r), 1u);
  EXPECT_TRUE(SORTASCMAP_GETASC(CollectReducer_GetSortAscMap(r), 0));
  EXPECT_TRUE(CollectReducer_HasLimit(r));
  EXPECT_EQ(CollectReducer_GetLimitOffset(r), 0u);
  EXPECT_EQ(CollectReducer_GetLimitCount(r), 10u);
  r->Free(r);
}

TEST_F(CollectParserTest, FieldsAndLimitWithoutSortBy) {
  registerKeys({"price"});
  Reducer *r = parseCollectOk({
      "FIELDS", "1", "@price",
      "LIMIT", "5", "100",
  });
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(CollectReducer_GetSortKeysLen(r), 0u);
  EXPECT_TRUE(CollectReducer_HasLimit(r));
  EXPECT_EQ(CollectReducer_GetLimitOffset(r), 5u);
  EXPECT_EQ(CollectReducer_GetLimitCount(r), 100u);
  r->Free(r);
}

TEST_F(CollectParserTest, LimitBeforeSortByIsValid) {
  registerKeys({"price"});
  Reducer *r = parseCollectOk({
      "FIELDS", "1", "@price",
      "LIMIT", "0", "10",
      "SORTBY", "2", "@price", "ASC",
  });
  ASSERT_NE(r, nullptr);
  EXPECT_TRUE(CollectReducer_HasLimit(r));
  EXPECT_EQ(CollectReducer_GetLimitOffset(r), 0u);
  EXPECT_EQ(CollectReducer_GetLimitCount(r), 10u);
  EXPECT_EQ(CollectReducer_GetSortKeysLen(r), 1u);
  EXPECT_TRUE(SORTASCMAP_GETASC(CollectReducer_GetSortAscMap(r), 0));
  r->Free(r);
}

TEST_F(CollectParserTest, MultipleSortKeysWithDirections) {
  registerKeys({"a", "b", "c"});
  Reducer *r = parseCollectOk({
      "FIELDS", "3", "@a", "@b", "@c",
      "SORTBY", "5", "@a", "ASC", "@b", "DESC", "@c",
  });
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(CollectReducer_GetSortKeysLen(r), 3u);
  uint64_t map = CollectReducer_GetSortAscMap(r);
  EXPECT_TRUE(SORTASCMAP_GETASC(map, 0));
  EXPECT_FALSE(SORTASCMAP_GETASC(map, 1));
  EXPECT_TRUE(SORTASCMAP_GETASC(map, 2));
  r->Free(r);
}

TEST_F(CollectParserTest, SortByDefaultsToAscending) {
  registerKeys({"price"});
  Reducer *r = parseCollectOk({
      "FIELDS", "1", "@price",
      "SORTBY", "1", "@price",
  });
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(CollectReducer_GetSortKeysLen(r), 1u);
  EXPECT_TRUE(SORTASCMAP_GETASC(CollectReducer_GetSortAscMap(r), 0));
  r->Free(r);
}

TEST_F(CollectParserTest, SortByConsecutiveFieldsDefaultAsc) {
  registerKeys({"a", "b", "c"});
  Reducer *r = parseCollectOk({
      "FIELDS", "1", "@a",
      "SORTBY", "3", "@a", "@b", "@c",
  });
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(CollectReducer_GetSortKeysLen(r), 3u);
  uint64_t map = CollectReducer_GetSortAscMap(r);
  EXPECT_TRUE(SORTASCMAP_GETASC(map, 0));
  EXPECT_TRUE(SORTASCMAP_GETASC(map, 1));
  EXPECT_TRUE(SORTASCMAP_GETASC(map, 2));
  r->Free(r);
}

TEST_F(CollectParserTest, SortByMixedConsecutiveFieldsAndDirections) {
  registerKeys({"a", "b", "c", "d", "e", "f"});
  Reducer *r = parseCollectOk({
      "FIELDS", "1", "@a",
      "SORTBY", "8", "@a", "@b", "@c", "ASC", "@d", "DESC", "@e", "@f",
  });
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(CollectReducer_GetSortKeysLen(r), 6u);
  uint64_t map = CollectReducer_GetSortAscMap(r);
  EXPECT_TRUE(SORTASCMAP_GETASC(map, 0));
  EXPECT_TRUE(SORTASCMAP_GETASC(map, 1));
  EXPECT_TRUE(SORTASCMAP_GETASC(map, 2));
  EXPECT_FALSE(SORTASCMAP_GETASC(map, 3));
  EXPECT_TRUE(SORTASCMAP_GETASC(map, 4));
  EXPECT_TRUE(SORTASCMAP_GETASC(map, 5));
  r->Free(r);
}

TEST_F(CollectParserTest, SortByMaxFields) {
  registerKeys({"x", "a", "b", "c", "d", "e", "f", "g", "h"});
  Reducer *r = parseCollectOk({
      "FIELDS", "1", "@x",
      "SORTBY", "8", "@a", "@b", "@c", "@d", "@e", "@f", "@g", "@h",
  });
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(CollectReducer_GetSortKeysLen(r), 8u);
  uint64_t map = CollectReducer_GetSortAscMap(r);
  for (int i = 0; i < 8; i++) {
    EXPECT_TRUE(SORTASCMAP_GETASC(map, i)) << "sort key " << i;
  }
  r->Free(r);
}

TEST_F(CollectParserTest, LimitZeroOffset) {
  registerKeys({"x"});
  Reducer *r = parseCollectOk({
      "FIELDS", "1", "@x",
      "LIMIT", "0", "50",
  });
  ASSERT_NE(r, nullptr);
  EXPECT_TRUE(CollectReducer_HasLimit(r));
  EXPECT_EQ(CollectReducer_GetLimitOffset(r), 0u);
  EXPECT_EQ(CollectReducer_GetLimitCount(r), 50u);
  r->Free(r);
}

TEST_F(CollectParserTest, JsonPathField) {
  registerKeys({"$..price"});
  Reducer *r = parseCollectOk({"FIELDS", "1", "$..price"});
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(CollectReducer_GetFieldKeysLen(r), 1u);
  EXPECT_FALSE(CollectReducer_HasWildcard(r));
  r->Free(r);
}

// ====== Validation / error tests ======

TEST_F(CollectParserTest, SortByJsonPathRejected) {
  registerKeys({"$..price"});
  expectError(
      {"FIELDS", "1", "$..price", "SORTBY", "1", "$..price"},
      "Missing prefix: name requires '@' prefix");
}

TEST_F(CollectParserTest, EmptyArgs) {
  expectError({}, "FIELDS: Required positional argument missing or out of order");
}

TEST_F(CollectParserTest, MissingFieldsRequired) {
  registerKeys({"price"});
  expectError({"SORTBY", "1", "@price"}, "FIELDS: Required positional argument missing or out of order");
}

TEST_F(CollectParserTest, FieldsMustBeFirstParam) {
  registerKeys({"price"});
  expectError({"SORTBY", "1", "@price", "FIELDS", "1", "@price"},
      "FIELDS: Required positional argument missing or out of order");
}

TEST_F(CollectParserTest, FieldWithoutAtPrefix) {
  expectError({"FIELDS", "1", "price"},
      "Missing prefix: name requires '@' prefix, JSON path require '$' prefix");
}

TEST_F(CollectParserTest, FieldEmptyAfterAt) {
  expectError({"FIELDS", "1", "@"}, "Property not loaded nor in pipeline");
}

TEST_F(CollectParserTest, FieldNotInPipeline) {
  expectError({"FIELDS", "1", "@nonexistent"}, "Property not loaded nor in pipeline");
}

TEST_F(CollectParserTest, FieldsSecondFieldNotInPipeline) {
  registerKeys({"price"});
  expectError({"FIELDS", "2", "@price", "@unknown"},
      "Property not loaded nor in pipeline");
}

TEST_F(CollectParserTest, DuplicateWildcard) {
  registerKeys({"price"});
  expectError({"FIELDS", "3", "*", "@price", "*"}, "`*` can only appear once in FIELDS");
}

TEST_F(CollectParserTest, UnknownSubcommand) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "FOO", "1", "2"}, "Bad arguments for COLLECT: FOO: Unknown argument");
}

TEST_F(CollectParserTest, SortByFieldNotInPipeline) {
  registerKeys({"x"});
  expectError(
      {"FIELDS", "1", "@x", "SORTBY", "1", "@unknown"},
      "Property not loaded nor in pipeline");
}

TEST_F(CollectParserTest, SortByZeroCount) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "0"},
      "Bad arguments for COLLECT: SORTBY: Invalid argument count");
}

TEST_F(CollectParserTest, SortByFieldWithoutAtPrefix) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "1", "bad_field"},
      "Missing prefix: name requires '@' prefix");
}

TEST_F(CollectParserTest, SortByTooManyFields) {
  registerKeys({"x", "a", "b", "c", "d", "e", "f", "g", "h", "i"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "9", "@a", "@b", "@c", "@d", "@e", "@f", "@g", "@h", "@i"},
      "SORTBY exceeds maximum of 8 fields");
}

TEST_F(CollectParserTest, SortByExceedsMaxTokens) {
  registerKeys({"x", "a", "b", "c", "d", "e", "f", "g", "h", "i"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "17", "@a", "ASC", "@b", "ASC", "@c", "ASC", "@d", "ASC",
                  "@e", "ASC", "@f", "ASC", "@g", "ASC", "@h", "ASC", "@i"},
      "Bad arguments for COLLECT: SORTBY: Invalid argument count");
}

TEST_F(CollectParserTest, FieldsExceedsMax) {
  std::vector<const char *> args;
  args.push_back("FIELDS");
  args.push_back("1026");
  std::vector<std::string> field_strs(1026);
  for (int i = 0; i < 1026; i++) {
    field_strs[i] = "@f" + std::to_string(i);
    args.push_back(field_strs[i].c_str());
  }
  expectError(std::move(args), "Bad arguments for COLLECT: FIELDS: Invalid argument count");
}

TEST_F(CollectParserTest, SortByInvalidTokenBetweenFields) {
  registerKeys({"x", "a", "b"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "3", "@a", "INVALID", "@b"},
      "Missing prefix: name requires '@' prefix");
}

TEST_F(CollectParserTest, LimitNegativeOffset) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "LIMIT", "-1", "10"},
      "LIMIT offset must be a non-negative integer");
}

TEST_F(CollectParserTest, LimitNegativeCount) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "LIMIT", "0", "-5"},
      "LIMIT count must be a positive integer");
}

TEST_F(CollectParserTest, LimitZeroCountWithNonZeroOffset) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "LIMIT", "10", "0"},
      "LIMIT count must be a positive integer");
}

TEST_F(CollectParserTest, LimitZeroCountWithZeroOffset) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "LIMIT", "0", "0"},
      "LIMIT count must be a positive integer");
}

TEST_F(CollectParserTest, LimitNonNumericOffset) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "LIMIT", "abc", "10"},
      "LIMIT offset must be a non-negative integer");
}

TEST_F(CollectParserTest, FieldsZeroCountRequiresAtLeastOne) {
  expectError({"FIELDS", "0"}, "Bad arguments for COLLECT: FIELDS: Invalid argument count");
}

TEST_F(CollectParserTest, SortByOnlyDirectionsNoFields) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "1", "ASC"},
      "Missing prefix: name requires '@' prefix");
}

TEST_F(CollectParserTest, SortByDescBeforeFirstSortField) {
  registerKeys({"x", "price"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "2", "DESC", "@price"},
      "Missing prefix: name requires '@' prefix");
}

TEST_F(CollectParserTest, SortByDuplicateAscAfterField) {
  registerKeys({"x", "price"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "3", "@price", "ASC", "ASC"},
      "Missing prefix: name requires '@' prefix");
}

TEST_F(CollectParserTest, LimitCountExceedsAggregateMax) {
  registerKeys({"x"});
  std::string count =
      std::to_string(static_cast<unsigned long long>(MAX_AGGREGATE_REQUEST_RESULTS) + 1ULL);
  expectError({"FIELDS", "1", "@x", "LIMIT", "0", count.c_str()},
      "LIMIT count exceeds maximum of");
}

TEST_F(CollectParserTest, LimitOffsetExceedsAggregateMax) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "LIMIT", "9999999999", "10"},
      "LIMIT offset exceeds maximum of");
}

