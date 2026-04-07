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

/*
 * Layout must match CollectReducer in src/aggregate/reducers/collect.c exactly
 * (member order and types). If you change the implementation struct, update this
 * copy or parser tests may read wrong memory / invoke undefined behavior.
 */
struct CollectReducer {
  Reducer base;

  int num_fields;
  const RLookupKey **field_keys;
  bool has_wildcard;

  int num_sort_keys;
  const RLookupKey **sort_keys;
  uint64_t sortAscMap;

  bool has_limit;
  uint64_t limit_offset;
  uint64_t limit_count;
};

#include "spec.h"
#include "config.h"
#include "result_processor.h"
#include "redismock/redismock.h"

#include <climits>
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

  Reducer *parseCollect(std::vector<const char *> &args, QueryError *status) {
    ArgsCursor ac;
    ArgsCursor_InitCString(&ac, args.data(), args.size());
    ReducerOptions opts = REDUCEROPTS_INIT("COLLECT", &ac, &lk, NULL, status, true);
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

  CollectReducer *parseCollectOk(std::vector<const char *> args) {
    QueryError status = QueryError_Default();
    Reducer *r = parseCollect(args, &status);
    EXPECT_NE(r, nullptr) << QueryError_GetUserError(&status);
    if (r == nullptr) {
      QueryError_ClearError(&status);
      return nullptr;
    }
    QueryError_ClearError(&status);
    return reinterpret_cast<CollectReducer *>(r);
  }
};

// ====== Happy path tests ======

TEST_F(CollectParserTest, FieldsOnly) {
  registerKeys({"price", "name"});
  CollectReducer *cr = parseCollectOk({"FIELDS", "2", "@price", "@name"});
  ASSERT_NE(cr, nullptr);
  EXPECT_EQ(cr->num_fields, 2);
  EXPECT_FALSE(cr->has_wildcard);
  EXPECT_EQ(cr->num_sort_keys, 0);
  EXPECT_FALSE(cr->has_limit);
  cr->base.Free(&cr->base);
}

TEST_F(CollectParserTest, FieldsWildcard) {
  CollectReducer *cr = parseCollectOk({"FIELDS", "1", "*"});
  ASSERT_NE(cr, nullptr);
  EXPECT_EQ(cr->num_fields, 0);
  EXPECT_TRUE(cr->has_wildcard);
  EXPECT_EQ(cr->num_sort_keys, 0);
  cr->base.Free(&cr->base);
}

TEST_F(CollectParserTest, WildcardAmongFields) {
  registerKeys({"price", "name"});
  CollectReducer *cr = parseCollectOk({"FIELDS", "3", "@price", "*", "@name"});
  ASSERT_NE(cr, nullptr);
  EXPECT_EQ(cr->num_fields, 2);
  EXPECT_TRUE(cr->has_wildcard);
  cr->base.Free(&cr->base);
}

TEST_F(CollectParserTest, FieldsAndSortBy) {
  registerKeys({"price", "name"});
  CollectReducer *cr = parseCollectOk({
      "FIELDS", "2", "@price", "@name",
      "SORTBY", "3", "@price", "DESC", "@name",
  });
  ASSERT_NE(cr, nullptr);
  EXPECT_EQ(cr->num_fields, 2);
  EXPECT_EQ(cr->num_sort_keys, 2);
  EXPECT_FALSE(SORTASCMAP_GETASC(cr->sortAscMap, 0));
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 1));
  cr->base.Free(&cr->base);
}

TEST_F(CollectParserTest, FieldsSortByAndLimit) {
  registerKeys({"price"});
  CollectReducer *cr = parseCollectOk({
      "FIELDS", "1", "@price",
      "SORTBY", "2", "@price", "ASC",
      "LIMIT", "0", "10",
  });
  ASSERT_NE(cr, nullptr);
  EXPECT_EQ(cr->num_sort_keys, 1);
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 0));
  EXPECT_TRUE(cr->has_limit);
  EXPECT_EQ(cr->limit_offset, 0u);
  EXPECT_EQ(cr->limit_count, 10u);
  cr->base.Free(&cr->base);
}

TEST_F(CollectParserTest, FieldsAndLimitWithoutSortBy) {
  registerKeys({"price"});
  CollectReducer *cr = parseCollectOk({
      "FIELDS", "1", "@price",
      "LIMIT", "5", "100",
  });
  ASSERT_NE(cr, nullptr);
  EXPECT_EQ(cr->num_sort_keys, 0);
  EXPECT_TRUE(cr->has_limit);
  EXPECT_EQ(cr->limit_offset, 5u);
  EXPECT_EQ(cr->limit_count, 100u);
  cr->base.Free(&cr->base);
}

TEST_F(CollectParserTest, MultipleSortKeysWithDirections) {
  registerKeys({"a", "b", "c"});
  CollectReducer *cr = parseCollectOk({
      "FIELDS", "3", "@a", "@b", "@c",
      "SORTBY", "5", "@a", "ASC", "@b", "DESC", "@c",
  });
  ASSERT_NE(cr, nullptr);
  EXPECT_EQ(cr->num_sort_keys, 3);
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 0));
  EXPECT_FALSE(SORTASCMAP_GETASC(cr->sortAscMap, 1));
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 2));
  cr->base.Free(&cr->base);
}

TEST_F(CollectParserTest, SortByDefaultsToAscending) {
  registerKeys({"price"});
  CollectReducer *cr = parseCollectOk({
      "FIELDS", "1", "@price",
      "SORTBY", "1", "@price",
  });
  ASSERT_NE(cr, nullptr);
  EXPECT_EQ(cr->num_sort_keys, 1);
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 0));
  cr->base.Free(&cr->base);
}

TEST_F(CollectParserTest, SortByConsecutiveFieldsDefaultAsc) {
  registerKeys({"a", "b", "c"});
  CollectReducer *cr = parseCollectOk({
      "FIELDS", "1", "@a",
      "SORTBY", "3", "@a", "@b", "@c",
  });
  ASSERT_NE(cr, nullptr);
  EXPECT_EQ(cr->num_sort_keys, 3);
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 0));
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 1));
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 2));
  cr->base.Free(&cr->base);
}

TEST_F(CollectParserTest, SortByMixedConsecutiveFieldsAndDirections) {
  registerKeys({"a", "b", "c", "d", "e", "f"});
  CollectReducer *cr = parseCollectOk({
      "FIELDS", "1", "@a",
      "SORTBY", "8", "@a", "@b", "@c", "ASC", "@d", "DESC", "@e", "@f",
  });
  ASSERT_NE(cr, nullptr);
  EXPECT_EQ(cr->num_sort_keys, 6);
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 0));
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 1));
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 2));
  EXPECT_FALSE(SORTASCMAP_GETASC(cr->sortAscMap, 3));
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 4));
  EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, 5));
  cr->base.Free(&cr->base);
}

TEST_F(CollectParserTest, SortByMaxFields) {
  registerKeys({"x", "a", "b", "c", "d", "e", "f", "g", "h"});
  CollectReducer *cr = parseCollectOk({
      "FIELDS", "1", "@x",
      "SORTBY", "8", "@a", "@b", "@c", "@d", "@e", "@f", "@g", "@h",
  });
  ASSERT_NE(cr, nullptr);
  EXPECT_EQ(cr->num_sort_keys, 8);
  for (int i = 0; i < 8; i++) {
    EXPECT_TRUE(SORTASCMAP_GETASC(cr->sortAscMap, i)) << "sort key " << i;
  }
  cr->base.Free(&cr->base);
}

TEST_F(CollectParserTest, LimitZeroOffset) {
  registerKeys({"x"});
  CollectReducer *cr = parseCollectOk({
      "FIELDS", "1", "@x",
      "LIMIT", "0", "50",
  });
  ASSERT_NE(cr, nullptr);
  EXPECT_TRUE(cr->has_limit);
  EXPECT_EQ(cr->limit_offset, 0u);
  EXPECT_EQ(cr->limit_count, 50u);
  cr->base.Free(&cr->base);
}

TEST_F(CollectParserTest, JsonPathField) {
  registerKeys({"$..price"});
  CollectReducer *cr = parseCollectOk({"FIELDS", "1", "$..price"});
  ASSERT_NE(cr, nullptr);
  EXPECT_EQ(cr->num_fields, 1);
  EXPECT_FALSE(cr->has_wildcard);
  cr->base.Free(&cr->base);
}

// ====== Validation / error tests ======

TEST_F(CollectParserTest, SortByJsonPathRejected) {
  registerKeys({"$..price"});
  expectError(
      {"FIELDS", "1", "$..price", "SORTBY", "1", "$..price"},
      "MISSING ASC or DESC after sort field");
}

TEST_F(CollectParserTest, EmptyArgs) {
  expectError({}, "Bad arguments for COLLECT");
}

TEST_F(CollectParserTest, MissingFieldsRequired) {
  registerKeys({"price"});
  expectError({"SORTBY", "1", "@price"}, "Bad arguments for COLLECT");
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

TEST_F(CollectParserTest, DuplicateWildcard) {
  registerKeys({"price"});
  expectError({"FIELDS", "3", "*", "@price", "*"}, "Wildcard `*` can only appear once in FIELDS");
}

TEST_F(CollectParserTest, UnknownSubcommand) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "FOO", "1", "2"}, "Bad arguments for COLLECT");
}

TEST_F(CollectParserTest, SortByFieldWithoutAtPrefix) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "1", "bad_field"},
      "MISSING ASC or DESC after sort field");
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
      "Bad arguments for COLLECT");
}

TEST_F(CollectParserTest, FieldsExceedsMax) {
  std::vector<const char *> args;
  args.push_back("FIELDS");
  args.push_back("1025");
  std::vector<std::string> field_strs(1025);
  for (int i = 0; i < 1025; i++) {
    field_strs[i] = "@f" + std::to_string(i);
    args.push_back(field_strs[i].c_str());
  }
  expectError(std::move(args), "Invalid argument count");
}

TEST_F(CollectParserTest, SortByInvalidTokenBetweenFields) {
  registerKeys({"x", "a", "b"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "3", "@a", "INVALID", "@b"},
      "MISSING ASC or DESC after sort field");
}

TEST_F(CollectParserTest, LimitNegativeOffset) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "LIMIT", "-1", "10"},
      "LIMIT offset must be a non-negative integer");
}

TEST_F(CollectParserTest, LimitNegativeCount) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "LIMIT", "0", "-5"},
      "LIMIT count must be a non-negative integer");
}

TEST_F(CollectParserTest, LimitNonNumericOffset) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "LIMIT", "abc", "10"},
      "LIMIT offset must be a non-negative integer");
}

TEST_F(CollectParserTest, FieldsZeroCountRequiresAtLeastOne) {
  // ArgParser enforces min FIELDS args before handleCollect runs; count 0 is rejected there.
  expectError({"FIELDS", "0"}, "Invalid argument count");
}

TEST_F(CollectParserTest, SortByOnlyDirectionsNoFields) {
  registerKeys({"x"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "1", "ASC"},
      "MISSING ASC or DESC after sort field");
}

TEST_F(CollectParserTest, SortByDescBeforeFirstSortField) {
  registerKeys({"x", "price"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "2", "DESC", "@price"},
      "MISSING ASC or DESC after sort field");
}

TEST_F(CollectParserTest, SortByDuplicateAscAfterField) {
  registerKeys({"x", "price"});
  expectError({"FIELDS", "1", "@x", "SORTBY", "3", "@price", "ASC", "ASC"},
      "MISSING ASC or DESC after sort field");
}

TEST_F(CollectParserTest, LimitCountExceedsAggregateMax) {
  registerKeys({"x"});
  std::string count =
      std::to_string(static_cast<unsigned long long>(MAX_AGGREGATE_REQUEST_RESULTS) + 1ULL);
  expectError({"FIELDS", "1", "@x", "LIMIT", "0", count.c_str()},
      "LIMIT count exceeds maximum of");
}

TEST_F(CollectParserTest, LimitOffsetPlusCountOverflow) {
  registerKeys({"x"});
  std::string offset = std::to_string(static_cast<unsigned long long>(LLONG_MAX));
  expectError({"FIELDS", "1", "@x", "LIMIT", offset.c_str(), "1"},
      "Invalid LIMIT offset + count value");
}

TEST_F(CollectParserTest, RejectsWhenUnstableFeaturesDisabled) {
  RSGlobalConfig.enableUnstableFeatures = false;
  registerKeys({"price"});
  expectError({"FIELDS", "1", "@price"},
      "`COLLECT` is unavailable when `ENABLE_UNSTABLE_FEATURES` is off");
}
