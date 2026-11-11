/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "filter_summary.h"
#include "indexes.h"
#include "partial_update_fixture.h"

class FilterSummaryTest : public PartialUpdateTest {
 protected:
  FilterSummaryTest() : PartialUpdateTest("summaryidx") {
  }

  void SetUp() override {
    PartialUpdateTest::SetUp();
    QueryError error = QueryError_Default();
    RMCK::ArgvList args(ctx, "FT.CREATE", indexName.c_str(), "ON", "HASH", "SCHEMA", "category",
                        "AS", "kind", "TAG", "price", "NUMERIC", "title", "TEXT", "other", "TEXT",
                        "missing", "NUMERIC", "ignored", "NUMERIC", "NOINDEX");
    spec = Indexes_CreateNewSpec(ctx, args, args.size(), &error);
    ASSERT_NE(spec, nullptr) << QueryError_GetUserError(&error);
    QueryError_ClearError(&error);
  }

  void index(const char* key) {
    Indexes_UpdateMatchingWithSchemaRules(ctx, RMCK::RString(key), DocumentType_Hash, nullptr, 0);
  }

  bool mayMatch(t_docId id, FilterQuickCheckTermKind kind, uint16_t field, const char* value) {
    FilterQuickCheckPredicate predicate = {};
    predicate.type = FilterQuickCheck_TERM;
    predicate.term = {kind, field, value, strlen(value)};
    FilterQuickCheck* check = FilterQuickCheck_Compile(&predicate);
    auto context = FilterSummary_CreateContext(spec, check);
    bool result = FilterQuickCheck_MayMatch(&context, id);
    FilterQuickCheck_Free(check);
    return result;
  }
};

TEST_F(FilterSummaryTest, IndexesAllSupportedFieldsFromNormalizedInput) {
  RMCK::hset(ctx, "doc:1", "category", " ELECTRONICS , Books ");
  RMCK::hset(ctx, "doc:1", "price", "12.5");
  RMCK::hset(ctx, "doc:1", "title", "Running HELLO");
  RMCK::hset(ctx, "doc:1", "other", "World");
  RMCK::hset(ctx, "doc:1", "ignored", "7");
  index("doc:1");
  t_docId id = docIdOf("doc:1");
  ASSERT_NE(id, 0u);
  EXPECT_TRUE(mayMatch(id, FilterQuickCheck_TAG, 0, "electronics"));
  EXPECT_TRUE(mayMatch(id, FilterQuickCheck_TAG, 0, "books"));
  EXPECT_FALSE(mayMatch(id, FilterQuickCheck_TAG, 0, "clothing"));
  EXPECT_TRUE(mayMatch(id, FilterQuickCheck_TEXT, 2, "hello"));
  EXPECT_TRUE(mayMatch(id, FilterQuickCheck_TEXT, 2, "+run"));
  EXPECT_TRUE(mayMatch(id, FilterQuickCheck_TEXT, 3, "world"));
  EXPECT_FALSE(mayMatch(id, FilterQuickCheck_TEXT, 3, "hello"));

  auto context = FilterSummary_CreateContext(spec, nullptr);
  FilterQuickCheckNumericSummary bounds = {};
  ASSERT_TRUE(context.numericLookup(context.numericLookupContext, id, 1, &bounds));
  EXPECT_EQ(bounds.state, FilterQuickCheckNumeric_BOUNDS);
  EXPECT_EQ(bounds.min, 12.5);
  EXPECT_EQ(bounds.max, 12.5);
  ASSERT_TRUE(context.numericLookup(context.numericLookupContext, id, 4, &bounds));
  EXPECT_EQ(bounds.state, FilterQuickCheckNumeric_EMPTY);
  EXPECT_FALSE(context.numericLookup(context.numericLookupContext, id, 5, &bounds));
  const auto* md = DocTable_Borrow(&spec->docs, id);
  ASSERT_NE(md, nullptr);
  EXPECT_GT(FilterSummary_MemoryUsage(md->filterSummary), sizeof(FilterQuickCheckSignature));
  DMD_Return(md);
}

TEST_F(FilterSummaryTest, ReplacementDeletionAndSchemaChangeCannotLeaveStaleNegatives) {
  RMCK::hset(ctx, "doc:1", "category", "old");
  RMCK::hset(ctx, "doc:1", "price", "10");
  index("doc:1");
  t_docId oldId = docIdOf("doc:1");
  ASSERT_NE(oldId, 0u);

  RMCK::hset(ctx, "doc:1", "category", "new");
  RMCK::hset(ctx, "doc:1", "price", "100");
  index("doc:1");
  t_docId newId = docIdOf("doc:1");
  ASSERT_GT(newId, oldId);
  EXPECT_TRUE(mayMatch(newId, FilterQuickCheck_TAG, 0, "new"));
  EXPECT_FALSE(mayMatch(newId, FilterQuickCheck_TAG, 0, "old"));
  EXPECT_TRUE(mayMatch(oldId, FilterQuickCheck_TAG, 0, "new"));

  auto context = FilterSummary_CreateContext(spec, nullptr);
  FilterQuickCheckNumericSummary bounds = {};
  ASSERT_TRUE(context.numericLookup(context.numericLookupContext, newId, 1, &bounds));
  EXPECT_EQ(bounds.min, 100);
  EXPECT_EQ(bounds.max, 100);

  const auto* md = DocTable_Borrow(&spec->docs, newId);
  ASSERT_NE(md, nullptr);
  size_t summarySize = FilterSummary_MemoryUsage(md->filterSummary);
  size_t beforeDelete = spec->docs.memsize;
  DMD_Return(md);
  IndexSpec_DeleteDocById(spec, newId);
  EXPECT_FALSE(context.numericLookup(context.numericLookupContext, newId, 1, &bounds));
  EXPECT_LE(spec->docs.memsize + summarySize, beforeDelete);

  RMCK::hset(ctx, "doc:2", "category", "present");
  index("doc:2");
  t_docId secondId = docIdOf("doc:2");
  ASSERT_NE(secondId, 0u);
  EXPECT_FALSE(mayMatch(secondId, FilterQuickCheck_TAG, 0, "absent"));
  auto* added = IndexSpec_CreateField(spec, "later", nullptr);
  ASSERT_NE(added, nullptr);
  added->types = INDEXFLD_T_TAG;
  EXPECT_TRUE(mayMatch(secondId, FilterQuickCheck_TAG, 0, "absent"));
}
