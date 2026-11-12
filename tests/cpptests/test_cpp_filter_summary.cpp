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
    auto context = FilterSummary_CreateContext(spec, &predicate);
    bool result = FilterSummary_MayMatch(&context, id);
    FilterSummary_FreeContext(&context);
    return result;
  }

  bool numericMayMatch(t_docId id, uint16_t field, double min, double max) {
    FilterQuickCheckPredicate predicate = {};
    predicate.type = FilterQuickCheck_NUMERIC;
    predicate.numeric = {field, min, max, true, true};
    auto context = FilterSummary_CreateContext(spec, &predicate);
    bool result = FilterSummary_MayMatch(&context, id);
    FilterSummary_FreeContext(&context);
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

  EXPECT_TRUE(numericMayMatch(id, 1, 12.5, 12.5));
  EXPECT_FALSE(numericMayMatch(id, 1, 12.6, 13));
  EXPECT_FALSE(numericMayMatch(id, 1, 11, 12.4));
  EXPECT_FALSE(numericMayMatch(id, 4, 0, 100));
  EXPECT_TRUE(numericMayMatch(id, 5, 0, 1));
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

  EXPECT_TRUE(numericMayMatch(newId, 1, 100, 100));
  EXPECT_FALSE(numericMayMatch(newId, 1, 10, 10));

  const auto* md = DocTable_Borrow(&spec->docs, newId);
  ASSERT_NE(md, nullptr);
  size_t summarySize = FilterSummary_MemoryUsage(md->filterSummary);
  size_t beforeDelete = spec->docs.memsize;
  DMD_Return(md);
  IndexSpec_DeleteDocById(spec, newId);
  EXPECT_TRUE(numericMayMatch(newId, 1, 0, 1));
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

TEST_F(FilterSummaryTest, NumericBoundsCoverCompressedValuesAfterConfigChange) {
  const bool previousCompression = RSGlobalConfig.numericCompress;
  const char* values[] = {"1.01", "1.02", "-1.01", "-1.02", "1e-50"};
  for (size_t i = 0; i < sizeof(values) / sizeof(*values); ++i) {
    RSGlobalConfig.numericCompress = (i == 0);
    RMCK::hset(ctx, "doc:1", "price", values[i]);
    index("doc:1");
    RSGlobalConfig.numericCompress = previousCompression;

    const double original = strtod(values[i], nullptr);
    const double stored = (double)(float)original;
    EXPECT_TRUE(numericMayMatch(docIdOf("doc:1"), 1, stored, stored)) << values[i];
  }
}

TEST_F(FilterSummaryTest, NoFieldsTextSummaryMatchesAcrossFields) {
  spec->flags = static_cast<IndexFlags>(spec->flags & ~Index_StoreFieldFlags);
  RMCK::hset(ctx, "doc:1", "title", "Hello");
  RMCK::hset(ctx, "doc:1", "other", "World");
  RMCK::hset(ctx, "doc:1", "category", "books");
  index("doc:1");
  const t_docId id = docIdOf("doc:1");
  EXPECT_TRUE(mayMatch(id, FilterQuickCheck_TEXT, 2, "world"));
  EXPECT_TRUE(mayMatch(id, FilterQuickCheck_TEXT, 3, "hello"));
  EXPECT_FALSE(mayMatch(id, FilterQuickCheck_TEXT, 2, "absent"));
  EXPECT_FALSE(mayMatch(id, FilterQuickCheck_TAG, 0, "world"));
}

TEST_F(FilterSummaryTest, MixedPredicatesReleaseDocumentAndDoNotRetainPreviousSummary) {
  RMCK::hset(ctx, "doc:1", "category", "books");
  RMCK::hset(ctx, "doc:1", "price", "10");
  RMCK::hset(ctx, "doc:1", "missing", "20");
  index("doc:1");
  RMCK::hset(ctx, "doc:2", "category", "books");
  RMCK::hset(ctx, "doc:2", "price", "100");
  index("doc:2");
  FilterQuickCheckPredicate leaves[3] = {};
  leaves[0].type = FilterQuickCheck_NUMERIC;
  leaves[0].numeric = {1, 10, 10, true, true};
  leaves[1].type = FilterQuickCheck_TERM;
  leaves[1].term = {FilterQuickCheck_TAG, 0, "books", 5};
  leaves[2].type = FilterQuickCheck_NUMERIC;
  leaves[2].numeric = {4, 20, 20, true, true};
  FilterQuickCheckPredicate predicate = {};
  predicate.type = FilterQuickCheck_AND;
  predicate.operands = {leaves, 3};
  auto context = FilterSummary_CreateContext(spec, &predicate);
  const auto* md = DocTable_Borrow(&spec->docs, docIdOf("doc:1"));
  ASSERT_NE(md, nullptr);
  const auto references = md->ref_count;
  EXPECT_EQ(FilterSummary_MayMatch(&context, docIdOf("doc:1")), 1);
  EXPECT_EQ(md->ref_count, references);
  EXPECT_EQ(FilterSummary_MayMatch(&context, docIdOf("doc:2")), 0);
  EXPECT_EQ(FilterSummary_MayMatch(&context, 999999), 1);
  EXPECT_EQ(FilterSummary_MayMatch(&context, docIdOf("doc:1")), 1);
  EXPECT_EQ(md->ref_count, references);
  DMD_Return(md);
  FilterSummary_FreeContext(&context);
}

TEST_F(FilterSummaryTest, CompiledSlotsRemainConservativeAfterSchemaChange) {
  RMCK::hset(ctx, "doc:1", "price", "10");
  index("doc:1");
  FilterQuickCheckPredicate predicate = {};
  predicate.type = FilterQuickCheck_NUMERIC;
  predicate.numeric = {1, 100, 100, true, true};
  auto context = FilterSummary_CreateContext(spec, &predicate);
  EXPECT_EQ(FilterSummary_MayMatch(&context, docIdOf("doc:1")), 0);
  auto* added = IndexSpec_CreateField(spec, "later", nullptr);
  ASSERT_NE(added, nullptr);
  added->types = INDEXFLD_T_NUMERIC;
  EXPECT_EQ(FilterSummary_MayMatch(&context, docIdOf("doc:1")), 1);
  FilterSummary_FreeContext(&context);
  // A new plan must also treat a document's older summary as unavailable.
  context = FilterSummary_CreateContext(spec, &predicate);
  EXPECT_EQ(FilterSummary_MayMatch(&context, docIdOf("doc:1")), 1);
  FilterSummary_FreeContext(&context);
}
