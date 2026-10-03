/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

extern "C" {
#include "util/dict.h"
}
#include "aggregate/expr/expression.h"
#include "result_processor.h"
#include "search_result_ffi.h"
#include "query.h"
#include "metrics_ffi.h"
#include "gtest/gtest.h"
#include "spec.h"
#include "redismock/util.h"
#include "query_flags.h"

#include <vector>

// Both entries share the cursor: the handoff resumes, rather than replays, row processing.
struct OwnedRowSource : ResultProcessor {
  unsigned cursor = 0;
  unsigned count = 6;
  unsigned nextStop = 6;
  unsigned nextCalls = 0;
  unsigned drainCalls = 0;
  RPDrainStatus terminal = RP_DRAIN_EOF;
  const RLookupKey *key = nullptr;
  RSIndexResult *index = nullptr;

  OwnedRowSource() {
    *static_cast<ResultProcessor *>(this) = {};
    Next = [](ResultProcessor *base, SearchResult *row) -> int {
      auto *self = static_cast<OwnedRowSource *>(base);
      ++self->nextCalls;
      if (self->cursor == self->nextStop) return RS_RESULT_TIMEDOUT;
      if (self->cursor == self->count) return RS_RESULT_EOF;
      self->yield(row);
      return RS_RESULT_OK;
    };
    Drain = [](ResultProcessor *base, SearchResult *row) {
      auto *self = static_cast<OwnedRowSource *>(base);
      ++self->drainCalls;
      if (self->cursor == self->count) return self->terminal;
      self->yield(row);
      return RP_DRAIN_OK;
    };
  }

  void yield(SearchResult *row) {
    SearchResult_SetDocId(row, ++cursor);
    SearchResult_SetScore(row, cursor);
    if (key) {
      RLookup_WriteOwnKey(key, SearchResult_GetRowDataMut(row), RSValue_NewNumber(cursor));
    }
    if (index) SearchResult_SetBorrowedIndexResult(row, index);
  }
};

class OwnedRowDrainTest : public ::testing::Test {
 protected:
  QueryError error = QueryError_Default();
  QueryProcessingCtx qctx = {};
  RLookup lookup = RLookup_New();
  const RLookupKey *input = RLookup_GetKey_Write(&lookup, "input", RLOOKUP_F_NOFLAGS);
  const RLookupKey *output = RLookup_GetKey_Write(&lookup, "output", RLOOKUP_F_NOFLAGS);
  OwnedRowSource source;
  SearchResult row = SearchResult_New();
  std::vector<ResultProcessor *> processors;
  std::vector<RSExpr *> expressions;

  void SetUp() override {
    qctx.err = &error;
    qctx.resultLimit = 37;
    qctx.totalResults = 6;
    source.key = input;
    source.parent = &qctx;
    qctx.endProc = &source;
  }

  void TearDown() override {
    SearchResult_Destroy(&row);
    for (auto *rp : processors) rp->Free(rp);
    for (auto *ast : expressions) ExprAST_Free(ast);
    QueryError_ClearError(&error);
    RLookup_Cleanup(&lookup);
  }

  ResultProcessor *append(ResultProcessor *rp) {
    rp->upstream = qctx.endProc;
    rp->parent = &qctx;
    qctx.endProc = rp;
    processors.push_back(rp);
    return rp;
  }

  RSExpr *expression(const char *text) {
    auto *hidden = NewHiddenString(text, strlen(text), false);
    auto *ast = ExprAST_Parse(hidden, &error);
    HiddenString_Free(hidden, false);
    EXPECT_NE(nullptr, ast);
    if (ast) EXPECT_EQ(EXPR_EVAL_OK, ExprAST_GetLookupKeys(ast, &lookup, &error));
    expressions.push_back(ast);
    return ast;
  }

  double value(const RLookupKey *key) {
    const auto *v = RLookupRow_Get(key, SearchResult_GetRowData(&row));
    EXPECT_NE(nullptr, v);
    return v ? RSValue_Number_Get(v) : -1;
  }
};

TEST_F(OwnedRowDrainTest, PagerResumesInterruptedOffsetAndRestoresBudget) {
  source.nextStop = 1;
  auto *pager = append(RPPager_New(2, 2));
  ASSERT_EQ(RS_RESULT_TIMEDOUT, pager->Next(pager, &row));
  EXPECT_EQ(37, qctx.resultLimit);
  ASSERT_EQ(RP_DRAIN_OK, pager->Drain(pager, &row));
  EXPECT_EQ(3, SearchResult_GetDocId(&row));
  SearchResult_Clear(&row);
  ASSERT_EQ(RP_DRAIN_OK, pager->Drain(pager, &row));
  EXPECT_EQ(4, SearchResult_GetDocId(&row));
  SearchResult_Clear(&row);
  EXPECT_EQ(RP_DRAIN_EOF, pager->Drain(pager, &row));
  EXPECT_EQ(4, source.cursor);
  EXPECT_EQ(2, source.nextCalls);
  EXPECT_EQ(37, qctx.resultLimit);
}

TEST_F(OwnedRowDrainTest, PagerPreservesNextPrefixAndDoesNotReapplyOffset) {
  auto *pager = append(RPPager_New(2, 2));
  ASSERT_EQ(RS_RESULT_OK, pager->Next(pager, &row));
  EXPECT_EQ(3, SearchResult_GetDocId(&row));
  SearchResult_Clear(&row);
  ASSERT_EQ(RP_DRAIN_OK, pager->Drain(pager, &row));
  EXPECT_EQ(4, SearchResult_GetDocId(&row));
  SearchResult_Clear(&row);
  EXPECT_EQ(RP_DRAIN_EOF, pager->Drain(pager, &row));
  EXPECT_EQ(3, source.nextCalls);
  EXPECT_EQ(1, source.drainCalls);
}

TEST_F(OwnedRowDrainTest, ZeroLimitDoesNotDrainUpstream) {
  auto *pager = append(RPPager_New(2, 0));
  EXPECT_EQ(RP_DRAIN_EOF, pager->Drain(pager, &row));
  EXPECT_EQ(0, source.drainCalls);
}

TEST_F(OwnedRowDrainTest, FilterProjectPagerAndProfileComposeWithoutNext) {
  append(RPEvaluator_NewFilter(expression("@input > 2"), &lookup));
  append(RPEvaluator_NewProjector(expression("@input * 10"), &lookup, output));
  append(RPPager_New(1, 2));
  auto *profile = append(RPProfile_New(qctx.endProc, &qctx));
  ASSERT_EQ(RP_DRAIN_OK, profile->Drain(profile, &row));
  EXPECT_EQ(4, SearchResult_GetDocId(&row));
  EXPECT_EQ(40, value(output));
  SearchResult_Clear(&row);
  ASSERT_EQ(RP_DRAIN_OK, profile->Drain(profile, &row));
  EXPECT_EQ(5, SearchResult_GetDocId(&row));
  EXPECT_EQ(50, value(output));
  SearchResult_Clear(&row);
  EXPECT_EQ(RP_DRAIN_EOF, profile->Drain(profile, &row));
  EXPECT_EQ(4, qctx.totalResults);
  EXPECT_EQ(0, source.nextCalls);
  EXPECT_EQ(3, RPProfile_GetCount(profile));
}

TEST_F(OwnedRowDrainTest, FilterDoesNotUnderflowCursorCount) {
  qctx.totalResults = 0;
  auto *filter = append(RPEvaluator_NewFilter(expression("@input > 2"), &lookup));
  ASSERT_EQ(RP_DRAIN_OK, filter->Drain(filter, &row));
  EXPECT_EQ(3, SearchResult_GetDocId(&row));
  EXPECT_EQ(0, qctx.totalResults);
}

TEST_F(OwnedRowDrainTest, RejectedRowsAreClearedThroughEof) {
  auto *filter = append(RPEvaluator_NewFilter(expression("@input > 100"), &lookup));
  EXPECT_EQ(RP_DRAIN_EOF, filter->Drain(filter, &row));
  EXPECT_EQ(0, qctx.totalResults);
  EXPECT_EQ(nullptr, RLookupRow_Get(input, SearchResult_GetRowData(&row)));
  EXPECT_EQ(6, source.cursor);
  EXPECT_EQ(0, source.nextCalls);
}

TEST_F(OwnedRowDrainTest, TransparentCallbacksPreserveBothTerminalStatuses) {
  for (auto terminal : {RP_DRAIN_EOF, RP_DRAIN_ERROR}) {
    source.count = 0;
    source.terminal = terminal;
    ResultProcessor *callbacks[] = {RPMetricsLoader_New(), RPPager_New(1, 2),
                                    RPVectorNormalizer_New([](double v) { return v * 2; }, input),
                                    RPEvaluator_NewFilter(expression("@input > 2"), &lookup)};
    for (auto *rp : callbacks) {
      qctx.endProc = &source;
      append(rp);
      SearchResult_SetScore(&row, 17);
      EXPECT_EQ(terminal, rp->Drain(rp, &row));
      EXPECT_EQ(17, SearchResult_GetScore(&row));
      EXPECT_EQ(6, qctx.totalResults);
      EXPECT_EQ(37, qctx.resultLimit);
    }
  }
  EXPECT_EQ(0, source.nextCalls);
}

TEST_F(OwnedRowDrainTest, EvaluationFailureReturnsDiagnosticAndCallerOwnsRow) {
  auto *projector =
      append(RPEvaluator_NewProjector(expression("@input < 'invalid'"), &lookup, output));
  ASSERT_EQ(RP_DRAIN_ERROR, projector->Drain(projector, &row));
  EXPECT_TRUE(QueryError_HasError(&error));
  EXPECT_EQ(1, SearchResult_GetDocId(&row));
  EXPECT_EQ(1, value(input));
  EXPECT_EQ(0, source.nextCalls);
  // ERROR ends this sequence even if its diagnostic is subsequently consumed.
}

TEST_F(OwnedRowDrainTest, UpstreamTerminalStatusesDoNotEvaluateOrModifyOutput) {
  for (auto terminal : {RP_DRAIN_ERROR, RP_DRAIN_EOF}) {
    source.count = 0;
    source.terminal = terminal;
    auto *projector =
        append(RPEvaluator_NewProjector(expression("@input < 'invalid'"), &lookup, output));
    auto *profile = append(RPProfile_New(projector, &qctx));
    SearchResult_SetScore(&row, 19);
    EXPECT_EQ(terminal, profile->Drain(profile, &row));
    EXPECT_FALSE(QueryError_HasError(&error));
    EXPECT_EQ(19, SearchResult_GetScore(&row));
    qctx.endProc = &source;
  }
  EXPECT_EQ(0, source.nextCalls);
}

TEST_F(OwnedRowDrainTest, MetricsAndVectorNormalizationPreserveRowSemantics) {
  source.index = NewVirtualResult(1, RS_FIELDMASK_ALL);
  ResultMetrics_Add(source.index, input, 2.5);
  append(RPMetricsLoader_New());
  auto *normalizer = append(RPVectorNormalizer_New([](double v) { return v * 2; }, input));
  ASSERT_EQ(RP_DRAIN_OK, normalizer->Drain(normalizer, &row));
  EXPECT_EQ(5, value(input));
  EXPECT_EQ(5, SearchResult_GetScore(&row));
  SearchResult_Clear(&row);
  IndexResult_Free(source.index);
  source.index = nullptr;
  source.key = nullptr;
  ASSERT_EQ(RP_DRAIN_OK, normalizer->Drain(normalizer, &row));
  EXPECT_EQ(0, value(input));
  EXPECT_EQ(0, SearchResult_GetScore(&row));
  EXPECT_EQ(0, source.nextCalls);
}

struct OwnedLoaderSource : ResultProcessor {
  std::vector<RSDocumentMetadata *> documents;
  size_t cursor = 0;
  unsigned nextCalls = 0;
  RPDrainStatus terminal = RP_DRAIN_EOF;

  OwnedLoaderSource() {
    *static_cast<ResultProcessor *>(this) = {};
    Next = [](ResultProcessor *base, SearchResult *) -> int {
      ++static_cast<OwnedLoaderSource *>(base)->nextCalls;
      return RS_RESULT_TIMEDOUT;
    };
    Drain = [](ResultProcessor *base, SearchResult *row) {
      auto *self = static_cast<OwnedLoaderSource *>(base);
      if (self->cursor == self->documents.size()) return self->terminal;
      auto *dmd = self->documents[self->cursor++];
      DMD_Incref(dmd);
      SearchResult_SetDocumentMetadata(row, dmd);
      SearchResult_SetDocId(row, dmd->id);
      return RP_DRAIN_OK;
    };
  }
};

class OwnedLoaderDrainTest : public ::testing::Test {
 protected:
  RMCK::Context ctx;
  IndexSpec spec = {};
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, &spec);
  QueryProcessingCtx qctx = {};
  RLookup lookup = RLookup_New();
  OwnedLoaderSource source;
  ResultProcessor *loader = nullptr;
  SearchResult row = SearchResult_New();

  void SetUp() override {
    spec.docs = DocTable_New(1);
  }

  void TearDown() override {
    SearchResult_Destroy(&row);
    if (loader) loader->Free(loader);
    RLookup_Cleanup(&lookup);
    DocTable_Free(&spec.docs);
    RMCK::flushdb(ctx);
  }

  RSDocumentMetadata *document(const char *name, const char *value) {
    auto *dmd = DocTable_Put(&spec.docs, name, strlen(name), 1, Document_DefaultFlags, nullptr, 0,
                             DocumentType_Hash);
    DMD_Return(dmd);
    if (value) EXPECT_TRUE(RMCK::hset(ctx, name, "field", value));
    source.documents.push_back(dmd);
    return dmd;
  }

  const RLookupKey *create(const char *field = "field", uint32_t flags = 0) {
    const auto *key = RLookup_GetKey_Load(&lookup, field, field, 0);
    uint32_t state = 0;
    loader = RPLoader_New(&sctx, flags, &lookup, &key, 1, false, &state);
    loader->parent = &qctx;
    loader->upstream = &source;
    qctx.endProc = loader;
    RLookup_Seal(&lookup);
    return key;
  }

  void expectValue(const RLookupKey *key, const char *expected) {
    const auto *value = RLookupRow_Get(key, SearchResult_GetRowData(&row));
    ASSERT_NE(nullptr, value);
    size_t len = 0;
    const char *data = RSValue_StringPtrLen(value, &len);
    ASSERT_NE(nullptr, data);
    EXPECT_EQ(std::string(expected), std::string(data, len));
  }
};

TEST_F(OwnedLoaderDrainTest, SequentialLoadDropsMissingRowsAndTransfersOwnership) {
  auto *missing = document("drain:missing", nullptr);
  auto *live = document("drain:live", "value");
  const auto *key = create();
  ASSERT_EQ(RS_RESULT_TIMEDOUT, loader->Next(loader, &row));
  ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &row));
  expectValue(key, "value");
  EXPECT_EQ(1, qctx.skippedResults);
  EXPECT_EQ(1, missing->ref_count);
  EXPECT_EQ(2, live->ref_count);
  loader->Free(loader);
  loader = nullptr;
  expectValue(key, "value");
  SearchResult_Clear(&row);
  EXPECT_EQ(1, live->ref_count);
  EXPECT_EQ(1, source.nextCalls);
}

TEST_F(OwnedLoaderDrainTest, TerminalStatusDoesNotLoadOrCallNext) {
  create();
  source.terminal = RP_DRAIN_ERROR;
  EXPECT_EQ(RP_DRAIN_ERROR, loader->Drain(loader, &row));
  EXPECT_EQ(0, source.nextCalls);
  EXPECT_EQ(nullptr, SearchResult_GetDocumentMetadata(&row));
}

TEST_F(OwnedLoaderDrainTest, PromotionDoesNotRetainPlainLoaderDrain) {
  document("drain:safe", "value");
  create();
  SetLoadersForBG(&qctx);
  loader = qctx.endProc;
  EXPECT_EQ(RP_SAFE_LOADER, loader->type);
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &row));
  EXPECT_EQ(0, source.cursor);
  EXPECT_EQ(0, source.nextCalls);
}

TEST_F(OwnedLoaderDrainTest, KeyNameDrainCopiesNameWithoutLoadingFields) {
  auto *dmd = document("drain:key-name", nullptr);
  const bool unstable = RSGlobalConfig.enableUnstableFeatures;
  RSGlobalConfig.enableUnstableFeatures = true;
  const auto *key = create("__key", QEXEC_F_RUN_IN_BACKGROUND);
  RSGlobalConfig.enableUnstableFeatures = unstable;
  ASSERT_EQ(RP_KEY_NAME_LOADER, loader->type);
  ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &row));
  expectValue(key, "drain:key-name");
  EXPECT_EQ(dmd, SearchResult_GetDocumentMetadata(&row));
  EXPECT_EQ(0, source.nextCalls);
}
