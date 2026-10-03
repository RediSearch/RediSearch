/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "result_processor.h"
#include "search_result_ffi.h"
#include "value_ffi.h"
#include "query.h"

#include <vector>
#include <chrono>
#include <thread>

struct OwnedBufferSource : ResultProcessor {
  std::vector<double> scores = {1, 4, 2, 3};
  size_t cursor = 0;
  unsigned nextCalls = 0, drainCalls = 0;
  int terminal = RS_RESULT_TIMEDOUT;
  const RLookupKey *key = nullptr;

  OwnedBufferSource() {
    *static_cast<ResultProcessor *>(this) = {};
    Next = [](ResultProcessor *base, SearchResult *row) -> int {
      auto *self = static_cast<OwnedBufferSource *>(base);
      ++self->nextCalls;
      if (self->cursor == self->scores.size()) return self->terminal;
      const double score = self->scores[self->cursor++];
      SearchResult_SetDocId(row, self->cursor);
      SearchResult_SetScore(row, score);
      RLookup_WriteOwnKey(self->key, SearchResult_GetRowDataMut(row), RSValue_NewNumber(score));
      return RS_RESULT_OK;
    };
    Drain = [](ResultProcessor *base, SearchResult *) {
      ++static_cast<OwnedBufferSource *>(base)->drainCalls;
      ADD_FAILURE() << "Accumulator recovery must stop at its local buffer";
      return RP_DRAIN_ERROR;
    };
  }
};

class OwnedBufferDrainTest : public ::testing::Test {
 protected:
  QueryProcessingCtx qctx = {};
  QueryError error = QueryError_Default();
  RLookup lookup = RLookup_New();
  const RLookupKey *key = RLookup_GetKey_Write(&lookup, "score", RLOOKUP_F_NOFLAGS);
  OwnedBufferSource source;
  ResultProcessor *rp = nullptr;
  SearchResult row = SearchResult_New();

  void SetUp() override {
    source.key = key;
    qctx.timeoutPolicy = TimeoutPolicy_ReturnStrict;
    qctx.resultLimit = 10;
    qctx.err = &error;
  }

  void TearDown() override {
    SearchResult_Destroy(&row);
    if (rp) rp->Free(rp);
    RLookup_Cleanup(&lookup);
    QueryError_ClearError(&error);
  }

  void attach(ResultProcessor *processor) {
    rp = processor;
    rp->parent = &qctx;
    rp->upstream = &source;
    qctx.rootProc = &source;
    qctx.endProc = rp;
  }

  std::vector<double> drain() {
    std::vector<double> scores;
    RPDrainStatus status;
    while ((status = rp->Drain(rp, &row)) == RP_DRAIN_OK) {
      scores.push_back(SearchResult_GetScore(&row));
      SearchResult_Clear(&row);
    }
    EXPECT_EQ(RP_DRAIN_EOF, status);
    EXPECT_EQ(0, source.drainCalls);
    return scores;
  }
};

TEST_F(OwnedBufferDrainTest, UnstartedAccumulatorsDoNotPullSource) {
  for (auto *processor :
       {RPSorter_NewByScore(3, nullptr), RPMaxScoreNormalizer_New(key), RPDepleter_New()}) {
    attach(processor);
    EXPECT_EQ(RP_DRAIN_EOF, rp->Drain(rp, &row));
    EXPECT_EQ(0, source.nextCalls);
    EXPECT_EQ(0, source.drainCalls);
    rp->Free(rp);
    rp = nullptr;
  }
}

TEST_F(OwnedBufferDrainTest, SorterRecoversPartialTopNInNormalOrder) {
  attach(RPSorter_NewByScore(3, nullptr));
  ASSERT_EQ(RS_RESULT_TIMEDOUT, rp->Next(rp, &row));
  EXPECT_EQ(10, qctx.resultLimit);
  const unsigned calls = source.nextCalls;
  EXPECT_EQ((std::vector<double>{4, 3, 2}), drain());
  EXPECT_EQ(calls, source.nextCalls);
}

TEST_F(OwnedBufferDrainTest, FieldSorterKeepsAscendingOrderAndNextPrefix) {
  source.terminal = RS_RESULT_EOF;
  attach(RPSorter_NewByFields(4, &key, 1, 1));
  ASSERT_EQ(RS_RESULT_OK, rp->Next(rp, &row));
  EXPECT_EQ(1, SearchResult_GetScore(&row));
  SearchResult_Clear(&row);
  EXPECT_EQ((std::vector<double>{2, 3, 4}), drain());
}

TEST_F(OwnedBufferDrainTest, DrainedRowOutlivesSorterAndUndrainedRows) {
  attach(RPSorter_NewByScore(4, nullptr));
  ASSERT_EQ(RS_RESULT_TIMEDOUT, rp->Next(rp, &row));
  ASSERT_EQ(RP_DRAIN_OK, rp->Drain(rp, &row));
  rp->Free(rp);
  rp = nullptr;
  EXPECT_EQ(4, SearchResult_GetScore(&row));
  const auto *value = RLookupRow_Get(key, SearchResult_GetRowData(&row));
  ASSERT_NE(nullptr, value);
  EXPECT_EQ(4, RSValue_Number_Get(value));
}

TEST_F(OwnedBufferDrainTest, MaximumNormalizationUsesCommittedMaximum) {
  attach(RPMaxScoreNormalizer_New(key));
  ASSERT_EQ(RS_RESULT_TIMEDOUT, rp->Next(rp, &row));
  ASSERT_EQ(RP_DRAIN_OK, rp->Drain(rp, &row));
  EXPECT_DOUBLE_EQ(0.75, SearchResult_GetScore(&row));
  EXPECT_DOUBLE_EQ(0.75, RSValue_Number_Get(RLookupRow_Get(key, SearchResult_GetRowData(&row))));
  SearchResult_Clear(&row);
  EXPECT_EQ((std::vector<double>{0.5, 1, 0.25}), drain());
}

TEST_F(OwnedBufferDrainTest, ZeroMaximumDoesNotDivide) {
  source.scores = {0, 0};
  attach(RPMaxScoreNormalizer_New(key));
  ASSERT_EQ(RS_RESULT_TIMEDOUT, rp->Next(rp, &row));
  EXPECT_EQ((std::vector<double>{0, 0}), drain());
}

TEST_F(OwnedBufferDrainTest, DepleterResumesAfterNextPrefixWithoutUpstreamCalls) {
  source.terminal = RS_RESULT_EOF;
  attach(RPDepleter_New());
  ASSERT_EQ(RS_RESULT_OK, rp->Next(rp, &row));
  EXPECT_EQ(1, SearchResult_GetScore(&row));
  SearchResult_Clear(&row);
  const unsigned calls = source.nextCalls;
  EXPECT_EQ((std::vector<double>{4, 2, 3}), drain());
  EXPECT_EQ(calls, source.nextCalls);
}

TEST_F(OwnedBufferDrainTest, DepleterKeepsBufferedRowsWhenExecutionTimesOut) {
  attach(RPDepleter_New());
  ASSERT_EQ(RS_RESULT_TIMEDOUT, rp->Next(rp, &row));
  EXPECT_EQ((std::vector<double>{1, 4, 2, 3}), drain());
}

TEST_F(OwnedBufferDrainTest, DepleterPropagatesSuspensionBeforeYieldingAndResumesAccumulation) {
  source.scores = {1, 4};
  source.terminal = RS_RESULT_SUSPENDED;
  attach(RPDepleter_New());
  EXPECT_EQ(RS_RESULT_SUSPENDED, rp->Next(rp, &row));
  EXPECT_EQ(0, SearchResult_GetDocId(&row));
  source.scores.insert(source.scores.end(), {2, 3});
  source.terminal = RS_RESULT_EOF;
  ASSERT_EQ(RS_RESULT_OK, rp->Next(rp, &row));
  EXPECT_EQ(1, SearchResult_GetScore(&row));
  SearchResult_Clear(&row);
  EXPECT_EQ((std::vector<double>{4, 2, 3}), drain());
  EXPECT_EQ(6, source.nextCalls);
}

TEST_F(OwnedBufferDrainTest, DepleterProfileIncludesSuspendedTimeExactlyOnce) {
  source.terminal = RS_RESULT_SUSPENDED;
  attach(RPDepleter_New());
  auto *profile = RPProfile_New(rp, &qctx);
  qctx.endProc = profile;
  EXPECT_EQ(RS_RESULT_SUSPENDED, profile->Next(profile, &row));
  const auto before = RPProfile_GetTime(profile);
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  Profile_ResumeRPs(&qctx);
  const auto resumed = RPProfile_GetTime(profile);
  EXPECT_GE(resumed - before, std::chrono::nanoseconds(std::chrono::milliseconds(1)).count());
  Profile_ResumeRPs(&qctx);
  EXPECT_EQ(resumed, RPProfile_GetTime(profile));
  EXPECT_EQ(0, RPProfile_GetCount(profile));
  profile->Free(profile);
}

TEST_F(OwnedBufferDrainTest, SorterRetainsHeapAcrossSuspensionAndRestoresBudget) {
  source.terminal = RS_RESULT_SUSPENDED;
  attach(RPSorter_NewByScore(3, nullptr));
  EXPECT_EQ(RS_RESULT_SUSPENDED, rp->Next(rp, &row));
  EXPECT_EQ(10, qctx.resultLimit);
  source.scores.push_back(9);
  source.terminal = RS_RESULT_EOF;
  ASSERT_EQ(RS_RESULT_OK, rp->Next(rp, &row));
  EXPECT_EQ(9, SearchResult_GetScore(&row));
  SearchResult_Clear(&row);
  EXPECT_EQ((std::vector<double>{4, 3}), drain());
  EXPECT_EQ(10, qctx.resultLimit);
  EXPECT_EQ(7, source.nextCalls);
}

TEST_F(OwnedBufferDrainTest, NormalizerResumesAccumulationBeforeChoosingFinalMaximum) {
  source.terminal = RS_RESULT_SUSPENDED;
  attach(RPMaxScoreNormalizer_New(key));
  EXPECT_EQ(RS_RESULT_SUSPENDED, rp->Next(rp, &row));
  EXPECT_EQ(10, qctx.resultLimit);
  source.scores.push_back(8);
  source.terminal = RS_RESULT_EOF;
  ASSERT_EQ(RS_RESULT_OK, rp->Next(rp, &row));
  EXPECT_EQ(1, SearchResult_GetScore(&row));
  SearchResult_Clear(&row);
  EXPECT_EQ((std::vector<double>{0.375, 0.25, 0.5, 0.125}), drain());
  EXPECT_EQ(10, qctx.resultLimit);
  EXPECT_EQ(7, source.nextCalls);
}
