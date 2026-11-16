/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "config.h"
#include "extension.h"
#include "indexes.h"
#include "iterator_util.h"
#include "iterators/iterator_api.h"
#include "iterators_ffi.h"
#include "query_request.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "result_processor.h"
#include "rmalloc.h"
#include "score_bound.h"
#include "search_ctx.h"
#include "search_result.h"
#include "search_result_ffi.h"
#include "spec.h"
#include "types_ffi.h"

// The threshold passed down for heap minimum `theta`, which the test knows to apply.
static double minCompetitiveFor(double theta) {
  double minCompetitive = NAN;
  EXPECT_TRUE(ScoreBound_MinCompetitive(theta, &minCompetitive)) << theta;
  return minCompetitive;
}

class ScoreBoundTest : public ::testing::Test {};

TEST_F(ScoreBoundTest, dropsOnlyClearlyBelowTheThreshold) {
  const double theta = 2.5;
  const double minCompetitive = minCompetitiveFor(theta);
  const double edge = theta * (1 - SCORE_BOUND_REL_EPSILON);

  // A bound equal to the threshold may still enter the heap through the tie-break.
  EXPECT_FALSE(ScoreBound_CanDrop(theta, minCompetitive));
  EXPECT_FALSE(ScoreBound_CanDrop(edge, minCompetitive));
  EXPECT_FALSE(ScoreBound_CanDrop(std::nextafter(edge, INFINITY), minCompetitive));
  EXPECT_TRUE(ScoreBound_CanDrop(std::nextafter(edge, -INFINITY), minCompetitive));
  // Inside the margin, near ties are kept.
  EXPECT_FALSE(ScoreBound_CanDrop(theta * (1 - SCORE_BOUND_REL_EPSILON / 2), minCompetitive));
  EXPECT_TRUE(ScoreBound_CanDrop(theta * (1 - 2 * SCORE_BOUND_REL_EPSILON), minCompetitive));
  EXPECT_FALSE(ScoreBound_CanDrop(INFINITY, minCompetitive));
}

// A heap minimum that is not positive could drop nothing, so it gives no threshold at all.
TEST_F(ScoreBoundTest, nonPositiveHeapMinimumGivesNoThreshold) {
  for (double theta : {0.0, -0.0, -1.0, -(double)INFINITY, (double)NAN}) {
    double minCompetitive = 42;
    EXPECT_FALSE(ScoreBound_MinCompetitive(theta, &minCompetitive)) << theta;
    EXPECT_EQ(minCompetitive, 42) << theta;
  }
}

// C iterators that leave the hooks unset report no bounds and read as if there were no threshold.
TEST_F(ScoreBoundTest, unsetHooksAreSafeDefaults) {
  MockIterator *mock = new MockIterator(t_docId{1}, t_docId{2}, t_docId{3});
  QueryIterator *it = &mock->base;
  EXPECT_FALSE(QueryIterator_ReportsScoreBounds(it));
  ASSERT_EQ(QueryIterator_ReadWithThreshold(it, INFINITY), ITERATOR_OK);
  EXPECT_EQ(it->lastDocId, 1);
  EXPECT_EQ(QueryIterator_CurrentMaxScore(it), INFINITY);
  t_docId lastDocId = 7;
  double maxScore = 7;
  EXPECT_FALSE(QueryIterator_CurrentBlockBound(it, &lastDocId, &maxScore));
  EXPECT_EQ(lastDocId, 7);
  EXPECT_EQ(maxScore, 7);
  ASSERT_EQ(QueryIterator_ReadWithThreshold(it, INFINITY), ITERATOR_OK);
  EXPECT_EQ(it->lastDocId, 2);
  it->Free(it);
}

/****************************** threshold through the RP chain ******************************/

// Row source standing in for the index RP: yields doc ids 1..count with score = doc id, and
// records the thresholds it is called with.
struct ThresholdSource : public ResultProcessor {
  size_t count;
  size_t produced = 0;
  size_t thresholdCalls = 0;
  double lastThreshold = NAN;
  RSIndexResult *indexResult;

  explicit ThresholdSource(size_t count) : ResultProcessor{}, count(count) {
    indexResult = NewVirtualResult(1, RS_FIELDMASK_ALL);
    type = RP_INDEX;
    Next = next;
    NextWithThreshold = nextWithThreshold;
    Free = [](ResultProcessor *rp) { delete static_cast<ThresholdSource *>(rp); };
  }
  ~ThresholdSource() {
    IndexResult_Free(indexResult);
  }

  static int next(ResultProcessor *rp, SearchResult *res) {
    auto *self = static_cast<ThresholdSource *>(rp);
    if (self->produced == self->count) {
      return RS_RESULT_EOF;
    }
    const t_docId id = ++self->produced;
    self->indexResult->docId = id;
    SearchResult_SetDocId(res, id);
    SearchResult_SetScore(res, (double)id);
    SearchResult_SetBorrowedIndexResult(res, self->indexResult);
    return RS_RESULT_OK;
  }

  static int nextWithThreshold(ResultProcessor *rp, SearchResult *res, double minCompetitive) {
    auto *self = static_cast<ThresholdSource *>(rp);
    self->thresholdCalls++;
    self->lastThreshold = minCompetitive;
    return next(rp, res);
  }
};

// A result processor type that does not opt in to thresholds.
struct PassThrough : public ResultProcessor {
  PassThrough() : ResultProcessor{} {
    type = RP_MAX;
    Next = [](ResultProcessor *rp, SearchResult *res) {
      return rp->upstream->Next(rp->upstream, res);
    };
    Free = [](ResultProcessor *rp) { delete static_cast<PassThrough *>(rp); };
  }
};

static double scoreByDocId(const ScoringFunctionArgs *, const RSIndexResult *res,
                           const RSDocumentMetadata *, double) {
  return (double)res->docId;
}

static double scoreZero(const ScoringFunctionArgs *, const RSIndexResult *,
                        const RSDocumentMetadata *, double) {
  return 0;
}

class ScoreThresholdChainTest : public ::testing::Test {
 protected:
  QueryProcessingCtx qctx = {};
  ThresholdSource *source = nullptr;
  RLookup lookup = RLookup_New();
  bool savedSwitch = RSGlobalConfig.diskScoreSkippingQuery;

  void SetUp() override {
    // The source lends its index result; the sorter must not deep-copy it.
    qctx.skipIndexResultDeepCopy = true;
    RSGlobalConfig.diskScoreSkippingQuery = true;
  }

  void TearDown() override {
    QITR_FreeChain(&qctx);
    RLookup_Cleanup(&lookup);
    RSGlobalConfig.diskScoreSkippingQuery = savedSwitch;
  }

  void push(ResultProcessor *rp) {
    QITR_PushRP(&qctx, rp);
  }

  void pushSource(size_t count) {
    source = new ThresholdSource(count);
    push(source);
  }

  ResultProcessor *newScorer(RSScoringFunction sf = scoreByDocId) {
    ExtScoringFunctionCtx funcs = {.sf = sf};
    ScoringFunctionArgs args = {};
    return RPScorer_New(&funcs, &args, NULL);
  }

  void pushProfile() {
    ResultProcessor *profile = RPProfile_New(qctx.endProc, &qctx);
    qctx.endProc = profile;
  }

  void drain() {
    SearchResult res = SearchResult_New();
    ResultProcessor *end = qctx.endProc;
    while (end->Next(end, &res) == RS_RESULT_OK) {
      SearchResult_Clear(&res);
    }
    SearchResult_Destroy(&res);
  }
};

// Once the heap of K holds docs 1..K, every further pull carries the heap minimum: docs K+1..N
// and the final EOF pull, the last one with heap minimum N-K+1.
TEST_F(ScoreThresholdChainTest, reachesTheSourceThroughScorerProfileAndMetrics) {
  const size_t n = 20, k = 5;
  pushSource(n);
  push(RPMetricsLoader_New());
  pushProfile();
  push(newScorer());
  pushProfile();
  push(RPSorter_NewByScore(k, NULL));
  drain();
  EXPECT_EQ(source->thresholdCalls, n - k + 1);
  EXPECT_EQ(source->lastThreshold, minCompetitiveFor((double)(n - k + 1)));
}

TEST_F(ScoreThresholdChainTest, unknownProcessorBlocksIt) {
  pushSource(20);
  push(newScorer());
  push(new PassThrough());
  push(RPSorter_NewByScore(5, NULL));
  drain();
  EXPECT_EQ(source->thresholdCalls, 0);
}

TEST_F(ScoreThresholdChainTest, sortBySorterPassesNone) {
  pushSource(20);
  push(newScorer());
  const RLookupKey *key = RLookup_GetKey_Write(&lookup, "f", RLOOKUP_F_NOFLAGS);
  push(RPSorter_NewByFields(5, &key, 1, SORTASCMAP_INIT));
  drain();
  EXPECT_EQ(source->thresholdCalls, 0);
}

TEST_F(ScoreThresholdChainTest, heapThatNeverFillsPassesNone) {
  pushSource(4);
  push(newScorer());
  push(RPSorter_NewByScore(5, NULL));
  drain();
  EXPECT_EQ(source->thresholdCalls, 0);
}

TEST_F(ScoreThresholdChainTest, nonPositiveHeapMinimumPassesNone) {
  pushSource(20);
  push(newScorer(scoreZero));
  push(RPSorter_NewByScore(5, NULL));
  drain();
  EXPECT_EQ(source->thresholdCalls, 0);
}

TEST_F(ScoreThresholdChainTest, killSwitchPassesNone) {
  RSGlobalConfig.diskScoreSkippingQuery = false;
  pushSource(20);
  push(newScorer());
  push(RPSorter_NewByScore(5, NULL));
  drain();
  EXPECT_EQ(source->thresholdCalls, 0);
}

/****************************** index RP read path ******************************/

static size_t thresholdReads = 0;

static IteratorStatus countingReadWithThreshold(QueryIterator *it, double) {
  thresholdReads++;
  return it->Read(it);
}

static bool reportsBounds(const QueryIterator *) {
  return true;
}

// An in-memory index with no documents, so the index RP reads every candidate to EOF.
class EmptyIndexTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx = nullptr;
  RedisSearchCtx *sctx = nullptr;
  QueryRequestLimits limits = {};
  QueryProcessingCtx qctx = {};
  ResultProcessor *rp = nullptr;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(NULL);
    RMCK::flushdb(ctx);
    static int indexCount = 0;
    const std::string name = "score_threshold" + std::to_string(++indexCount);
    QueryError qerr = QueryError_Default();
    RMCK::ArgvList args(ctx, "FT.CREATE", name.c_str(), "ON", "HASH", "SCHEMA", "t", "TEXT");
    ASSERT_TRUE(Indexes_CreateNewSpec(ctx, args, args.size(), &qerr));
    sctx = NewSearchCtxC(ctx, name.c_str(), true);
    ASSERT_TRUE(sctx);
    TimeoutConfig timeoutConfig = {0, TimeoutPolicy_Return};
    QueryRequestLimits_Init(&limits, &timeoutConfig);
    sctx->limits = &limits;
  }

  void TearDown() override {
    if (rp) rp->Free(rp);
    if (sctx) SearchCtx_Free(sctx);
    RMCK::flushdb(ctx);
    RedisModule_FreeThreadSafeContext(ctx);
  }

  // The index RP takes ownership of `root`.
  void nextWithThreshold(QueryIterator *root, double minCompetitive) {
    pushIndexRP(root);
    SearchResult res = SearchResult_New();
    EXPECT_EQ(RP_NextWithThreshold(rp, &res, minCompetitive), RS_RESULT_EOF);
    SearchResult_Destroy(&res);
  }

  // The index RP takes ownership of `root`.
  void next(QueryIterator *root) {
    pushIndexRP(root);
    SearchResult res = SearchResult_New();
    EXPECT_EQ(rp->Next(rp, &res), RS_RESULT_EOF);
    SearchResult_Destroy(&res);
  }

 private:
  void pushIndexRP(QueryIterator *root) {
    rp = RPQueryIterator_New(root, NULL, 0, sctx, false, false);
    QITR_PushRP(&qctx, rp);
  }
};

class ScoreThresholdIndexTest : public EmptyIndexTest {
 protected:
  MockIterator *mock = nullptr;

  void SetUp() override {
    EmptyIndexTest::SetUp();
    mock = new MockIterator(t_docId{1}, t_docId{2}, t_docId{3});
    mock->base.ReadWithThreshold = countingReadWithThreshold;
    thresholdReads = 0;
  }

  void nextWithThreshold(double minCompetitive) {
    EmptyIndexTest::nextWithThreshold(&mock->base, minCompetitive);
  }
};

TEST_F(ScoreThresholdIndexTest, rootWithoutBoundsTakesThePlainRead) {
  nextWithThreshold(1.0);
  EXPECT_EQ(thresholdReads, 0);
  EXPECT_GT(mock->readCount, 0);
}

TEST_F(ScoreThresholdIndexTest, rootWithBoundsReadsWithTheThreshold) {
  mock->base.ReportsScoreBounds = reportsBounds;
  nextWithThreshold(1.0);
  EXPECT_GT(thresholdReads, 0);
}
