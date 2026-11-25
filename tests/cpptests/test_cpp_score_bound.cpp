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
#include "search_ctx.h"
#include "search_result.h"
#include "search_result_ffi.h"
#include "spec.h"
#include "types_ffi.h"

class ScoreBoundTest : public ::testing::Test {};

// The defaults `QueryIterator_SetNoScoreBounds` installs report no bounds and read as if there
// were no threshold.
TEST_F(ScoreBoundTest, noScoreBoundsDefaults) {
  MockIterator *mock = new MockIterator(t_docId{1}, t_docId{2}, t_docId{3});
  QueryIterator *it = &mock->base;
  EXPECT_FALSE(it->ReportsScoreBounds(it));
  ASSERT_EQ(it->ReadWithThreshold(it, INFINITY), ITERATOR_OK);
  EXPECT_EQ(it->lastDocId, 1);
  EXPECT_EQ(it->CurrentMaxScore(it), INFINITY);
  t_docId lastDocId = 7;
  double maxScore = 7;
  EXPECT_FALSE(it->CurrentBlockBound(it, &lastDocId, &maxScore));
  EXPECT_EQ(lastDocId, 7);
  EXPECT_EQ(maxScore, 7);
  ASSERT_EQ(it->ReadWithThreshold(it, INFINITY), ITERATOR_OK);
  EXPECT_EQ(it->lastDocId, 2);
  it->Free(it);
}

/****************************** index RP fixtures ******************************/

static size_t thresholdReads = 0;
static double lastThreshold = NAN;

static IteratorStatus countingReadWithThreshold(QueryIterator *it, double minCompetitive) {
  thresholdReads++;
  lastThreshold = minCompetitive;
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
    qctx.minCompetitiveScore = minCompetitive;
    next(root);
  }

  // The index RP takes ownership of `root`.
  void next(QueryIterator *root) {
    pushIndexRP(root);
    SearchResult res = SearchResult_New();
    EXPECT_EQ(rp->Next(rp, &res), RS_RESULT_EOF);
    SearchResult_Destroy(&res);
  }

 protected:
  void pushIndexRP(QueryIterator *root) {
    rp = RPQueryIterator_New(root, NULL, 0, sctx, false, false, false);
    QITR_PushRP(&qctx, rp);
  }
};

/****************************** threshold through the RP chain ******************************/

// A row source other than the index RP, yielding doc ids 1..count.
struct OtherRowSource : public ResultProcessor {
  size_t count;
  size_t produced = 0;
  RSIndexResult *indexResult;

  explicit OtherRowSource(size_t count) : ResultProcessor{}, count(count) {
    indexResult = NewVirtualResult(1, RS_FIELDMASK_ALL);
    type = RP_NETWORK;
    Next = next;
    Free = [](ResultProcessor *rp) { delete static_cast<OtherRowSource *>(rp); };
  }
  ~OtherRowSource() {
    IndexResult_Free(indexResult);
  }

  static int next(ResultProcessor *rp, SearchResult *res) {
    auto *self = static_cast<OtherRowSource *>(rp);
    if (self->produced == self->count) {
      return RS_RESULT_EOF;
    }
    const t_docId id = ++self->produced;
    self->indexResult->docId = id;
    SearchResult_SetDocId(res, id);
    SearchResult_SetBorrowedIndexResult(res, self->indexResult);
    return RS_RESULT_OK;
  }
};

// Forwards every row unchanged under whatever type it is given.
struct PassThrough : public ResultProcessor {
  explicit PassThrough(ResultProcessorType rpType = RP_MAX) : ResultProcessor{} {
    type = rpType;
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

static double scoreNegative(const ScoringFunctionArgs *, const RSIndexResult *res,
                            const RSDocumentMetadata *, double) {
  return -(double)res->docId;
}

static double scoreNaN(const ScoringFunctionArgs *, const RSIndexResult *,
                       const RSDocumentMetadata *, double) {
  return NAN;
}

// The index RP over a mock root yielding docs 1..count, which the doc table holds, followed by
// the processors a test pushes. `thresholdReads` counts the reads the root is given a threshold
// for.
class ScoreThresholdChainTest : public EmptyIndexTest {
 protected:
  static constexpr size_t kDocs = 20;
  RLookup lookup = RLookup_New();

  void SetUp() override {
    EmptyIndexTest::SetUp();
    for (size_t id = 1; id <= kDocs; id++) {
      const std::string key = "doc" + std::to_string(id);
      RSDocumentMetadata *dmd = DocTable_Put(&sctx->spec->docs, key.c_str(), key.size(), 1.0,
                                             Document_DefaultFlags, nullptr, 0, DocumentType_Hash);
      ASSERT_EQ(dmd->id, id);
      DMD_Return(dmd);
    }
    thresholdReads = 0;
    lastThreshold = NAN;
  }

  void TearDown() override {
    QITR_FreeChain(&qctx);
    rp = nullptr;
    RLookup_Cleanup(&lookup);
    EmptyIndexTest::TearDown();
  }

  void push(ResultProcessor *processor) {
    QITR_PushRP(&qctx, processor);
  }

  // `reportsScoreBounds` false stands for a RAM root.
  void pushIndex(size_t count, bool reportsScoreBounds = true) {
    ASSERT_LE(count, kDocs);
    auto *mock = new MockIterator();
    for (t_docId id = 1; id <= count; id++) {
      mock->docIds.push_back(id);
    }
    mock->base.ReadWithThreshold = countingReadWithThreshold;
    if (reportsScoreBounds) {
      mock->base.ReportsScoreBounds = reportsBounds;
    }
    pushIndexRP(&mock->base);
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
TEST_F(ScoreThresholdChainTest, reachesTheRootThroughScorerProfileAndMetrics) {
  const size_t n = 20, k = 5;
  pushIndex(n);
  push(RPMetricsLoader_New());
  pushProfile();
  push(newScorer());
  pushProfile();
  push(RPSorter_NewByScore(k, NULL, true));
  drain();
  EXPECT_EQ(thresholdReads, n - k + 1);
  EXPECT_EQ(lastThreshold, (double)(n - k + 1));
  EXPECT_EQ(qctx.minCompetitiveScore, (double)(n - k + 1));
}

// A RAM root reports no bounds, so its index RP never reads the threshold: a full heap does not
// publish one.
TEST_F(ScoreThresholdChainTest, rootWithoutBoundsLeavesItUnpublished) {
  pushIndex(20, false);
  push(newScorer());
  push(RPSorter_NewByScore(5, NULL, true));
  drain();
  EXPECT_EQ(thresholdReads, 0);
  EXPECT_EQ(qctx.minCompetitiveScore, 0);
}

TEST_F(ScoreThresholdChainTest, unknownProcessorBlocksIt) {
  pushIndex(20);
  push(newScorer());
  push(new PassThrough());
  push(RPSorter_NewByScore(5, NULL, true));
  drain();
  EXPECT_EQ(thresholdReads, 0);
}

// Only the index RP skips by the threshold, so a chain rooted elsewhere gets none.
TEST_F(ScoreThresholdChainTest, rowSourceOtherThanTheIndexGetsNone) {
  // The source lends its index result; the sorter must not deep-copy it.
  qctx.skipIndexResultDeepCopy = true;
  push(new OtherRowSource(20));
  push(newScorer());
  push(RPSorter_NewByScore(5, NULL, true));
  drain();
  EXPECT_EQ(qctx.minCompetitiveScore, 0);
}

struct ProcessorTypeCase {
  const char *name;
  ResultProcessorType type;
  bool keepsThreshold;
};

class ScoreThresholdProcessorTypeTest : public ScoreThresholdChainTest,
                                        public ::testing::WithParamInterface<ProcessorTypeCase> {};

// A processor of the given type between the scorer and the sorter: the threshold reaches the
// root only through the types that keep every row's score from its own candidate.
TEST_P(ScoreThresholdProcessorTypeTest, betweenScorerAndSorter) {
  const size_t n = 20, k = 5;
  pushIndex(n);
  push(newScorer());
  push(new PassThrough(GetParam().type));
  push(RPSorter_NewByScore(k, NULL, true));
  drain();
  EXPECT_EQ(thresholdReads, GetParam().keepsThreshold ? n - k + 1 : 0);
}

INSTANTIATE_TEST_SUITE_P(
    Types, ScoreThresholdProcessorTypeTest,
    ::testing::Values(
        ProcessorTypeCase{"Metrics", RP_METRICS, true},
        ProcessorTypeCase{"Projector", RP_PROJECTOR, true},
        ProcessorTypeCase{"Filter", RP_FILTER, true}, ProcessorTypeCase{"Loader", RP_LOADER, true},
        ProcessorTypeCase{"SafeLoader", RP_SAFE_LOADER, true},
        ProcessorTypeCase{"KeyNameLoader", RP_KEY_NAME_LOADER, true},
        ProcessorTypeCase{"DiskAsyncLoader", RP_DISK_ASYNC_LOADER, true},
        ProcessorTypeCase{"Profile", RP_PROFILE, true},
        ProcessorTypeCase{"Timeout", RP_TIMEOUT, true}, ProcessorTypeCase{"Pause", RP_PAUSE, true},
        ProcessorTypeCase{"MaxScoreNormalizer", RP_MAX_SCORE_NORMALIZER, false},
        ProcessorTypeCase{"VectorNormalizer", RP_VECTOR_NORMALIZER, false},
        ProcessorTypeCase{"HybridMerger", RP_HYBRID_MERGER, false},
        ProcessorTypeCase{"Group", RP_GROUP, false}, ProcessorTypeCase{"Sorter", RP_SORTER, false},
        ProcessorTypeCase{"Depleter", RP_DEPLETER, false},
        ProcessorTypeCase{"SafeDepleter", RP_SAFE_DEPLETER, false}),
    [](const ::testing::TestParamInfo<ProcessorTypeCase> &info) {
      return std::string(info.param.name);
    });

TEST_F(ScoreThresholdChainTest, sortBySorterPassesNone) {
  pushIndex(20);
  push(newScorer());
  const RLookupKey *key = RLookup_GetKey_Write(&lookup, "f", RLOOKUP_F_NOFLAGS);
  push(RPSorter_NewByFields(5, &key, 1, SORTASCMAP_INIT));
  drain();
  EXPECT_EQ(thresholdReads, 0);
}

TEST_F(ScoreThresholdChainTest, heapThatNeverFillsPassesNone) {
  pushIndex(4);
  push(newScorer());
  push(RPSorter_NewByScore(5, NULL, true));
  drain();
  EXPECT_EQ(thresholdReads, 0);
}

TEST_F(ScoreThresholdChainTest, nonPositiveHeapMinimumPassesNone) {
  pushIndex(20);
  push(newScorer(scoreZero));
  push(RPSorter_NewByScore(5, NULL, true));
  drain();
  EXPECT_EQ(thresholdReads, 0);
}

TEST_F(ScoreThresholdChainTest, negativeHeapMinimumPassesNone) {
  pushIndex(20);
  push(newScorer(scoreNegative));
  push(RPSorter_NewByScore(5, NULL, true));
  drain();
  EXPECT_EQ(thresholdReads, 0);
}

TEST_F(ScoreThresholdChainTest, nanHeapMinimumPassesNone) {
  pushIndex(20);
  push(newScorer(scoreNaN));
  push(RPSorter_NewByScore(5, NULL, true));
  drain();
  EXPECT_EQ(thresholdReads, 0);
}

TEST_F(ScoreThresholdChainTest, killSwitchPassesNone) {
  pushIndex(20);
  push(newScorer());
  push(RPSorter_NewByScore(5, NULL, false));
  drain();
  EXPECT_EQ(thresholdReads, 0);
}

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

// The sorter publishes its heap minimum as is; only a positive one is a threshold.
TEST_F(ScoreThresholdIndexTest, nonPositiveThresholdTakesThePlainRead) {
  mock->base.ReportsScoreBounds = reportsBounds;
  nextWithThreshold(0);
  EXPECT_EQ(thresholdReads, 0);
  EXPECT_GT(mock->readCount, 0);
}

TEST_F(ScoreThresholdIndexTest, nanThresholdTakesThePlainRead) {
  mock->base.ReportsScoreBounds = reportsBounds;
  nextWithThreshold(NAN);
  EXPECT_EQ(thresholdReads, 0);
  EXPECT_GT(mock->readCount, 0);
}
