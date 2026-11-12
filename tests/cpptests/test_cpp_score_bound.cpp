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

class ScoreBoundTest : public ::testing::Test {};

TEST_F(ScoreBoundTest, dropsOnlyClearlyBelowTheThreshold) {
  const double theta = 2.5;
  const double minCompetitive = ScoreBound_MinCompetitive(theta);
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

TEST_F(ScoreBoundTest, nonPositiveThresholdDropsNothing) {
  for (double theta : {0.0, -0.0, -1.0}) {
    const double minCompetitive = ScoreBound_MinCompetitive(theta);
    EXPECT_EQ(minCompetitive, -INFINITY);
    EXPECT_FALSE(ScoreBound_CanDrop(0, minCompetitive));
    EXPECT_FALSE(ScoreBound_CanDrop(-1e300, minCompetitive));
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

  ResultProcessor *newScorer() {
    ExtScoringFunctionCtx funcs = {.sf = scoreByDocId};
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
  EXPECT_EQ(source->lastThreshold, ScoreBound_MinCompetitive((double)(n - k + 1)));
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
    rp = RPQueryIterator_New(root, NULL, 0, sctx, false, false);
    QITR_PushRP(&qctx, rp);
    SearchResult res = SearchResult_New();
    EXPECT_EQ(RP_NextWithThreshold(rp, &res, minCompetitive), RS_RESULT_EOF);
    SearchResult_Destroy(&res);
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

TEST_F(ScoreThresholdIndexTest, withdrawnThresholdTakesThePlainRead) {
  mock->base.ReportsScoreBounds = reportsBounds;
  nextWithThreshold(-INFINITY);
  EXPECT_EQ(thresholdReads, 0);
}

/****************************** composites over blocked C leaves ******************************/

// A C leaf over dense runs of doc ids, one run per storage block, every document of a block
// scoring the block's score. It reports every score-bound hook and records the documents it
// positions on; blocks a skip jumps over are not visited.
class BlockedLeaf {
 public:
  struct Block {
    t_docId first, last;
    double score;
  };

  QueryIterator base;
  std::vector<Block> blocks;
  size_t block = 0;
  std::vector<t_docId> visited;
  size_t thresholdedReads = 0;

  explicit BlockedLeaf(std::vector<Block> blocks) : base{}, blocks(std::move(blocks)) {
    base.type = MAX_ITERATOR;
    base.current = NewVirtualResult(1, RS_FIELDMASK_ALL);
    base.Read = [](QueryIterator *it) { return self(it)->land(it->lastDocId + 1); };
    base.SkipTo = [](QueryIterator *it, t_docId target) {
      const IteratorStatus rc = self(it)->land(target);
      return rc == ITERATOR_OK && it->lastDocId != target ? ITERATOR_NOTFOUND : rc;
    };
    base.ReadWithThreshold = [](QueryIterator *it, double) {
      self(it)->thresholdedReads++;
      return it->Read(it);
    };
    base.NumEstimated = [](const QueryIterator *it) {
      size_t n = 0;
      for (const Block &b : self(it)->blocks) n += b.last - b.first + 1;
      return n;
    };
    base.Rewind = [](QueryIterator *it) {
      self(it)->block = 0;
      it->lastDocId = it->current->docId = 0;
      it->atEOF = false;
    };
    base.Revalidate = [](QueryIterator *, IndexSpec *) { return VALIDATE_OK; };
    base.Free = [](QueryIterator *it) { delete self(it); };
    base.ReportsScoreBounds = [](const QueryIterator *) { return true; };
    base.CurrentMaxScore = [](const QueryIterator *it) {
      return self(it)->positioned() ? self(it)->blocks[self(it)->block].score : INFINITY;
    };
    base.CurrentBlockBound = [](const QueryIterator *it, t_docId *lastDocId, double *maxScore) {
      if (!self(it)->positioned()) return false;
      const Block &b = self(it)->blocks[self(it)->block];
      *lastDocId = b.last;
      *maxScore = b.score;
      return true;
    };
  }

  ~BlockedLeaf() {
    IndexResult_Free(base.current);
  }

 private:
  static BlockedLeaf *self(QueryIterator *it) {
    return reinterpret_cast<BlockedLeaf *>(it);
  }
  static const BlockedLeaf *self(const QueryIterator *it) {
    return reinterpret_cast<const BlockedLeaf *>(it);
  }

  bool positioned() const {
    return base.lastDocId != 0 && !base.atEOF;
  }

  // Positions on the first document at or after `target`.
  IteratorStatus land(t_docId target) {
    if (base.atEOF) return ITERATOR_EOF;
    while (block < blocks.size() && blocks[block].last < target) block++;
    if (block == blocks.size()) {
      base.atEOF = true;
      return ITERATOR_EOF;
    }
    const t_docId id = std::max(target, blocks[block].first);
    base.lastDocId = base.current->docId = id;
    visited.push_back(id);
    return ITERATOR_OK;
  }
};

static std::vector<t_docId> idRange(t_docId first, t_docId last) {
  std::vector<t_docId> ids;
  for (t_docId id = first; id <= last; id++) ids.push_back(id);
  return ids;
}

static QueryIterator **childArray(std::initializer_list<BlockedLeaf *> leaves) {
  QueryIterator **its = (QueryIterator **)rm_malloc(leaves.size() * sizeof(*its));
  size_t i = 0;
  for (BlockedLeaf *leaf : leaves) its[i++] = &leaf->base;
  return its;
}

using ScoreBoundCompositeTest = EmptyIndexTest;

// The index RP reads a Rust intersection of C leaves with the threshold; the intersection skips
// the weak window of its leading leaf and gives its children plain reads only.
TEST_F(ScoreBoundCompositeTest, intersectionSkipsWindowsThroughTheIndexRP) {
  auto *a = new BlockedLeaf({{1, 10, 1.0}, {11, 20, 3.0}});
  auto *b = new BlockedLeaf({{1, 20, 1.0}});
  QueryIterator *root = NewIntersectionIterator(childArray({a, b}), 2, -1, false, 1.0);
  ASSERT_TRUE(QueryIterator_ReportsScoreBounds(root));

  nextWithThreshold(root, ScoreBound_MinCompetitive(3.0));

  std::vector<t_docId> expected = {1, 2};
  for (t_docId id : idRange(11, 20)) expected.push_back(id);
  EXPECT_EQ(a->visited, expected);
  EXPECT_EQ(a->thresholdedReads + b->thresholdedReads, 0);
}

// The same through a Rust union of C leaves, flat and heap.
TEST_F(ScoreBoundCompositeTest, unionSkipsWindowsThroughTheIndexRP) {
  for (uint32_t minUnionIterHeap : {UINT32_MAX, 0u}) {
    SCOPED_TRACE(minUnionIterHeap);
    if (rp) {
      rp->Free(rp);
      rp = nullptr;
      qctx = {};
    }
    // `far` positions on its only document up front, so the union's window covers both leaves.
    auto *far = new BlockedLeaf({{40, 40, 0.0}});
    auto *a = new BlockedLeaf({{1, 4, 1.0}, {5, 8, 1.0}, {9, 12, 4.0}});
    IteratorsConfig config{};
    config.minUnionIterHeap = minUnionIterHeap;
    QueryIterator *root =
        NewUnionIterator(childArray({far, a}), 2, false, 1.0, QN_UNION, NULL, &config);
    ASSERT_TRUE(QueryIterator_ReportsScoreBounds(root));

    nextWithThreshold(root, ScoreBound_MinCompetitive(3.0));

    EXPECT_EQ(a->visited, (std::vector<t_docId>{1, 5, 9, 10, 11, 12}));
    EXPECT_EQ(a->thresholdedReads + far->thresholdedReads, 0);
  }
}

// Without a threshold the same intersection visits every document.
TEST_F(ScoreBoundCompositeTest, plainNextSkipsNothing) {
  auto *a = new BlockedLeaf({{1, 10, 1.0}, {11, 20, 3.0}});
  auto *b = new BlockedLeaf({{1, 20, 1.0}});
  QueryIterator *root = NewIntersectionIterator(childArray({a, b}), 2, -1, false, 1.0);

  nextWithThreshold(root, -INFINITY);

  EXPECT_EQ(a->visited, idRange(1, 20));
}
