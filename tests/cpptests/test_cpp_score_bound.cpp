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
    base.ReadWithThreshold = [](QueryIterator *it, double minCompetitiveScore) {
      self(it)->thresholdedReads++;
      IteratorStatus rc;
      do {
        rc = it->Read(it);
      } while (rc == ITERATOR_OK && it->CurrentMaxScore(it) < minCompetitiveScore);
      return rc;
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
    base.Suspend = Default_Suspend;
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
  ASSERT_TRUE(root->ReportsScoreBounds(root));

  nextWithThreshold(root, 3.0);

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
    ASSERT_TRUE(root->ReportsScoreBounds(root));

    nextWithThreshold(root, 3.0);

    EXPECT_EQ(a->visited, (std::vector<t_docId>{1, 5, 9, 10, 11, 12}));
    EXPECT_EQ(a->thresholdedReads + far->thresholdedReads, 0);
  }
}

// Without a threshold the same intersection visits every document.
TEST_F(ScoreBoundCompositeTest, plainNextSkipsNothing) {
  auto *a = new BlockedLeaf({{1, 10, 1.0}, {11, 20, 3.0}});
  auto *b = new BlockedLeaf({{1, 20, 1.0}});
  QueryIterator *root = NewIntersectionIterator(childArray({a, b}), 2, -1, false, 1.0);

  next(root);

  EXPECT_EQ(a->visited, idRange(1, 20));
}
