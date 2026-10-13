/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "gtest/gtest.h"
#include "common.h"
#include <thread>
#include <vector>
extern "C" {
#include "hybrid/hybrid_request.h"
#include "obfuscation/hidden.h"
#include "result_processor.h"
#include "search_result_ffi.h"
#include "sorting_vector_ffi.h"
#include "search_ctx.h"
#include "search_disk.h"
#include "spec.h"
extern RedisSearchDiskAPI* disk;
extern RedisSearchDisk* disk_db;
int HybridRequest_StartCursors(HybridRequest* req, RedisModuleCtx* replyCtx, QueryError* status,
                               bool backgroundDepletion);
}

class DiskSnapshotCounterTest : public ::testing::Test {
 protected:
  static bool failNextCreate;
  static int sentinel;

  RedisSearchDiskAPI* savedDisk = nullptr;
  RedisSearchDiskAPI api{};
  RedisSearchDiskIndexSpec* index = reinterpret_cast<RedisSearchDiskIndexSpec*>(&sentinel);

  void SetUp() override {
    failNextCreate = false;
    api.index.createSnapshot = [](RedisSearchDiskIndexSpec*) -> RedisSearchDiskSnapshot* {
      return failNextCreate ? nullptr : reinterpret_cast<RedisSearchDiskSnapshot*>(&sentinel);
    };
    api.index.freeSnapshot = [](RedisSearchDiskSnapshot*) {};
    savedDisk = disk;
    disk = &api;
    ASSERT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
  }

  void TearDown() override {
    disk = savedDisk;
  }
};

bool DiskSnapshotCounterTest::failNextCreate = false;
int DiskSnapshotCounterTest::sentinel = 0;

TEST_F(DiskSnapshotCounterTest, CountsCreatedAndFreedSnapshots) {
  RedisSearchDiskSnapshot* a = SearchDisk_CreateSnapshot(index);
  RedisSearchDiskSnapshot* b = SearchDisk_CreateSnapshot(index);
  ASSERT_NE(a, nullptr);
  ASSERT_NE(b, nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 2u);

  SearchDisk_FreeSnapshot(a);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);
  SearchDisk_FreeSnapshot(b);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(DiskSnapshotCounterTest, FailedCreateIsNotCounted) {
  failNextCreate = true;
  EXPECT_EQ(SearchDisk_CreateSnapshot(index), nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(DiskSnapshotCounterTest, FreeingNullIsNotCounted) {
  RedisSearchDiskSnapshot* snapshot = SearchDisk_CreateSnapshot(index);
  SearchDisk_FreeSnapshot(nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);
  SearchDisk_FreeSnapshot(snapshot);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

// A StrongRef over a snapshot must free it through SearchDisk_FreeSnapshot on the last release.
static void freeSnapshot(void* snapshot) {
  SearchDisk_FreeSnapshot(reinterpret_cast<RedisSearchDiskSnapshot*>(snapshot));
}

TEST_F(DiskSnapshotCounterTest, SnapshotRefFreesOnLastRelease) {
  StrongRef ref = StrongRef_New(SearchDisk_CreateSnapshot(index), freeSnapshot);
  StrongRef second = StrongRef_Clone(ref);
  StrongRef third = StrongRef_Clone(ref);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);

  StrongRef_Release(ref);
  StrongRef_Release(second);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);
  StrongRef_Release(third);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(DiskSnapshotCounterTest, ContextsShareOneSnapshotAndReleaseIndependently) {
  RedisSearchCtx first = SEARCH_CTX_STATIC(nullptr, nullptr);
  RedisSearchCtx second = SEARCH_CTX_STATIC(nullptr, nullptr);
  first.diskSnapshot = SearchDisk_CreateSnapshot(index);
  first.diskSnapshotRef = StrongRef_New(first.diskSnapshot, freeSnapshot);

  SearchCtx_ShareDiskSnapshot(&second, &first);
  EXPECT_EQ(second.diskSnapshot, first.diskSnapshot);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);

  // The first context going away must not pull the snapshot out from under the second.
  SearchCtx_CleanUp(&first);
  EXPECT_EQ(first.diskSnapshot, nullptr);
  EXPECT_NE(second.diskSnapshot, nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);

  SearchCtx_CleanUp(&second);
  EXPECT_EQ(second.diskSnapshot, nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(DiskSnapshotCounterTest, SharingWithoutASnapshotLeavesTheContextWithout) {
  RedisSearchCtx first = SEARCH_CTX_STATIC(nullptr, nullptr);
  RedisSearchCtx second = SEARCH_CTX_STATIC(nullptr, nullptr);

  SearchCtx_ShareDiskSnapshot(&second, &first);
  EXPECT_EQ(second.diskSnapshot, nullptr);
  EXPECT_EQ(second.diskSnapshotRef.rm, nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(DiskSnapshotCounterTest, ConcurrentReleasesFreeTheSnapshotExactlyOnce) {
  constexpr int kOwners = 8;
  StrongRef ref = StrongRef_New(SearchDisk_CreateSnapshot(index), freeSnapshot);
  std::vector<StrongRef> refs{ref};
  for (int i = 1; i < kOwners; i++) {
    refs.push_back(StrongRef_Clone(ref));
  }

  std::vector<std::thread> owners;
  for (StrongRef owned : refs) {
    owners.emplace_back([owned] { StrongRef_Release(owned); });
  }
  for (auto& t : owners) {
    t.join();
  }
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

// A two-subquery hybrid request on a disk-backed index, with just enough state for
// HybridRequest_TakeSharedDiskSnapshot: the subqueries' search contexts and their errors.
class HybridSharedSnapshotTest : public DiskSnapshotCounterTest {
 protected:
  bool savedFlex = false;
  RedisSearchDisk* savedDiskDb = nullptr;
  IndexSpec spec{};
  AREQ first{};
  AREQ second{};
  AREQ* subqueries[2] = {&first, &second};
  RedisSearchCtx firstCtx = SEARCH_CTX_STATIC(nullptr, &spec);
  RedisSearchCtx secondCtx = SEARCH_CTX_STATIC(nullptr, &spec);
  HybridRequest hybrid{};

  void SetUp() override {
    DiskSnapshotCounterTest::SetUp();
    savedFlex = isFlex;
    savedDiskDb = disk_db;
    isFlex = true;
    disk_db = reinterpret_cast<RedisSearchDisk*>(&sentinel);
    spec.diskSpec = index;
    spec.specName = NewHiddenString("idx", 3, true);
    first.sctx = &firstCtx;
    second.sctx = &secondCtx;
    first.base.reply.err = QueryError_Default();
    second.base.reply.err = QueryError_Default();
    hybrid.requests = subqueries;
    hybrid.nrequests = 2;
  }

  void TearDown() override {
    QueryError_ClearError(&first.base.reply.err);
    QueryError_ClearError(&second.base.reply.err);
    HiddenString_Free(spec.specName, true);
    isFlex = savedFlex;
    disk_db = savedDiskDb;
    DiskSnapshotCounterTest::TearDown();
  }
};

TEST_F(HybridSharedSnapshotTest, SubqueriesShareOneLiveSnapshotUntilTheLastIsDone) {
  ASSERT_EQ(HybridRequest_TakeSharedDiskSnapshot(&hybrid), REDISMODULE_OK);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);
  ASSERT_NE(firstCtx.diskSnapshot, nullptr);
  EXPECT_EQ(secondCtx.diskSnapshot, firstCtx.diskSnapshot);

  // A subquery cursor can outlive the request, so each context must keep the snapshot alive.
  SearchCtx_CleanUp(&firstCtx);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);
  EXPECT_NE(secondCtx.diskSnapshot, nullptr);
  SearchCtx_CleanUp(&secondCtx);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(HybridSharedSnapshotTest, FailureLeavesNoSnapshotAndIsReportedOnTheFirstSubquery) {
  failNextCreate = true;
  EXPECT_EQ(HybridRequest_TakeSharedDiskSnapshot(&hybrid), REDISMODULE_ERR);
  EXPECT_TRUE(QueryError_HasError(&first.base.reply.err));
  EXPECT_FALSE(QueryError_HasError(&second.base.reply.err));
  EXPECT_EQ(firstCtx.diskSnapshot, nullptr);
  EXPECT_EQ(secondCtx.diskSnapshot, nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(HybridSharedSnapshotTest, IndexWithoutDiskBackingTakesNoSnapshot) {
  spec.diskSpec = nullptr;
  ASSERT_EQ(HybridRequest_TakeSharedDiskSnapshot(&hybrid), REDISMODULE_OK);
  EXPECT_EQ(firstCtx.diskSnapshot, nullptr);
  EXPECT_EQ(secondCtx.diskSnapshot, nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

// Stand-in result processors that record when they are freed and how many snapshots were still
// alive at that moment, to pin the order the teardown must keep.
struct FakeRP {
  ResultProcessor base;
  int id;
};
static std::vector<int> freedIds;
static std::vector<size_t> liveSnapshotsWhenFreed;

static void fakeRPFree(ResultProcessor* rp) {
  freedIds.push_back(reinterpret_cast<FakeRP*>(rp)->id);
  liveSnapshotsWhenFreed.push_back(SearchDisk_GetLiveSnapshotCount());
}

class HybridReleaseUpstreamTest : public HybridSharedSnapshotTest {
 protected:
  FakeRP chains[2][3]{};  // per subquery: [0] root, [1] middle, [2] depleter

  void SetUp() override {
    HybridSharedSnapshotTest::SetUp();
    freedIds.clear();
    liveSnapshotsWhenFreed.clear();
    AREQ* subs[2] = {&first, &second};
    for (int q = 0; q < 2; q++) {
      QueryProcessingCtx* qctx = &subs[q]->pipeline.qctx;
      *qctx = {};
      for (int i = 0; i < 3; i++) {
        chains[q][i].id = q * 10 + i;
        chains[q][i].base.Free = fakeRPFree;
        chains[q][i].base.type = i == 2 ? RP_DEPLETER : RP_INDEX;
        QITR_PushRP(qctx, &chains[q][i].base);
      }
    }
  }
};

TEST_F(HybridReleaseUpstreamTest, FreesUpstreamBeforeTheSnapshotAndKeepsTheDepleter) {
  ASSERT_EQ(HybridRequest_TakeSharedDiskSnapshot(&hybrid), REDISMODULE_OK);
  HybridRequest_ReleaseDepletedUpstream(&hybrid);

  // The middle and root processors go, downstream first, while the snapshot is still alive.
  // The shared snapshot is only freed once the second subquery drops its reference.
  EXPECT_EQ(freedIds, (std::vector<int>{1, 0, 11, 10}));
  EXPECT_EQ(liveSnapshotsWhenFreed, (std::vector<size_t>{1, 1, 1, 1}));
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
  EXPECT_EQ(firstCtx.diskSnapshot, nullptr);
  EXPECT_EQ(secondCtx.diskSnapshot, nullptr);

  // What is left is the depleter alone, so the request's own teardown frees just that.
  for (AREQ* sub : {&first, &second}) {
    QueryProcessingCtx* qctx = &sub->pipeline.qctx;
    EXPECT_EQ(qctx->endProc, qctx->rootProc);
    EXPECT_EQ(qctx->endProc->type, RP_DEPLETER);
    EXPECT_EQ(qctx->endProc->upstream, nullptr);
  }
  freedIds.clear();
  QITR_FreeChain(&first.pipeline.qctx);
  QITR_FreeChain(&second.pipeline.qctx);
  EXPECT_EQ(freedIds, (std::vector<int>{2, 12}));
}

TEST_F(HybridReleaseUpstreamTest, ProfileModeKeepsTheChainAndTheSnapshot) {
  hybrid.reqflags |= QEXEC_F_PROFILE;
  ASSERT_EQ(HybridRequest_TakeSharedDiskSnapshot(&hybrid), REDISMODULE_OK);
  HybridRequest_ReleaseDepletedUpstream(&hybrid);

  EXPECT_TRUE(freedIds.empty());
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);
  EXPECT_NE(firstCtx.diskSnapshot, nullptr);
  EXPECT_NE(first.pipeline.qctx.endProc->upstream, nullptr);

  SearchCtx_CleanUp(&firstCtx);
  SearchCtx_CleanUp(&secondCtx);
}

TEST_F(HybridReleaseUpstreamTest, IndexWithoutDiskBackingIsLeftAlone) {
  spec.diskSpec = nullptr;
  ASSERT_EQ(HybridRequest_TakeSharedDiskSnapshot(&hybrid), REDISMODULE_OK);
  HybridRequest_ReleaseDepletedUpstream(&hybrid);

  EXPECT_TRUE(freedIds.empty());
  EXPECT_NE(first.pipeline.qctx.endProc->upstream, nullptr);
}

// Drives HybridRequest_StartCursors with real foreground depleters over stand-in upstreams, to
// pin which outcomes of depletion release the snapshot and which leave it for the teardown.
class HybridStartCursorsTest : public HybridSharedSnapshotTest {
 protected:
  static int upstreamRc;
  static QueryRequestTimeout* timeoutToTrip;  // marked timed out from inside the upstream read
  FakeRP upstream[2][2]{};                    // per subquery: [0] root, [1] middle
  QueryError status = QueryError_Default();

  static int fakeNext(ResultProcessor*, SearchResult*) {
    if (timeoutToTrip) {
      QueryRequestTimeout_MarkTimedOut(timeoutToTrip);
    }
    return upstreamRc;
  }

  void SetUp() override {
    HybridSharedSnapshotTest::SetUp();
    freedIds.clear();
    liveSnapshotsWhenFreed.clear();
    upstreamRc = RS_RESULT_EOF;
    timeoutToTrip = nullptr;
    TimeoutConfig timeoutConfig = {.queryTimeoutMS = 0, .timeoutPolicy = TimeoutPolicy_Fail};
    QueryRequestTimeout_Init(&hybrid.base.timeout, &timeoutConfig);
    QueryRequestTimeout_BeginCycle(&hybrid.base.timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
    hybrid.base.replyDeferred = true;  // the reply callback replies, not StartCursors

    AREQ* subs[2] = {&first, &second};
    for (int q = 0; q < 2; q++) {
      QueryProcessingCtx* qctx = &subs[q]->pipeline.qctx;
      *qctx = {};
      for (int i = 0; i < 2; i++) {
        upstream[q][i].id = q * 10 + i;
        upstream[q][i].base.Free = fakeRPFree;
        upstream[q][i].base.Next = fakeNext;
        upstream[q][i].base.type = RP_INDEX;
        QITR_PushRP(qctx, &upstream[q][i].base);
      }
      QITR_PushRP(qctx, RPDepleter_New());
      qctx->timeoutPolicy = TimeoutPolicy_Fail;
    }
    ASSERT_EQ(HybridRequest_TakeSharedDiskSnapshot(&hybrid), REDISMODULE_OK);
  }

  void TearDown() override {
    // What AREQ_Free does to the pipelines, then to the contexts.
    QITR_FreeChain(&first.pipeline.qctx);
    QITR_FreeChain(&second.pipeline.qctx);
    SearchCtx_CleanUp(&firstCtx);
    SearchCtx_CleanUp(&secondCtx);
    QueryError_ClearError(&status);
    EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
    HybridSharedSnapshotTest::TearDown();
  }

  bool released() const {
    return firstCtx.diskSnapshot == nullptr && secondCtx.diskSnapshot == nullptr;
  }
};
int HybridStartCursorsTest::upstreamRc = RS_RESULT_EOF;
QueryRequestTimeout* HybridStartCursorsTest::timeoutToTrip = nullptr;

TEST_F(HybridStartCursorsTest, SuccessfulDepletionReleasesUpstreamAndSnapshotBeforePublishing) {
  ASSERT_EQ(HybridRequest_StartCursors(&hybrid, nullptr, &status, false), REDISMODULE_OK);
  EXPECT_TRUE(released());
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
  EXPECT_EQ(freedIds, (std::vector<int>{1, 0, 11, 10}));
  EXPECT_EQ(liveSnapshotsWhenFreed, (std::vector<size_t>{1, 1, 1, 1}));
  EXPECT_EQ(first.base.cursorInfo.disposition, CURSOR_DISPOSITION_PAUSE);
  EXPECT_EQ(second.base.cursorInfo.disposition, CURSOR_DISPOSITION_PAUSE);
}

TEST_F(HybridStartCursorsTest, PartialTimeoutUnderReturnPolicyKeepsTheChainForTheTeardown) {
  // In-flight disk reads are drained when the iterators are freed, which must not delay the reply.
  upstreamRc = RS_RESULT_TIMEDOUT;
  hybrid.base.timeout.config.timeoutPolicy = TimeoutPolicy_Return;
  ASSERT_EQ(HybridRequest_StartCursors(&hybrid, nullptr, &status, false), REDISMODULE_OK);
  EXPECT_FALSE(released());
  EXPECT_TRUE(freedIds.empty());
  EXPECT_EQ(first.base.cursorInfo.disposition, CURSOR_DISPOSITION_PAUSE);
}

TEST_F(HybridStartCursorsTest, TimeoutUnderFailPolicyKeepsTheChainForTheTeardown) {
  upstreamRc = RS_RESULT_TIMEDOUT;
  EXPECT_EQ(HybridRequest_StartCursors(&hybrid, nullptr, &status, false), REDISMODULE_ERR);
  EXPECT_FALSE(released());
  EXPECT_TRUE(freedIds.empty());
}

TEST_F(HybridStartCursorsTest, DepletionErrorKeepsTheChainForTheTeardown) {
  upstreamRc = RS_RESULT_ERROR;
  EXPECT_EQ(HybridRequest_StartCursors(&hybrid, nullptr, &status, false), REDISMODULE_ERR);
  EXPECT_FALSE(released());
  EXPECT_TRUE(freedIds.empty());
}

TEST_F(HybridStartCursorsTest, TimeoutAfterDepletionLeavesATrimmedChainThatTearsDownCleanly) {
  // The blocked-client timeout fires while depleting: depletion itself succeeds and releases,
  // then the publication check fails and the container frees the subqueries.
  timeoutToTrip = &hybrid.base.timeout;
  EXPECT_EQ(HybridRequest_StartCursors(&hybrid, nullptr, &status, false), REDISMODULE_ERR);
  EXPECT_TRUE(released());
  EXPECT_EQ(freedIds, (std::vector<int>{1, 0, 11, 10}));
  EXPECT_EQ(first.base.cursorInfo.disposition, CURSOR_DISPOSITION_FREE);

  // TearDown frees only the depleters left behind; each must be freed exactly once.
}

// Rows buffered by the depleters must stay readable after the iterators and the snapshot they were
// read from are gone: they own their document metadata and carry no borrowed index result.
TEST_F(HybridStartCursorsTest, BufferedRowsAreReadableAfterTheEarlyRelease) {
  static int rowsLeft;
  rowsLeft = 3;
  for (int q = 0; q < 2; q++) {
    upstream[q][0].base.Next = [](ResultProcessor*, SearchResult* res) {
      if (rowsLeft == 0) {
        return static_cast<int>(RS_RESULT_EOF);
      }
      RSDocumentMetadata* dmd = reinterpret_cast<RSDocumentMetadata*>(rm_calloc(1, sizeof(*dmd)));
      dmd->ref_count = 1;
      dmd->sortVector = RSSortingVector_Empty();
      dmd->id = rowsLeft--;
      SearchResult_SetDocId(res, dmd->id);
      SearchResult_SetDocumentMetadata(res, dmd);
      return static_cast<int>(RS_RESULT_OK);
    };
    // The middle processor passes rows through from the root.
    upstream[q][1].base.Next = [](ResultProcessor* rp, SearchResult* res) {
      return rp->upstream->Next(rp->upstream, res);
    };
  }
  ASSERT_EQ(HybridRequest_StartCursors(&hybrid, nullptr, &status, false), REDISMODULE_OK);
  ASSERT_TRUE(released());
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);

  ResultProcessor* depleter = first.pipeline.qctx.endProc;
  for (t_docId expected : {3, 2, 1}) {
    SearchResult res = SearchResult_New();
    ASSERT_EQ(depleter->Next(depleter, &res), RS_RESULT_OK);
    EXPECT_EQ(SearchResult_GetDocId(&res), expected);
    ASSERT_NE(SearchResult_GetDocumentMetadata(&res), nullptr);
    EXPECT_EQ(SearchResult_GetDocumentMetadata(&res)->id, expected);
    SearchResult_Destroy(&res);
  }
  SearchResult res = SearchResult_New();
  EXPECT_EQ(depleter->Next(depleter, &res), RS_RESULT_EOF);
  SearchResult_Destroy(&res);
}
