/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "module.h"
#include "rmr.h"
#include "coord/config.h"
#include "slot_ranges.h"
#include "concurrent_ctx.h"
#include "indexes.h"
#include "info/info_redis/threads/current_thread.h"
#include "redismock/util.h"
#include <atomic>
#include <chrono>
#include <future>
#include <thread>

extern size_t NumShards;

class ConnectionStateTimingTest : public testing::TestWithParam<size_t> {
 protected:
  static ConnectionStateTimingTest* current;
  decltype(RedisModule_BlockClient) savedBlock = RedisModule_BlockClient;
  decltype(RedisModule_UnblockClient) savedUnblock = RedisModule_UnblockClient;
  decltype(RedisModule_BlockedClientMeasureTimeStart) savedStart =
      RedisModule_BlockedClientMeasureTimeStart;
  decltype(RedisModule_BlockedClientMeasureTimeEnd) savedEnd =
      RedisModule_BlockedClientMeasureTimeEnd;
  decltype(RedisModule_GetBlockedClientPrivateData) savedPrivateData =
      RedisModule_GetBlockedClientPrivateData;
  decltype(RedisModule_ReplyWithCString) savedReplyCString = RedisModule_ReplyWithCString;
  decltype(RedisModule_ReplyWithArray) savedReplyArray = RedisModule_ReplyWithArray;
  decltype(RedisModule_ReplyWithMap) savedReplyMap = RedisModule_ReplyWithMap;
  decltype(RedisModule_BlockClientSetPrivateData) savedSetData =
      RedisModule_BlockClientSetPrivateData;
  decltype(RedisModule_ReplyWithError) savedReplyError = RedisModule_ReplyWithError;
  RedisModuleCmdFunc timeoutCallback = nullptr;
  bool allowTimeout = false;
  int expectedEnds = 1;
  bool replyAfterTiming = true;
  RedisModuleCmdFunc reply = nullptr;
  void (*freeData)(RedisModuleCtx*, void*) = nullptr;
  std::promise<void*> completion;
  void* privateData = nullptr;
  std::atomic<int> starts{0};
  std::atomic<int> ends{0};
  std::thread::id commandThread = std::this_thread::get_id();
  size_t savedNumShards = NumShards;
  size_t savedValidationTimeout = clusterConfig.topologyValidationTimeoutMS;

  RedisModuleBlockedClient* handle() {
    return reinterpret_cast<RedisModuleBlockedClient*>(this);
  }

  static void freeCluster() {
    // MR_FreeCluster requires the Redis lock, which the mock harness does not acquire.
    RedisModule_ThreadSafeContextLock(RSDummyContext);
    MR_FreeCluster();
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
  }

  void SetUp() override {
    current = this;
    freeCluster();
    MR_InitLocalNodeId();
    MR_Init(GetParam(), 1, 0);
    clusterConfig.topologyValidationTimeoutMS = 1;
    RedisModule_BlockClient = [](RedisModuleCtx*, RedisModuleCmdFunc reply,
                                 RedisModuleCmdFunc timeout,
                                 void (*freeData)(RedisModuleCtx*, void*), long long timeoutMs) {
      if (!current->allowTimeout) EXPECT_EQ(timeout, nullptr);
      current->timeoutCallback = timeout;
      EXPECT_EQ(timeoutMs, 0);
      current->reply = reply;
      current->freeData = freeData;
      return current->handle();
    };
    RedisModule_BlockedClientMeasureTimeStart = [](RedisModuleBlockedClient* bc) {
      EXPECT_EQ(bc, current->handle());
      EXPECT_NE(std::this_thread::get_id(), current->commandThread);
      EXPECT_EQ(current->starts.fetch_add(1), 0);
      return REDISMODULE_OK;
    };
    RedisModule_BlockedClientMeasureTimeEnd = [](RedisModuleBlockedClient* bc) {
      EXPECT_EQ(bc, current->handle());
      EXPECT_EQ(current->starts.load(), 1);
      EXPECT_EQ(current->ends.fetch_add(1), 0);
      return REDISMODULE_OK;
    };
    RedisModule_UnblockClient = [](RedisModuleBlockedClient* bc, void* data) {
      EXPECT_EQ(bc, current->handle());
      EXPECT_EQ(current->ends.load(), current->expectedEnds);
      current->completion.set_value(data);
      return REDISMODULE_OK;
    };
    RedisModule_BlockClientSetPrivateData = [](RedisModuleBlockedClient* bc, void* data) {
      EXPECT_EQ(bc, current->handle());
      current->privateData = data;
    };
    RedisModule_ReplyWithError = [](RedisModuleCtx*, const char*) { return REDISMODULE_OK; };
    RedisModule_GetBlockedClientPrivateData = [](RedisModuleCtx*) { return current->privateData; };
    RedisModule_ReplyWithCString = [](RedisModuleCtx*, const char*) { return REDISMODULE_OK; };
    RedisModule_ReplyWithArray = [](RedisModuleCtx*, long) { return REDISMODULE_OK; };
    RedisModule_ReplyWithMap = [](RedisModuleCtx*, long len) {
      if (current->replyAfterTiming) EXPECT_EQ(current->ends.load(), 1);
      return REDISMODULE_OK;
    };
  }

  void TearDown() override {
    freeCluster();
    NumShards = savedNumShards;
    clusterConfig.topologyValidationTimeoutMS = savedValidationTimeout;
    if (privateData) freeData(nullptr, privateData);
    RedisModule_BlockClient = savedBlock;
    RedisModule_UnblockClient = savedUnblock;
    RedisModule_BlockedClientMeasureTimeStart = savedStart;
    RedisModule_BlockedClientMeasureTimeEnd = savedEnd;
    RedisModule_GetBlockedClientPrivateData = savedPrivateData;
    RedisModule_ReplyWithMap = savedReplyMap;
    RedisModule_ReplyWithCString = savedReplyCString;
    RedisModule_ReplyWithArray = savedReplyArray;
    RedisModule_BlockClientSetPrivateData = savedSetData;
    RedisModule_ReplyWithError = savedReplyError;
    current = nullptr;
  }

  void readyRuntimes() {
    RedisModuleSlotRangeArray slots = {0};
    // An unreachable node exercises the validation-failure path, which releases
    // queued I/O jobs without requiring a Redis peer for this test.
    MRClusterTopology* topology = MR_NewTopology(1);
    MRClusterNode node = {};
    node.id = rm_strdup("timing-test");
    node.endpoint.host = rm_strdup("127.0.0.1");
    node.endpoint.port = 0;
    MRClusterShard shard = MR_NewClusterShard(&node, SlotRangeArray_Clone(&slots));
    MRClusterTopology_AddShard(topology, &shard);
    MR_UpdateTopology(topology, &slots);
  }

  void collect() {
    auto done = completion.get_future();
    MR_GetConnectionPoolState(nullptr);
    // Without a topology the I/O jobs stay queued, so dispatch must not start timing.
    EXPECT_EQ(starts.load(), 0);
    readyRuntimes();
    ASSERT_EQ(done.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    privateData = done.get();
    EXPECT_EQ(starts.load(), 1);
    EXPECT_EQ(ends.load(), 1);
  }
};

ConnectionStateTimingTest* ConnectionStateTimingTest::current = nullptr;

TEST_P(ConnectionStateTimingTest, EndsBeforeReply) {
  ASSERT_NO_FATAL_FAILURE(collect());
  reply(nullptr, nullptr, 0);
  EXPECT_EQ(ends.load(), 1);
}

TEST_P(ConnectionStateTimingTest, EndsWithoutReply) {
  ASSERT_NO_FATAL_FAILURE(collect());
  // A disconnected client never receives the normal reply callback.
  EXPECT_EQ(ends.load(), 1);
}

INSTANTIATE_TEST_SUITE_P(IOThreads, ConnectionStateTimingTest, testing::Values(1, 3));

class FanoutTimingTest : public ConnectionStateTimingTest {
 protected:
  void SetUp() override {
    allowTimeout = true;
    ConnectionStateTimingTest::SetUp();
  }

  std::future<void*> fanout() {
    auto done = completion.get_future();
    auto* ctx = MR_CreateCtx(reinterpret_cast<RedisModuleCtx*>(this), nullptr, nullptr, 1);
    const char* argv[] = {"_FT.INFO", "idx"};
    const size_t lengths[] = {8, 3};
    MRCommand command = MR_NewCommandArgvLen(2, argv, lengths);
    MR_Fanout(ctx, [](MRCtx*, int, MRReply**) { return REDISMODULE_OK; }, command, true);
    EXPECT_EQ(starts.load(), 0);
    EXPECT_NE(timeoutCallback, nullptr);
    EXPECT_EQ(privateData, ctx);
    return done;
  }
};

TEST_P(FanoutTimingTest, ZeroSendsFinishBeforeUnblock) {
  auto done = fanout();
  readyRuntimes();
  ASSERT_EQ(done.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  EXPECT_EQ(done.get(), privateData);
  EXPECT_EQ(starts.load(), 1);
  EXPECT_EQ(ends.load(), 1);
}

TEST_P(FanoutTimingTest, ForcedTimeoutBeforePickupPreventsLateTiming) {
  expectedEnds = 0;
  auto done = fanout();
  timeoutCallback(nullptr, nullptr, 0);
  readyRuntimes();
  ASSERT_EQ(done.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  EXPECT_EQ(done.get(), privateData);
  EXPECT_EQ(starts.load(), 0);
  EXPECT_EQ(ends.load(), 0);
}

TEST_P(FanoutTimingTest, ForcedTimeoutFinishesAnActiveInterval) {
  auto done = fanout();
  auto* ctx = static_cast<MRCtx*>(privateData);
  std::thread([&] { MRCtx_StartTiming(ctx); }).join();
  timeoutCallback(nullptr, nullptr, 0);
  EXPECT_EQ(ends.load(), 1);
  readyRuntimes();
  ASSERT_EQ(done.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  EXPECT_EQ(done.get(), privateData);
  EXPECT_EQ(starts.load(), 1);
  EXPECT_EQ(ends.load(), 1);
}

TEST_P(FanoutTimingTest, WorkerFinishBeforeTimeoutIsNotCountedAgain) {
  auto done = fanout();
  auto* ctx = static_cast<MRCtx*>(privateData);
  std::thread([&] {
    MRCtx_StartTiming(ctx);
    MRCtx_FinishTiming(ctx);
  }).join();
  timeoutCallback(nullptr, nullptr, 0);
  EXPECT_EQ(ends.load(), 1);
  readyRuntimes();
  ASSERT_EQ(done.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  EXPECT_EQ(done.get(), privateData);
  EXPECT_EQ(ends.load(), 1);
}

INSTANTIATE_TEST_SUITE_P(IOThreads, FanoutTimingTest, testing::Values(1, 3));

class QueuedSearchTimingTest : public ConnectionStateTimingTest {
 protected:
  decltype(RedisModule_BlockClientGetPrivateData) savedGetData =
      RedisModule_BlockClientGetPrivateData;
  decltype(RedisModule_SetDisconnectCallback) savedDisconnect = RedisModule_SetDisconnectCallback;
  decltype(RedisModule_GetThreadSafeContext) savedContext = RedisModule_GetThreadSafeContext;
  decltype(RedisModule_ReplyWithLongLong) savedInteger = RedisModule_ReplyWithLongLong;
  decltype(RedisModule_ReplySetArrayLength) savedArrayLength = RedisModule_ReplySetArrayLength;
  RequestConfig savedConfig = RSGlobalConfig.requestConfigParams;
  long long savedForegroundLimit = RSGlobalConfig.maxForegroundTimeoutLimitMS;
  int savedDistThreadPool = DIST_THREADPOOL;
  RedisModuleCtx* ctx = nullptr;
  IndexSpec* spec = nullptr;
  redisearch_thpool_t* pool = nullptr;
  RedisModuleDisconnectFunc disconnectCallback = nullptr;

  void SetUp() override {
    allowTimeout = true;
    expectedEnds = 0;
    replyAfterTiming = false;
    ConnectionStateTimingTest::SetUp();
    ctx = savedContext(nullptr);
    NumShards = 2;
    RSGlobalConfig.requestConfigParams.timeoutPolicy = TimeoutPolicy_Fail;
    RSGlobalConfig.requestConfigParams.queryTimeoutMS = 0;
    RSGlobalConfig.maxForegroundTimeoutLimitMS = 0;
    RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Ignore;
    DIST_THREADPOOL = ConcurrentSearch_CreatePool(1);
    pool = ConcurrentSearch_GetPool(DIST_THREADPOOL);
    redisearch_thpool_pause_threads(pool);

    RedisModule_BlockClientGetPrivateData = [](RedisModuleBlockedClient* bc) -> void* {
      auto* self = static_cast<QueuedSearchTimingTest*>(current);
      EXPECT_EQ(bc, self->handle());
      return self->privateData;
    };
    RedisModule_SetDisconnectCallback = [](RedisModuleBlockedClient* bc,
                                           RedisModuleDisconnectFunc callback) {
      auto* self = static_cast<QueuedSearchTimingTest*>(current);
      EXPECT_EQ(bc, self->handle());
      EXPECT_NE(callback, nullptr);
      self->disconnectCallback = callback;
    };
    RedisModule_GetThreadSafeContext = [](RedisModuleBlockedClient* bc) {
      auto* self = static_cast<QueuedSearchTimingTest*>(current);
      EXPECT_TRUE(bc == nullptr || bc == self->handle());
      return self->savedContext(nullptr);
    };
    RedisModule_ReplyWithLongLong = [](RedisModuleCtx*, long long) { return REDISMODULE_OK; };
    RedisModule_ReplySetArrayLength = [](RedisModuleCtx*, long) {};

    RMCK::ArgvList args(ctx, "FT.CREATE", "queued-search-timing", "SKIPINITIALSCAN", "SCHEMA", "t",
                        "TEXT");
    QueryError error = QueryError_Default();
    spec = Indexes_CreateNewSpec(ctx, args, args.size(), &error);
    EXPECT_FALSE(QueryError_HasError(&error)) << QueryError_GetUserError(&error);
    QueryError_ClearError(&error);
    ASSERT_NE(spec, nullptr);
  }

  void TearDown() override {
    // An assertion before cancellation must still release the queued command through its callback.
    if (privateData && redisearch_thpool_paused(pool) &&
        !MRCtx_IsTimedOut(static_cast<MRCtx*>(privateData))) {
      if (timeoutCallback) {
        timeoutCallback(ctx, nullptr, 0);
      } else if (disconnectCallback) {
        disconnectCallback(ctx, handle());
      }
    }
    if (redisearch_thpool_paused(pool)) redisearch_thpool_resume_threads(pool);
    redisearch_thpool_wait(pool);
    ConcurrentSearch_ThreadPoolDestroy();
    DIST_THREADPOOL = savedDistThreadPool;
    ConnectionStateTimingTest::TearDown();
    RedisModule_BlockClientGetPrivateData = savedGetData;
    RedisModule_SetDisconnectCallback = savedDisconnect;
    RedisModule_GetThreadSafeContext = savedContext;
    RedisModule_ReplyWithLongLong = savedInteger;
    RedisModule_ReplySetArrayLength = savedArrayLength;
    CurrentThread_ClearIndexSpec();
    if (spec) Indexes_RemoveSpecFromGlobals(spec->own_ref, false);
    RSGlobalConfig.requestConfigParams = savedConfig;
    RSGlobalConfig.maxForegroundTimeoutLimitMS = savedForegroundLimit;
    RedisModule_FreeThreadSafeContext(ctx);
  }

  void timeoutBeforePickup(RSTimeoutPolicy policy) {
    RSGlobalConfig.requestConfigParams.timeoutPolicy = policy;
    RMCK::ArgvList args(ctx, "FT.SEARCH", "queued-search-timing", "*");
    auto done = completion.get_future();
    ASSERT_EQ(DistSearchCommandImp(ctx, args, args.size(), false), REDISMODULE_OK);
    ASSERT_NE(privateData, nullptr);
    ASSERT_NE(timeoutCallback, nullptr);
    ASSERT_EQ(redisearch_thpool_high_priority_pending_jobs(pool), 1);
    EXPECT_EQ(starts.load(), 0);
    EXPECT_EQ(ends.load(), 0);
    EXPECT_EQ(done.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

    ASSERT_EQ(timeoutCallback(ctx, args, args.size()), REDISMODULE_OK);
    EXPECT_TRUE(MRCtx_IsTimedOut(static_cast<MRCtx*>(privateData)));
    EXPECT_EQ(starts.load(), 0);
    EXPECT_EQ(ends.load(), 0);
    EXPECT_EQ(done.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

    redisearch_thpool_resume_threads(pool);
    redisearch_thpool_wait(pool);
    ASSERT_EQ(done.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    EXPECT_EQ(done.get(), privateData);
    EXPECT_EQ(starts.load(), 0);
    EXPECT_EQ(ends.load(), 0);
  }
};

TEST_P(QueuedSearchTimingTest, FailTimeoutBeforePickupNeverMeasuresQueueTime) {
  timeoutBeforePickup(TimeoutPolicy_Fail);
}

TEST_P(QueuedSearchTimingTest, StrictTimeoutBeforePickupNeverMeasuresQueueTime) {
  timeoutBeforePickup(TimeoutPolicy_ReturnStrict);
}

TEST_P(QueuedSearchTimingTest, DebugSearchStartsOnPickupAndEndsOnceBeforeUnblock) {
  // Debug coordinator queries support RETURN; disconnected peers give deterministic completion.
  RSGlobalConfig.requestConfigParams.timeoutPolicy = TimeoutPolicy_Return;
  expectedEnds = 1;
  RMCK::ArgvList args(ctx, "FT.SEARCH", "queued-search-timing", "*", "TIMEOUT_AFTER_N", "0",
                      "DEBUG_PARAMS_COUNT", "2");
  auto done = completion.get_future();
  ASSERT_EQ(DistSearchCommandImp(ctx, args, args.size(), true), REDISMODULE_OK);
  ASSERT_NE(privateData, nullptr);
  EXPECT_EQ(timeoutCallback, nullptr);
  ASSERT_EQ(redisearch_thpool_high_priority_pending_jobs(pool), 1);
  EXPECT_EQ(starts.load(), 0);
  EXPECT_EQ(ends.load(), 0);

  redisearch_thpool_resume_threads(pool);
  redisearch_thpool_wait(pool);
  EXPECT_EQ(starts.load(), 1);
  EXPECT_EQ(ends.load(), 0);
  readyRuntimes();
  ASSERT_EQ(done.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  EXPECT_EQ(done.get(), privateData);
  EXPECT_EQ(starts.load(), 1);
  EXPECT_EQ(ends.load(), 1);
}

INSTANTIATE_TEST_SUITE_P(IOThreads, QueuedSearchTimingTest, testing::Values(1, 3));

class ClusterInfoTimingTest : public ConnectionStateTimingTest {
 protected:
  static decltype(RedisModule_GetThreadSafeContext) savedContext;
  decltype(RedisModule_ReplyWithLongLong) savedInteger = RedisModule_ReplyWithLongLong;
  decltype(RedisModule_ReplyWithStringBuffer) savedBuffer = RedisModule_ReplyWithStringBuffer;
  decltype(RedisModule_ReplySetArrayLength) savedArrayLength = RedisModule_ReplySetArrayLength;
  decltype(RedisModule_ReplySetMapLength) savedMapLength = RedisModule_ReplySetMapLength;

  void SetUp() override {
    ConnectionStateTimingTest::SetUp();
    replyAfterTiming = false;
    savedContext = RedisModule_GetThreadSafeContext;
    RedisModule_ReplyWithLongLong = [](RedisModuleCtx*, long long) { return REDISMODULE_OK; };
    RedisModule_ReplyWithStringBuffer = [](RedisModuleCtx*, const char*, size_t) {
      return REDISMODULE_OK;
    };
    RedisModule_ReplySetArrayLength = [](RedisModuleCtx*, long) {};
    RedisModule_ReplySetMapLength = [](RedisModuleCtx*, long) {};
    RedisModule_GetThreadSafeContext = [](RedisModuleBlockedClient* bc) {
      EXPECT_EQ(bc, static_cast<ClusterInfoTimingTest*>(current)->handle());
      return savedContext(nullptr);
    };
  }

  void TearDown() override {
    ConnectionStateTimingTest::TearDown();
    RedisModule_GetThreadSafeContext = savedContext;
    RedisModule_ReplyWithLongLong = savedInteger;
    RedisModule_ReplyWithStringBuffer = savedBuffer;
    RedisModule_ReplySetArrayLength = savedArrayLength;
    RedisModule_ReplySetMapLength = savedMapLength;
  }
};

decltype(RedisModule_GetThreadSafeContext) ClusterInfoTimingTest::savedContext = nullptr;

TEST_P(ClusterInfoTimingTest, StartsWhenWorkerPicksUpAndEndsBeforeUnblock) {
  auto done = completion.get_future();
  MR_uvReplyClusterInfo(nullptr);
  EXPECT_EQ(starts.load(), 0);
  readyRuntimes();
  ASSERT_EQ(done.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  EXPECT_EQ(done.get(), nullptr);
  EXPECT_EQ(starts.load(), 1);
  EXPECT_EQ(ends.load(), 1);
}

INSTANTIATE_TEST_SUITE_P(IOThreads, ClusterInfoTimingTest, testing::Values(1, 3));
