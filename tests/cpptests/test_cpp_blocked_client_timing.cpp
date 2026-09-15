/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "aggregate/aggregate.h"
#include "hybrid/hybrid_exec.h"
#include "indexes.h"
#include "info/info_redis/block_client.h"
#include "info/info_redis/threads/current_thread.h"
#include "info/info_redis/threads/main_thread.h"
#include "profile/options.h"
#include "redismock/util.h"
#include "util/blocked_client_timing.h"
#include "util/workers.h"
#include <atomic>
#include <chrono>
#include <thread>

extern "C" int RSExecuteAggregateOrSearch(RedisModuleCtx* ctx, RedisModuleString** argv, int argc,
                                          CommandType type, ProfileOptions profileOptions);

class BlockedClientTimingTest : public testing::Test {
 protected:
  static BlockedClientTimingTest* current;
  decltype(RedisModule_BlockedClientMeasureTimeStart) savedStart =
      RedisModule_BlockedClientMeasureTimeStart;
  decltype(RedisModule_BlockedClientMeasureTimeEnd) savedEnd =
      RedisModule_BlockedClientMeasureTimeEnd;
  BlockedClientTiming timing;
  int starts = 0;
  int ends = 0;
  int now = 0;
  int startedAt = 0;
  int duration = 0;

  void SetUp() override {
    current = this;
    BlockedClientTiming_Init(&timing);
    RedisModule_BlockedClientMeasureTimeStart = [](RedisModuleBlockedClient* bc) {
      EXPECT_EQ(bc, reinterpret_cast<RedisModuleBlockedClient*>(current));
      ++current->starts;
      current->startedAt = current->now;
      return REDISMODULE_OK;
    };
    RedisModule_BlockedClientMeasureTimeEnd = [](RedisModuleBlockedClient* bc) {
      EXPECT_EQ(bc, reinterpret_cast<RedisModuleBlockedClient*>(current));
      ++current->ends;
      current->duration += current->now - current->startedAt;
      return REDISMODULE_OK;
    };
    begin();
  }

  void TearDown() override {
    BlockedClientTiming_Destroy(&timing);
    RedisModule_BlockedClientMeasureTimeStart = savedStart;
    RedisModule_BlockedClientMeasureTimeEnd = savedEnd;
    current = nullptr;
  }

  void begin() {
    BlockedClientTiming_Begin(&timing, reinterpret_cast<RedisModuleBlockedClient*>(this));
  }
};

BlockedClientTimingTest* BlockedClientTimingTest::current = nullptr;

TEST_F(BlockedClientTimingTest, TimeoutBeforePickupPreventsLateMeasurement) {
  BlockedClientTiming_Finish(&timing);
  BlockedClientTiming_Start(&timing);
  BlockedClientTiming_Finish(&timing);
  EXPECT_EQ(starts, 0);
  EXPECT_EQ(ends, 0);
}

TEST_F(BlockedClientTimingTest, TimeoutDuringWorkCommitsDurationBeforeReturning) {
  now = 100;
  BlockedClientTiming_Start(&timing);
  now = 140;
  BlockedClientTiming_Finish(&timing);
  EXPECT_EQ(duration, 40);
  now = 200;
  BlockedClientTiming_Finish(&timing);
  BlockedClientTiming_Start(&timing);
  EXPECT_EQ(duration, 40);
  EXPECT_EQ(starts, 1);
  EXPECT_EQ(ends, 1);
}

TEST_F(BlockedClientTimingTest, WorkerAndTimeoutFinishOnlyOnce) {
  BlockedClientTiming_Start(&timing);
  now = 20;
  std::thread worker([&] { BlockedClientTiming_Finish(&timing); });
  BlockedClientTiming_Finish(&timing);
  worker.join();
  EXPECT_EQ(duration, 20);
  EXPECT_EQ(ends, 1);
}

TEST_F(BlockedClientTimingTest, DuplicateStartDoesNotOverwriteTimestamp) {
  now = 10;
  BlockedClientTiming_Start(&timing);
  now = 20;
  BlockedClientTiming_Start(&timing);
  now = 30;
  BlockedClientTiming_Finish(&timing);
  EXPECT_EQ(duration, 20);
  EXPECT_EQ(starts, 1);
}

TEST_F(BlockedClientTimingTest, NewCycleCanMeasureAfterPreviousTimeout) {
  BlockedClientTiming_Finish(&timing);
  begin();
  now = 50;
  BlockedClientTiming_Start(&timing);
  now = 70;
  BlockedClientTiming_Finish(&timing);
  EXPECT_EQ(duration, 20);
  EXPECT_EQ(starts, 1);
  EXPECT_EQ(ends, 1);
}

class QueuedQueryTimingTest : public testing::TestWithParam<CommandType> {
 protected:
  int dispatch() {
    if (GetParam() == COMMAND_HYBRID) {
      const float vector[] = {1, 2};
      RMCK::ArgvList args(ctx, "FT.HYBRID", "queued-timing", "SEARCH", "*", "VSIM", "@v", "$BLOB",
                          "PARAMS", "2", "BLOB");
      args.add(reinterpret_cast<const char*>(vector), sizeof(vector));
      return hybridCommandHandler(ctx, args, args.size(), false, EXEC_NO_FLAGS, nullptr);
    }
    const char* command = GetParam() == COMMAND_SEARCH ? "FT.SEARCH" : "FT.AGGREGATE";
    RMCK::ArgvList args(ctx, command, "queued-timing", "*");
    return RSExecuteAggregateOrSearch(ctx, args, args.size(), GetParam(), EXEC_NO_FLAGS);
  }

  static int startAndWaitForTimeout(RedisModuleBlockedClient* bc) {
    auto* self = static_cast<QueuedQueryTimingTest*>(current);
    EXPECT_EQ(bc, self->handle());
    ++self->starts;
    // Start holds the timing mutex. A deadline lets the worker exit even if the callback
    // incorrectly waits for that mutex before publishing the timeout.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!QueryRequestTimeout_IsBlockedClientTimedOut(&self->request->timeout) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(QueryRequestTimeout_IsBlockedClientTimedOut(&self->request->timeout));
    return REDISMODULE_OK;
  }

  static QueuedQueryTimingTest* current;
  decltype(RedisModule_BlockClient) savedBlock = RedisModule_BlockClient;
  decltype(RedisModule_UnblockClient) savedUnblock = RedisModule_UnblockClient;
  decltype(RedisModule_BlockClientSetPrivateData) savedSetData =
      RedisModule_BlockClientSetPrivateData;
  decltype(RedisModule_BlockClientGetPrivateData) savedGetData =
      RedisModule_BlockClientGetPrivateData;
  decltype(RedisModule_GetBlockedClientPrivateData) savedCallbackData =
      RedisModule_GetBlockedClientPrivateData;
  decltype(RedisModule_SetDisconnectCallback) savedDisconnect = RedisModule_SetDisconnectCallback;
  decltype(RedisModule_GetUsedMemoryRatio) savedMemoryRatio = RedisModule_GetUsedMemoryRatio;
  decltype(RedisModule_GetThreadSafeContext) savedGetContext = RedisModule_GetThreadSafeContext;
  decltype(RedisModule_BlockedClientMeasureTimeStart) savedStart =
      RedisModule_BlockedClientMeasureTimeStart;
  decltype(RedisModule_BlockedClientMeasureTimeEnd) savedEnd =
      RedisModule_BlockedClientMeasureTimeEnd;
  size_t savedWorkers = RSGlobalConfig.numWorkerThreads;
  RequestConfig savedConfig = RSGlobalConfig.requestConfigParams;
  RedisModuleCtx* ctx = nullptr;
  IndexSpec* spec = nullptr;
  QueryRequest* request = nullptr;
  RedisModuleCmdFunc timeoutCallback = nullptr;
  void (*freeData)(RedisModuleCtx*, void*) = nullptr;
  std::atomic<int> starts{0};
  std::atomic<int> ends{0};
  std::atomic<int> unblocks{0};

  RedisModuleBlockedClient* handle() {
    return reinterpret_cast<RedisModuleBlockedClient*>(this);
  }

  void SetUp() override {
    current = this;
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    if (!MainThread_GetBlockedQueries()) {
      ASSERT_EQ(MainThread_InitBlockedQueries(), 0);
    }
    RSGlobalConfig.numWorkerThreads = 1;
    workersThreadPool_SetNumWorkers();
    ASSERT_EQ(workersThreadPool_pause(), REDISMODULE_OK);
    RSGlobalConfig.requestConfigParams.timeoutPolicy = TimeoutPolicy_Fail;
    RSGlobalConfig.requestConfigParams.queryTimeoutMS = 0;
    RedisModule_GetUsedMemoryRatio = []() { return 0.0f; };
    RedisModule_GetThreadSafeContext = [](RedisModuleBlockedClient* bc) {
      EXPECT_TRUE(bc == nullptr || bc == current->handle());
      return current->savedGetContext(nullptr);
    };

    RedisModule_BlockClient = [](RedisModuleCtx*, RedisModuleCmdFunc reply,
                                 RedisModuleCmdFunc timeout,
                                 void (*freeData)(RedisModuleCtx*, void*), long long timeoutMS) {
      EXPECT_NE(reply, nullptr);
      EXPECT_NE(timeout, nullptr);
      EXPECT_EQ(timeoutMS, 0);
      current->timeoutCallback = timeout;
      current->freeData = freeData;
      return current->handle();
    };
    RedisModule_BlockClientSetPrivateData = [](RedisModuleBlockedClient* bc, void* data) {
      EXPECT_EQ(bc, current->handle());
      current->request = static_cast<QueryRequest*>(data);
    };
    RedisModule_BlockClientGetPrivateData = [](RedisModuleBlockedClient* bc) -> void* {
      EXPECT_EQ(bc, current->handle());
      return current->request;
    };
    RedisModule_GetBlockedClientPrivateData = [](RedisModuleCtx*) -> void* {
      return current->request;
    };
    RedisModule_SetDisconnectCallback = [](RedisModuleBlockedClient* bc,
                                           RedisModuleDisconnectFunc callback) {
      EXPECT_EQ(bc, current->handle());
      EXPECT_NE(callback, nullptr);
    };
    RedisModule_BlockedClientMeasureTimeStart = [](RedisModuleBlockedClient* bc) {
      EXPECT_EQ(bc, current->handle());
      ++current->starts;
      return REDISMODULE_OK;
    };
    RedisModule_BlockedClientMeasureTimeEnd = [](RedisModuleBlockedClient* bc) {
      EXPECT_EQ(bc, current->handle());
      ++current->ends;
      return REDISMODULE_OK;
    };
    RedisModule_UnblockClient = [](RedisModuleBlockedClient* bc, void* data) {
      EXPECT_EQ(bc, current->handle());
      EXPECT_EQ(data, current->request);
      ++current->unblocks;
      return REDISMODULE_OK;
    };

    RMCK::ArgvList args(ctx, "FT.CREATE", "queued-timing", "SKIPINITIALSCAN", "SCHEMA", "t", "TEXT",
                        "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
                        "DISTANCE_METRIC", "L2");
    QueryError error = QueryError_Default();
    spec = Indexes_CreateNewSpec(ctx, args, args.size(), &error);
    const bool hasError = QueryError_HasError(&error);
    EXPECT_FALSE(hasError) << QueryError_GetUserError(&error);
    QueryError_ClearError(&error);
    ASSERT_NE(spec, nullptr);
  }

  void TearDown() override {
    // A failed assertion before the explicit timeout must still let the queued worker exit.
    if (request && !QueryRequestTimeout_IsBlockedClientTimedOut(&request->timeout)) {
      timeoutCallback(ctx, request->args.argv, request->args.argc);
    }
    if (workerThreadPool_isPaused()) {
      workersThreadPool_resume();
    }
    workersThreadPool_wait();
    if (request) {
      freeData(ctx, request);
      request = nullptr;
    }
    CurrentThread_ClearIndexSpec();
    if (spec) {
      Indexes_RemoveSpecFromGlobals(spec->own_ref, false);
    }
    RedisModule_BlockClient = savedBlock;
    RedisModule_UnblockClient = savedUnblock;
    RedisModule_BlockClientSetPrivateData = savedSetData;
    RedisModule_BlockClientGetPrivateData = savedGetData;
    RedisModule_GetBlockedClientPrivateData = savedCallbackData;
    RedisModule_SetDisconnectCallback = savedDisconnect;
    RedisModule_GetUsedMemoryRatio = savedMemoryRatio;
    RedisModule_GetThreadSafeContext = savedGetContext;
    RedisModule_BlockedClientMeasureTimeStart = savedStart;
    RedisModule_BlockedClientMeasureTimeEnd = savedEnd;
    RSGlobalConfig.requestConfigParams = savedConfig;
    RSGlobalConfig.numWorkerThreads = savedWorkers;
    workersThreadPool_SetNumWorkers();
    workersThreadPool_wait();
    RedisModule_FreeThreadSafeContext(ctx);
    current = nullptr;
  }
};

QueuedQueryTimingTest* QueuedQueryTimingTest::current = nullptr;

TEST_P(QueuedQueryTimingTest, TimeoutBeforeWorkerPickupNeverMeasuresQueueTime) {
  ASSERT_EQ(dispatch(), REDISMODULE_OK);
  ASSERT_NE(request, nullptr);
  ASSERT_NE(timeoutCallback, nullptr);
  ASSERT_EQ(workersThreadPool_HighPriorityPendingJobsCount(), 1);
  EXPECT_EQ(starts.load(), 0);
  EXPECT_EQ(ends.load(), 0);
  EXPECT_EQ(unblocks.load(), 0);

  ASSERT_EQ(timeoutCallback(ctx, request->args.argv, request->args.argc), REDISMODULE_OK);
  EXPECT_TRUE(QueryRequestTimeout_IsBlockedClientTimedOut(&request->timeout));
  EXPECT_EQ(starts.load(), 0);
  EXPECT_EQ(ends.load(), 0);

  ASSERT_EQ(workersThreadPool_resume(), REDISMODULE_OK);
  workersThreadPool_wait();
  EXPECT_EQ(unblocks.load(), 1);
  EXPECT_EQ(starts.load(), 0);
  EXPECT_EQ(ends.load(), 0);
}

TEST_P(QueuedQueryTimingTest, TimeoutPublicationDoesNotWaitForMeasurementStart) {
  ASSERT_EQ(dispatch(), REDISMODULE_OK);
  ASSERT_NE(request, nullptr);
  ASSERT_NE(timeoutCallback, nullptr);

  RedisModule_BlockedClientMeasureTimeStart = startAndWaitForTimeout;
  ASSERT_EQ(workersThreadPool_resume(), REDISMODULE_OK);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (starts.load() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(starts.load(), 1);
  ASSERT_EQ(timeoutCallback(ctx, request->args.argv, request->args.argc), REDISMODULE_OK);
  workersThreadPool_wait();
  EXPECT_EQ(starts.load(), 1);
  EXPECT_EQ(ends.load(), 1);
  EXPECT_EQ(unblocks.load(), 1);
}

INSTANTIATE_TEST_SUITE_P(QueryCommands, QueuedQueryTimingTest,
                         testing::Values(COMMAND_SEARCH, COMMAND_AGGREGATE, COMMAND_HYBRID));
