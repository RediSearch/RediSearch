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
      EXPECT_EQ(timeout, nullptr);
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
      EXPECT_EQ(current->ends.load(), 1);
      current->completion.set_value(data);
      return REDISMODULE_OK;
    };
    RedisModule_GetBlockedClientPrivateData = [](RedisModuleCtx*) { return current->privateData; };
    RedisModule_ReplyWithCString = [](RedisModuleCtx*, const char*) { return REDISMODULE_OK; };
    RedisModule_ReplyWithArray = [](RedisModuleCtx*, long) { return REDISMODULE_OK; };
    RedisModule_ReplyWithMap = [](RedisModuleCtx*, long len) {
      EXPECT_GE(len, 0);
      EXPECT_EQ(current->ends.load(), 1);
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
    current = nullptr;
  }

  void collect() {
    auto done = completion.get_future();
    MR_GetConnectionPoolState(nullptr);
    // Without a topology the I/O jobs stay queued, so dispatch must not start timing.
    EXPECT_EQ(starts.load(), 0);
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
