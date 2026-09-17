/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "coord/rpnet.h"
#include "search_result.h"
#include "search_result_ffi.h"
#include "coord/rmr/chan.h"
#include "hiredis/hiredis.h"
#include "hiredis/read.h"
#include "rlookup.h"
#include "value_ffi.h"
#include "query_error_ffi.h"
#include "debug_commands.h"
#include "coord/rmr/io_runtime_ctx.h"
#include <thread>
#include <string>
#include <atomic>

static MRReply *parseReply(const char *wire) {
  redisReader *reader = redisReaderCreate();
  EXPECT_EQ(REDIS_OK, redisReaderFeed(reader, wire, strlen(wire)));
  void *reply = nullptr;
  EXPECT_EQ(REDIS_OK, redisReaderGetReply(reader, &reply));
  redisReaderFree(reader);
  return static_cast<MRReply *>(reply);
}

class RPNetBufferedDrainTest : public ::testing::Test {
 protected:
  RLookup lookup = RLookup_New();
  AREQ request = {};
  QueryProcessingCtx qctx = {};
  MRChannel *channel = MR_NewChannel();
  RPNet *network = nullptr;
  const RLookupKey *key = nullptr;

  void SetUp() override {
    key = RLookup_GetKey_Write(&lookup, "n", 0);
    RLookup_Seal(&lookup);
    MRCommand command = {};
    command.protocol = 2;
    network = RPNet_New(&command, rpnetNext);
    network->lookup = &lookup;
    network->areq = &request;
    network->base.parent = &qctx;
    network->drainChannel = channel;
  }

  void TearDown() override {
    network->base.Free(&network->base);
    while (auto *reply = static_cast<MRReply *>(MRChannel_TryPop(channel))) MRReply_Free(reply);
    MRChannel_Free(channel);
    RLookup_Cleanup(&lookup);
  }

  double number(const SearchResult *result) {
    double value = 0;
    EXPECT_TRUE(RSValue_ToNumber(RLookupRow_Get(key, SearchResult_GetRowData(result)), &value));
    return value;
  }
};

TEST_F(RPNetBufferedDrainTest, lookupIteratorKeepsOriginalBoundAcrossDynamicGrowth) {
  auto iterator = RLookup_Iter(&lookup);
  SearchResult result = SearchResult_New();
  for (int i = 0; i < 100; ++i) {
    auto name = "dynamic_" + std::to_string(i);
    RLookupRow_WriteByNameOwned(&lookup, name.data(), name.size(),
                                SearchResult_GetRowDataMut(&result), RSValue_NewNumber(i));
  }
  const RLookupKey *current = nullptr;
  ASSERT_TRUE(RLookupIterator_Next(&iterator, &current));
  EXPECT_EQ(key, current);
  EXPECT_FALSE(RLookupIterator_Next(&iterator, &current));
  EXPECT_FALSE(RLookupIterator_Next(&iterator, &current));
  auto fresh = RLookup_Iter(&lookup);
  size_t count = 0;
  while (RLookupIterator_Next(&fresh, &current)) {
    EXPECT_EQ(count, RLookupKey_GetDstIdx(current));
    ++count;
  }
  EXPECT_EQ(101, count);
  SearchResult_Destroy(&result);
}

TEST_F(RPNetBufferedDrainTest, queuedRowsRemainSerializableAndEOFTerminal) {
  MRChannel_Push(channel,
                 parseReply("*2\r\n*3\r\n:2\r\n*2\r\n+n\r\n:1\r\n*2\r\n+n\r\n:2\r\n:0\r\n"));
  SearchResult result = SearchResult_New();
  for (int expected = 1; expected <= 2; ++expected) {
    ASSERT_EQ(RP_DRAIN_OK, network->base.Drain(&network->base, &result));
    EXPECT_EQ(expected, number(&result));
    SearchResult_Clear(&result);
  }
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
  EXPECT_EQ(0, qctx.totalResults);
  EXPECT_EQ(2, network->drainMetadata->sourceResults);
  MRChannel_Push(channel, parseReply("*2\r\n*1\r\n:0\r\n:0\r\n"));
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
  EXPECT_EQ(1, MRChannel_Size(channel));
  SearchResult_Destroy(&result);
}

TEST_F(RPNetBufferedDrainTest, resp3RetainsWarningsAndCreatesDynamicFields) {
  network->cmd.protocol = 3;
  MRChannel_Push(
      channel, parseReply("*2\r\n%2\r\n+results\r\n*1\r\n%1\r\n+extra_attributes\r\n%2\r\n+n\r\n:"
                          "7\r\n+dynamic\r\n+value\r\n+warning\r\n*1\r\n+warning text\r\n:0\r\n"));
  SearchResult result = SearchResult_New();
  ASSERT_EQ(RP_DRAIN_OK, network->base.Drain(&network->base, &result));
  EXPECT_EQ(7, number(&result));
  EXPECT_EQ(2, RLookup_GetRowLen(&lookup));
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
  ASSERT_NE(nullptr, network->drainMetadata);
  EXPECT_EQ(0, request.stateflags);
  SearchResult_Destroy(&result);
}

TEST_F(RPNetBufferedDrainTest, emptyRepliesAndErrorsDoNotWait) {
  MRChannel_Push(channel, parseReply("*2\r\n*1\r\n:0\r\n:0\r\n"));
  MRChannel_Push(channel, parseReply("-ERR shard failed\r\n"));
  SearchResult result = SearchResult_New();
  EXPECT_EQ(RP_DRAIN_ERROR, network->base.Drain(&network->base, &result));
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
  EXPECT_TRUE(QueryError_HasError(&network->drainMetadata->error));
  SearchResult_Destroy(&result);
}

TEST_F(RPNetBufferedDrainTest, concurrentNextAndDrainNeverDuplicateClaimedRow) {
  network->current.root =
      parseReply("*2\r\n*3\r\n:2\r\n*2\r\n+n\r\n:1\r\n*2\r\n+n\r\n:2\r\n:0\r\n");
  network->current.rows = MRReply_ArrayElement(network->current.root, 0);
  network->current.index = 1;
  SearchResult bg = SearchResult_New(), drained = SearchResult_New();
  int nextStatus = RS_RESULT_EOF;
  std::thread worker([&] { nextStatus = network->base.Next(&network->base, &bg); });
  int seen[3] = {};
  while (network->base.Drain(&network->base, &drained) == RP_DRAIN_OK) {
    double value = number(&drained);
    if (value >= 1 && value <= 2) ++seen[static_cast<int>(value)];
    SearchResult_Clear(&drained);
  }
  worker.join();
  if (nextStatus == RS_RESULT_OK)
    ++seen[static_cast<int>(number(&bg))];
  else
    EXPECT_EQ(RS_RESULT_TIMEDOUT, nextStatus);
  EXPECT_LE(seen[1], 1);
  EXPECT_LE(seen[2], 1);
  EXPECT_GE(seen[1] + seen[2], 1);
  SearchResult_Destroy(&bg);
  SearchResult_Destroy(&drained);
}

TEST_F(RPNetBufferedDrainTest, nextPublishesUnclaimedRowsBeforeDrain) {
  network->current.root =
      parseReply("*2\r\n*3\r\n:2\r\n*2\r\n+n\r\n:1\r\n*2\r\n+n\r\n:2\r\n:0\r\n");
  network->current.rows = MRReply_ArrayElement(network->current.root, 0);
  network->current.index = 1;
  SearchResult result = SearchResult_New();
  ASSERT_EQ(RS_RESULT_OK, network->base.Next(&network->base, &result));
  EXPECT_EQ(nullptr, network->drainMetadata);
  EXPECT_EQ(1, number(&result));
  SearchResult_Clear(&result);
  ASSERT_EQ(RP_DRAIN_OK, network->base.Drain(&network->base, &result));
  EXPECT_EQ(2, number(&result));
  SearchResult_Clear(&result);
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
  EXPECT_EQ(RS_RESULT_TIMEDOUT, network->base.Next(&network->base, &result));
  SearchResult_Destroy(&result);
}

TEST_F(RPNetBufferedDrainTest, drainCompletesWhileDownstreamOwnsAnInFlightRow) {
  network->current.root =
      parseReply("*2\r\n*3\r\n:2\r\n*2\r\n+n\r\n:1\r\n*2\r\n+n\r\n:2\r\n:0\r\n");
  network->current.rows = MRReply_ArrayElement(network->current.root, 0);
  network->current.index = 1;
  std::atomic_bool claimed = false, resume = false;
  SearchResult bg = SearchResult_New(), drained = SearchResult_New();
  std::thread worker([&] {
    EXPECT_EQ(RS_RESULT_OK, network->base.Next(&network->base, &bg));
    claimed.store(true, std::memory_order_release);
    while (!resume.load(std::memory_order_acquire)) std::this_thread::yield();
    EXPECT_EQ(1, number(&bg));
    SearchResult_Destroy(&bg);
  });
  while (!claimed.load(std::memory_order_acquire)) std::this_thread::yield();
  QueryRequestTimeout_Init(&request.base.timeout, TimeoutPolicy_ReturnStrict, 1000);
  QueryRequestTimeout_BeginCycle(&request.base.timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestTimeout_MarkTimedOut(&request.base.timeout);
  EXPECT_EQ(RP_DRAIN_OK, network->base.Drain(&network->base, &drained));
  EXPECT_EQ(2, number(&drained));
  SearchResult_Clear(&drained);
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &drained));
  resume.store(true, std::memory_order_release);
  worker.join();
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &drained));
  SearchResult_Destroy(&drained);
}

TEST_F(RPNetBufferedDrainTest, metadataCanBeTakenAtLimitWithoutTouchingLiveBookkeeping) {
  network->cmd.protocol = 3;
  network->cmd.forProfiling = true;
  qctx.totalResults = 99;
  request.reqflags = QEXEC_FORMAT_DEFAULT;
  std::string wire =
      "*2\r\n%2\r\n+results\r\n%3\r\n+results\r\n*2\r\n%1\r\n+extra_attributes\r\n%1\r\n+n\r\n:"
      "1\r\n"
      "%1\r\n+extra_attributes\r\n%1\r\n+n\r\n:2\r\n+format\r\n+EXPAND\r\n+warning\r\n*1\r\n+";
  wire += QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT);
  wire += "\r\n+profile\r\n%0\r\n:0\r\n";
  MRChannel_Push(channel, parseReply(wire.c_str()));
  SearchResult result = SearchResult_New();
  ASSERT_EQ(nullptr, network->drainMetadata);
  ASSERT_EQ(RP_DRAIN_OK, network->base.Drain(&network->base, &result));
  auto *metadata = RPNet_TakeDrainMetadata(network);
  ASSERT_NE(nullptr, metadata);
  EXPECT_EQ(2, metadata->sourceResults);
  EXPECT_TRUE(metadata->hasFormat);
  EXPECT_EQ(QEXEC_FORMAT_EXPAND, metadata->formatFlags);
  EXPECT_EQ(QEXEC_S_SHARD_TIMED_OUT_WARNING, metadata->stateFlags);
  ASSERT_NE(nullptr, metadata->profiles);
  EXPECT_EQ(1, array_len(metadata->profiles));
  EXPECT_EQ(nullptr, RPNet_TakeDrainMetadata(network));
  EXPECT_EQ(99, qctx.totalResults);
  EXPECT_EQ(QEXEC_FORMAT_DEFAULT, request.reqflags);
  EXPECT_EQ(0, request.stateflags);
  SearchResult_Clear(&result);
  ASSERT_EQ(RP_DRAIN_OK, network->base.Drain(&network->base, &result));
  EXPECT_EQ(2, number(&result));
  SearchResult_Clear(&result);
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
  auto *remaining = RPNet_TakeDrainMetadata(network);
  ASSERT_NE(nullptr, remaining);
  EXPECT_EQ(2, remaining->sourceResults);
  EXPECT_EQ(nullptr, remaining->profiles);
  RPNetDrainMetadata_Free(remaining);
  RPNetDrainMetadata_Free(metadata);
  SearchResult_Destroy(&result);
}

#ifdef ENABLE_ASSERT
TEST_F(RPNetBufferedDrainTest, privateBatchIsOfferedToDrainBeforeTerminalEOF) {
  for (bool closeBeforeResume : {false, true}) {
    SCOPED_TRACE(closeBeforeResume);
    network->phase = RPNET_READING;
    network->sourceResults = 0;
    qctx.totalResults = 0;
    IORuntimeCtx runtime = {};
    runtime.queue = RQ_New(1, 0);
    RQ_IncrPending(runtime.queue);
    MRCommand command = {};
    command.protocol = 3;
    MRIteratorConfig config = {};
    config.successCB = [](MRIteratorCallbackCtx *, MRReply *) {};
    config.ioRuntime = &runtime;
    auto *iterator = MR_CreateIterator(&command, &config);
    auto *fixtureChannel = channel;
    channel = MRIterator_GetChannel(iterator);
    network->it = iterator;
    network->cmd.protocol = 3;
    network->drainChannel = nullptr;
    RPNet_PublishIterator(network);
    QueryRequestTimeout_Init(&request.base.timeout, TimeoutPolicy_ReturnStrict, 1000);
    QueryRequestTimeout_BeginCycle(&request.base.timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
    MRChannel_Push(
        channel,
        parseReply("*2\r\n%2\r\n+results\r\n*1\r\n%1\r\n+extra_attributes\r\n%1\r\n+n\r\n:0\r\n"
                   "+format\r\n+STRING\r\n:0\r\n"));
    SearchResult published = SearchResult_New();
    ASSERT_EQ(RS_RESULT_OK, network->base.Next(&network->base, &published));
    EXPECT_EQ(0, number(&published));
    EXPECT_EQ(1, network->sourceResults);
    SearchResult_Destroy(&published);
    std::string first =
        "*2\r\n%3\r\n+results\r\n*2\r\n%1\r\n+extra_attributes\r\n%1\r\n+n\r\n:1\r\n"
        "%1\r\n+extra_attributes\r\n%1\r\n+n\r\n:2\r\n+format\r\n+EXPAND\r\n+warning\r\n*1\r\n+";
    first += QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT);
    first += "\r\n:0\r\n";
    MRChannel_Push(channel, parseReply(first.c_str()));
    ASSERT_TRUE(SyncPoint_Arm(SYNC_POINT_RPNET_BEFORE_BATCH_PUBLISH));
    SearchResult bg = SearchResult_New(), result = SearchResult_New();
    int status = RS_RESULT_EOF;
    std::thread worker([&] { status = network->base.Next(&network->base, &bg); });
    while (!SyncPoint_IsWaiting(SYNC_POINT_RPNET_BEFORE_BATCH_PUBLISH)) std::this_thread::yield();
    MRChannel_Push(
        channel,
        parseReply(
            "*2\r\n%1\r\n+results\r\n*1\r\n%1\r\n+extra_attributes\r\n%1\r\n+n\r\n:3\r\n:0\r\n"));
    QueryRequestTimeout_MarkTimedOut(&request.base.timeout);
    EXPECT_EQ(RP_DRAIN_OK, network->base.Drain(&network->base, &result));
    EXPECT_EQ(3, number(&result));
    SearchResult_Clear(&result);
    if (closeBeforeResume) EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
    SyncPoint_Signal(SYNC_POINT_RPNET_BEFORE_BATCH_PUBLISH);
    worker.join();
    EXPECT_EQ(RS_RESULT_TIMEDOUT, status);
    EXPECT_EQ(closeBeforeResume, network->pendingBatch == nullptr);
    if (!closeBeforeResume)
      for (int expected : {1, 2}) {
        EXPECT_EQ(RP_DRAIN_OK, network->base.Drain(&network->base, &result));
        EXPECT_EQ(expected, number(&result));
        SearchResult_Clear(&result);
      }
    EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
    EXPECT_EQ(nullptr, network->pendingBatch);
    auto *metadata = RPNet_TakeDrainMetadata(network);
    ASSERT_NE(nullptr, metadata);
    EXPECT_EQ(closeBeforeResume ? 0 : QEXEC_S_SHARD_TIMED_OUT_WARNING, metadata->stateFlags);
    EXPECT_EQ(closeBeforeResume ? 0 : QEXEC_FORMAT_EXPAND, metadata->formatFlags);
    EXPECT_EQ(closeBeforeResume ? 2 : 4, metadata->sourceResults);
    EXPECT_EQ(3, qctx.totalResults);
    RPNetDrainMetadata_Free(metadata);
    SearchResult_Destroy(&result);
    SearchResult_Destroy(&bg);
    network->it = nullptr;
    MRIterator_ResolveShard(iterator, 0, 0);
    MRIterator_Release(iterator);
    RQ_Free(runtime.queue);
    channel = fixtureChannel;
    network->drainChannel = channel;
  }
}
#endif

TEST_F(RPNetBufferedDrainTest, errorPoliciesPreserveFollowingRowsWhenAllowed) {
  struct Case {
    QueryErrorCode code;
    bool fail;
    bool hybrid;
    bool fatal;
  };
  const Case cases[] = {
      {QUERY_ERROR_CODE_TIMED_OUT, false, false, false},
      {QUERY_ERROR_CODE_TIMED_OUT, true, false, true},
      {QUERY_ERROR_CODE_OUT_OF_MEMORY, false, false, false},
      {QUERY_ERROR_CODE_OUT_OF_MEMORY, true, false, true},
      {QUERY_ERROR_CODE_GENERIC, false, true, true},
      {QUERY_ERROR_CODE_UNAVAILABLE_SLOTS, false, false, true},
  };
  for (const auto &test : cases) {
    SCOPED_TRACE(static_cast<int>(test.code));
    network->phase = RPNET_READING;
    request.reqConfig.timeoutPolicy = test.fail ? TimeoutPolicy_Fail : TimeoutPolicy_Return;
    request.reqConfig.oomPolicy = test.fail ? OomPolicy_Fail : OomPolicy_Return;
    network->hybridSubquery = test.hybrid ? RPNET_HYBRID_SEARCH : RPNET_HYBRID_NONE;
    QueryError error = {};
    QueryError_SetCode(&error, test.code);
    const char *message = QueryError_GetUserError(&error);
    ASSERT_EQ(test.code, QueryError_GetCodeFromMessage(message));
    MRChannel_Push(channel, MRReply_CreateError(message, strlen(message)));
    QueryError_ClearError(&error);
    MRChannel_Push(channel, parseReply("*2\r\n*2\r\n:1\r\n*2\r\n+n\r\n:8\r\n:0\r\n"));
    SearchResult result = SearchResult_New();
    EXPECT_EQ(test.fatal ? RP_DRAIN_ERROR : RP_DRAIN_OK,
              network->base.Drain(&network->base, &result));
    if (!test.fatal) EXPECT_EQ(8, number(&result));
    EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
    EXPECT_EQ(test.fatal ? 1 : 0, MRChannel_Size(channel));
    EXPECT_EQ(0, request.stateflags);
    EXPECT_EQ(0, qctx.totalResults);
    while (auto *reply = static_cast<MRReply *>(MRChannel_TryPop(channel))) MRReply_Free(reply);
    SearchResult_Destroy(&result);
  }
}

TEST_F(RPNetBufferedDrainTest, profileScoresAndWithCountUsePrivateDrainState) {
  network->cmd.protocol = 3;
  network->cmd.forProfiling = true;
  network->withCount = true;
  for (bool explain : {false, true}) {
    network->phase = RPNET_READING;
    network->explainScores = explain;
    std::string wire =
        "*2\r\n%2\r\n+results\r\n%1\r\n+results\r\n*1\r\n%2\r\n+extra_attributes\r\n%1\r\n+n\r\n:"
        "9\r\n+score\r\n";
    wire += explain ? "*2\r\n,3.5\r\n+explanation\r\n" : ",3.5\r\n";
    wire += "+profile\r\n%0\r\n:0\r\n";
    MRChannel_Push(channel, parseReply(wire.c_str()));
    SearchResult result = SearchResult_New();
    ASSERT_EQ(RP_DRAIN_OK, network->base.Drain(&network->base, &result));
    EXPECT_EQ(9, number(&result));
    EXPECT_EQ(3.5, SearchResult_GetScore(&result));
    EXPECT_EQ(explain, SearchResult_GetScoreExplain(&result) != nullptr);
    EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
    EXPECT_EQ(0, network->drainMetadata->sourceResults);
    EXPECT_EQ(0, qctx.totalResults);
    SearchResult_Destroy(&result);
  }
}

TEST_F(RPNetBufferedDrainTest, hybridMappingWarningsRespectFailPolicies) {
  network->hybridSubquery = RPNET_HYBRID_SEARCH;
  for (bool fail : {false, true}) {
    for (auto warning : {QUERY_WARNING_CODE_TIMED_OUT, QUERY_WARNING_CODE_OUT_OF_MEMORY_SHARD}) {
      network->phase = RPNET_READING;
      request.reqConfig.timeoutPolicy = fail ? TimeoutPolicy_Fail : TimeoutPolicy_Return;
      request.reqConfig.oomPolicy = fail ? OomPolicy_Fail : OomPolicy_Return;
      std::string wire = "+";
      wire += QueryWarning_Strwarning(warning);
      wire += "\r\n";
      MRChannel_Push(channel, parseReply(wire.c_str()));
      MRChannel_Push(channel, parseReply("*2\r\n*2\r\n:1\r\n*2\r\n+n\r\n:8\r\n:0\r\n"));
      SearchResult result = SearchResult_New();
      EXPECT_EQ(fail ? RP_DRAIN_ERROR : RP_DRAIN_OK, network->base.Drain(&network->base, &result));
      auto *metadata = RPNet_TakeDrainMetadata(network);
      ASSERT_NE(nullptr, metadata);
      EXPECT_EQ(fail, QueryError_HasError(&metadata->error));
      if (fail) {
        EXPECT_EQ(warning == QUERY_WARNING_CODE_TIMED_OUT ? QUERY_ERROR_CODE_TIMED_OUT
                                                          : QUERY_ERROR_CODE_OUT_OF_MEMORY,
                  QueryError_GetCode(&metadata->error));
        EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
      } else {
        EXPECT_EQ(8, number(&result));
        SearchResult_Clear(&result);
        EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
      }
      RPNetDrainMetadata_Free(metadata);
      RPNetDrainMetadata_Free(RPNet_TakeDrainMetadata(network));
      while (auto *reply = static_cast<MRReply *>(MRChannel_TryPop(channel))) MRReply_Free(reply);
      SearchResult_Destroy(&result);
    }
  }
}

TEST(RPNetDrainTest, constructorProvidesDrainWithoutChainInsertion) {
  MRCommand command = {};
  RPNet *network = RPNet_New(&command, nullptr);
  SearchResult result = SearchResult_New();
  ASSERT_NE(nullptr, network->base.Drain);
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
  network->base.Free(&network->base);
  SearchResult_Destroy(&result);
}
