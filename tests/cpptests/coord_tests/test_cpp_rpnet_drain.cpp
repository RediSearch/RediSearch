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
#include <thread>
#include <string>

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
    network->lookup = network->drainLookup = &lookup;
    network->areq = &request;
    network->base.parent = &qctx;
    network->drainChannel = channel;
    network->drainProtocol = 2;
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
  EXPECT_EQ(2, network->drainedCount);
  MRChannel_Push(channel, parseReply("*2\r\n*1\r\n:0\r\n:0\r\n"));
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
  EXPECT_EQ(1, MRChannel_Size(channel));
  SearchResult_Destroy(&result);
}

TEST_F(RPNetBufferedDrainTest, resp3RetainsWarningsAndCreatesDynamicFields) {
  network->drainProtocol = 3;
  MRChannel_Push(
      channel, parseReply("*2\r\n%2\r\n+results\r\n*1\r\n%1\r\n+extra_attributes\r\n%2\r\n+n\r\n:"
                          "7\r\n+dynamic\r\n+value\r\n+warning\r\n*1\r\n+warning text\r\n:0\r\n"));
  SearchResult result = SearchResult_New();
  ASSERT_EQ(RP_DRAIN_OK, network->base.Drain(&network->base, &result));
  EXPECT_EQ(7, number(&result));
  EXPECT_EQ(2, RLookup_GetRowLen(&lookup));
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
  ASSERT_EQ(1, array_len(network->drainedReplies));
  EXPECT_EQ(0, request.stateflags);
  SearchResult_Destroy(&result);
}

TEST_F(RPNetBufferedDrainTest, emptyRepliesAndErrorsDoNotWait) {
  MRChannel_Push(channel, parseReply("*2\r\n*1\r\n:0\r\n:0\r\n"));
  MRChannel_Push(channel, parseReply("-ERR shard failed\r\n"));
  SearchResult result = SearchResult_New();
  EXPECT_EQ(RP_DRAIN_ERROR, network->base.Drain(&network->base, &result));
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
  EXPECT_EQ(2, array_len(network->drainedReplies));
  SearchResult_Destroy(&result);
}

TEST_F(RPNetBufferedDrainTest, concurrentNextAndDrainNeverDuplicateClaimedRow) {
  network->current.root =
      parseReply("*2\r\n*3\r\n:2\r\n*2\r\n+n\r\n:1\r\n*2\r\n+n\r\n:2\r\n:0\r\n");
  network->current.rows = MRReply_ArrayElement(network->current.root, 0);
  network->curIdx = 1;
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
  network->curIdx = 1;
  SearchResult result = SearchResult_New();
  ASSERT_EQ(RS_RESULT_OK, network->base.Next(&network->base, &result));
  EXPECT_EQ(1, number(&result));
  SearchResult_Clear(&result);
  ASSERT_EQ(RP_DRAIN_OK, network->base.Drain(&network->base, &result));
  EXPECT_EQ(2, number(&result));
  SearchResult_Clear(&result);
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
  EXPECT_EQ(RS_RESULT_TIMEDOUT, network->base.Next(&network->base, &result));
  SearchResult_Destroy(&result);
}

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
    network->draining = network->drainEOF = false;
    network->drainTimeoutPolicy = test.fail ? TimeoutPolicy_Fail : TimeoutPolicy_Return;
    network->drainOomPolicy = test.fail ? OomPolicy_Fail : OomPolicy_Return;
    network->drainHybridSubquery = test.hybrid ? RPNET_HYBRID_SEARCH : RPNET_HYBRID_NONE;
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
  network->drainProtocol = 3;
  network->drainProfiling = true;
  network->drainWithCount = true;
  for (bool explain : {false, true}) {
    network->draining = network->drainEOF = false;
    network->drainExplain = explain;
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
    EXPECT_EQ(0, network->drainedCount);
    EXPECT_EQ(0, qctx.totalResults);
    SearchResult_Destroy(&result);
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
