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
#include "coord/rmr/chan.h"
#include "pipeline_execution.h"
#include "search_result_ffi.h"
#include "query_flags.h"
#include "hiredis/hiredis.h"
#include "hiredis/read.h"
#include <chrono>
#include <future>
#include <thread>

#ifdef ENABLE_ASSERT
// debug_commands.h includes C-only atomic declarations unrelated to these hooks.
extern "C" {
bool SyncPoint_Arm(const char *name);
bool SyncPoint_IsWaiting(const char *name);
void SyncPoint_Signal(const char *name);
}
#endif

using namespace std::chrono_literals;

struct OwnedNetStep {
  QueryProcessingCtx *context;
  ResultProcessor *rp;
  SearchResult *row;
  std::promise<void> suspended;
  unsigned calls = 0;
  int result = RS_RESULT_ERROR;

  static void run(PipelineAccess *access, void *data) {
    auto *step = static_cast<OwnedNetStep *>(data);
    ++step->calls;
    PipelineAccess_Publish(access, step->context);
    step->result = step->rp->Next(step->rp, step->row);
    if (step->result == RS_RESULT_SUSPENDED) step->suspended.set_value();
  }
};

class OwnedRPNetTest : public ::testing::Test {
 protected:
  AREQ *request = nullptr;
  RPNet *net = nullptr;
  RLookup lookup = RLookup_New();
  SearchResult row = SearchResult_New();
  IORuntimeCtx runtime = {};
  MRWorkQueue queue = {};
  PipelineExecution *execution = nullptr;

  void SetUp() override {
    ASSERT_EQ(0, uv_mutex_init(&queue.lock));
    queue.pending = 1;
    runtime.queue = &queue;
    request = AREQ_New(nullptr, 0);
    request->reqConfig.timeoutPolicy = TimeoutPolicy_ReturnStrict;
    QueryRequestTimeout_Init(&request->base.timeout, TimeoutPolicy_ReturnStrict, 1000);
    QueryRequestTimeout_BeginCycle(&request->base.timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
    const char *args[] = {"_FT.AGGREGATE", "idx", "*"};
    const size_t lengths[] = {13, 3, 1};
    MRCommand cmd = MR_NewCommandArgvLen(3, args, lengths);
    cmd.protocol = 2;
    net = RPNet_New(&cmd, rpnetNext);
    net->lookup = &lookup;
    net->areq = request;
    QITR_PushRP(AREQ_QueryProcessingCtx(request), &net->base);
    execution = PipelineExecution_New(&request->base.timeout);
  }

  void TearDown() override {
    PipelineExecution_Free(execution);
    if (net && net->it && MRIterator_GetPending(net->it)) {
      // Finish the synthetic producer reference without scheduling network work.
      MRIterator_ResolveShard(net->it, 0, 0);
    }
    AREQ_Free(request);
    SearchResult_Destroy(&row);
    RLookup_Cleanup(&lookup);
    uv_mutex_destroy(&queue.lock);
  }

  void attachIterator() {
    MRIteratorConfig config = {};
    config.successCB = [](MRIteratorCallbackCtx *, MRReply *) { ADD_FAILURE(); };
    config.ioRuntime = &runtime;
    net->it = MR_CreateIterator(&net->cmd, &config);
  }

  void push(const char *wire) {
    redisReader *reader = redisReaderCreate();
    ASSERT_EQ(REDIS_OK, redisReaderFeed(reader, wire, strlen(wire)));
    void *reply = nullptr;
    ASSERT_EQ(REDIS_OK, redisReaderGetReply(reader, &reply));
    ASSERT_NE(nullptr, reply);
    redisReaderFree(reader);
    MRIterator_PushReply(net->it, static_cast<MRReply *>(reply));
  }

  void expectValue(const char *expected) {
    const auto *key = RLookup_GetKey_Read(&lookup, "field", 0);
    ASSERT_NE(nullptr, key);
    const auto *value = RLookupRow_Get(key, SearchResult_GetRowData(&row));
    ASSERT_NE(nullptr, value);
    size_t len = 0;
    const char *text = RSValue_StringPtrLen(value, &len);
    ASSERT_NE(nullptr, text);
    EXPECT_EQ(std::string(expected), std::string(text, len));
  }
};

TEST_F(OwnedRPNetTest, UnstartedDrainDoesNotInitializeTransport) {
  EXPECT_EQ(RP_DRAIN_EOF, net->base.Drain(&net->base, &row));
  EXPECT_EQ(nullptr, net->it);
}

TEST_F(OwnedRPNetTest, DrainConsumesQueuedRowsWithoutWaitingForProducerCompletion) {
  attachIterator();
  net->cmd.forCursor = true;
  push(
      "*2\r\n*3\r\n:2\r\n*2\r\n$5\r\nfield\r\n$3\r\none\r\n"
      "*2\r\n$5\r\nfield\r\n$3\r\ntwo\r\n:0\r\n");
  ASSERT_EQ(RP_DRAIN_OK, net->base.Drain(&net->base, &row));
  expectValue("one");
  SearchResult_Clear(&row);
  ASSERT_EQ(RP_DRAIN_OK, net->base.Drain(&net->base, &row));
  expectValue("two");
  SearchResult_Clear(&row);
  EXPECT_EQ(RP_DRAIN_EOF, net->base.Drain(&net->base, &row));
  EXPECT_EQ(1, MRIterator_GetPending(net->it));
  EXPECT_EQ(2, AREQ_QueryProcessingCtx(request)->totalResults);
  EXPECT_EQ(0, queue.sz);
}

TEST_F(OwnedRPNetTest, DrainRetainsProfileAlreadyClaimedByNextExactlyOnce) {
  attachIterator();
  net->cmd.forProfiling = true;
  net->shardsProfile = array_new(MRReply *, 2);
  push(
      "*3\r\n*3\r\n:2\r\n*2\r\n$5\r\nfield\r\n$3\r\none\r\n"
      "*2\r\n$5\r\nfield\r\n$3\r\ntwo\r\n:0\r\n*1\r\n$7\r\nprofile\r\n");
  ASSERT_EQ(RS_RESULT_OK, net->base.Next(&net->base, &row));
  EXPECT_EQ(1, array_len(net->shardsProfile));
  auto *profile = net->shardsProfile[0];
  SearchResult_Clear(&row);
  ASSERT_EQ(RP_DRAIN_OK, net->base.Drain(&net->base, &row));
  expectValue("two");
  EXPECT_EQ(RP_DRAIN_EOF, net->base.Drain(&net->base, &row));
  ASSERT_EQ(1, array_len(net->shardsProfile));
  EXPECT_EQ(profile, net->shardsProfile[0]);
  ASSERT_EQ(1, profile->elements);
  EXPECT_STREQ("profile", profile->element[0]->str);
}

TEST_F(OwnedRPNetTest, DrainTakesTerminalProfileAndReturnedPayloadOutlivesReply) {
  attachIterator();
  net->cmd.forProfiling = true;
  net->shardsProfile = array_new(MRReply *, 2);
  push(
      "*3\r\n*2\r\n:1\r\n*2\r\n$5\r\nfield\r\n$3\r\none\r\n"
      ":0\r\n*1\r\n$7\r\nprofile\r\n");
  ASSERT_EQ(RP_DRAIN_OK, net->base.Drain(&net->base, &row));
  ASSERT_EQ(1, array_len(net->shardsProfile));
  EXPECT_EQ(RP_DRAIN_EOF, net->base.Drain(&net->base, &row));
  EXPECT_EQ(nullptr, net->current.root);
  expectValue("one");
  ASSERT_EQ(1, net->shardsProfile[0]->elements);
  EXPECT_STREQ("profile", net->shardsProfile[0]->element[0]->str);
}

TEST_F(OwnedRPNetTest, TimeoutRecoveryFinishesBeforeNetworkInputArrives) {
  attachIterator();
  OwnedNetStep step{AREQ_QueryProcessingCtx(request), &net->base, &row};
  auto suspended = step.suspended.get_future();
  auto worker = std::async(std::launch::async, [&] {
    return PipelineExecution_RunNext(execution, OwnedNetStep::run, &step);
  });
  suspended.wait();
  QueryRequestTimeout_MarkTimedOut(&request->base.timeout);
  auto drainer = std::async(std::launch::async, [&] {
    PipelineExecution_RunDrain(
        execution,
        [](PipelineAccess *access, void *) {
          auto *rp = PipelineAccess_Context(access)->endProc;
          auto output = SearchResult_New();
          EXPECT_EQ(RP_DRAIN_EOF, rp->Drain(rp, &output));
          SearchResult_Destroy(&output);
        },
        nullptr);
  });
  const auto drained = drainer.wait_for(1s);
  push("*2\r\n*2\r\n:1\r\n*2\r\n$5\r\nfield\r\n$3\r\none\r\n:0\r\n");
  drainer.get();
  EXPECT_FALSE(worker.get());
  EXPECT_EQ(std::future_status::ready, drained);
  EXPECT_EQ(1, step.calls);
  EXPECT_EQ(1, MRIterator_GetChannelSize(net->it));
  EXPECT_EQ(0, AREQ_QueryProcessingCtx(request)->totalResults);
}

TEST_F(OwnedRPNetTest, NaturalWakeReentersBeforeConsumingReply) {
  attachIterator();
  OwnedNetStep step{AREQ_QueryProcessingCtx(request), &net->base, &row};
  auto suspended = step.suspended.get_future();
  auto worker = std::async(std::launch::async, [&] {
    return PipelineExecution_RunNext(execution, OwnedNetStep::run, &step);
  });
  suspended.wait();
  push("*2\r\n*2\r\n:1\r\n*2\r\n$5\r\nfield\r\n$3\r\none\r\n:0\r\n");
  EXPECT_TRUE(worker.get());
  EXPECT_EQ(2, step.calls);
  EXPECT_EQ(RS_RESULT_OK, step.result);
  EXPECT_EQ(1, AREQ_QueryProcessingCtx(request)->totalResults);
  expectValue("one");
  EXPECT_EQ(RP_DRAIN_EOF, net->base.Drain(&net->base, &row));
  EXPECT_EQ(0, MRIterator_GetChannelSize(net->it));
}

#ifdef ENABLE_ASSERT
TEST_F(OwnedRPNetTest, FinalReplyPublishedAfterEmptyPopIsNotMistakenForEof) {
  attachIterator();
  constexpr const char *point = "RpnetEmptyOwnedPop";
  ASSERT_TRUE(SyncPoint_Arm(point));
  OwnedNetStep step{AREQ_QueryProcessingCtx(request), &net->base, &row};
  auto worker = std::async(std::launch::async, [&] {
    return PipelineExecution_RunNext(execution, OwnedNetStep::run, &step);
  });
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!SyncPoint_IsWaiting(point) && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  const bool reached = SyncPoint_IsWaiting(point);
  push("*2\r\n*2\r\n:1\r\n*2\r\n$5\r\nfield\r\n$3\r\none\r\n:0\r\n");
  MRIterator_ResolveShard(net->it, 0, 0);
  SyncPoint_Signal(point);
  EXPECT_TRUE(worker.get());
  ASSERT_TRUE(reached);
  EXPECT_EQ(1, step.calls);
  ASSERT_EQ(RS_RESULT_OK, step.result);
  expectValue("one");
  EXPECT_EQ(0, MRIterator_GetPending(net->it));
  EXPECT_EQ(0, MRIterator_GetChannelSize(net->it));
  EXPECT_EQ(RP_DRAIN_EOF, net->base.Drain(&net->base, &row));
}
#endif

TEST_F(OwnedRPNetTest, Resp3DrainRecordsTimeoutWarningWithoutRestartingNext) {
  attachIterator();
  net->cmd.protocol = 3;
  request->reqConfig.timeoutPolicy = TimeoutPolicy_Return;
  const std::string warning = QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT);
  const std::string wire =
      "*2\r\n%3\r\n+results\r\n*1\r\n%1\r\n+extra_attributes\r\n"
      "%1\r\n+field\r\n+one\r\n+format\r\n+STRING\r\n+warning\r\n"
      "*1\r\n+" +
      warning + "\r\n:0\r\n";
  push(wire.c_str());
  ASSERT_EQ(RP_DRAIN_OK, net->base.Drain(&net->base, &row));
  EXPECT_EQ(RP_DRAIN_EOF, net->base.Drain(&net->base, &row));
  expectValue("one");
  EXPECT_TRUE(request->stateflags & QEXEC_S_SHARD_TIMED_OUT_WARNING);
}

TEST_F(OwnedRPNetTest, DrainReportsFatalShardErrorThroughExistingDiagnostic) {
  attachIterator();
  push("-ERR synthetic shard failure\r\n");
  EXPECT_EQ(RP_DRAIN_ERROR, net->base.Drain(&net->base, &row));
  EXPECT_TRUE(QueryError_HasError(AREQ_QueryProcessingCtx(request)->err));
}
