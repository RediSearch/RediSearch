/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "redismock/redismock.h"
extern "C" {
#include "reply.h"
}
#include "hybrid/hybrid_request.h"
#include "hiredis/hiredis.h"
#include "coord/rpnet.h"

#include <memory>
#include <string>
#include <tuple>
#include <vector>

extern "C" void printShardsHybridProfile(RedisModule_Reply *reply, void *ctx);

class HybridProfileTest : public ::testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
  RedisModuleCtx *ctx = nullptr;
  std::vector<std::unique_ptr<MRReply, decltype(&MRReply_Free)>> replies;

  bool shardResp3() const {
    return std::get<0>(GetParam());
  }

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
  }
  void TearDown() override {
    RedisModule_FreeThreadSafeContext(ctx);
  }

  MRReply *parse(const std::string &wire) {
    redisReader *reader = redisReaderCreate();
    EXPECT_EQ(redisReaderFeed(reader, wire.data(), wire.size()), REDIS_OK);
    void *reply = nullptr;
    EXPECT_EQ(redisReaderGetReply(reader, &reply), REDIS_OK);
    redisReaderFree(reader);
    EXPECT_NE(reply, nullptr);
    replies.emplace_back(static_cast<MRReply *>(reply), MRReply_Free);
    return replies.back().get();
  }

  std::string wrap(const std::string &profile) {
    return (shardResp3() ? "%1\r\n+Shards\r\n*1\r\n" : "*2\r\n+Shards\r\n*1\r\n") + profile;
  }

  MRReply *validProfile() {
    return parse(wrap(std::string(shardResp3() ? "%2" : "*4") +
                      "\r\n+Shard ID\r\n+shard-1\r\n+Warning\r\n+None\r\n"));
  }

  void check(const std::vector<MRReply *> &search, const std::vector<MRReply *> &vsim, size_t count,
             const std::string &expected) {
    AREQ searchAreq = {}, vsimAreq = {};
    RPNet searchRpnet = {}, vsimRpnet = {};
    searchRpnet.cmd.protocol = vsimRpnet.cmd.protocol = shardResp3() ? 3 : 2;
    // Allocate exactly the collected count, so ASan catches reading past either list.
    if (!search.empty()) {
      searchRpnet.shardsProfile = array_new(MRReply *, search.size());
      for (auto *profile : search) array_append(searchRpnet.shardsProfile, profile);
    }
    if (!vsim.empty()) {
      vsimRpnet.shardsProfile = array_new(MRReply *, vsim.size());
      for (auto *profile : vsim) array_append(vsimRpnet.shardsProfile, profile);
    }
    AREQ_QueryProcessingCtx(&searchAreq)->rootProc = &searchRpnet.base;
    AREQ_QueryProcessingCtx(&vsimAreq)->rootProc = &vsimRpnet.base;
    AREQ *requests[] = {&searchAreq, &vsimAreq};
    HybridRequest hreq = {};
    hreq.requests = requests;

    RMCK_GetReplyLog(ctx).clear();
    RedisModule_Reply reply = RedisModule_NewReply(ctx);
    reply.resp3 = std::get<1>(GetParam());
    RedisModule_Reply_Array(&reply);
    printShardsHybridProfile(&reply, &hreq);
    RedisModule_Reply_ArrayEnd(&reply);
#ifdef ENABLE_ASSERT
    EXPECT_EQ(std::string(reply.json), expected);
#endif
    RedisModule_EndReply(&reply);
    EXPECT_EQ(RMCK_GetReplyLog(ctx).back(), "setarray:" + std::to_string(count));
    array_free(searchRpnet.shardsProfile);
    array_free(vsimRpnet.shardsProfile);
  }

  const std::string searchOnly =
      "{ \"Shard ID\": \"shard-1\", \"SEARCH\": { \"Warning\": \"None\" } }";
  const std::string vsimOnly = "{ \"Shard ID\": \"shard-1\", \"VSIM\": { \"Warning\": \"None\" } }";
  const std::string both =
      "{ \"Shard ID\": \"shard-1\", \"SEARCH\": { \"Warning\": \"None\" }, "
      "\"VSIM\": { \"Warning\": \"None\" } }";
};

TEST_P(HybridProfileTest, NoProfiles) {
  check({}, {}, 0, "[  ]");
}

TEST_P(HybridProfileTest, CompleteProfiles) {
  auto *search = validProfile();
  auto *vsim = validProfile();
  check({search}, {vsim}, 1, "[ " + both + " ]");
  // RESP2 profiles are converted to maps in place and must remain readable.
  check({search}, {vsim}, 1, "[ " + both + " ]");
}

TEST_P(HybridProfileTest, MissingVsimProfiles) {
  check({validProfile()}, {}, 1, "[ " + searchOnly + " ]");
  check({validProfile(), validProfile(), validProfile()}, {validProfile()}, 3,
        "[ " + both + ", " + searchOnly + ", " + searchOnly + " ]");
}

TEST_P(HybridProfileTest, MissingSearchProfiles) {
  check({}, {validProfile()}, 1, "[ " + vsimOnly + " ]");
  check({validProfile()}, {validProfile(), validProfile(), validProfile()}, 3,
        "[ " + both + ", " + vsimOnly + ", " + vsimOnly + " ]");
}

TEST_P(HybridProfileTest, ErrorProfiles) {
  auto *error = parse("-timeout\r\n");
  check({error}, {validProfile()}, 1,
        "[ { \"Shard ID\": \"shard-1\", \"SEARCH\": \"ERR: timeout\", "
        "\"VSIM\": { \"Warning\": \"None\" } } ]");
  check({validProfile()}, {error}, 1,
        "[ { \"Shard ID\": \"shard-1\", \"SEARCH\": { \"Warning\": \"None\" }, "
        "\"VSIM\": \"ERR: timeout\" } ]");
  check({error}, {}, 1, "[ { \"Shard ID\": null, \"SEARCH\": \"ERR: timeout\" } ]");
  check({}, {error}, 1, "[ { \"Shard ID\": null, \"VSIM\": \"ERR: timeout\" } ]");
  check({error}, {error}, 1,
        "[ { \"Shard ID\": null, \"SEARCH\": \"ERR: timeout\", \"VSIM\": \"ERR: timeout\" } ]");
}

INSTANTIATE_TEST_SUITE_P(Protocols, HybridProfileTest,
                         ::testing::Combine(::testing::Bool(), ::testing::Bool()));
