/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "gtest/gtest.h"

extern "C" {
#include "hiredis/hiredis.h"
#include "rmr/reply.h"
}

#include "aggregate/aggregate.h"
#include "hybrid/hybrid_request.h"
#include "redismock/redismock.h"
#include "reply.h"
#include "rpnet.h"

#include <algorithm>
#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

#ifdef ENABLE_ASSERT
// The formatter is intentionally exercised directly with synthetic shard replies. This is the
// production callback installed by HybridRequest_prepareForExecution, but it has no public header.
extern "C" void printShardsHybridProfile(RedisModule_Reply *reply, void *ctx);

namespace {

std::string bulk(const std::string &value) {
  return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}

std::string array(const std::vector<std::string> &values) {
  std::string wire = "*" + std::to_string(values.size()) + "\r\n";
  for (const auto &value : values) wire += value;
  return wire;
}

std::string map(const std::vector<std::string> &values) {
  std::string wire = "%" + std::to_string(values.size() / 2) + "\r\n";
  for (const auto &value : values) wire += value;
  return wire;
}

MRReply *parse(const std::string &wire) {
  redisReader *reader = redisReaderCreate();
  EXPECT_NE(reader, nullptr);
  if (!reader) return nullptr;
  EXPECT_EQ(redisReaderFeed(reader, wire.data(), wire.size()), REDIS_OK);
  void *rawReply = nullptr;
  EXPECT_EQ(redisReaderGetReply(reader, &rawReply), REDIS_OK);
  redisReaderFree(reader);
  return static_cast<MRReply *>(rawReply);
}

std::string profile(int protocol, const std::string &shardId, const std::string &marker) {
  const std::vector<std::string> fields = {bulk("Shard ID"), bulk(shardId), bulk("marker"),
                                           bulk(marker)};
  if (protocol == 3) return map(fields);
  return array(fields);
}

std::string shardReply(int protocol, const std::string &shardId, const std::string &marker) {
  if (protocol == 3) {
    return map({bulk("Shards"), array({profile(protocol, shardId, marker)})});
  }
  return array({array({}), array({profile(protocol, shardId, marker)})});
}

std::string errorReply(const std::string &message) {
  return "-" + message + "\r\n";
}

struct ProfileHarness {
  HybridRequest request = {};
  AREQ search = {};
  AREQ vsim = {};
  RPNet searchNet = {};
  RPNet vsimNet = {};
  RedisModuleCtx *redisCtx = RedisModule_GetThreadSafeContext(nullptr);

  explicit ProfileHarness(int shardProtocol) {
    searchNet.cmd.protocol = shardProtocol;
    vsimNet.cmd.protocol = shardProtocol;
    request.requests = array_new(AREQ *, HYBRID_REQUEST_NUM_SUBQUERIES);
    array_append(request.requests, &search);
    array_append(request.requests, &vsim);
    AREQ_QueryProcessingCtx(&search)->rootProc = (ResultProcessor *)&searchNet;
    AREQ_QueryProcessingCtx(&vsim)->rootProc = (ResultProcessor *)&vsimNet;
  }

  ~ProfileHarness() {
    freeProfiles(&searchNet);
    freeProfiles(&vsimNet);
    array_free(request.requests);
    RedisModule_FreeThreadSafeContext(redisCtx);
  }

  static void freeProfiles(RPNet *net) {
    if (!net->shardsProfile) return;
    array_foreach(net->shardsProfile, shardProfile, if (shardProfile) MRReply_Free(shardProfile));
    array_free(net->shardsProfile);
  }

  void add(bool isSearch, const std::string &wire) {
    RPNet *net = isSearch ? &searchNet : &vsimNet;
    if (!net->shardsProfile) net->shardsProfile = array_new(MRReply *, 2);
    array_append(net->shardsProfile, parse(wire));
  }

  void add(bool isSearch, MRReply *reply) {
    RPNet *net = isSearch ? &searchNet : &vsimNet;
    if (!net->shardsProfile) net->shardsProfile = array_new(MRReply *, 2);
    array_append(net->shardsProfile, reply);
  }

  void add(bool isSearch, int protocol, const std::string &shardId, const std::string &marker) {
    add(isSearch, shardReply(protocol, shardId, marker));
  }

  std::string print(int protocol) {
    RedisModule_Reply reply = RedisModule_NewReply(redisCtx);
    reply.resp3 = protocol == 3;
    RedisModule_Reply_Array(&reply);
    printShardsHybridProfile(&reply, &request);
    RedisModule_Reply_ArrayEnd(&reply);
    std::string result = reply.json ? reply.json : "";
    RedisModule_EndReply(&reply);
    return result;
  }
};

void expectContains(const std::string &reply, const std::string &fragment) {
  EXPECT_NE(reply.find(fragment), std::string::npos) << reply;
}

void expectAbsent(const std::string &reply, const std::string &fragment) {
  EXPECT_EQ(reply.find(fragment), std::string::npos) << reply;
}

size_t occurrences(const std::string &value, const std::string &needle) {
  size_t count = 0;
  size_t pos = 0;
  while ((pos = value.find(needle, pos)) != std::string::npos) {
    ++count;
    pos += needle.size();
  }
  return count;
}

}  // namespace

TEST(HybridShardProfile, BalancedAndEmptyShardListsPreserveReplyContents) {
  for (int protocol : {2, 3}) {
    ProfileHarness harness(protocol);
    harness.add(true, protocol, "search-id", "search-value");
    harness.add(false, protocol, "vsim-id", "vsim-value");
    const std::string reply = harness.print(protocol);
    expectContains(reply, "\"Shard ID\": \"search-id\"");
    expectContains(reply, "\"SEARCH\": { \"marker\": \"search-value\" }");
    expectContains(reply, "\"VSIM\": { \"marker\": \"vsim-value\" }");
    expectAbsent(reply, "vsim-id");
  }

  for (int protocol : {2, 3}) {
    ProfileHarness harness(protocol);
    EXPECT_EQ(harness.print(protocol), "[  ]");
  }
}

TEST(HybridShardProfile, ShardProtocolIsIndependentOfClientProtocol) {
  for (int shardProtocol : {2, 3}) {
    for (int clientProtocol : {2, 3}) {
      ProfileHarness harness(shardProtocol);
      harness.add(true, shardProtocol, "search-id", "search-value");
      harness.add(false, shardProtocol, "vsim-id", "vsim-value");
      const std::string reply = harness.print(clientProtocol);
      expectContains(reply, "\"Shard ID\": \"search-id\"");
      expectContains(reply, "\"SEARCH\": { \"marker\": \"search-value\" }");
      expectContains(reply, "\"VSIM\": { \"marker\": \"vsim-value\" }");
    }
  }
}

TEST(HybridShardProfile, MismatchedCountsKeepEveryAvailableSubqueryProfile) {
  for (int protocol : {2, 3}) {
    for (const auto [searchCount, vsimCount] :
         {std::pair<size_t, size_t>{2, 0}, {2, 1}, {0, 2}, {1, 2}}) {
      ProfileHarness harness(protocol);
      for (size_t i = 0; i < searchCount; ++i) {
        harness.add(true, protocol, "search-" + std::to_string(i), "s-" + std::to_string(i));
      }
      for (size_t i = 0; i < vsimCount; ++i) {
        harness.add(false, protocol, "vsim-" + std::to_string(i), "v-" + std::to_string(i));
      }

      const std::string reply = harness.print(protocol);
      EXPECT_EQ(occurrences(reply, "\"Shard ID\""), std::max(searchCount, vsimCount)) << reply;
      for (size_t i = 0; i < searchCount; ++i) {
        expectContains(reply, "\"Shard ID\": \"search-" + std::to_string(i) + "\"");
        expectContains(reply, "\"marker\": \"s-" + std::to_string(i) + "\"");
      }
      for (size_t i = 0; i < vsimCount; ++i) {
        if (i >= searchCount) {
          expectContains(reply, "\"Shard ID\": \"vsim-" + std::to_string(i) + "\"");
        }
        expectContains(reply, "\"VSIM\": { \"marker\": \"v-" + std::to_string(i) + "\" }");
      }
    }
  }
}

TEST(HybridShardProfile, MissingOrMalformedSearchProfileUsesVsimShardId) {
  for (int protocol : {2, 3}) {
    ProfileHarness harness(protocol);
    harness.add(true, protocol, "search-0", "search-0");
    harness.add(true, protocol, "search-1", "search-1");
    harness.add(false, protocol, "vsim-0", "vsim-0");
    harness.add(false, protocol, "vsim-1", "vsim-1");

    // Replace the second SEARCH reply with a malformed nested profile.
    MRReply_Free(harness.searchNet.shardsProfile[1]);
    harness.searchNet.shardsProfile[1] =
        parse(protocol == 3 ? map({bulk("Shards"), array({":1\r\n"})})
                            : array({array({}), array({":1\r\n"})}));

    const std::string reply = harness.print(protocol);
    expectContains(reply, "\"Shard ID\": \"vsim-1\"");
    expectContains(reply, "\"VSIM\": { \"marker\": \"vsim-1\" }");
    expectContains(reply, "\"SEARCH\": { \"marker\": \"search-0\" }");
    EXPECT_EQ(occurrences(reply, "\"SEARCH\":"), 1) << reply;
    expectAbsent(reply, "\"marker\": \"search-1\"");
  }
}

TEST(HybridShardProfile, ErrorRepliesArePreservedAndValidSideSuppliesShardId) {
  for (int protocol : {2, 3}) {
    ProfileHarness searchError(protocol);
    searchError.add(true, errorReply("SEARCH shard failed"));
    searchError.add(false, protocol, "vsim-id", "vsim-value");
    const std::string searchErrorReply = searchError.print(protocol);
    expectContains(searchErrorReply, "\"Shard ID\": \"vsim-id\"");
    expectContains(searchErrorReply, "\"SEARCH\": \"ERR: SEARCH shard failed\"");
    expectContains(searchErrorReply, "\"VSIM\": { \"marker\": \"vsim-value\" }");

    ProfileHarness vsimError(protocol);
    vsimError.add(true, protocol, "search-id", "search-value");
    vsimError.add(false, errorReply("VSIM shard failed"));
    const std::string vsimErrorReply = vsimError.print(protocol);
    expectContains(vsimErrorReply, "\"Shard ID\": \"search-id\"");
    expectContains(vsimErrorReply, "\"SEARCH\": { \"marker\": \"search-value\" }");
    expectContains(vsimErrorReply, "\"VSIM\": \"ERR: VSIM shard failed\"");
  }
}

TEST(HybridShardProfile, MalformedResp2AndResp3NestingIsOmittedWithoutLosingOtherSide) {
  for (int protocol : {2, 3}) {
    const std::vector<std::string> malformed = protocol == 3
        ? std::vector<std::string>{
              "$-1\r\n",                                      // null root
              ":1\r\n",                                       // wrong root type
              map({}),
              map({bulk("Shards"), array({map({})})}),
              map({bulk("Other"), array({})}),                 // missing Shards
              map({bulk("Shards"), ":1\r\n"}),               // wrong Shards type
              map({bulk("Shards"), array({})}),                // empty Shards array
              map({bulk("Shards"), array({"$-1\r\n"})}),      // null shard profile
              map({bulk("Shards"), array({":1\r\n"})}),       // wrong profile type
          }
        : std::vector<std::string>{
              "$-1\r\n",                                      // null root
              ":1\r\n",                                       // wrong root type
              array({}),                                        // RESP2 outer array has length zero
              array({array({})}),                               // RESP2 outer array has length one
              array({array({}), ":1\r\n"}),                   // wrong Shards type
              array({array({}), array({})}),                    // empty Shards array
              array({array({}), array({"$-1\r\n"})}),          // null shard profile
              array({array({}), array({array({})})}),
              array({array({}), array({":1\r\n"})}),           // wrong profile type
          };

    for (const auto &badReply : malformed) {
      ProfileHarness harness(protocol);
      harness.add(true, badReply);
      harness.add(false, protocol, "good-id", "good-value");
      const std::string reply = harness.print(protocol);
      expectContains(reply, "\"Shard ID\": \"good-id\"");
      expectContains(reply, "\"VSIM\": { \"marker\": \"good-value\" }");
      expectAbsent(reply, "\"SEARCH\":");
    }

    ProfileHarness nullCurrent(protocol);
    nullCurrent.add(true, static_cast<MRReply *>(nullptr));
    nullCurrent.add(false, protocol, "nonnull-id", "nonnull-value");
    const std::string nullReply = nullCurrent.print(protocol);
    expectContains(nullReply, "\"Shard ID\": \"nonnull-id\"");
    expectContains(nullReply, "\"VSIM\": { \"marker\": \"nonnull-value\" }");
    expectAbsent(nullReply, "\"SEARCH\":");

    // A RESP3 map or RESP2 flat key/value array with an odd number of elements is not a profile.
    const std::string oddProfile =
        protocol == 3
            ? map({bulk("Shard ID"), bulk("bad-id"), bulk("marker"), bulk("bad-value")})
            : array({bulk("Shard ID"), bulk("bad-id"), bulk("marker"), bulk("bad-value")});
    MRReply *odd = parse(protocol == 3 ? map({bulk("Shards"), array({oddProfile})})
                                       : array({array({}), array({oddProfile})}));
    ASSERT_NE(odd, nullptr);
    MRReply *profileReply = protocol == 3
                                ? MRReply_ArrayElement(MRReply_MapElement(odd, "Shards"), 0)
                                : MRReply_ArrayElement(MRReply_ArrayElement(odd, 1), 0);
    // Preserve ownership while exposing a missing value at the final key.
    MRReply_Free(profileReply->element[3]);
    profileReply->element[3] = nullptr;
    profileReply->elements = 3;

    ProfileHarness harness(protocol);
    harness.add(true, protocol, "search-good", "search-good");
    harness.add(false, protocol, "vsim-good", "vsim-good");
    MRReply_Free(harness.searchNet.shardsProfile[0]);
    harness.searchNet.shardsProfile[0] = odd;
    const std::string reply = harness.print(protocol);
    expectContains(reply, "\"Shard ID\": \"vsim-good\"");
    expectContains(reply, "\"VSIM\": { \"marker\": \"vsim-good\" }");
    expectAbsent(reply, "bad-id");
    expectAbsent(reply, "\"SEARCH\":");

    // A profile map must have string/status keys and a value at every key position.
    const std::string invalidKeyReply =
        protocol == 3
            ? map({bulk("Shards"),
                   array({map({":9\r\n", bulk("bad-value"), bulk("marker"), bulk("bad-marker")})})})
            : array({array({}), array({array({":9\r\n", bulk("bad-value"), bulk("marker"),
                                              bulk("bad-marker")})})});
    ProfileHarness invalidKey(protocol);
    invalidKey.add(true, invalidKeyReply);
    invalidKey.add(false, protocol, "key-fallback-id", "key-fallback-value");
    const std::string invalidKeyOutput = invalidKey.print(protocol);
    expectContains(invalidKeyOutput, "\"Shard ID\": \"key-fallback-id\"");
    expectContains(invalidKeyOutput, "\"VSIM\": { \"marker\": \"key-fallback-value\" }");
    expectAbsent(invalidKeyOutput, "\"SEARCH\":");

    // A null pointer in a map value slot is malformed even when the element count is even.
    MRReply *missingValue = parse(
        protocol == 3 ? map({bulk("Shards"), array({profile(protocol, "bad-id", "bad-value")})})
                      : array({array({}), array({profile(protocol, "bad-id", "bad-value")})}));
    ASSERT_NE(missingValue, nullptr);
    MRReply *missingValueProfile =
        protocol == 3 ? MRReply_ArrayElement(MRReply_MapElement(missingValue, "Shards"), 0)
                      : MRReply_ArrayElement(MRReply_ArrayElement(missingValue, 1), 0);
    MRReply_Free(missingValueProfile->element[3]);
    missingValueProfile->element[3] = nullptr;
    ProfileHarness nullValue(protocol);
    nullValue.add(true, missingValue);
    nullValue.add(false, protocol, "value-fallback-id", "value-fallback-value");
    const std::string nullValueOutput = nullValue.print(protocol);
    expectContains(nullValueOutput, "\"Shard ID\": \"value-fallback-id\"");
    expectContains(nullValueOutput, "\"VSIM\": { \"marker\": \"value-fallback-value\" }");
    expectAbsent(nullValueOutput, "\"SEARCH\":");
  }
}

TEST(HybridShardProfile, BothMalformedProfilesSkipTheShardSlot) {
  for (int protocol : {2, 3}) {
    ProfileHarness harness(protocol);
    harness.add(true,
                protocol == 3 ? map({bulk("Shards"), array({})}) : array({array({}), array({})}));
    harness.add(false,
                protocol == 3 ? map({bulk("Shards"), array({})}) : array({array({}), array({})}));
    EXPECT_EQ(harness.print(protocol), "[  ]");
  }
}
#endif  // ENABLE_ASSERT
