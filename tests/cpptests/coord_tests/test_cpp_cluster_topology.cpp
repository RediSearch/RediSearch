/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "cluster_topology.h"
#include "endpoint.h"
#include "rmalloc.h"
#include "rmutil/alloc.h"
#include "redismodule.h"

#include <array>
#include <cstdint>
#include <initializer_list>
#include <span>

namespace {

static RedisModuleSlotRangeArray *makeSlotRanges(std::initializer_list<std::pair<uint16_t, uint16_t>> ranges) {
  size_t total = sizeof(RedisModuleSlotRangeArray) + ranges.size() * sizeof(RedisModuleSlotRange);
  auto *arr = static_cast<RedisModuleSlotRangeArray *>(rm_malloc(total));
  arr->num_ranges = static_cast<int32_t>(ranges.size());
  int32_t i = 0;
  for (const auto &r : ranges) {
    arr->ranges[i].start = r.first;
    arr->ranges[i].end = r.second;
    i++;
  }
  return arr;
}

static MRClusterTopology *makeTopology(std::span<const char *const> hosts,
                                       std::initializer_list<std::pair<uint16_t, uint16_t>> slots = {}) {
  auto *topo = static_cast<MRClusterTopology *>(rm_new(MRClusterTopology));
  topo->numShards = hosts.size();
  topo->capShards = hosts.size();
  topo->shards = static_cast<MRClusterShard *>(rm_calloc(hosts.size(), sizeof(MRClusterShard)));
  for (size_t i = 0; i < hosts.size(); i++) {
    MRClusterNode *node = &topo->shards[i].node;
    int rc = MREndpoint_Parse(hosts[i], &node->endpoint);
    (void)rc;
    node->id = rm_strdup(hosts[i]);
    topo->shards[i].slotRanges = makeSlotRanges(slots);
  }
  return topo;
}

class MRClusterTopologyConnectivityEqualTest : public ::testing::Test {};

TEST_F(MRClusterTopologyConnectivityEqualTest, SamePointerIsEqual) {
  std::array<const char *, 2> hosts = {"h1:6379", "h2:6379"};
  MRClusterTopology *t = makeTopology(hosts);
  EXPECT_TRUE(MRClusterTopology_ConnectivityEqual(t, t));
  MRClusterTopology_Free(t);
}

TEST_F(MRClusterTopologyConnectivityEqualTest, NullVsNonNullIsNotEqual) {
  std::array<const char *, 1> hosts = {"h1:6379"};
  MRClusterTopology *t = makeTopology(hosts);
  EXPECT_FALSE(MRClusterTopology_ConnectivityEqual(nullptr, t));
  EXPECT_FALSE(MRClusterTopology_ConnectivityEqual(t, nullptr));
  MRClusterTopology_Free(t);
}

TEST_F(MRClusterTopologyConnectivityEqualTest, BothNullAreEqual) {
  // Two absent topologies are trivially equal, allowing callers to short-circuit
  // a NULL -> NULL "update" without a spurious refresh.
  EXPECT_TRUE(MRClusterTopology_ConnectivityEqual(nullptr, nullptr));
}

TEST_F(MRClusterTopologyConnectivityEqualTest, EmptyTopologiesAreEqual) {
  // With no shards, there is no connectivity to differ on.
  MRClusterTopology *a = static_cast<MRClusterTopology *>(rm_new(MRClusterTopology));
  a->numShards = 0; a->capShards = 0; a->shards = nullptr;
  MRClusterTopology *b = static_cast<MRClusterTopology *>(rm_new(MRClusterTopology));
  b->numShards = 0; b->capShards = 0; b->shards = nullptr;
  EXPECT_TRUE(MRClusterTopology_ConnectivityEqual(a, b));
  MRClusterTopology_Free(a);
  MRClusterTopology_Free(b);
}

TEST_F(MRClusterTopologyConnectivityEqualTest, DifferentShardCountIsNotEqual) {
  std::array<const char *, 2> hosts_a = {"h1:6379", "h2:6379"};
  std::array<const char *, 3> hosts_b = {"h1:6379", "h2:6379", "h3:6379"};
  MRClusterTopology *a = makeTopology(hosts_a);
  MRClusterTopology *b = makeTopology(hosts_b);
  EXPECT_FALSE(MRClusterTopology_ConnectivityEqual(a, b));
  MRClusterTopology_Free(a);
  MRClusterTopology_Free(b);
}

TEST_F(MRClusterTopologyConnectivityEqualTest, SameContentIsEqual) {
  std::array<const char *, 3> hosts = {"h1:6379", "h2:6379", "h3:6379"};
  MRClusterTopology *a = makeTopology(hosts, {{0, 5000}, {5001, 10000}});
  MRClusterTopology *b = makeTopology(hosts, {{0, 5000}, {5001, 10000}});
  EXPECT_TRUE(MRClusterTopology_ConnectivityEqual(a, b));
  MRClusterTopology_Free(a);
  MRClusterTopology_Free(b);
}

TEST_F(MRClusterTopologyConnectivityEqualTest, SameContentDifferentShardOrderIsEqual) {
  std::array<const char *, 3> hosts_a = {"h1:6379", "h2:6379", "h3:6379"};
  std::array<const char *, 3> hosts_b = {"h3:6379", "h1:6379", "h2:6379"};
  MRClusterTopology *a = makeTopology(hosts_a, {{0, 100}});
  MRClusterTopology *b = makeTopology(hosts_b, {{0, 100}});
  EXPECT_TRUE(MRClusterTopology_ConnectivityEqual(a, b));
  MRClusterTopology_Free(a);
  MRClusterTopology_Free(b);
}

TEST_F(MRClusterTopologyConnectivityEqualTest, DifferentHostForSameNodeIdIsNotEqual) {
  std::array<const char *, 2> hosts_a = {"h1:6379", "h2:6379"};
  MRClusterTopology *a = makeTopology(hosts_a);
  MRClusterTopology *b = makeTopology(hosts_a);
  // Mutate host of first shard of b
  rm_free(b->shards[0].node.endpoint.host);
  b->shards[0].node.endpoint.host = rm_strdup("other-host");
  EXPECT_FALSE(MRClusterTopology_ConnectivityEqual(a, b));
  MRClusterTopology_Free(a);
  MRClusterTopology_Free(b);
}

TEST_F(MRClusterTopologyConnectivityEqualTest, DifferentPortForSameNodeIdIsNotEqual) {
  std::array<const char *, 2> hosts_a = {"h1:6379", "h2:6379"};
  MRClusterTopology *a = makeTopology(hosts_a);
  MRClusterTopology *b = makeTopology(hosts_a);
  b->shards[0].node.endpoint.port = 7000;
  EXPECT_FALSE(MRClusterTopology_ConnectivityEqual(a, b));
  MRClusterTopology_Free(a);
  MRClusterTopology_Free(b);
}

TEST_F(MRClusterTopologyConnectivityEqualTest, DifferentNodeIdsIsNotEqual) {
  std::array<const char *, 2> hosts_a = {"h1:6379", "h2:6379"};
  std::array<const char *, 2> hosts_b = {"h1:6379", "h3:6379"};
  MRClusterTopology *a = makeTopology(hosts_a);
  MRClusterTopology *b = makeTopology(hosts_b);
  EXPECT_FALSE(MRClusterTopology_ConnectivityEqual(a, b));
  MRClusterTopology_Free(a);
  MRClusterTopology_Free(b);
}

TEST_F(MRClusterTopologyConnectivityEqualTest, DifferentSlotRangesIsStillEqual) {
  // Slot range differences do not affect connectivity equality: the routing
  // topology pointer still needs to be refreshed by the caller, but the
  // connection validation handshake can be skipped.
  std::array<const char *, 1> hosts = {"h1:6379"};
  MRClusterTopology *a = makeTopology(hosts, {{0, 100}});
  MRClusterTopology *b = makeTopology(hosts, {{0, 200}});
  EXPECT_TRUE(MRClusterTopology_ConnectivityEqual(a, b));
  MRClusterTopology_Free(a);
  MRClusterTopology_Free(b);
}

}  // namespace
