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
#include "redismock/util.h"
#include "redismodule.h"

#include "rmr/cluster_topology.h"

#include <climits>
#include <cstring>
#include <string>
#include <vector>

namespace {

// Real Redis node IDs are 40 chars. Use distinct, padded IDs in tests so that
// callers that copy exactly REDISMODULE_NODE_ID_LEN bytes don't get truncated
// content.
constexpr const char *NODE_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
constexpr const char *NODE_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
constexpr const char *NODE_C = "cccccccccccccccccccccccccccccccccccccccc";

inline void addNode(const char *id, const char *ip, int port, int flags,
                    const std::vector<RedisModuleSlotRange> &slots = {}) {
  RMCK_ClusterMock_AddNode(id, ip, port, flags, slots);
}

int findShardByNodeId(const MRClusterTopology *topo, const char *id) {
  for (uint32_t i = 0; i < topo->numShards; i++) {
    if (std::strcmp(topo->shards[i].node.id, id) == 0) return static_cast<int>(i);
  }
  return -1;
}

}  // namespace

class ClusterTopologyFromAPITest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx = nullptr;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    RMCK_ClusterMock_Reset();
  }

  void TearDown() override {
    RMCK_ClusterMock_Reset();
    if (ctx) RedisModule_FreeThreadSafeContext(ctx);
  }
};

// ============================================================================
// Happy path: three masters with one slot range each, local node is shard A.
// ============================================================================
TEST_F(ClusterTopologyFromAPITest, ThreeMasters_LocalIsMaster) {
  addNode(NODE_A, "127.0.0.1", 6379, REDISMODULE_NODE_MASTER | REDISMODULE_NODE_MYSELF,
          {{0, 5460}});
  addNode(NODE_B, "127.0.0.2", 6380, REDISMODULE_NODE_MASTER, {{5461, 10922}});
  addNode(NODE_C, "127.0.0.3", 6381, REDISMODULE_NODE_MASTER, {{10923, 16383}});

  uint32_t my_shard_idx = UINT32_MAX;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, "hunter2", 7, &my_shard_idx);
  ASSERT_NE(topo, nullptr);
  EXPECT_EQ(topo->numShards, 3u);
  ASSERT_NE(my_shard_idx, UINT32_MAX);
  EXPECT_STREQ(topo->shards[my_shard_idx].node.id, NODE_A);

  // Verify each shard's content
  int ia = findShardByNodeId(topo, NODE_A);
  int ib = findShardByNodeId(topo, NODE_B);
  int ic = findShardByNodeId(topo, NODE_C);
  ASSERT_GE(ia, 0);
  ASSERT_GE(ib, 0);
  ASSERT_GE(ic, 0);

  EXPECT_STREQ(topo->shards[ia].node.endpoint.host, "127.0.0.1");
  EXPECT_EQ(topo->shards[ia].node.endpoint.port, 6379);
  EXPECT_STREQ(topo->shards[ia].node.endpoint.password, "hunter2");
  ASSERT_EQ(topo->shards[ia].slotRanges->num_ranges, 1);
  EXPECT_EQ(topo->shards[ia].slotRanges->ranges[0].start, 0);
  EXPECT_EQ(topo->shards[ia].slotRanges->ranges[0].end, 5460);

  EXPECT_STREQ(topo->shards[ib].node.endpoint.host, "127.0.0.2");
  EXPECT_EQ(topo->shards[ib].node.endpoint.port, 6380);
  EXPECT_STREQ(topo->shards[ic].node.endpoint.host, "127.0.0.3");
  EXPECT_EQ(topo->shards[ic].node.endpoint.port, 6381);

  MRClusterTopology_Free(topo);
}

// ============================================================================
// Auth NULL/empty should leave the password unset on every shard.
// ============================================================================
TEST_F(ClusterTopologyFromAPITest, NoAuth_PasswordIsNull) {
  addNode(NODE_A, "127.0.0.1", 6379, REDISMODULE_NODE_MASTER | REDISMODULE_NODE_MYSELF,
          {{0, 16383}});

  uint32_t my_shard_idx = UINT32_MAX;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, nullptr, 0, &my_shard_idx);
  ASSERT_NE(topo, nullptr);
  ASSERT_EQ(topo->numShards, 1u);
  EXPECT_EQ(topo->shards[0].node.endpoint.password, nullptr);

  MRClusterTopology_Free(topo);
}

TEST_F(ClusterTopologyFromAPITest, EmptyAuthString_PasswordIsNull) {
  addNode(NODE_A, "127.0.0.1", 6379, REDISMODULE_NODE_MASTER | REDISMODULE_NODE_MYSELF,
          {{0, 16383}});

  uint32_t my_shard_idx = UINT32_MAX;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, "", 0, &my_shard_idx);
  ASSERT_NE(topo, nullptr);
  EXPECT_EQ(topo->shards[0].node.endpoint.password, nullptr);

  MRClusterTopology_Free(topo);
}

// ============================================================================
// Replicas (without REDISMODULE_NODE_MASTER) must be filtered out.
// ============================================================================
TEST_F(ClusterTopologyFromAPITest, ReplicaFilteredOut) {
  addNode(NODE_A, "127.0.0.1", 6379, REDISMODULE_NODE_MASTER | REDISMODULE_NODE_MYSELF,
          {{0, 16383}});
  // Replica of A — slot ranges may still appear on it via the API, but it
  // must be skipped because the MASTER flag is absent.
  addNode(NODE_B, "127.0.0.2", 6380, REDISMODULE_NODE_SLAVE, {{0, 16383}});

  uint32_t my_shard_idx = UINT32_MAX;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, nullptr, 0, &my_shard_idx);
  ASSERT_NE(topo, nullptr);
  EXPECT_EQ(topo->numShards, 1u);
  EXPECT_STREQ(topo->shards[0].node.id, NODE_A);

  MRClusterTopology_Free(topo);
}

// ============================================================================
// A master without slots (e.g., newly added node) is not part of the topology.
// ============================================================================
TEST_F(ClusterTopologyFromAPITest, SlotlessMasterFilteredOut) {
  addNode(NODE_A, "127.0.0.1", 6379, REDISMODULE_NODE_MASTER | REDISMODULE_NODE_MYSELF,
          {{0, 16383}});
  addNode(NODE_B, "127.0.0.2", 6380, REDISMODULE_NODE_MASTER, /* no slots */ {});

  uint32_t my_shard_idx = UINT32_MAX;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, nullptr, 0, &my_shard_idx);
  ASSERT_NE(topo, nullptr);
  EXPECT_EQ(topo->numShards, 1u);
  EXPECT_STREQ(topo->shards[0].node.id, NODE_A);

  MRClusterTopology_Free(topo);
}

// ============================================================================
// Nodes with bad endpoints (port=0, missing IP) are skipped.
// ============================================================================
TEST_F(ClusterTopologyFromAPITest, InvalidEndpointsSkipped) {
  addNode(NODE_A, "127.0.0.1", 6379, REDISMODULE_NODE_MASTER | REDISMODULE_NODE_MYSELF,
          {{0, 5460}});
  addNode(NODE_B, "127.0.0.2", 0 /* invalid port */, REDISMODULE_NODE_MASTER, {{5461, 10922}});
  addNode(NODE_C, "", 6381 /* missing host */, REDISMODULE_NODE_MASTER, {{10923, 16383}});

  uint32_t my_shard_idx = UINT32_MAX;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, nullptr, 0, &my_shard_idx);
  ASSERT_NE(topo, nullptr);
  EXPECT_EQ(topo->numShards, 1u);
  EXPECT_STREQ(topo->shards[0].node.id, NODE_A);

  MRClusterTopology_Free(topo);
}

// ============================================================================
// Local node is a replica → my_shard_idx stays UINT32_MAX, NOT an error.
// ============================================================================
TEST_F(ClusterTopologyFromAPITest, LocalIsReplica_NotAnError) {
  addNode(NODE_A, "127.0.0.1", 6379, REDISMODULE_NODE_MASTER, {{0, 16383}});
  addNode(NODE_B, "127.0.0.2", 6380, REDISMODULE_NODE_SLAVE | REDISMODULE_NODE_MYSELF, {});

  uint32_t my_shard_idx = 12345;  // start non-default to verify it gets reset
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, nullptr, 0, &my_shard_idx);
  ASSERT_NE(topo, nullptr);
  EXPECT_EQ(topo->numShards, 1u);
  EXPECT_EQ(my_shard_idx, UINT32_MAX);

  MRClusterTopology_Free(topo);
}

// ========================================================================================
// Local node is a slot-less master → my_shard_idx stays UINT32_MAX (the node
// is "known" so it's not an error), matching the standard RedisEnterprise_ParseTopology.
// ========================================================================================
TEST_F(ClusterTopologyFromAPITest, LocalIsSlotlessMaster_NotAnError) {
  addNode(NODE_A, "127.0.0.1", 6379, REDISMODULE_NODE_MASTER, {{0, 16383}});
  addNode(NODE_B, "127.0.0.2", 6380, REDISMODULE_NODE_MASTER | REDISMODULE_NODE_MYSELF,
          /* no slots */ {});

  uint32_t my_shard_idx = 12345;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, nullptr, 0, &my_shard_idx);
  ASSERT_NE(topo, nullptr);
  EXPECT_EQ(topo->numShards, 1u);
  EXPECT_EQ(my_shard_idx, UINT32_MAX);

  MRClusterTopology_Free(topo);
}

// ============================================================================
// Empty cluster (no nodes returned) → NULL.
// ============================================================================
TEST_F(ClusterTopologyFromAPITest, EmptyCluster_ReturnsNull) {
  uint32_t my_shard_idx = 12345;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, nullptr, 0, &my_shard_idx);
  EXPECT_EQ(topo, nullptr);
  EXPECT_EQ(my_shard_idx, UINT32_MAX);
}

// ============================================================================
// No master shards with slots → NULL.
// ============================================================================
TEST_F(ClusterTopologyFromAPITest, NoValidShards_ReturnsNull) {
  // Local replica plus a slot-less master — nothing survives the filters.
  addNode(NODE_A, "127.0.0.1", 6379, REDISMODULE_NODE_MASTER, /* no slots */ {});
  addNode(NODE_B, "127.0.0.2", 6380, REDISMODULE_NODE_SLAVE | REDISMODULE_NODE_MYSELF, {});

  uint32_t my_shard_idx = UINT32_MAX;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, nullptr, 0, &my_shard_idx);
  EXPECT_EQ(topo, nullptr);
}

// ============================================================================
// MYSELF flag missing entirely → error (local node not in cluster).
// ============================================================================
TEST_F(ClusterTopologyFromAPITest, MyselfNotFound_ReturnsNull) {
  addNode(NODE_A, "127.0.0.1", 6379, REDISMODULE_NODE_MASTER, {{0, 16383}});

  uint32_t my_shard_idx = UINT32_MAX;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, nullptr, 0, &my_shard_idx);
  EXPECT_EQ(topo, nullptr);
}

// ============================================================================
// Multiple slot ranges per shard are preserved in order.
// ============================================================================
TEST_F(ClusterTopologyFromAPITest, MultipleSlotRangesPerShard) {
  addNode(NODE_A, "127.0.0.1", 6379, REDISMODULE_NODE_MASTER | REDISMODULE_NODE_MYSELF,
          {{0, 100}, {500, 1000}, {12000, 16383}});

  uint32_t my_shard_idx = UINT32_MAX;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, nullptr, 0, &my_shard_idx);
  ASSERT_NE(topo, nullptr);
  ASSERT_EQ(topo->numShards, 1u);
  ASSERT_EQ(topo->shards[0].slotRanges->num_ranges, 3);
  EXPECT_EQ(topo->shards[0].slotRanges->ranges[0].start, 0);
  EXPECT_EQ(topo->shards[0].slotRanges->ranges[0].end, 100);
  EXPECT_EQ(topo->shards[0].slotRanges->ranges[1].start, 500);
  EXPECT_EQ(topo->shards[0].slotRanges->ranges[1].end, 1000);
  EXPECT_EQ(topo->shards[0].slotRanges->ranges[2].start, 12000);
  EXPECT_EQ(topo->shards[0].slotRanges->ranges[2].end, 16383);

  MRClusterTopology_Free(topo);
}

// ============================================================================
// FromAPI frees each module-owned slot-range array right after cloning it for
// the topology, so the topology's data must come from the clone — not from a
// borrowed pointer that's already invalid by the time FromAPI returns.
// ============================================================================
TEST_F(ClusterTopologyFromAPITest, SlotRangesAreOwned) {
  addNode(NODE_A, "127.0.0.1", 6379, REDISMODULE_NODE_MASTER | REDISMODULE_NODE_MYSELF,
          {{0, 16383}});

  uint32_t my_shard_idx = UINT32_MAX;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, nullptr, 0, &my_shard_idx);
  ASSERT_NE(topo, nullptr);

  // Drop the mock node table — the topology's slot ranges must survive
  // because they were cloned out of the module-owned arrays (already freed
  // inside FromAPI), not borrowed from the mock's source state.
  RMCK_ClusterMock_Reset();

  ASSERT_EQ(topo->numShards, 1u);
  EXPECT_EQ(topo->shards[0].slotRanges->num_ranges, 1);
  EXPECT_EQ(topo->shards[0].slotRanges->ranges[0].start, 0);
  EXPECT_EQ(topo->shards[0].slotRanges->ranges[0].end, 16383);

  MRClusterTopology_Free(topo);
}
