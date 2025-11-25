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
#include "rmutil/alloc.h"
#include "rmutil/rm_assert.h"
#include "redismodule.h"

#include "rmr/redise.h"

#include <vector>
#include <string>

using namespace RMCK;

// Helper class to manage test setup and teardown
class ClusterSetTest : public ::testing::Test {
protected:
    RedisModuleCtx *ctx = nullptr;

    void SetUp() override {
        ctx = RedisModule_GetThreadSafeContext(NULL);
    }

    void TearDown() override {
        if (ctx) {
            RedisModule_FreeThreadSafeContext(ctx);
        }
    }

    // Helper to verify a shard's slot ranges
    bool VerifySlotRanges(const MRClusterShard& shard,
                         const std::vector<std::pair<uint16_t, uint16_t>>& expected) {
        if (shard.slotRanges->num_ranges != expected.size()) {
            return false;
        }
        for (size_t i = 0; i < expected.size(); i++) {
            if (shard.slotRanges->ranges[i].start != expected[i].first ||
                shard.slotRanges->ranges[i].end != expected[i].second) {
                return false;
            }
        }
        return true;
    }
};

// ============================================================================
// Test with single range per shard, no replicas
// ============================================================================

TEST_F(ClusterSetTest, BasicTopologyParsing_SingleRangePerShard) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "HASHFUNC", "CRC16",
        "NUMSLOTS", "16384",
        "RANGES", "3",
        "SHARD", "shard1", "SLOTRANGE", "0", "5460", "ADDR", "127.0.0.1:6379", "MASTER",
        "SHARD", "shard2", "SLOTRANGE", "5461", "10922", "ADDR", "127.0.0.2:6379", "MASTER",
        "SHARD", "shard3", "SLOTRANGE", "10923", "16383", "ADDR", "127.0.0.3:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr) << "Topology parsing should succeed";
    EXPECT_EQ(topo->numShards, 3) << "Should have 3 shards";
    EXPECT_NE(my_shard_idx, UINT32_MAX) << "Should find my shard";

    // Verify my shard
    EXPECT_STREQ(topo->shards[my_shard_idx].node.id, "shard1");

    // Verify all shards have correct slot ranges
    bool found_shard1 = false, found_shard2 = false, found_shard3 = false;
    for (uint32_t i = 0; i < topo->numShards; i++) {
        if (strcmp(topo->shards[i].node.id, "shard1") == 0) {
            found_shard1 = true;
            EXPECT_TRUE(VerifySlotRanges(topo->shards[i], {{0, 5460}}));
            EXPECT_STREQ(topo->shards[i].node.endpoint.host, "127.0.0.1");
            EXPECT_EQ(topo->shards[i].node.endpoint.port, 6379);
        } else if (strcmp(topo->shards[i].node.id, "shard2") == 0) {
            found_shard2 = true;
            EXPECT_TRUE(VerifySlotRanges(topo->shards[i], {{5461, 10922}}));
            EXPECT_STREQ(topo->shards[i].node.endpoint.host, "127.0.0.2");
        } else if (strcmp(topo->shards[i].node.id, "shard3") == 0) {
            found_shard3 = true;
            EXPECT_TRUE(VerifySlotRanges(topo->shards[i], {{10923, 16383}}));
            EXPECT_STREQ(topo->shards[i].node.endpoint.host, "127.0.0.3");
        }
    }
    EXPECT_TRUE(found_shard1 && found_shard2 && found_shard3) << "All shards should be present";

    MRClusterTopology_Free(topo);
}

TEST_F(ClusterSetTest, SingleShardFullRange) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "only shard",
        "RANGES", "1",
        "SHARD", "only shard", "SLOTRANGE", "0", "16383", "ADDR", "localhost:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_EQ(topo->numShards, 1);
    EXPECT_EQ(my_shard_idx, 0);
    EXPECT_TRUE(VerifySlotRanges(topo->shards[0], {{0, 16383}}));
    EXPECT_STREQ(topo->shards[0].node.endpoint.host, "localhost");
    EXPECT_EQ(topo->shards[0].node.endpoint.port, 6379);

    MRClusterTopology_Free(topo);
}

TEST_F(ClusterSetTest, WithUnixSocket) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383",
        "ADDR", "127.0.0.1:6379", "UNIXADDR", "/tmp/redis.sock", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_EQ(topo->numShards, 1);
    EXPECT_STREQ(topo->shards[0].node.endpoint.host, "127.0.0.1");
    EXPECT_STREQ(topo->shards[0].node.endpoint.unixSock, "/tmp/redis.sock");

    MRClusterTopology_Free(topo);
}

// ============================================================================
// Test with multiple ranges per shard
// ============================================================================

TEST_F(ClusterSetTest, MultipleRangesPerShard_TwoRanges) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "4",
        "SHARD", "shard1", "SLOTRANGE", "0", "1000", "ADDR", "127.0.0.1:6379", "MASTER",
        "SHARD", "shard1", "SLOTRANGE", "8000", "9000", "MASTER",  // Second range for shard1
        "SHARD", "shard2", "SLOTRANGE", "1001", "7999", "ADDR", "127.0.0.2:6379", "MASTER",
        "SHARD", "shard2", "SLOTRANGE", "9001", "16383", "MASTER"  // Second range for shard2
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_EQ(topo->numShards, 2) << "Should have 2 shards";

    // Find shards and verify ranges
    bool found_shard1 = false, found_shard2 = false;
    for (uint32_t i = 0; i < topo->numShards; i++) {
        if (strcmp(topo->shards[i].node.id, "shard1") == 0) {
            found_shard1 = true;
            EXPECT_TRUE(VerifySlotRanges(topo->shards[i], {{0, 1000}, {8000, 9000}}))
                << "Shard1 should have two ranges";
        } else if (strcmp(topo->shards[i].node.id, "shard2") == 0) {
            found_shard2 = true;
            EXPECT_TRUE(VerifySlotRanges(topo->shards[i], {{1001, 7999}, {9001, 16383}}))
                << "Shard2 should have two ranges";
        }
    }
    EXPECT_TRUE(found_shard1 && found_shard2);

    MRClusterTopology_Free(topo);
}

TEST_F(ClusterSetTest, MultipleRangesPerShard_ThreeRanges) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "multi shard",
        "RANGES", "3",
        "SHARD", "multi shard", "SLOTRANGE", "0", "100", "ADDR", "127.0.0.1:6379", "MASTER",
        "SHARD", "multi shard", "SLOTRANGE", "500", "600", "MASTER",
        "SHARD", "multi shard", "SLOTRANGE", "1000", "1100", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_EQ(topo->numShards, 1);
    EXPECT_TRUE(VerifySlotRanges(topo->shards[0], {{0, 100}, {500, 600}, {1000, 1100}}))
        << "Shard should have three ranges";

    MRClusterTopology_Free(topo);
}

TEST_F(ClusterSetTest, MultipleRangesPerShard_MixedConfiguration) {
    // Mix of shards with single and multiple ranges
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard2",
        "RANGES", "5",
        "SHARD", "shard1", "SLOTRANGE", "0", "5000", "ADDR", "127.0.0.1:6379", "MASTER",  // Single range
        "SHARD", "shard2", "SLOTRANGE", "5001", "7000", "ADDR", "127.0.0.2:6379", "MASTER",  // Multiple ranges
        "SHARD", "shard2", "SLOTRANGE", "8000", "9000", "ADDR", "127.0.0.2:6379", "MASTER",
        "SHARD", "shard2", "SLOTRANGE", "10000", "11000", "MASTER",
        "SHARD", "shard3", "SLOTRANGE", "11001", "16383", "ADDR", "127.0.0.3:6379", "MASTER"  // Single range
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_EQ(topo->numShards, 3);

    for (uint32_t i = 0; i < topo->numShards; i++) {
        if (strcmp(topo->shards[i].node.id, "shard1") == 0) {
            EXPECT_EQ(topo->shards[i].slotRanges->num_ranges, 1);
        } else if (strcmp(topo->shards[i].node.id, "shard2") == 0) {
            EXPECT_EQ(topo->shards[i].slotRanges->num_ranges, 3);
            EXPECT_TRUE(VerifySlotRanges(topo->shards[i], {{5001, 7000}, {8000, 9000}, {10000, 11000}}));
        } else if (strcmp(topo->shards[i].node.id, "shard3") == 0) {
            EXPECT_EQ(topo->shards[i].slotRanges->num_ranges, 1);
        }
    }

    MRClusterTopology_Free(topo);
}

// ============================================================================
// Test with replicas (should be ignored)
// ============================================================================

TEST_F(ClusterSetTest, WithReplicas_ReplicasIgnored) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "master1",
        "RANGES", "4",
        "SHARD", "master1", "SLOTRANGE", "0", "8191", "ADDR", "127.0.0.1:6379", "MASTER",
        "SHARD", "replica1", "SLOTRANGE", "0", "8191", "ADDR", "127.0.0.1:6380",  // No MASTER - replica
        "SHARD", "master2", "SLOTRANGE", "8192", "16383", "ADDR", "127.0.0.2:6379", "MASTER",
        "SHARD", "replica2", "SLOTRANGE", "8192", "16383", "ADDR", "127.0.0.2:6380"  // No MASTER - replica
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_EQ(topo->numShards, 2) << "Should only have 2 master shards, replicas ignored";

    // Verify only masters are present
    for (uint32_t i = 0; i < topo->numShards; i++) {
        EXPECT_TRUE(strcmp(topo->shards[i].node.id, "master1") == 0 ||
                   strcmp(topo->shards[i].node.id, "master2") == 0)
            << "Only master shards should be present";
    }

    MRClusterTopology_Free(topo);
}

TEST_F(ClusterSetTest, MultipleReplicasPerMaster) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "master1",
        "RANGES", "5",
        "SHARD", "master1", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6379", "MASTER",
        "SHARD", "replica1", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6380",
        "SHARD", "replica2", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6381",
        "SHARD", "replica3", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6382",
        "SHARD", "replica4", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6383"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_EQ(topo->numShards, 1) << "Should only have 1 master shard";
    EXPECT_STREQ(topo->shards[0].node.id, "master1");

    MRClusterTopology_Free(topo);
}

TEST_F(ClusterSetTest, ReplicasWithMultipleRanges) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "master1",
        "RANGES", "6",
        "SHARD", "master1", "SLOTRANGE", "0", "1000", "ADDR", "127.0.0.1:6379", "MASTER",
        "SHARD", "master1", "SLOTRANGE", "8000", "9000", "MASTER",
        "SHARD", "replica1", "SLOTRANGE", "0", "1000", "ADDR", "127.0.0.1:6380",
        "SHARD", "replica1", "SLOTRANGE", "8000", "9000", "ADDR", "127.0.0.1:6380",
        "SHARD", "master2", "SLOTRANGE", "1001", "16383", "ADDR", "127.0.0.2:6379", "MASTER",
        "SHARD", "replica2", "SLOTRANGE", "1001", "16383", "ADDR", "127.0.0.2:6380",
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_EQ(topo->numShards, 2) << "Should only have 2 master shards";

    // Verify master1 has multiple ranges
    for (uint32_t i = 0; i < topo->numShards; i++) {
        if (strcmp(topo->shards[i].node.id, "master1") == 0) {
            EXPECT_TRUE(VerifySlotRanges(topo->shards[i], {{0, 1000}, {8000, 9000}}));
        }
    }

    MRClusterTopology_Free(topo);
}

TEST_F(ClusterSetTest, MissingSLOTRANGE) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "2",
        "SHARD", "shard1", "SLOTRANGE", "0", "1000", "ADDR", "127.0.0.1:6379", "MASTER",
        "SHARD", "shard2", "ADDR", "127.0.0.1:6379", "MASTER"  // Missing SLOTRANGE - should be ignored
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_EQ(topo->numShards, 1) << "Should only have 1 valid shard";
    EXPECT_STREQ(topo->shards[0].node.id, "shard1");

    MRClusterTopology_Free(topo);

}

// ============================================================================
// Error path tests
// ============================================================================

TEST_F(ClusterSetTest, Error_MissingMYID) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Missing value for MYID at offset 2");

}

TEST_F(ClusterSetTest, Error_MissingRANGES) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Unexpected argument: `SHARD` at offset 2");

}

TEST_F(ClusterSetTest, Error_BadHashFunc) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "HASHFUNC", "INVALID",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Bad value for HASHFUNC: INVALID at offset 4");

}

TEST_F(ClusterSetTest, Error_NumSlotsTooLarge) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "NUMSLOTS", "20000",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Bad value for NUMSLOTS: 20000 at offset 4");

}

TEST_F(ClusterSetTest, Error_TooFewRanges) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "0",
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Bad value for RANGES: 0 at offset 3");

}

TEST_F(ClusterSetTest, Error_TooFewRangesGiven) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "2",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Expected `SHARD` but got `(nil)` at offset 12");

}

TEST_F(ClusterSetTest, Error_TooManyRangesGiven) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "8000", "ADDR", "127.0.0.1:6379", "MASTER",
        "SHARD", "shard2", "SLOTRANGE", "8001", "16383", "ADDR", "127.0.0.2:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Expected end of command but got `SHARD` at offset 12");

}

TEST_F(ClusterSetTest, Error_InvalidSlotRange_StartGreaterThanEnd) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "1000", "500", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Bad values for SLOTRANGE: 1000, 500 at offset 9");

}

TEST_F(ClusterSetTest, Error_InvalidSlotRange_EndTooLarge) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16384", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Bad value for SLOTRANGE end: 16384 at offset 8");

}

TEST_F(ClusterSetTest, Error_InvalidSlotRange_EndTooLargeCustomNumSlots) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "NUMSLOTS", "10000",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "10000", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Bad value for SLOTRANGE end: 10000 at offset 10");

}

TEST_F(ClusterSetTest, Error_MissingADDR) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383", "MASTER"  // Missing ADDR
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Missing value for ADDR at offset 10");

}

TEST_F(ClusterSetTest, Error_InvalidADDR) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383", "ADDR", "invalid_address", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Bad value for ADDR: invalid_address at offset 11");

}

TEST_F(ClusterSetTest, Error_MultipleADDR) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "2",
        "SHARD", "shard1", "SLOTRANGE", "0", "8000", "ADDR", "127.0.0.1:6379", "MASTER",
        "SHARD", "shard1", "SLOTRANGE", "8001", "16383", "ADDR", "127.0.0.2:6379", "MASTER"  // Different ADDR for same shard
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Conflicting ADDR for shard `shard1` at offset 20");

}

TEST_F(ClusterSetTest, Error_MultipleUNIXADDR) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "2",
        "SHARD", "shard1", "SLOTRANGE", "0", "8000",
        "ADDR", "127.0.0.1:6379", "UNIXADDR", "/tmp/redis1.sock", "MASTER",
        "SHARD", "shard1", "SLOTRANGE", "8001", "16383",
        "UNIXADDR", "/tmp/redis2.sock", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Conflicting UNIXADDR for shard `shard1` at offset 22");

}

TEST_F(ClusterSetTest, Error_MYIDNotFound) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "nonexistent",
        "RANGES", "2",
        "SHARD", "shard1", "SLOTRANGE", "0", "8191", "ADDR", "127.0.0.1:6379", "MASTER",
        "SHARD", "shard2", "SLOTRANGE", "8192", "16383", "ADDR", "127.0.0.2:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "MYID `nonexistent` does not correspond to any shard at offset 20");

}

TEST_F(ClusterSetTest, Error_UnexpectedArgument) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "UNEXPECTED", "value",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Unexpected argument: `UNEXPECTED` at offset 2");

}

TEST_F(ClusterSetTest, Error_MissingSHARD) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6379", "MASTER"  // Missing SHARD keyword
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Expected `SHARD` but got `SLOTRANGE` at offset 5");

}

TEST_F(ClusterSetTest, Error_IncompleteSLOTRANGE_MissingEnd) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Bad value for SLOTRANGE end: ADDR at offset 8");

}

TEST_F(ClusterSetTest, Error_RANGESCountMismatch_TooFew) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "3",  // Declares 3 but only provides 1
        "SHARD", "shard1", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Expected `SHARD` but got `(nil)` at offset 12");

}

TEST_F(ClusterSetTest, Error_ExtraArgumentsAfterRanges) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6379", "MASTER",
        "EXTRA", "argument"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Expected end of command but got `EXTRA` at offset 12");

}

TEST_F(ClusterSetTest, Error_ZeroRANGES) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "0"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Bad value for RANGES: 0 at offset 3");

}

TEST_F(ClusterSetTest, Error_MissingADDRValue) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383", "ADDR", "MASTER"  // ADDR without value
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Bad value for ADDR: MASTER at offset 11");

}

TEST_F(ClusterSetTest, Error_MissingUNIXADDRValue) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383",
        "ADDR", "127.0.0.1:6379", "UNIXADDR", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "MYID `shard1` does not correspond to any shard at offset 13");

}

TEST_F(ClusterSetTest, Error_MultipleSLOTRANGE_SameBlock) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "100",
        "SLOTRANGE", "200", "300",
        "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Multiple SLOTRANGE specified for shard `shard1` at offset 10");

}

TEST_F(ClusterSetTest, Error_MultipleADDR_SameBlock) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "100",
        "ADDR", "127.0.0.1:6379",
        "ADDR", "127.0.0.1:6380",
        "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Multiple ADDR specified for shard `shard1` at offset 13");

}

TEST_F(ClusterSetTest, Error_MultipleUNIXADDR_SameBlock) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "100",
        "ADDR", "127.0.0.1:6379",
        "UNIXADDR", "/tmp/1",
        "UNIXADDR", "/tmp/2",
        "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Multiple UNIXADDR specified for shard `shard1` at offset 15");

}

TEST_F(ClusterSetTest, Error_ConflictingADDR_Password) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "2",
        "SHARD", "shard1", "SLOTRANGE", "0", "100", "ADDR", "user:pass1@127.0.0.1:6379", "MASTER",
        "SHARD", "shard1", "SLOTRANGE", "200", "300", "ADDR", "user:pass2@127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Conflicting ADDR for shard `shard1` at offset 20");

}

TEST_F(ClusterSetTest, Error_ConflictingADDR_Port) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "2",
        "SHARD", "shard1", "SLOTRANGE", "0", "100", "ADDR", "127.0.0.1:6379", "MASTER",
        "SHARD", "shard1", "SLOTRANGE", "200", "300", "ADDR", "127.0.0.1:6380", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "Conflicting ADDR for shard `shard1` at offset 20");

}

TEST_F(ClusterSetTest, Error_SLOTRANGE_OutOfOrder) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "2",
        "SHARD", "shard1", "SLOTRANGE", "0", "100", "ADDR", "127.0.0.1:6379", "MASTER",
        "SHARD", "shard1", "SLOTRANGE", "50", "150", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    EXPECT_EQ(topo, nullptr);
    EXPECT_EQ(RMCK_GetLastError(ctx), "SLOTRANGE out of order for shard `shard1` at offset 18");

}

// ============================================================================
// Edge case tests
// ============================================================================

TEST_F(ClusterSetTest, EdgeCase_SingleSlotRange) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "100", "100", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_TRUE(VerifySlotRanges(topo->shards[0], {{100, 100}}))
        << "Should support single-slot ranges";

    MRClusterTopology_Free(topo);
}

TEST_F(ClusterSetTest, EdgeCase_CRC12HashFunc) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "HASHFUNC", "CRC12",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr) << "Should accept CRC12 as valid hash function";

    MRClusterTopology_Free(topo);
}

TEST_F(ClusterSetTest, EdgeCase_CustomNUMSLOTS) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "NUMSLOTS", "8192",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "8191", "ADDR", "127.0.0.1:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_TRUE(VerifySlotRanges(topo->shards[0], {{0, 8191}}));

    MRClusterTopology_Free(topo);
}

TEST_F(ClusterSetTest, EdgeCase_HostnameWithDomain) {
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard1",
        "RANGES", "1",
        "SHARD", "shard1", "SLOTRANGE", "0", "16383",
        "ADDR", "redis-node.example.com:6379", "MASTER"
    };

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_STREQ(topo->shards[0].node.endpoint.host, "redis-node.example.com");
    EXPECT_EQ(topo->shards[0].node.endpoint.port, 6379);

    MRClusterTopology_Free(topo);
}

TEST_F(ClusterSetTest, EdgeCase_ManyShards) {
    // Test with 10 shards
    std::vector<std::string> args = {
        "search.CLUSTERSET",
        "MYID", "shard5",
        "RANGES", "10"
    };

    uint16_t range_size = 16384 / 10;
    for (int i = 0; i < 10; i++) {
        uint16_t start = i * range_size;
        uint16_t end = (i == 9) ? 16383 : (i + 1) * range_size - 1;
        args.push_back("SHARD");
        args.push_back("shard" + std::to_string(i + 1));
        args.push_back("SLOTRANGE");
        args.push_back(std::to_string(start));
        args.push_back(std::to_string(end));
        args.push_back("ADDR");
        args.push_back("127.0.0." + std::to_string(i + 1) + ":6379");
        args.push_back("MASTER");
    }

    ArgvList argv(ctx, args);
    uint32_t my_shard_idx = UINT32_MAX;
    MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argv.size(), &my_shard_idx);

    ASSERT_NE(topo, nullptr);
    EXPECT_EQ(topo->numShards, 10);
    EXPECT_NE(my_shard_idx, UINT32_MAX);

    MRClusterTopology_Free(topo);
}
