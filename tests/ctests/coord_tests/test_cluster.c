/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "minunit.h"
#include "endpoint.h"
#include "command.h"
#include "cluster.h"
#include "crc16.h"
#include "crc12.h"

#include "hiredis/hiredis.h"
#include "rmutil/alloc.h"

struct RedisModuleString {
  char *str;
  size_t len;
  size_t refcount;
};

static RedisModuleString *Mock_CreateString(RedisModuleCtx *ctx, const char *ptr, size_t len) {
  RedisModuleString *str = rm_malloc(sizeof(*str));
  str->str = rm_strndup(ptr, len);
  str->len = len;
  str->refcount = 1;
  return str;
}

static RedisModuleString *Mock_CreateStringFromString(RedisModuleCtx *ctx, const RedisModuleString *str) {
  return Mock_CreateString(ctx, str->str, str->len);
}

static RedisModuleString *Mock_HoldString(RedisModuleCtx *ctx, RedisModuleString *str) {
  if (!str) return NULL;
  str->refcount++;
  return str;
}

static void Mock_FreeString(RedisModuleCtx *ctx, RedisModuleString *str) {
  if (!str) return;
  if (--str->refcount == 0) {
    rm_free(str->str);
    rm_free(str);
  }
}

static int Mock_StringCompare(const RedisModuleString *s1, const RedisModuleString *s2) {
  return strcmp(s1->str, s2->str);
}

static void Mock_TrimStringAllocation(RedisModuleString *str) {
  // No-op for the mock
}


void testEndpoint() {

  MREndpoint ep;

  mu_assert_int_eq(REDIS_OK, MREndpoint_Parse("localhost:6379", &ep));
  mu_check(!strcmp(ep.host, "localhost"));
  mu_assert_int_eq(6379, ep.port);
  MREndpoint_Free(&ep);

  // ipv6 tests
  mu_assert_int_eq(REDIS_OK, MREndpoint_Parse("::0:6379", &ep));
  mu_check(!strcmp(ep.host, "::0"));
  mu_assert_int_eq(6379, ep.port);
  MREndpoint_Free(&ep);

  mu_assert_int_eq(REDIS_OK, MREndpoint_Parse("[fe80::8749:8fe8:f206:2ab9]:6380", &ep));
  mu_check(!strcmp(ep.host, "fe80::8749:8fe8:f206:2ab9"));
  mu_assert_int_eq(6380, ep.port);
  MREndpoint_Free(&ep);

  mu_assert_int_eq(REDIS_OK, MREndpoint_Parse("pass@[fe80::8749:8fe8:f206:2ab9]:6380", &ep));
  mu_check(!strcmp(ep.host, "fe80::8749:8fe8:f206:2ab9"));
  mu_check(!strcmp(ep.password, "pass"));
  mu_assert_int_eq(6380, ep.port);
  MREndpoint_Free(&ep);

  mu_assert_int_eq(REDIS_ERR, MREndpoint_Parse("localhost", &ep));
  MREndpoint_Free(&ep);
  mu_assert_int_eq(REDIS_ERR, MREndpoint_Parse("[fe80::8749:8fe8:f206:2ab9]", &ep));
  MREndpoint_Free(&ep);
  mu_assert_int_eq(REDIS_ERR, MREndpoint_Parse("pass@[fe80::8749:8fe8:f206:2ab9]", &ep));
  MREndpoint_Free(&ep);
  mu_assert_int_eq(REDIS_ERR, MREndpoint_Parse("localhost:-1", &ep));
  MREndpoint_Free(&ep);
  mu_assert_int_eq(REDIS_ERR, MREndpoint_Parse("localhost:655350", &ep));
  MREndpoint_Free(&ep);
  mu_assert_int_eq(REDIS_ERR, MREndpoint_Parse("localhost:", &ep));
  MREndpoint_Free(&ep);
  mu_assert_int_eq(REDIS_ERR, MREndpoint_Parse(":-1", &ep));
  MREndpoint_Free(&ep);
}

static MRClusterTopology *getTopology(size_t numSlots, size_t numNodes,  const char **hosts){

  MRClusterTopology *topo = rm_malloc(sizeof(*topo));
  topo->hashFunc = MRHashFunc_CRC16;
  topo->numShards = numNodes;
  topo->numSlots = numSlots;
  topo->shards = rm_calloc(numNodes, sizeof(MRClusterShard));
  size_t slotRange = numSlots / numNodes;

  MRClusterNode nodes[numNodes];
  for (int i = 0; i < numNodes; i++) {
    if (REDIS_OK!=MREndpoint_Parse(hosts[i], &nodes[i].endpoint)) {
      return NULL;
    }
    nodes[i].flags = MRNode_Master;
    nodes[i].id = RedisModule_CreateString(NULL, hosts[i], strlen(hosts[i]));
  }
  int i = 0;
  for (size_t slot = 0; slot < topo->numSlots; slot += slotRange) {
    topo->shards[i] = (MRClusterShard){
      .numNodes = 1,
    };
    topo->shards[i].nodes = rm_calloc(1, sizeof(MRClusterNode)),
    topo->shards[i].nodes[0] = nodes[i];
    topo->shards[i].ranges = rm_calloc(1, sizeof(mr_slot_range_t));
    topo->shards[i].capRanges = 1;
    topo->shards[i].numRanges = 1;
    topo->shards[i].ranges[0].start = slot;
    topo->shards[i].ranges[0].end = slot + slotRange - 1;

    i++;
  }

  return topo;
}

static const char *GetShardKey(const MRCommand *cmd, size_t *len) {
  *len = cmd->lens[1];
  return cmd->strs[1];
}
static mr_slot_t CRCShardFunc(const MRCommand *cmd, const IORuntimeCtx *ioRuntime) {

  size_t len;
  const char *k = GetShardKey(cmd, &len);
  if (!k) return 0;
  // Default to crc16
  uint16_t crc = (ioRuntime->topo->hashFunc == MRHashFunc_CRC12) ? crc12(k, len) : crc16(k, len);
  return crc % ioRuntime->topo->numSlots;
}

void testShardingFunc() {

  MRCommand cmd = MR_NewCommand(2, "foo", "baz");
  const char *host = "localhost:6379";
  MRClusterTopology *topo = getTopology(4096, 1, &host);
  MRCluster *cl = MR_NewCluster(topo, 2, 3);
  for (int i = 0; i < cl->num_io_threads; i++) {
    IORuntimeCtx *ioRuntime = MRCluster_GetIORuntimeCtx(cl, i);
    mr_slot_t shard = CRCShardFunc(&cmd, ioRuntime);
    mu_assert_int_eq(shard, 717);
  }
  MRCommand_Free(&cmd);
  MRCluster_Free(cl);
}

void testClusterTopology_Clone() {
  int n = 4;
  const char *hosts[] = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409"};
  MRClusterTopology *topo = getTopology(4096, n, hosts);

  // Clone the topology
  MRClusterTopology *cloned = MRClusterTopology_Clone(topo);

  // Verify the clone has the same basic properties
  mu_check(cloned != NULL);
  mu_check(cloned != topo); // Different memory address
  mu_check(cloned->numShards == topo->numShards);
  mu_check(cloned->numSlots == topo->numSlots);
  mu_check(cloned->hashFunc == topo->hashFunc);

  // Verify each shard was properly cloned
  for (int j = 0; j < topo->numShards; j++) {
    MRClusterShard *original_sh = &topo->shards[j];
    MRClusterShard *cloned_sh = &cloned->shards[j];

    mu_check(cloned_sh->ranges[0].start == original_sh->ranges[0].start);
    mu_check(cloned_sh->ranges[0].end == original_sh->ranges[0].end);
    mu_check(cloned_sh->numNodes == original_sh->numNodes);

    // Verify each node in the shard
    for (int k = 0; k < original_sh->numNodes; k++) {
      mu_check(strcmp(cloned_sh->nodes[k].id->str, original_sh->nodes[k].id->str) == 0);
      mu_check(cloned_sh->nodes[k].id != original_sh->nodes[k].id); // Different memory address
      mu_check(strcmp(cloned_sh->nodes[k].endpoint.host, original_sh->nodes[k].endpoint.host) == 0);
      mu_check(cloned_sh->nodes[k].endpoint.port == original_sh->nodes[k].endpoint.port);
      mu_check(cloned_sh->nodes[k].flags == original_sh->nodes[k].flags);
    }
  }

  // Modify the original to prove independence
  topo->numSlots = 8192;
  topo->shards[0].ranges[0].start = 999;

  // Verify the clone remains unchanged
  mu_check(cloned->numSlots == 4096);
  mu_check(cloned->shards[0].ranges[0].start != 999);

  // Clean up
  MRClusterTopology_Free(topo);
  MRClusterTopology_Free(cloned);
}

MRClusterShard *_MRCluster_FindShard(MRClusterTopology *topo, mr_slot_t slot);

void testCluster() {
  for (int num_io_threads = 1; num_io_threads <= 4; num_io_threads++) {
    int n = 4;
    const char *hosts[] = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409"};
    MRClusterTopology *topo = getTopology(4096, n, hosts);

    mu_check(topo->numShards == n);
    mu_check(topo->numSlots == 4096);
    for (int j = 0; j < topo->numShards; j++) {
      MRClusterShard *sh = &topo->shards[j];
      mu_check(sh->numNodes == 1);
      mu_check(sh->ranges[0].start == j * (4096 / n));
      mu_check(sh->ranges[0].end == sh->ranges[0].start + (4096 / n) - 1);
      mu_check(!strcmp(sh->nodes[0].id->str, hosts[j]));
    }

    MRCluster *cl = MR_NewCluster(topo, 2, num_io_threads);
    mu_check(cl != NULL);
    //  mu_check(cl->tp == tp);
    for (int i = 0; i < cl->num_io_threads; i++) {
      IORuntimeCtx *ioRuntime = MRCluster_GetIORuntimeCtx(cl, i);
      mu_check(ioRuntime->topo->numShards == n);
      mu_check(ioRuntime->topo->numSlots == 4096);
      for (int j = 0; j < ioRuntime->topo->numShards; j++) {
        MRClusterShard *sh = &ioRuntime->topo->shards[j];
        mu_check(sh->numNodes == 1);
        mu_check(sh->ranges[0].start == j * (4096 / n));
        mu_check(sh->ranges[0].end == sh->ranges[0].start + (4096 / n) - 1);
        mu_check(!strcmp(sh->nodes[0].id->str, hosts[j]));
      }
    }

    MRCluster_Free(cl);
  }
}

void testClusterSharding() {
  int n = 4;
  const char *hosts[] = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409"};
  MRClusterTopology *topo = getTopology(4096, n, hosts);

  MRCluster *cl = MR_NewCluster(topo, 2, 3);
  MRCommand cmd = MR_NewCommand(4, "_FT.SEARCH", "foob", "bar", "baz");
  for (int i = 0; i < cl->num_io_threads; i++) {
    IORuntimeCtx *ioRuntime = MRCluster_GetIORuntimeCtx(cl, i);
    mr_slot_t slot = CRCShardFunc(&cmd, ioRuntime);
    mu_check(slot > 0);
    MRClusterShard *sh = _MRCluster_FindShard(ioRuntime->topo, slot);
    mu_check(sh != NULL);
    mu_check(sh->numNodes == 1);
    mu_check(!strcmp(sh->nodes[0].id->str, hosts[3]));
  }
  MRCommand_Free(&cmd);
  MRCluster_Free(cl);
}

static void dummyLog(RedisModuleCtx *ctx, const char *level, const char *fmt, ...) {}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  RedisModule_Log = dummyLog;
  RedisModule_CreateString = Mock_CreateString;
  RedisModule_CreateStringFromString = Mock_CreateStringFromString;
  RedisModule_HoldString = Mock_HoldString;
  RedisModule_FreeString = Mock_FreeString;
  RedisModule_StringCompare = Mock_StringCompare;
  RedisModule_TrimStringAllocation = Mock_TrimStringAllocation;
  MU_RUN_TEST(testEndpoint);
  MU_RUN_TEST(testShardingFunc);
  MU_RUN_TEST(testCluster);
  MU_RUN_TEST(testClusterSharding);
  MU_RUN_TEST(testClusterTopology_Clone);
  MU_REPORT();

  return minunit_status;
}
