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

#include "hiredis/hiredis.h"
#include "rmutil/alloc.h"

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
    nodes[i].id = rm_strdup(hosts[i]);
  }
  int i = 0;
  for (size_t slot = 0; slot < topo->numSlots; slot += slotRange) {
    topo->shards[i] = (MRClusterShard){
      .numNodes = 1,
    };
    topo->shards[i].nodes = rm_calloc(1, sizeof(MRClusterNode));
    topo->shards[i].nodes[0] = nodes[i];

    i++;
  }

  return topo;
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

  // Verify each shard was properly cloned
  for (int j = 0; j < topo->numShards; j++) {
    MRClusterShard *original_sh = &topo->shards[j];
    MRClusterShard *cloned_sh = &cloned->shards[j];

    mu_check(cloned_sh->numNodes == original_sh->numNodes);

    // Verify each node in the shard
    for (int k = 0; k < original_sh->numNodes; k++) {
      mu_check(strcmp(cloned_sh->nodes[k].id, original_sh->nodes[k].id) == 0);
      mu_check(cloned_sh->nodes[k].id != original_sh->nodes[k].id); // Different memory address
      mu_check(strcmp(cloned_sh->nodes[k].endpoint.host, original_sh->nodes[k].endpoint.host) == 0);
      mu_check(cloned_sh->nodes[k].endpoint.port == original_sh->nodes[k].endpoint.port);
      mu_check(cloned_sh->nodes[k].flags == original_sh->nodes[k].flags);
    }
  }

  // Modify the original to prove independence
  topo->numSlots = 8192;

  // Verify the clone remains unchanged
  mu_check(cloned->numSlots == 4096);

  // Clean up
  MRClusterTopology_Free(topo);
  MRClusterTopology_Free(cloned);
}

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
      mu_check(!strcmp(sh->nodes[0].id, hosts[j]));
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
        mu_check(!strcmp(sh->nodes[0].id, hosts[j]));
      }
    }

    MRCluster_Free(cl);
  }
}

static void dummyLog(RedisModuleCtx *ctx, const char *level, const char *fmt, ...) {}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  RedisModule_Log = dummyLog;
  MU_RUN_TEST(testEndpoint);
  MU_RUN_TEST(testCluster);
  MU_RUN_TEST(testClusterTopology_Clone);
  MU_REPORT();

  return minunit_status;
}
