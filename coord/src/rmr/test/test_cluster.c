/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "minunit.h"
#include "endpoint.h"
#include "command.h"
#include "cluster.h"
#include "crc16.h"
#include "crc12.h"

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
  mu_check(!strcmp(ep.auth, "pass"));
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
    nodes[i].id = rm_strdup(hosts[i]);
  }
  int i = 0;
  for (size_t slot = 0; slot < topo->numSlots; slot += slotRange) {
    topo->shards[i] = (MRClusterShard){
        .startSlot = slot, .endSlot = slot + slotRange - 1, .numNodes = 1,

    };
    topo->shards[i].nodes = rm_calloc(1, sizeof(MRClusterNode)),
    topo->shards[i].nodes[0] = nodes[i];

    i++;
  }

  return topo;
}

static const char *GetShardKey(const MRCommand *cmd, size_t *len) {
  *len = cmd->lens[1];
  return cmd->strs[1];
}
static mr_slot_t CRCShardFunc(const MRCommand *cmd, const MRCluster *cl) {

  if(cmd->targetSlot >= 0){
    return cmd->targetSlot;
  }

  size_t len;
  const char *k = GetShardKey(cmd, &len);
  if (!k) return 0;
  // Default to crc16
  uint16_t crc = (cl->topo->hashFunc == MRHashFunc_CRC12) ? crc12(k, len) : crc16(k, len);
  return crc % cl->topo->numSlots;
}

void testShardingFunc() {

  MRCommand cmd = MR_NewCommand(2, "foo", "baz");
  MRClusterTopology *topo = getTopology(4096, 1, NULL);
  MRCluster *cl = MR_NewCluster(topo, 2);
  mr_slot_t shard = CRCShardFunc(&cmd, cl);
  mu_assert_int_eq(shard, 717);
  MRCommand_Free(&cmd);
  MRClust_Free(cl);
}

MRClusterShard *_MRCluster_FindShard(MRCluster *cl, mr_slot_t slot);

void testCluster() {

  int n = 4;
  const char *hosts[] = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409"};
  MRClusterTopology *topo = getTopology(4096, n, hosts);

  MRCluster *cl = MR_NewCluster(topo, 2);
  mu_check(cl != NULL);
  //  mu_check(cl->tp == tp);
  mu_check(cl->topo->numShards == n);
  mu_check(cl->topo->numSlots == 4096);

  for (int i = 0; i < cl->topo->numShards; i++) {
    MRClusterShard *sh = &cl->topo->shards[i];
    mu_check(sh->numNodes == 1);
    mu_check(sh->startSlot == i * (4096 / n));
    mu_check(sh->endSlot == sh->startSlot + (4096 / n) - 1);
    mu_check(!strcmp(sh->nodes[0].id, hosts[i]));

    printf("%d..%d --> %s\n", sh->startSlot, sh->endSlot, sh->nodes[0].id);
  }

  MRClust_Free(cl);
}

void testClusterSharding() {
  int n = 4;
  const char *hosts[] = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409"};
  MRClusterTopology *topo = getTopology(4096, n, hosts);

  MRCluster *cl = MR_NewCluster(topo, 2);
  MRCommand cmd = MR_NewCommand(4, "_FT.SEARCH", "foob", "bar", "baz");
  mr_slot_t slot = CRCShardFunc(&cmd, cl);
  printf("%d\n", slot);
  mu_check(slot > 0);
  MRClusterShard *sh = _MRCluster_FindShard(cl, slot);
  mu_check(sh != NULL);
  mu_check(sh->numNodes == 1);
  mu_check(!strcmp(sh->nodes[0].id, hosts[3]));
  printf("%d..%d --> %s\n", sh->startSlot, sh->endSlot, sh->nodes[0].id);

  MRCommand_Free(&cmd);
  MRClust_Free(cl);
}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  MU_RUN_TEST(testEndpoint);
  MU_RUN_TEST(testShardingFunc);
  MU_RUN_TEST(testCluster);
  MU_RUN_TEST(testClusterSharding);
  MU_REPORT();

  return minunit_status;
}
