/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "minunit.h"
#include "redise_parser/parse.h"
#include "cluster.h"
#include "rmutil/alloc.h"

void testParser() {
  const char *q =
      "MYID 1 HASREPLICATION HASHFUNC CRC16 NUMSLOTS 1337 RANGES 2 SHARD 1 SLOTRANGE 0 2047 ADDR "
      "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.7:20293 UNIXADDR "
      "unix:/tmp/redis-1.sock MASTER SHARD 2 SLOTRANGE 0 2047 ADDR "
      "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.50:20293 SHARD 3 SLOTRANGE 2048 "
      "4095 ADDR 7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.7:27262 UNIXADDR "
      "unix:/tmp/redis-3.sock MASTER SHARD 4 SLOTRANGE 2048 4095 ADDR "
      "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.50:27262";

  char *err = NULL;
  MRClusterTopology *topo = MR_ParseTopologyRequest(q, strlen(q), &err);
  if (err != NULL) {
    mu_fail(err);
  }
  mu_check(topo != NULL);

  mu_check(topo->numShards == 2);
  mu_check(topo->numSlots == 1337);
  mu_check(topo->hashFunc == MRHashFunc_CRC16);

  mu_check(topo->shards[0].numNodes == 2);
  mu_check(topo->shards[0].startSlot == 0);
  mu_check(topo->shards[0].endSlot == 2047);
  mu_check(topo->shards[1].startSlot == 2048);
  mu_check(topo->shards[1].endSlot == 4095);

  mu_check(!strcmp(topo->shards[0].nodes[0].id, "1"));
  mu_check(!strcmp(topo->shards[0].nodes[0].endpoint.host, "10.0.1.7"));
  mu_check(topo->shards[0].nodes[0].endpoint.port == 20293);
  mu_check(topo->shards[0].nodes[0].flags == (MRNode_Coordinator | MRNode_Master | MRNode_Self));
  mu_check(!strcmp(topo->shards[0].nodes[1].id, "2"));
  mu_check(topo->shards[0].nodes[1].flags == MRNode_Coordinator);
  mu_check(!strcmp(topo->shards[0].nodes[1].endpoint.host, "10.0.1.50"));
  mu_check(topo->shards[0].nodes[1].endpoint.port == 20293);

  mu_check(!strcmp(topo->shards[1].nodes[0].id, "3"));
  mu_check(!strcmp(topo->shards[1].nodes[0].endpoint.host, "10.0.1.7"));
  mu_check(topo->shards[1].nodes[0].endpoint.port == 27262);
  mu_check(topo->shards[1].nodes[0].flags == (MRNode_Coordinator | MRNode_Master));

  mu_check(!strcmp(topo->shards[1].nodes[1].id, "4"));
  mu_check(!strcmp(topo->shards[1].nodes[1].endpoint.host, "10.0.1.50"));
  mu_check(topo->shards[1].nodes[1].endpoint.port == 27262);
  mu_check(topo->shards[1].nodes[1].flags == MRNode_Coordinator);

  for (int i = 0; i < topo->numShards; i++) {
    MRClusterShard *sh = &topo->shards[i];
    // printf("Shard %d: %d...%d, %d nodes: \n", i, sh->startSlot, sh->endSlot, sh->numNodes);
    for (int n = 0; n < sh->numNodes; n++) {

      mu_check(!strcmp(topo->shards[i].nodes[n].endpoint.auth,
                       "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j"));

      MRClusterNode *node = &sh->nodes[n];

      printf("\t node %d: id %s, flags %x ep %s@%s:%d\n", n, node->id, node->flags,
             node->endpoint.auth, node->endpoint.host, node->endpoint.port);
    }
  }

  MRClusterTopology_Free(topo);

  // check failure path
  err = NULL;
  q = "foo bar baz";
  topo = MR_ParseTopologyRequest(q, strlen(q), &err);
  mu_check(topo == NULL);
  mu_check(err != NULL);
  printf("\n%s\n", err);
  rm_free(err);
}

void testHashFunc() {

  // Test Defaults
  const char *q =
      "MYID 1 RANGES 2 SHARD 1 SLOTRANGE 0 2047 ADDR "
      "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.7:20293 UNIXADDR "
      "unix:/tmp/redis-1.sock MASTER SHARD 2 SLOTRANGE 0 2047 ADDR "
      "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.50:20293 SHARD 3 SLOTRANGE 2048 "
      "4095 ADDR 7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.7:27262 UNIXADDR "
      "unix:/tmp/redis-3.sock MASTER SHARD 4 SLOTRANGE 2048 4095 ADDR "
      "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.50:27262";

  char *err = NULL;
  MRClusterTopology *topo = MR_ParseTopologyRequest(q, strlen(q), &err);
  if (err != NULL) {
    mu_fail(err);
  }
  mu_check(topo != NULL);

  mu_assert_int_eq(2, topo->numShards);
  mu_assert_int_eq(4096, topo->numSlots);
  mu_assert_int_eq(MRHashFunc_None, topo->hashFunc);
  MRClusterTopology_Free(topo);

  q = "MYID 1 HASHFUNC CRC16 NUMSLOTS 1337 "
      "RANGES 1 "
      "SHARD 1 SLOTRANGE 0 2047 ADDR "
      "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.7:20293";

  err = NULL;
  topo = MR_ParseTopologyRequest(q, strlen(q), &err);
  if (err != NULL) {
    mu_fail(err);
  }
  mu_check(topo != NULL);

  mu_check(topo->numShards == 1);
  mu_check(topo->numSlots == 1337);
  mu_check(topo->hashFunc == MRHashFunc_CRC16);
  MRClusterTopology_Free(topo);

  // Test error in func
  q = "MYID 1 HASHFUNC CRC13 NUMSLOTS 1337 "
      "RANGES 1 "
      "SHARD 1 SLOTRANGE 0 2047 ADDR "
      "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.7:20293";

  err = NULL;
  topo = MR_ParseTopologyRequest(q, strlen(q), &err);
  mu_check(topo == NULL);
  mu_check(err != NULL);
  rm_free(err);

  // Test error in slotnum
  q = "MYID 1 HASHFUNC CRC13 NUMSLOTS 1337 "
      "RANGES 1 "
      "SHARD 1 SLOTRANGE 0 2047 ADDR "
      "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.7:20293";

  err = NULL;
  topo = MR_ParseTopologyRequest(q, strlen(q), &err);
  mu_check(topo == NULL);
  mu_check(err != NULL);
  rm_free(err);

  // Test error
}
int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  MU_RUN_TEST(testParser);
  MU_RUN_TEST(testHashFunc);

  MU_REPORT();

  return minunit_status;
}
