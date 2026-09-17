/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "coord/tests/utils/minunit.h"
#include "rmutil/alloc.h"
#include "hiredis/alloc.h"
#include "cluster.h"

// Exercise private pool state without sockets or exposing test-only production APIs.
#include "../conn.c"

static dictType testPoolType;

static MRConnManager newManager(void) {
  testPoolType = nodeIdToConnPoolType;
  testPoolType.valDestructor = NULL;
  return (MRConnManager){.map = dictCreate(&testPoolType, NULL)};
}

static void testConnectivityDoesNotSelect(void) {
  MRConnManager mgr = newManager();
  MRConn connections[4] = {0};
  MRConn *entries[] = {&connections[0], &connections[1], &connections[2], &connections[3]};
  MRConnPool pool = {.num = 4, .rr = 2, .conns = entries};
  mu_assert_int_eq(DICT_OK, dictAdd(mgr.map, "node", &pool));
  mu_check(!MRConnManager_HasConnectedConnection(&mgr, "missing"));

  for (int state = MRConn_Disconnected; state <= MRConn_Freeing; state++) {
    for (int i = 0; i < 4; i++) {
      connections[i].state = state;
    }
    mu_check(MRConnManager_HasConnectedConnection(&mgr, "node") == (state == MRConn_Connected));
    mu_assert_int_eq(2, pool.rr);
  }

  connections[3].state = MRConn_Connected;
  mu_check(MRConnManager_HasConnectedConnection(&mgr, "node"));
  mu_assert_int_eq(2, pool.rr);
  mu_check(MRConn_Get(&mgr, "node") == &connections[3]);

  pool.num = 0;
  mu_check(!MRConnManager_HasConnectedConnection(&mgr, "node"));
  mu_assert_int_eq(0, pool.rr);
  dictRelease(mgr.map);
}

static void testGetterSkipsDisconnectedSlots(void) {
  MRConnManager mgr = newManager();
  MRConn connections[4] = {0};
  MRConn *entries[] = {&connections[0], &connections[1], &connections[2], &connections[3]};
  MRConnPool pool = {.num = 4, .rr = 3, .conns = entries};
  mu_assert_int_eq(DICT_OK, dictAdd(mgr.map, "node", &pool));
  connections[1].state = MRConn_Connected;
  connections[2].state = MRConn_Connected;

  mu_check(MRConnManager_HasConnectedConnection(&mgr, "node"));
  mu_assert_int_eq(3, pool.rr);
  mu_check(MRConn_Get(&mgr, "node") == &connections[1]);
  mu_assert_int_eq(2, pool.rr);
  mu_check(MRConn_Get(&mgr, "node") == &connections[2]);
  mu_assert_int_eq(3, pool.rr);

  connections[1].state = MRConn_Disconnected;
  connections[2].state = MRConn_Disconnected;
  mu_check(MRConn_Get(&mgr, "node") == NULL);
  mu_assert_int_eq(3, pool.rr);
  pool.num = 0;
  mu_check(MRConn_Get(&mgr, "node") == NULL);
  mu_assert_int_eq(3, pool.rr);
  dictRelease(mgr.map);
}

static void freePendingCommands(redisAsyncContext *ac) {
  sdsfree(ac->c.obuf);
  while (ac->replies.head) {
    redisCallback *cb = ac->replies.head;
    ac->replies.head = cb->next;
    hi_free(cb);
  }
}

static void testValidatedFanoutRotation(void) {
  for (uint32_t size = 1; size <= 6; size++) {
    MRConnManager mgr = newManager();
    MRConn connections[6] = {0};
    MRConn *entries[6];
    redisAsyncContext contexts[6] = {0};
    for (uint32_t i = 0; i < size; i++) {
      contexts[i].c.obuf = sdsempty();
      connections[i].conn = &contexts[i];
      connections[i].state = MRConn_Connected;
      entries[i] = &connections[i];
    }
    MRConnPool pool = {.num = size, .conns = entries};
    mu_assert_int_eq(DICT_OK, dictAdd(mgr.map, "node", &pool));
    MRClusterNode node = {.id = "node", .flags = MRNode_Master};
    MRClusterShard shard = {.numNodes = 1, .nodes = &node};
    MRClusterTopology topo = {.numShards = 1, .shards = &shard};
    MRCluster runtime = {.mgr = mgr, .topo = &topo};
    const char *argv[] = {"PING"};
    MRCommand cmd = MR_NewCommandArgv(1, argv);
    for (uint32_t request = 0; request < size * 3; request++) {
      uint32_t expected = request % size;
      size_t before[6];
      for (uint32_t i = 0; i < size; i++) before[i] = sdslen(contexts[i].c.obuf);
      mu_assert_int_eq(1, MRCluster_FanoutCommand(&runtime, true, &cmd, NULL, NULL, true));
      for (uint32_t i = 0; i < size; i++) {
        mu_check((sdslen(contexts[i].c.obuf) > before[i]) == (i == expected));
      }
    }
    MRCommand_Free(&cmd);
    for (uint32_t i = 0; i < size; i++) freePendingCommands(&contexts[i]);
    dictRelease(mgr.map);
  }
}

static void testFailedValidationDoesNotSendOrSelect(void) {
  MRConnManager mgr = newManager();
  redisAsyncContext context = {0};
  context.c.obuf = sdsempty();
  MRConn connection = {.conn = &context, .state = MRConn_Connected};
  MRConn *entries[] = {&connection, &connection};
  MRConnPool pool = {.num = 2, .conns = entries};
  mu_assert_int_eq(DICT_OK, dictAdd(mgr.map, "connected", &pool));
  MRClusterNode nodes[] = {{.id = "connected", .flags = MRNode_Master},
                           {.id = "missing", .flags = MRNode_Master}};
  MRClusterShard shards[] = {{.numNodes = 1, .nodes = &nodes[0]},
                             {.numNodes = 1, .nodes = &nodes[1]}};
  MRClusterTopology topo = {.numShards = 2, .shards = shards};
  MRCluster runtime = {.mgr = mgr, .topo = &topo};
  const char *argv[] = {"PING"};
  MRCommand cmd = MR_NewCommandArgv(1, argv);
  mu_assert_int_eq(0, MRCluster_FanoutCommand(&runtime, true, &cmd, NULL, NULL, true));
  mu_assert_int_eq(0, sdslen(context.c.obuf));
  mu_assert_int_eq(0, pool.rr);
  MRCommand_Free(&cmd);
  freePendingCommands(&context);
  dictRelease(mgr.map);
}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  MU_RUN_TEST(testConnectivityDoesNotSelect);
  MU_RUN_TEST(testGetterSkipsDisconnectedSlots);
  MU_RUN_TEST(testValidatedFanoutRotation);
  MU_RUN_TEST(testFailedValidationDoesNotSendOrSelect);
  MU_REPORT();
  return minunit_status;
}
