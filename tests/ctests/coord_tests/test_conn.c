/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "minunit.h"
#include "rmutil/alloc.h"
#include "hiredis/alloc.h"
#include "cluster.h"

// Exercise private pool state without sockets or exposing test-only production APIs.
#include "../../../src/coord/rmr/conn.c"

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

  for (int state = MRConn_Connecting; state <= MRConn_Freeing; state++) {
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
    MRClusterShard shard = {.node = {.id = "node"}};
    MRClusterTopology topo = {.numShards = 1, .shards = &shard};
    IORuntimeCtx runtime = {.conn_mgr = mgr, .topo = &topo};
    const char *argv[] = {"PING"};
    const size_t lens[] = {4};
    MRCommand cmd = MR_NewCommandArgvLen(1, argv, lens);
    for (uint32_t request = 0; request < size * 3; request++) {
      uint32_t expected = request % size;
      size_t before[6];
      for (uint32_t i = 0; i < size; i++) before[i] = sdslen(contexts[i].c.obuf);
      mu_assert_int_eq(1, MRCluster_FanoutCommand(&runtime, &cmd, NULL, NULL, true));
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
  MRClusterShard shards[] = {{.node = {.id = "connected"}}, {.node = {.id = "missing"}}};
  MRClusterTopology topo = {.numShards = 2, .shards = shards};
  IORuntimeCtx runtime = {.conn_mgr = mgr, .topo = &topo};
  const char *argv[] = {"PING"};
  const size_t lens[] = {4};
  MRCommand cmd = MR_NewCommandArgvLen(1, argv, lens);
  mu_assert_int_eq(0, MRCluster_FanoutCommand(&runtime, &cmd, NULL, NULL, true));
  mu_assert_int_eq(0, sdslen(context.c.obuf));
  mu_assert_int_eq(0, pool.rr);
  MRCommand_Free(&cmd);
  freePendingCommands(&context);
  dictRelease(mgr.map);
}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  MU_RUN_TEST(testConnectivityDoesNotSelect);
  MU_RUN_TEST(testValidatedFanoutRotation);
  MU_RUN_TEST(testFailedValidationDoesNotSendOrSelect);
  MU_REPORT();
  return minunit_status;
}
