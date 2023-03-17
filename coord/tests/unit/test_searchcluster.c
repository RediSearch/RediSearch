/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redismodule.h"
#include "rmr/command.h"
#include "crc16_tags.h"
#include "search_cluster.h"
#include "alias.h"
#include "minunit.h"
#include "rmutil/alloc.h"

const char *FNVTagFunc(const char *key, size_t len, size_t k);
// void testTagFunc() {

//   SearchCluster sc = NewSearchCluster(100, FNVTagFunc);
//   MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "idx", "foo");

//   const char *tag = FNVTagFunc("hello", strlen("hello"), sc.size);
//   printf("%s\n", tag);
// }

void testCommandMux() {
  SearchCluster sc = NewSearchCluster(100, crc16_slot_table, 16384);
  MRCommand cmd = MR_NewCommand(3, "_FT.SEARCH", "idx", "foo");

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&sc, &cmd);

  MRCommand mxcmd;
  int i = 0;
  while (cg.Next(cg.ctx, &mxcmd)) {
    i += 1;
    MRCommand_Free(&mxcmd);
    if (i > 100) mu_fail("number of iterations exceeded");
  }
  cg.Free(cg.ctx);
  SearchCluster_Release(&sc);
}

int main(int argc, char **argv) {
  // MU_RUN_TEST(testTagFunc);
  RMUTil_InitAlloc();
  IndexAlias_InitGlobal();
  MU_RUN_TEST(testCommandMux);

  MU_REPORT();
  return minunit_status;
}

//REDISMODULE_INIT_SYMBOLS()
