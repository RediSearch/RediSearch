#include "redismodule.h"
#include <rmr/command.h>
#include <crc16_tags.h>
#include "search_cluster.h"
#include "alias.h"
#include "minunit.h"

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
}

int main(int argc, char **argv) {
  // MU_RUN_TEST(testTagFunc);
  RedisModule_Alloc = malloc;
  RedisModule_Calloc = calloc;
  RedisModule_Realloc = realloc;
  RedisModule_Free = free;
  IndexAlias_InitGlobal();
  MU_RUN_TEST(testCommandMux);

  MU_REPORT();
  return minunit_status;
}

//REDISMODULE_INIT_SYMBOLS()
