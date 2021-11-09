#include "redise.h"
#include "redise_parser/parse.h"

MRClusterTopology *RedisEnterprise_ParseTopology(RedisModuleCtx *ctx, RedisModuleString **argv,
                                                 int argc) {

  size_t totalLen = 0;
  const char *cargs[argc];
  size_t lens[argc];
  for (int i = 1; i < argc; i++) {
    cargs[i - 1] = RedisModule_StringPtrLen(argv[i], &lens[i - 1]);
    totalLen += lens[i - 1] + 1;
  }

  char *str = calloc(totalLen + 2, 1);
  char *p = str;
  for (int i = 0; i < argc - 1; i++) {
    strncpy(p, cargs[i], lens[i]);
    p += lens[i];
    *p++ = ' ';
  }
  p--;
  *p = 0;
  RedisModule_Log(ctx, "notice", "Got topology update: %s", str);
  char *err = NULL;
  MRClusterTopology *topo = MR_ParseTopologyRequest(str, strlen(str), &err);
  if (err != NULL) {
    RedisModule_Log(ctx, "warning", "Could not parse cluster topology: %s\n", err);
    RedisModule_ReplyWithError(ctx, err);
    return NULL;
  }
  free(str);

  return topo;
}
