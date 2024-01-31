/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

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

  char *str = rm_calloc(totalLen + 2, 1);
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
  rm_free(str);
  if (err != NULL) {
    RedisModule_Log(ctx, "warning", "Could not parse cluster topology: %s", err);
    RedisModule_ReplyWithError(ctx, err);
    rm_free(err);
    return NULL;
  }

  return topo;
}
