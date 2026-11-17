/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "redismodule.h"
#include "spec.h"

#ifdef __cplusplus
extern "C" {
#endif

struct MRCtx;

typedef struct {
  RedisModuleString **args;
  int count;
  int expectedShards;
  bool prefix;
} InfoSelection;

int InfoSelection_Parse(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                        InfoSelection *selection);
bool InfoSelection_Matches(const InfoSelection *selection, const char *key);
struct MRCtx *InfoSelection_CreateRequest(RedisModuleCtx *ctx, const InfoSelection *selection,
                                          int numShards);

int IndexInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int IndexObfuscatedInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#ifdef __cplusplus
}
#endif
