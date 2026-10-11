/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#define REDISMODULE_MAIN
#include "rejson_api.h"

static RedisJSONAPI* api;

static int PathFlags(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModuleString* error = NULL;
  JSONPath path = api->pathParse(RedisModule_StringPtrLen(argv[1], NULL), ctx, &error);
  if (!path) {
    RedisModule_ReplyWithError(
        ctx, error ? RedisModule_StringPtrLen(error, NULL) : "ERR pathParse failed");
    if (error) {
      RedisModule_FreeString(ctx, error);
    }
    return REDISMODULE_OK;
  }
  RedisModule_ReplyWithArray(ctx, 2);
  RedisModule_ReplyWithLongLong(ctx, api->pathIsSingle(path));
  RedisModule_ReplyWithLongLong(ctx, api->pathHasDefinedOrder(path));
  api->pathFree(path);
  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  if (RedisModule_Init(ctx, "jsonpath_test", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  api = RedisModule_GetSharedAPI(ctx, "RedisJSON_V8");
  if (!api) {
    return REDISMODULE_ERR;
  }
  return RedisModule_CreateCommand(ctx, "jsonpath_test.flags", PathFlags, "readonly", 0, 0, 0);
}
