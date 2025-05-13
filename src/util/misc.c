/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "misc.h"
#include "redismodule.h"
#include <stdlib.h>
#include <ctype.h>
#include <string.h>


void GenericAofRewrite_DisabledHandler(RedisModuleIO *aof, RedisModuleString *key, void *value) {
  RedisModule_Log(RedisModule_GetContextFromIO(aof), "error",
                  "Requested AOF, but this is unsupported for this module");
  abort();
}

int GetRedisErrorCodeLength(const char* error) {
  const char* errorSpace = strchr(error, ' ');
  return errorSpace ? errorSpace - error : 0;
}

