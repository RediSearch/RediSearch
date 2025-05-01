/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "redisearch.h"
#include "redismodule.h"
#include "json.h"

extern RedisJSONAPI *japi;
extern RedisModuleCtx *RSDummyContext;

#ifdef __cplusplus
extern "C" {
#endif

static inline DocumentType getDocType(RedisModuleKey *key) {
  int keyType = RedisModule_KeyType(key);
  if (keyType == REDISMODULE_KEYTYPE_HASH) {
    return DocumentType_Hash;
  } else if (keyType == REDISMODULE_KEYTYPE_MODULE && japi && japi->isJSON(key)) {
    return DocumentType_Json;
  }
  // All other types, including REDISMODULE_KEYTYPE_EMPTY, are not supported
  return DocumentType_Unsupported;
}

static inline DocumentType getDocTypeFromString(RedisModuleString *keyStr) {
  RedisModuleKey *key = RedisModule_OpenKey(RSDummyContext, keyStr, REDISMODULE_READ);
  DocumentType type = getDocType(key);
  RedisModule_CloseKey(key);
  return type;
}

#ifdef __cplusplus
}
#endif
