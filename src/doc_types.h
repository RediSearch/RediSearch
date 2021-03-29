#pragma once

#include "redisearch.h"
#include "redismodule.h"
#include "json.h"

extern RedisModuleCtx *RSDummyContext;

#ifdef __cplusplus
extern "C" {
#endif

static inline DocumentType getDocType(RedisModuleKey *key) {
  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_HASH) {
    return DocumentType_Hash;
  } else if (1) {
    // TODO: fix once can figure out JSON
    return DocumentType_Json;
  }
  return DocumentType_None;
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
