#pragma once

#include "redisearch.h"
#include "redismodule.h"
#include "json.h"

extern RedisJSONAPI_V1 *japi;
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
