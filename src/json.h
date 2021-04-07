#pragma once

#include "rejson_api.h"
#include "redismodule.h"
#include "document.h"
#include "doc_types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern RedisJSONAPI_V1 *japi;
extern RedisModuleCtx *RSDummyContext;

#define JSON_ROOT "$"

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange);
int Document_LoadSchemaFieldJson(Document *doc, RedisSearchCtx *sctx);

const char *JSON_ToString(RedisModuleCtx *ctx, RedisJSON json, JSONType type, size_t *len);
RedisModuleString *JSON_ToStringR(RedisModuleCtx *ctx, RedisJSON json, JSONType type);
int JSON_GetStringR_POC(RedisModuleCtx *ctx, const char *keyName, const char *path, RedisModuleString **val);

/* Get a C string back from ReJSON and duplicate it. 
 * String must be freed */
static inline int RedisJSON_GetString(RedisJSONKey key, const char *path, const char **str, size_t *len) {
  const char *tmpStr;
  int rv = japi->getStringFromKey(key, path, &tmpStr, len);
  if (rv == REDISMODULE_OK) {
    *str = rm_strndup(tmpStr, *len);
  }
  return rv;
}

#ifdef __cplusplus
}
#endif
