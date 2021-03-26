#pragma once

#include "rejson_api.h"
#include "redismodule.h"
#include "document.h"
#include "doc_types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern RedisJSONAPI_V1 *japi;

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange);
int Document_LoadSchemaFieldJson(Document *doc, RedisSearchCtx *sctx);

const char *JSON_ToString(RedisModuleCtx *ctx, RedisJSON json, JSONType type, size_t *len);
RedisModuleString *JSON_ToStringR(RedisModuleCtx *ctx, RedisJSON json, JSONType type);
int JSON_GetStringR_POC(RedisModuleCtx *ctx, const char *keyName, const char *path, RedisModuleString **val);

static inline int RedisJSON_GetString(RedisJSONKey key, const char *path, char **str, size_t *len) {
  size_t size;
  JSONType type;
  RedisJSON json = japi->get(key, path, &type, &size);
  int rv = japi->getString(json, str, len);
  japi->close(json);
  return rv;
}

static inline int RedisJSON_GetNumeric(RedisJSONKey key, const char *path, double *dbl){
  int rv = REDISMODULE_ERR;
  size_t size;
  JSONType type;
  RedisJSON json = japi->get(key, path, &type, &size);
  if (!json) {
    return rv;
  }
  if (type == JSONType_Double) {
    rv = japi->getDouble(json, dbl);
  } else if (type == JSONType_Int) {
    long long integer;
    rv = japi->getInt(json, &integer);
    *dbl = integer;
  }
  japi->close(json);
  return rv;
}

#ifdef __cplusplus
}
#endif
