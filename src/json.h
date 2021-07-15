#pragma once

#include "rejson_api.h"
#include "redismodule.h"
#include "field_spec.h"

#ifdef __cplusplus
extern "C" {
#endif

extern RedisJSONAPI_V1 *japi;
// extern RedisModuleCtx *RSDummyContext;

#define JSON_ROOT "$"

struct DocumentField;

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange);

/* Creates a Redis Module String from JSONType string, int, double, bool */ 
int JSON_GetRedisModuleString(JSONResultsIterator jsonIter, size_t len,
                              FieldType ftype, struct DocumentField *df);

/* Checks if JSONType fits the FieldType */
int FieldSpec_CheckJsonType(FieldType fieldType, JSONType type);

#ifdef __cplusplus
}
#endif
