#pragma once

#include "rejson_api.h"
#include "redismodule.h"
#include "field_spec.h"

#ifdef __cplusplus
extern "C" {
#endif

extern RedisJSONAPI *japi;
extern int japi_ver;
// extern RedisModuleCtx *RSDummyContext;

#define JSON_ROOT "$"

struct DocumentField;

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange);

/* Creates a Redis Module String from JSONType string, int, double, bool */
int JSON_LoadDocumentField(JSONResultsIterator jsonIter, size_t len, FieldSpec *fs,
                           struct DocumentField *df, RedisModuleCtx *ctx);

/* Checks if JSONType fits the FieldType */
int FieldSpec_CheckJsonType(FieldType fieldType, JSONType type);

JSONPath pathParse(const char *path, RedisModuleString **err_msg);
void pathFree(JSONPath jsonpath);
int pathIsSingle(JSONPath jsonpath);
int pathHasDefinedOrder(JSONPath jsonpath);

#ifdef __cplusplus
}
#endif
