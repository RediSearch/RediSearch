#pragma once

#include "rejson_api.h"
#include "redismodule.h"
#include "field_spec.h"

#ifdef __cplusplus
extern "C" {
#endif

extern RedisJSONAPI_V1 *japi;
extern RedisJSONAPI_V2 *japi2;
// extern RedisModuleCtx *RSDummyContext;

#define JSON_ROOT "$"

struct DocumentField;

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange);

/* Creates a Redis Module String from JSONType string, int, double, bool */
int JSON_LoadDocumentField(RedisModuleCtx *ctx, JSONResultsIterator jsonIter, size_t len,
                              FieldSpec *fs, struct DocumentField *df);

/* Checks if JSONType fits the FieldType */
int FieldSpec_CheckJsonType(FieldType fieldType, JSONType type);

PathInfoFlags getPathFlags(const char *path);

#ifdef __cplusplus
}
#endif
