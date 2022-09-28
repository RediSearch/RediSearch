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

typedef enum {
  ITERABLE_ITER = 0,
  ITERABLE_ARRAY = 1
} JSONIterableType;

// An adapter for iterator operations, such as `next`, over an underlying container/collection or iterator
typedef struct {
  JSONIterableType type;
  union {
    JSONResultsIterator iter;
    struct {
      RedisJSON arr;
      size_t index;
    } array;
  };
} JSONIterable;

RedisJSON JSONIterable_Next(JSONIterable *iterable);

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange);

/* Creates a Redis Module String from JSONType string, int, double, bool */
int JSON_LoadDocumentField(JSONResultsIterator jsonIter, size_t len, FieldSpec *fs,
                           struct DocumentField *df);

/* Checks if JSONType fits the FieldType */
int FieldSpec_CheckJsonType(FieldType fieldType, JSONType type);

JSONPath pathParse(const char *path, RedisModuleString **err_msg);
void pathFree(JSONPath jsonpath);
int pathIsSingle(JSONPath jsonpath);
int pathHasDefinedOrder(JSONPath jsonpath);

#define JSONParse_error(status, err_msg, path, fieldName, indexName)                                    \
    do {                                                                                                \
    QueryError_SetErrorFmt(status, QUERY_EINVALPATH,                                                    \
                          "Invalid JSONPath '%s' in attribute '%s' in index '%s'",                      \
                          path, fieldName, indexName);                                                  \
    RedisModule_FreeString(RSDummyContext, err_msg);                                                    \
    } while (0)
    /* else {
    RedisModule_Log(RSDummyContext, "info",
                    "missing RedisJSON API to parse JSONPath '%s' in attribute '%s' in index '%s', assuming undefined ordering",
                    path, fieldName, indexName);
    } */

#ifdef __cplusplus
}
#endif
