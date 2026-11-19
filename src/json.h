/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "rejson_api.h"
#include "redismodule.h"
#include "field_spec.h"

#ifdef __cplusplus
extern "C" {
#endif

extern RedisJSONAPI *japi;
extern int japi_ver;
#ifndef REDISJSON_LEGACY_API
#define REDISJSON_LEGACY_API 0
#endif
#if REDISJSON_LEGACY_API
#define RedisJSONAPI_MIN_API_VER 8
#else
#define RedisJSONAPI_MIN_API_VER 10
#endif

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
      RedisJSONPtr value_ptr;
    } array;
  };
} JSONIterable;

RedisJSON JSONIterable_Next(JSONIterable *iterable);
void JSONIterable_Clean(JSONIterable *iterable); // Like free, but does not free the `iterable` pointer itself

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange);

/* Compile an owned indexing path with the Redis GIL held. Invalid paths yield NULL. */
JSONPath JSON_CompilePath(const char *path);

/* Evaluate an owned indexing path with the Redis GIL held and a live JSON root.
 * The handle and original string must remain alive through iteration. NULL paths yield no results.
 */
JSONResultsIterator JSON_GetWithCompiledPath(RedisJSON root, JSONPath path, const char *pathString);

/* Get the RedisJSON root from an already-open RedisModuleKey handle.
 *
 * Returns NULL if RedisJSON is not loaded, the key is NULL, or it does not
 * hold JSON. The caller owns the key handle and must keep it open while using
 * the returned root. */
RedisJSON JSON_GetJsonFromHandle(RedisModuleKey *key);

int jsonIterToValue(RedisModuleCtx *ctx, JSONResultsIterator iter, unsigned int apiVersion, RSValue **rsv);

/* Creates a Redis Module String from JSONType string, int, double, bool */
int JSON_LoadDocumentField(JSONResultsIterator jsonIter, size_t len, FieldSpec *fs,
                           struct DocumentField *df, RedisModuleCtx *ctx, bool rejectMultiValue,
                           QueryError *status);

/* Stores text values from a JSON iterable into a document field */
int JSON_StoreTextInDocField(size_t len, JSONIterable *iterable, struct DocumentField *df, QueryError *status);

/* Stores multi-vector values from a JSON iterable into a document field */
int JSON_StoreMultiVectorInDocField(FieldSpec *fs, JSONIterable *itr, size_t len, struct DocumentField *df, QueryError *status);

/* Checks if JSONType fits the FieldType */
int FieldSpec_CheckJsonType(FieldType fieldType, JSONType type, QueryError *status);

JSONPath pathParse(const HiddenString* path, RedisModuleString **err_msg);

void JSONParse_error(QueryError *status, const HiddenString *path, const HiddenString *fieldName,
                     const HiddenString *indexName);

#ifdef __cplusplus
}
#endif
