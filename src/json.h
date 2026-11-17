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
#define RedisJSONAPI_MIN_API_VER 10

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

/* Evaluate an owned indexing path with the Redis GIL held and a live JSON root.
 * The compiled handle must remain alive through iteration. NULL paths yield no results. */
JSONResultsIterator JSON_GetWithCompiledPath(RedisJSON root, JSONPath path);

/* Get the RedisJSON root from an already-open RedisModuleKey handle.
 *
 * Returns NULL if RedisJSON is not loaded, the key is NULL, or it does not
 * hold JSON. The caller owns the key handle and must keep it open while using
 * the returned root. */
RedisJSON JSON_GetJsonFromHandle(RedisModuleKey *key);

int jsonIterToValue(RedisModuleCtx *ctx, JSONResultsIterator iter, unsigned int apiVersion, RSValue **rsv);

/* Load the values of `fs`, a member of a NESTED group, from the elements of the group's array in
 * the document at `root`. `elementsIter` holds the array's elements; it is read from its start
 * and stays the caller's, to use again for the next member of the group. A value that is missing
 * or null in an element is skipped. `df` is left untouched when no element has a value. For a
 * NUMERIC member, `df->multisv` gets the field's sortable value. */
int JSON_LoadNestedMember(RedisModuleCtx *ctx, RedisJSON root, JSONResultsIterator elementsIter,
                          const FieldSpec *fs, struct DocumentField *df, QueryError *status);

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
