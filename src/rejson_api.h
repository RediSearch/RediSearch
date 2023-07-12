/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum JSONType {
  JSONType_String = 0,
  JSONType_Int = 1,
  JSONType_Double = 2,
  JSONType_Bool = 3,
  JSONType_Object = 4,
  JSONType_Array = 5,
  JSONType_Null = 6,
  JSONType__EOF
} JSONType;

typedef const void* RedisJSON;
typedef const void* JSONResultsIterator;
typedef const void* JSONPath;
typedef const void* JSONKeyValuesIterator;

typedef struct RedisJSONAPI {

  ////////////////
  // V1 entries //
  ////////////////

  /* RedisJSON functions */
  RedisJSON (*openKey)(RedisModuleCtx *ctx, RedisModuleString *key_name);
  RedisJSON (*openKeyFromStr)(RedisModuleCtx *ctx, const char *path);

  JSONResultsIterator (*get)(RedisJSON json, const char *path);
  
  RedisJSON (*next)(JSONResultsIterator iter);
  size_t (*len)(JSONResultsIterator iter);
  void (*freeIter)(JSONResultsIterator iter);

  RedisJSON (*getAt)(RedisJSON json, size_t index);

  /* RedisJSON value functions
   * Return REDISMODULE_OK if RedisJSON is of the correct JSONType,
   * else REDISMODULE_ERR is returned
   * */

  // Return the length of Object/Array
  int (*getLen)(RedisJSON json, size_t *count);

  // Return the JSONType
  JSONType (*getType)(RedisJSON json);

  // Return int value from a Numeric field
  int (*getInt)(RedisJSON json, long long *integer);

  // Return double value from a Numeric field
  int (*getDouble)(RedisJSON json, double *dbl);

  // Return 0 or 1 as int value from a Bool field
  int (*getBoolean)(RedisJSON json, int *boolean);

  // Return a Read-Only String value from a String field
  int (*getString)(RedisJSON json, const char **str, size_t *len);

  // Return JSON String representation (for any JSONType)
  // The caller gains ownership of `str`
  int (*getJSON)(RedisJSON json, RedisModuleCtx *ctx, RedisModuleString **str);

  // Return 1 if type of key is JSON
  int (*isJSON)(RedisModuleKey *redis_key);

  ////////////////
  // V2 entries //
  ////////////////

  // Return a parsed JSONPath
  // Return NULL if failed to parse, and the error message in `err_msg`
  // The caller gains ownership of `err_msg`
  JSONPath (*pathParse)(const char *path, RedisModuleCtx *ctx, RedisModuleString **err_msg);

  // Free a parsed JSONPath
  void (*pathFree)(JSONPath);
  
  // Query a parsed JSONPath
  int (*pathIsSingle)(JSONPath);
  int (*pathHasDefinedOrder)(JSONPath);

  ////////////////
  // V3 entries //
  ////////////////

  // Return JSON String representation from an iterator (without consuming the iterator)
  // The caller gains ownership of `str`
  int (*getJSONFromIter)(JSONResultsIterator iter, RedisModuleCtx *ctx, RedisModuleString **str);

  // Reset the iterator to the beginning
  void (*resetIter)(JSONResultsIterator iter);

  ////////////////
  // V4 entries //
  ////////////////

  // Get an iterator over the key-value pairs of a JSON Object
  JSONKeyValuesIterator (*getKeyValues)(RedisJSON json);
  // Get the next key-value pair
  // The caller gains ownership of `key_name`
  RedisJSON (*nextKeyValue)(JSONKeyValuesIterator iter, RedisModuleCtx *ctx, RedisModuleString **key_name);
  // Free the iterator
  void (*freeKeyValuesIter)(JSONKeyValuesIterator iter);

} RedisJSONAPI;

#ifdef __cplusplus
}
#endif

