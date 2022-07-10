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

typedef enum PathInfoFlags {
  PathInfoFlag_Invalid = 0x01,
  PathInfoFlag_Static = 0x02,
  PathInfoFlag_DefinedOrder = 0x04,
} PathInfoFlags;

typedef const void* RedisJSON;
typedef const void* JSONResultsIterator;
typedef const void* JSONPath;

typedef struct RedisJSONAPI_V1 {
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

} RedisJSONAPI_V1;

typedef struct RedisJSONAPI_V2 {
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

  // Return a parsed JSONPath
  // Return NULL if failed to parse, and the error message in `err_msg`
  // The caller gains ownership of `err_msg`
  JSONPath (*pathParse)(const char *path, RedisModuleCtx *ctx, RedisModuleString **err_msg);

  // Free a parsed JSONPath
  void (*pathFree)(JSONPath);
  
  // Query a parsed JSONPath
  int (*pathIsStatic)(JSONPath);
  int (*pathHasDefinedOrder)(JSONPath);

} RedisJSONAPI_V2;

#ifdef __cplusplus
}
#endif

