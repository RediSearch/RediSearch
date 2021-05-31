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

typedef const void *RedisJSONKey;
typedef const void *RedisJSON;

typedef struct RedisJSONAPI_V1 {
    /* RedisJSONKey functions */
    RedisJSONKey (*openKey)(RedisModuleCtx *ctx, RedisModuleString *key_name);
    RedisJSONKey (*openKeyFromStr)(RedisModuleCtx *ctx, const char *path);

    void (*closeKey)(RedisJSONKey key);

    /* RedisJSON functions
     * Return NULL if path/index does not exist
     * `type` is an optional out parameter returning the JSONType (can be NULL)
     **/
    RedisJSON (*get)(RedisJSONKey key, const char *path, JSONType *type);
    RedisJSON (*getAt)(RedisJSON json, size_t index, JSONType *type);

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
    int (*getIntFromKey)(RedisJSONKey key, const char *path, long long *integer);

    // Return double value from a Numeric field
    int (*getDouble)(RedisJSON json, double *dbl);
    int (*getDoubleFromKey)(RedisJSONKey key, const char *path, double *dbl);

    // Return 0 or 1 as int value from a Bool field
    int (*getBoolean)(RedisJSON json, int *boolean);
    int (*getBooleanFromKey)(RedisJSONKey key, const char *path, int *boolean);

    // Return a Read-Only String value from a String field
    int (*getString)(RedisJSON json, const char **str, size_t *len);
    int (*getStringFromKey)(RedisJSONKey key, const char *path, const char **str,
                            size_t *len);

    // Return JSON String representation (for any JSONType)
    // The caller gains ownership of `str`
    int (*getJSON)(RedisJSON json, RedisModuleCtx *ctx, RedisModuleString **str);
    int (*getJSONFromKey)(RedisJSONKey key, RedisModuleCtx *ctx, const char *path,
                          RedisModuleString **str);

    // Return 1 if type of key is JSON
    int (*isJSON)(RedisModuleKey *redis_key);

} RedisJSONAPI_V1;

#ifdef __cplusplus
}
#endif

