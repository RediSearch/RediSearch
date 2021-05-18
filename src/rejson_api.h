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
     * Return NULL if path does not exist
     * `count` can be NULL and return 0 for non array/object
     **/
    RedisJSON (*get)(RedisJSONKey key, const char *path, JSONType *type, size_t *count);

    RedisJSON (*getAt)(RedisJSON jsonIn, size_t index, JSONType *type, size_t *count);

    void (*close)(RedisJSON json);

    /* RedisJSON value functions
     * Return REDISMODULE_OK if RedisJSON is of the correct JSONType,
     * else REDISMODULE_ERR is returned
     **/
    // Return int value from a Numeric, String or Bool field
    int (*getInt)(RedisJSON json, long long *integer);
    int (*getIntFromKey)(RedisJSONKey key, const char *path, long long *integer);

    // Return double value from a Numeric, String or Bool field
    int (*getDouble)(RedisJSON json, double *dbl);
    int (*getDoubleFromKey)(RedisJSONKey key, const char *path, double *dbl);

    // Return 0 or 1 as int value from a Numeric, String or Bool field
    // Empty String returns 0
    int (*getBoolean)(RedisJSON json, int *boolean);
    int (*getBooleanFromKey)(RedisJSONKey key, const char *path, int *boolean);

    // Callee has ownership of `str` - valid until api `close` is called
    // Return the String for String
    // Return the String representation for Numeric
    // Return true or false for Bool
    // Return the String representation for Array and Object if strict=0
    // Return REDISMODULE_ERR for Array and Object if strict=1
    // Return empty string for null
    int (*getString)(RedisJSON json, const char **str, size_t *len, int isStrict);
    int (*getStringFromKey)(RedisJSONKey key, const char *path, const char **str, size_t *len, int isStrict);

    // Caller gains ownership of `str`
    int (*getRedisModuleString)(RedisJSON json, RedisModuleString **str, int isStrict);
    int (*getRedisModuleStringFromKey)(RedisJSONKey key, const char *path, RedisModuleString **str, int isStrict);
} RedisJSONAPI_V1;

#ifdef __cplusplus
}
#endif

