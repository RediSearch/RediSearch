#ifndef SRC_REJSON_API_H_
#define SRC_REJSON_API_H_

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
    RedisJSONKey (*openKey)(struct RedisModuleCtx* ctx, RedisModuleString* key_name);
    void (*closeKey)(RedisJSONKey key);
    /* RedisJSON functions
     * Return NULL if path does not exist
     * `count` can be NULL and return 0 for for non array/object
     **/
    RedisJSON (*getPath)(RedisJSONKey key, const char *path,
                            JSONType *type, size_t *count);
    RedisJSON (*getAt)(RedisJSON jsonIn, size_t index,
                            JSONType *type, size_t *count);
    void (*closeJSON)(RedisJSON json);
    /* RedisJSON value functions
     * Return REDISMODULE_OK if RedisJSON is of the correct JSONType,
     * else REDISMODULE_ERR is returned
     **/
    int (*getInt)(const RedisJSON *path, int *integer);
    int (*getDouble)(const RedisJSON *path, double *dbl);
    int (*getBoolean)(const RedisJSON *path, int *boolean);
    int (*getString)(const RedisJSON *path, char **str, size_t *len);

    void (*replyWithJSON)(const RedisJSON *json);
} RedisJSONAPI_V1;

RedisJSONAPI_V1 *japi;

#ifdef __cplusplus
}
#endif
#endif /* SRC_REJSON_API_H_ */
