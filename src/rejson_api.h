#ifndef SRC_REJSON_API_H_
#define SRC_REJSON_API_H_

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum JSONType {
    JSONType_String = 0,
    JSONType_Int = 1,
    JSONType_Float = 2,
    JSONType_Bool = 3,
    JSONType_Object = 4,
    JSONType_Array = 5,
    JSONType_Null = 6,
    JSONType_Err = 7,
    JSONType__EOF
} JSONType;

typedef const void *RedisJSONKey;
typedef const void *RedisJSONPath;

typedef struct RedisJSONAPI_V1 {
    RedisJSONKey (*openKey)(struct RedisModuleCtx* ctx, RedisModuleString* key_name);
    RedisJSONPath (*getPath)(RedisJSONKey opened_key, const char* path);
    int (*getInfo)(RedisJSONPath, const char **name, int *type, size_t *items);
    void (*closeKey)(RedisJSONKey);
    void (*closePath)(RedisJSONPath);
} RedisJSONAPI_V1;

RedisJSONAPI_V1 *japi;

#ifdef __cplusplus
}
#endif
#endif /* SRC_REJSON_API_H_ */
