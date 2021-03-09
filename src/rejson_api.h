#ifndef SRC_REJSON_API_H_
#define SRC_REJSON_API_H_

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*RedisModuleAPICallbackKeyChange)(RedisModuleCtx *ctx, RedisModuleKey* key, RedisModuleString* path, RedisModuleString* value);
typedef int (*RedisModuleAPICallbackEvent)(int event);
typedef struct RedisJSONAPI_V1 {
    void (*free_string)(const char* string);
    int (*register_callback_key_change)(RedisModuleAPICallbackKeyChange callback);
    int (*register_callback_event)(RedisModuleAPICallbackEvent callback);
    int (*get_json_path)(RedisModuleKey* key, const char* path);
} RedisJSONAPI_V1;

typedef RedisJSONAPI_V1* (*RedisJSON_GetApiV1)(RedisModuleCtx *ctx);

#ifdef __cplusplus
}
#endif
#endif /* SRC_REJSON_API_H_ */
