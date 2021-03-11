#ifndef SRC_REJSON_API_H_
#define SRC_REJSON_API_H_

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

//typedef int (*get_json_path)(RedisModuleKey* key, const char* path);
typedef RedisModuleString * (*get_json_path)(struct RedisModuleCtx* ctx, RedisModuleString* key_name, const char* path);

#ifdef __cplusplus
}
#endif
#endif /* SRC_REJSON_API_H_ */
