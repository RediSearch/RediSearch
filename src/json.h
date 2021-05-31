#pragma once

#include "rejson_api.h"
#include "redismodule.h"
//#include "document.h"
//#include "doc_types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern RedisJSONAPI_V1 *japi;
// extern RedisModuleCtx *RSDummyContext;

#define JSON_ROOT "$"

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange);

#ifdef __cplusplus
}
#endif
