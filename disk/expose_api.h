#pragma once
#include "search_disk_api.h"

#ifdef __cplusplus
extern "C" {
#endif

RedisSearchDiskAPI *SearchDisk_GetAPI(RedisModuleCtx *ctx);

#ifdef __cplusplus
}
#endif