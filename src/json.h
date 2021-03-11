#ifndef REJSON_H
#define REJSON_H

#include "rejson_api.h"

#ifdef __cplusplus
extern "C" {
#endif

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange);

#ifdef __cplusplus
}
#endif
#endif /* REJSON_H */