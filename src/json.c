
#include "json.h"
#include "rmutil/rm_assert.h"

#include <string.h>

// REJSON APIs
RedisJSONAPI_V1 *japi = NULL;

///////////////////////////////////////////////////////////////////////////////////////////////

void ModuleChangeHandler(struct RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, 
                         RedisModuleModuleChange *ei) {
  REDISMODULE_NOT_USED(e);
  if (sub != REDISMODULE_SUBEVENT_MODULE_LOADED || strcmp(ei->module_name, "ReJSON") || japi)
    return;
  // If RedisJSON module is loaded after RediSearch need to get the API exported by RedisJSON

  if (GetJSONAPIs(ctx, 0)) {
    RedisModule_Log(NULL, "notice", "Detected RedisJSON: Acquired RedisJSON_V1 API");
  } else {
    RedisModule_Log(NULL, "error", "Detected RedisJSON: Failed to acquired RedisJSON_V1 API");
  }
  //TODO: Once registered we can unsubscribe from ServerEvent RedisModuleEvent_ModuleChange
  // Unless we want to hanle ReJSON module unload
}

//---------------------------------------------------------------------------------------------

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange) {
    japi = RedisModule_GetSharedAPI(ctx, "RedisJSON_V1");
    if (japi) {
        RedisModule_Log(NULL, "notice", "Acquired RedisJSON_V1 API");
        return 1;
    }
    if (subscribeToModuleChange) {
        RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ModuleChange,
                                           (RedisModuleEventCallback) ModuleChangeHandler);
    }
    return 0;
}

///////////////////////////////////////////////////////////////////////////////////////////////
