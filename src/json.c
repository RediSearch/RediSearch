#include "json.h"
#include <string.h>
//#include "spec.h"

void ModuleChangeHandler(struct RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {

  REDISMODULE_NOT_USED(e);
  RedisModuleModuleChange *ei = data;
  if (sub == REDISMODULE_SUBEVENT_MODULE_LOADED) {    
    // If RedisJSON module is loaded after RediSearch
    // Need to get the API exported by RedisJSON
    if (strcmp(ei->module_name, "ReJSON") == 0) {
        printf("detected %p loading %s\n", ctx, ei->module_name);
        if (GetJSONAPIs(ctx, 0)) {
            //TODO: Once registered we can unsubscribe from ServerEvent RedisModuleEvent_ModuleChange
            // Unless we want to hanle ReJSON module unload
        }
    }
  }
}

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange) {
  get_json_path get_json_path_ptr = RedisModule_GetSharedAPI(ctx, "get_json_path");
  if (get_json_path_ptr) {
    // expose ptr for later use
    return 1;
  } else if (subscribeToModuleChange) {
    RedisModule_SubscribeToServerEvent(ctx,
        RedisModuleEvent_ModuleChange, ModuleChangeHandler);
  }
  return 0;
}
