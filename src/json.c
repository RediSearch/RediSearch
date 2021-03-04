#include "json.h"
#include <string.h>
#include "spec.h"

int JSONKeyChangeHandler(RedisModuleCtx *ctx, RedisModuleKey *key) {
  printf("==> in JSONKeyChangeHandler\n");
  //Indexes_UpdateMatchingWithSchemaRules(ctx, key, 0);
  return 1;
}

void ModuleChangeHandler(struct RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {

  REDISMODULE_NOT_USED(e);
  RedisModuleModuleChange *ei = data;
  if (sub == REDISMODULE_SUBEVENT_MODULE_LOADED) {    
    // If RedisJSON module is loaded after RediSearch
    // Need to get the API exported by RedisJSON and use it to register for events
    if (strcmp(ei->module_name, "ReJSON") == 0) {
        printf("detected %p loading %s\n", ctx, ei->module_name);
        if (RegisterJSONCallbacks(ctx, 0)) {
            //TODO: Once registered we can unsubscribe from ServerEvent RedisModuleEvent_ModuleChange
            // Unless we want to hanle ReJSON module unload
        }
    }
  }
}

int RegisterJSONCallbacks(RedisModuleCtx *ctx, int subscribeToModuleChange) {
  RedisJSON_GetApiV1 get_api_func_ptr = RedisModule_GetSharedAPI(ctx, "RedisJSON_GetApiV1");
  if (get_api_func_ptr) {
    RedisJSONAPI_V1 *rejson_api = get_api_func_ptr(ctx);
    if (rejson_api) {
      rejson_api->register_callback_key_change(JSONKeyChangeHandler);
      return 1;
    }
  } else if (subscribeToModuleChange) {
    RedisModule_SubscribeToServerEvent(ctx,
        RedisModuleEvent_ModuleChange, ModuleChangeHandler);
  }
  return 0;
}
