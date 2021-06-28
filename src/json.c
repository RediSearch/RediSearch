
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

int JSON_GetRedisModuleString(RedisModuleCtx *ctx, RedisJSON json, RedisModuleString **str) {
  size_t len;
  const char *cstr;
  long long intval;
  double dblval;
  int boolval;

  int rv = REDISMODULE_OK;

  switch (japi->getType(json)) {
    case JSONType_String:
      japi->getString(json, &cstr, &len);
      *str = RedisModule_CreateString(ctx, cstr, len);
      break;
    case JSONType_Int:
      japi->getInt(json, &intval);
      *str = RedisModule_CreateStringFromLongLong(ctx, intval);
      break;
    case JSONType_Double:
      japi->getDouble(json, &dblval);
      *str = RedisModule_CreateStringFromDouble(ctx, dblval);
      break;
    case JSONType_Bool:
      japi->getBoolean(json, &boolval);
      if (boolval) {
        *str = RedisModule_CreateString(ctx, "true", strlen("true"));
      } else {
        *str = RedisModule_CreateString(ctx, "false", strlen("false"));
      }
      break;
    case JSONType_Object:
    case JSONType_Array:
    case JSONType_Null:
      rv = REDISMODULE_ERR;
    case JSONType__EOF:
      RS_LOG_ASSERT(0, "Should not happen");
  }
  return rv;
}

int FieldSpec_CheckJsonType(FieldType fieldType, JSONType type) {
  int rv = REDISMODULE_ERR;
  switch (type) {

    // TEXT, TAG and GEO fields are represented as string 
  case JSONType_String:
    if (fieldType == INDEXFLD_T_FULLTEXT || fieldType == INDEXFLD_T_TAG || fieldType == INDEXFLD_T_GEO) {
      rv = REDISMODULE_OK;
    }
    break;
  // NUMERIC field is represented as either integer or double 
  case JSONType_Int:
  case JSONType_Double:
    if (fieldType == INDEXFLD_T_NUMERIC) {
      rv = REDISMODULE_OK;
    }
    break;
  // Boolean values can be represented only as TAG 
  case JSONType_Bool:    
  // An array can be represented only as TAG 
  case JSONType_Array:
    if (fieldType == INDEXFLD_T_TAG) {
      rv = REDISMODULE_OK;
    }
    break;
  // An object or null type are not supported
  case JSONType_Object:
  case JSONType_Null:
  case JSONType__EOF:
    break;
  }
  return rv;
}