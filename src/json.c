
#include "json.h"
#include "document.h"
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

int JSON_GetRedisModuleString(RedisModuleCtx *ctx, JSONResultsIterator jsonIter,
                              FieldType ftype, struct DocumentField *df) {
  RedisJSON json = japi->next(jsonIter);
  if (!json) {
    RS_LOG_ASSERT(0, "shouldn't happen");
  }

  if (FieldSpec_CheckJsonType(ftype, japi->getType(json))) {
    return REDISMODULE_ERR;
  }
  
  int boolval;
  long long intval;
  const char *str;
  size_t len;
  int rv = REDISMODULE_OK;

  switch (japi->getType(json)) {
    case JSONType_String:
      japi->getString(json, &str, &df->strlen);
      df->strval = rm_strndup(str, df->strlen);
      df->unionType = FLD_VAR_T_CSTR;
      break;
    case JSONType_Int:
      japi->getInt(json, &intval);
      df->numval = intval;
      df->unionType = FLD_VAR_T_NUM;
      break;
    case JSONType_Double:
      japi->getDouble(json, &df->numval);
      df->unionType = FLD_VAR_T_NUM;
      break;
    case JSONType_Bool:
      japi->getBoolean(json, &boolval);
      if (boolval) {
        df->strlen = strlen("true");
        df->strval = rm_strndup("true", df->strlen);
      } else {
        df->strlen = strlen("false");
        df->strval = rm_strndup("false", df->strlen);
      }
      df->unionType = FLD_VAR_T_CSTR;
      break;
    case JSONType_Array: {
      if (ftype == INDEXFLD_T_TAG) {
        // An array can be indexed only as `TAG` field
        size_t arrayLen;
        japi->getLen(json, &arrayLen);
        df->multiVal = rm_calloc(arrayLen, sizeof(*df->multiVal));
        df->arrayLen = arrayLen;
        
        int i = 0;
        for (int i = 0; i < arrayLen; ++i) {
          RedisJSON arrayJson = japi->getAt(json, i);
          if (japi->getType(arrayJson) != JSONType_String) {
            // TAG fields can index only strings
            for (int j = 0; j < i; ++j) {
              rm_free(df->multiVal[j]);
            }
            rm_free(df->multiVal);
            return REDISMODULE_ERR;
          }
          japi->getString(arrayJson, &str, &len);
          df->multiVal[i] = rm_strndup(str, len);
        }
        df->unionType = FLD_VAR_T_ARRAY;
        } else {
          rv = REDISMODULE_ERR;
        }
      }
      break;
    case JSONType_Object:
    case JSONType_Null:
      rv = REDISMODULE_ERR;
      break;
    case JSONType__EOF:
      RS_LOG_ASSERT(0, "Should not happen");
  }
  return rv;
}
