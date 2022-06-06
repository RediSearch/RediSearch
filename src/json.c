
#include "json.h"
#include "document.h"
#include "rmutil/rm_assert.h"
#include "vector_index.h"

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

  if (!GetJSONAPIs(ctx, 0)) {
    RedisModule_Log(ctx, "error", "Detected RedisJSON: failed to acquire ReJSON API");
  }
  //TODO: Once registered we can unsubscribe from ServerEvent RedisModuleEvent_ModuleChange
  // Unless we want to handle ReJSON module unload
}

//---------------------------------------------------------------------------------------------

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange) {
    japi = RedisModule_GetSharedAPI(ctx, "RedisJSON_V1");
    if (japi) {
        RedisModule_Log(ctx, "notice", "Acquired RedisJSON_V1 API");
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
    if (fieldType == INDEXFLD_T_TAG) {
      rv = REDISMODULE_OK;
    }
    break;
  case JSONType_Null:
    rv = REDISMODULE_OK;
    break;
  // VECTOR field is represented as array
  case JSONType_Array:
    if (fieldType == INDEXFLD_T_VECTOR) {
      rv = REDISMODULE_OK;
    }
    break;
  // An object or null type are not supported
  case JSONType_Object:
  case JSONType__EOF:
    break;
  }

  return rv;
}

// Uncomment when support for more types is added
// static int JSON_getInt32(RedisJSON json, int32_t *val) {
//   long long temp;
//   int ret = japi->getInt(json, &temp);
//   *val = (int32_t)temp;
//   return ret;
// }

static int JSON_getFloat32(RedisJSON json, float *val) {
  double temp;
  int ret = japi->getDouble(json, &temp);
  if (REDISMODULE_OK == ret) {
    *val = (float)temp;
    return ret;
  } else {
    // On RedisJSON<2.0.9, getDouble can't handle integer values.
    long long tempInt;
    ret = japi->getInt(json, &tempInt);
    *val = (float)tempInt;
    return ret;
  }
}

// Uncomment when support for more types is added
// static int JSON_getFloat64(RedisJSON json, double *val) {
//   int ret = japi->getDouble(json, val);
//   if (REDISMODULE_OK == ret) {
//     return ret;
//   } else {
//     long long temp;
//     ret = japi->getInt(json, &temp);
//     *val = (double)temp;
//     return ret;
//   }
// }

int JSON_StoreVectorInDocField(FieldSpec *fs, JSONResultsIterator arrIter, struct DocumentField *df) {
  VecSimType type;
  size_t dim;
  typedef int (*getJSONElementFunc)(RedisJSON, void *);
  getJSONElementFunc getElement;

  switch (fs->vectorOpts.vecSimParams.algo) {
    case VecSimAlgo_HNSWLIB:
      type = fs->vectorOpts.vecSimParams.hnswParams.type;
      dim = fs->vectorOpts.vecSimParams.hnswParams.dim;
      break;
    case VecSimAlgo_BF:
      type = fs->vectorOpts.vecSimParams.bfParams.type;
      dim = fs->vectorOpts.vecSimParams.bfParams.dim;
      break;
    default: return REDISMODULE_ERR;
  }
  if (japi->len(arrIter) != dim) {
    return REDISMODULE_ERR;
  }

  // The right function will put a value of the right type in the address given, or return REDISMODULE_ERR
  switch (type) {
    default:
    case VecSimType_FLOAT32:
      getElement = (getJSONElementFunc)JSON_getFloat32;
      break;
    // Uncomment when support for more types is added
    // case VecSimType_FLOAT64:
    //   getElement = (getJSONElementFunc)JSON_getFloat64;
    //   break;
    // case VecSimType_INT32:
    //   getElement = (getJSONElementFunc)JSON_getInt32;
    //   break;
    // case VecSimType_INT64:
    //   getElement = (getJSONElementFunc)japi->getInt;
    //   break;
  }

  if (!(df->strval = rm_malloc(fs->vectorOpts.expBlobSize))) {
    return REDISMODULE_ERR;
  }
  df->strlen = fs->vectorOpts.expBlobSize;

  RedisJSON json;
  unsigned char step = VecSimType_sizeof(type);
  char *offset = df->strval;
  // At this point iterator length matches blob length
  while ((json = japi->next(arrIter))) {
    if (getElement(json, offset) != REDISMODULE_OK) {
      rm_free(df->strval);
      return REDISMODULE_ERR;
    }
    offset += step;
  }
  df->unionType = FLD_VAR_T_CSTR;
  return REDISMODULE_OK;
}

int JSON_StoreInDocField(RedisJSON json, JSONType jsonType, FieldSpec *fs, struct DocumentField *df) {
  int rv = REDISMODULE_OK;

  int boolval;
  const char *str;
  long long intval;

  switch (jsonType) {
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
    case JSONType_Null:
      df->unionType = FLD_VAR_T_NULL;
      break;
    case JSONType_Array:;
      // Flattening the array to go over it with iterator api
      JSONResultsIterator arrIter = japi->get(json, "$.[*]");
      rv = JSON_StoreVectorInDocField(fs, arrIter, df);
      japi->freeIter(arrIter);
      break;
    case JSONType_Object:
      rv = REDISMODULE_ERR;
      break;
    case JSONType__EOF:
      RS_LOG_ASSERT(0, "Should not happen");
  }

  return rv;
}

int JSON_StoreTagsInDocField(size_t len, JSONResultsIterator jsonIter, struct DocumentField *df) {
    df->multiVal = rm_calloc(len , sizeof(*df->multiVal));
    df->arrayLen = len;

    int i = 0;
    size_t strlen;
    RedisJSON json;
    const char *str;
    while ((json = japi->next(jsonIter))) {
      if (japi->getType(json) != JSONType_String) {
        // TAG fields can index only strings
        for (int j = 0; j < i; ++j) {
          rm_free(df->multiVal[j]);
        }
        rm_free(df->multiVal);
        return REDISMODULE_ERR;
      }
      japi->getString(json, &str, &strlen);
      df->multiVal[i++] = rm_strndup(str, strlen);
    }
    RS_LOG_ASSERT (i == len, "Iterator count and len must be equal");
    df->unionType = FLD_VAR_T_ARRAY;
    return REDISMODULE_OK;
}

int JSON_LoadDocumentField(JSONResultsIterator jsonIter, size_t len,
                              FieldSpec *fs, struct DocumentField *df) {
  int rv = REDISMODULE_OK;

  if (len == 1) {
    RedisJSON json = japi->next(jsonIter);

    JSONType jsonType = japi->getType(json);
    if (FieldSpec_CheckJsonType(fs->types, jsonType) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }

    if (JSON_StoreInDocField(json, jsonType, fs, df) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  } else if (fs->types == INDEXFLD_T_TAG) {
    // Handling multiple values as a Tag list
    rv = JSON_StoreTagsInDocField(len, jsonIter, df);
  } else {
    rv = REDISMODULE_ERR;
  }

  return rv;
}
