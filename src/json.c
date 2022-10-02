
#include "json.h"
#include "document.h"
#include "rmutil/rm_assert.h"
#include "vector_index.h"

#include <string.h>

// REJSON APIs
RedisJSONAPI *japi = NULL;
int japi_ver = 0;

///////////////////////////////////////////////////////////////////////////////////////////////

void ModuleChangeHandler(struct RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub,
                         RedisModuleModuleChange *ei) {
  REDISMODULE_NOT_USED(e);
  if (sub != REDISMODULE_SUBEVENT_MODULE_LOADED || japi || strcmp(ei->module_name, "ReJSON"))
    return;
  // If RedisJSON module is loaded after RediSearch need to get the API exported by RedisJSON

  if (!GetJSONAPIs(ctx, 0)) {
    RedisModule_Log(ctx, "error", "Detected RedisJSON: failed to acquire ReJSON API");
  }  
}

//---------------------------------------------------------------------------------------------

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange) {
    japi = RedisModule_GetSharedAPI(ctx, "RedisJSON_V2");
    if (japi) {
      japi_ver = 2;
      RedisModule_Log(ctx, "notice", "Acquired RedisJSON_V2 API");
    } else {
      japi = RedisModule_GetSharedAPI(ctx, "RedisJSON_V1");
      if (japi) {
        japi_ver = 1;
        RedisModule_Log(ctx, "notice", "Acquired RedisJSON_V1 API");
      }
    }
    if (japi) {
      return 1;
    }
    if (subscribeToModuleChange) {
      RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ModuleChange,
                                         (RedisModuleEventCallback) ModuleChangeHandler);
    }
    return 0;
}

///////////////////////////////////////////////////////////////////////////////////////////////

JSONPath pathParse(const char *path, RedisModuleString **err_msg) {
  if (japi_ver >= 2) {
    return japi->pathParse(path, RSDummyContext, err_msg);
  } else {
    *err_msg = NULL;
    return NULL;
  }
}

void pathFree(JSONPath jsonpath) {
  if (japi_ver >= 2) {
    japi->pathFree(jsonpath);
  } else {
    // Should not attempt to free none-null path when the required API to parse is not available
    assert(jsonpath != NULL);
  }
}

int pathIsSingle(JSONPath jsonpath) {
  if (japi_ver >= 2) {
    return japi->pathIsSingle(jsonpath);
  } else {
    // Should not use none-null path when the required API to parse is not available
    assert(jsonpath != NULL);
  }
  return false;
}

int pathHasDefinedOrder(JSONPath jsonpath) {
  if (japi_ver >= 2) {
    return japi->pathHasDefinedOrder(jsonpath);
  } else {
    // Should not use none-null path when the required API to parse is not available
    assert(jsonpath != NULL);
  }
  return false;
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
    if (fieldType == INDEXFLD_T_TAG) {
      rv = REDISMODULE_OK;
    }
    break;
  case JSONType_Null:
    rv = REDISMODULE_OK;
    break;
  case JSONType_Array:
    if (fieldType == INDEXFLD_T_FULLTEXT  || fieldType == INDEXFLD_T_VECTOR || fieldType == INDEXFLD_T_NUMERIC || fieldType == INDEXFLD_T_GEO) {
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

static int JSON_getFloat64(RedisJSON json, double *val) {
  int ret = japi->getDouble(json, val);
  if (REDISMODULE_OK == ret) {
    return ret;
  } else {
    // On RedisJSON<2.0.9, getDouble can't handle integer values
    long long temp;
    ret = japi->getInt(json, &temp);
    *val = (double)temp;
    return ret;
  }
}

int JSON_StoreVectorInDocField(FieldSpec *fs, RedisJSON arr, struct DocumentField *df) {
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
  size_t arrLen;
  japi->getLen(arr, &arrLen);
  if (arrLen != dim) {
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

  unsigned char step = VecSimType_sizeof(type);
  char *offset = df->strval;
  // At this point array length matches blob length
  for (int i = 0; i < arrLen; ++i) {
    RedisJSON json = japi->getAt(arr, i);
    if (getElement(json, offset) != REDISMODULE_OK) {
      rm_free(df->strval);
      return REDISMODULE_ERR;
    }
    offset += step;
  }
  df->unionType = FLD_VAR_T_CSTR;
  return REDISMODULE_OK;
}

typedef enum {
  ITERABLE_ITER = 0,
  ITERABLE_ARRAY = 1
} JSONIterableType;

// An adapter for iterator operations, such as `next`, over an underlying container/collection or iterator
typedef struct {
  JSONIterableType type;
  union {
    JSONResultsIterator iter;
    struct {
      RedisJSON arr;
      size_t index;
    } array;
  };
} JSONIterable;

RedisJSON JSONIterable_Next(JSONIterable *iterable) {
  switch (iterable->type) {
    case ITERABLE_ITER:
      return japi->next(iterable->iter);
    
    case ITERABLE_ARRAY:
      return japi->getAt(iterable->array.arr, iterable->array.index++);

    default:
      return NULL;
  }
}

int JSON_StoreTextInDocField(size_t len, JSONIterable *iterable, struct DocumentField *df) {
  df->multiVal = rm_calloc(len , sizeof(*df->multiVal));
  int i = 0, nulls = 0;
  size_t strlen;
  RedisJSON json;
  const char *str;
  while ((json = JSONIterable_Next(iterable))) {
    JSONType jsonType = japi->getType(json);
    if (jsonType == JSONType_String) {
      japi->getString(json, &str, &strlen);
      df->multiVal[i++] = rm_strndup(str, strlen);
    } else if (jsonType == JSONType_Null) {
      nulls++; // Skip Nulls
    } else {
      // Text fields can handle only strings or Nulls
      goto error;
    }
  }
  RS_LOG_ASSERT ((i + nulls) == len, "TEXT iterator count and len must be equal");
  // Remain with surplus unused array entries from skipped null values until `Document_Clear` is called
  df->arrayLen = i;
  df->unionType = FLD_VAR_T_ARRAY;
  return REDISMODULE_OK;

error:
  for (int j = 0; j < i; ++j) {
    rm_free(df->multiVal[j]);
  }
  rm_free(df->multiVal);
  df->arrayLen = 0;
  return REDISMODULE_ERR;
}

int JSON_StoreTextInDocFieldFromIter(size_t len, JSONResultsIterator jsonIter, struct DocumentField *df) {
  JSONIterable iter = (JSONIterable) {.type = ITERABLE_ITER,
                                      .iter = jsonIter};
  return JSON_StoreTextInDocField(len, &iter, df);
}

int JSON_StoreTextInDocFieldFromArr(RedisJSON arr, struct DocumentField *df) {
  size_t len;
  japi->getLen(arr, &len);
  JSONIterable iter = (JSONIterable) {.type = ITERABLE_ARRAY,
                                      .array.arr = arr,
                                      .array.index = 0};
  return JSON_StoreTextInDocField(len, &iter, df);
}

int JSON_StoreNumericInDocField(size_t len, JSONIterable *iterable, struct DocumentField *df) {
  arrayof(double) arr = array_new(double, len);
  int nulls = 0;
  RedisJSON json;
  double dval;
  while ((json = JSONIterable_Next(iterable))) {
    JSONType jsonType = japi->getType(json);
    if (jsonType == JSONType_Double || jsonType == JSONType_Int) {
      JSON_getFloat64(json, &dval);
      array_ensure_append_1(arr, dval);
    } else if (jsonType == JSONType_Null) {
      ++nulls; // Skip Nulls (TODO: consider also failing or converting to a specific value, e.g., zero)
    } else {
      // Numeric fields can handle only numeric or Nulls
      goto error;
    }
  }
  RS_LOG_ASSERT ((array_len(arr) + nulls) == len, "NUMERIC iterator count and len must be equal");
  df->arrNumval = arr;
  df->unionType = FLD_VAR_T_ARRAY;
  return REDISMODULE_OK;

error:
  array_free(arr);
  return REDISMODULE_ERR;
}

int JSON_StoreNumericInDocFieldFromIter(size_t len, JSONResultsIterator jsonIter, struct DocumentField *df) {
  JSONIterable iter = (JSONIterable) {.type = ITERABLE_ITER,
                                      .iter = jsonIter};
  return JSON_StoreNumericInDocField(len, &iter, df);
}

int JSON_StoreNumericInDocFieldFromArr(RedisJSON arr, struct DocumentField *df) {
  size_t len;
  japi->getLen(arr, &len);
  JSONIterable iter = (JSONIterable) {.type = ITERABLE_ARRAY,
                                      .array.arr = arr,
                                      .array.index = 0};
  return JSON_StoreNumericInDocField(len, &iter, df);
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
    case JSONType_Array:
      switch (fs->types) {
        case INDEXFLD_T_FULLTEXT:
        case INDEXFLD_T_GEO:
          // (initially GEO is stored as TEXT)
          rv = JSON_StoreTextInDocFieldFromArr(json, df);
          break;
        case INDEXFLD_T_VECTOR:
          rv = JSON_StoreVectorInDocField(fs, json, df);
          break;
        case INDEXFLD_T_NUMERIC:
          rv = JSON_StoreNumericInDocFieldFromArr(json, df);
          break;
        default:
          rv = REDISMODULE_ERR;
          break;
      }      
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
    RS_LOG_ASSERT (i == len, "TAG iterator count and len must be equal");
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
  } else {
    switch (fs->types) {
      case INDEXFLD_T_TAG:
        // Handling multiple values as a Tag list
        rv = JSON_StoreTagsInDocField(len, jsonIter, df);
        break;
      case INDEXFLD_T_FULLTEXT:
      case INDEXFLD_T_GEO:
        // Handling multiple values as Text
        // (initially GEO is stored as TEXT)
        rv = JSON_StoreTextInDocFieldFromIter(len, jsonIter, df);
        break;
      case INDEXFLD_T_NUMERIC:
        // Handling multiple values as Numeric
        rv = JSON_StoreNumericInDocFieldFromIter(len, jsonIter, df);
        break;
      default:
        rv = REDISMODULE_ERR;
        break;
    }
  }

  return rv;
}
