
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
  // TEXT and VECTOR fields can be represented as array
  case JSONType_Array:
    if (fieldType == INDEXFLD_T_FULLTEXT  || fieldType == INDEXFLD_T_VECTOR || fieldType == INDEXFLD_T_NUMERIC) {
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

typedef int (*getJSONElementFunc)(RedisJSON, void *);
int JSON_StoreVectorAt(RedisJSON arr, size_t len, getJSONElementFunc getElement, char *target, unsigned char step) {
  for (int i = 0; i < len; ++i) {
    RedisJSON json = japi->getAt(arr, i);
    if (getElement(json, target) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
    target += step;
  }
  return REDISMODULE_OK;
}

getJSONElementFunc VecSimGetJSONCallback(VecSimType type) {
  // The right function will put a value of the right type in the address given, or return REDISMODULE_ERR
  switch (type) {
    default:
    case VecSimType_FLOAT32:
      return (getJSONElementFunc)JSON_getFloat32;
     case VecSimType_FLOAT64:
      return (getJSONElementFunc)JSON_getFloat64;
    // Uncomment when support for more types is added
    // case VecSimType_INT32:
    //   return (getJSONElementFunc)JSON_getInt32;
    // case VecSimType_INT64:
    //   return (getJSONElementFunc)japi->getInt;
  }
}

int JSON_StoreSingleVectorInDocField(FieldSpec *fs, RedisJSON arr, struct DocumentField *df) {
  VecSimType type;
  size_t dim;
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

  getElement = VecSimGetJSONCallback(type);

  if (!(df->strval = rm_malloc(fs->vectorOpts.expBlobSize))) {
    return REDISMODULE_ERR;
  }
  df->strlen = fs->vectorOpts.expBlobSize;

  // At this point array length matches blob length
  if (JSON_StoreVectorAt(arr, arrLen, getElement, df->strval, VecSimType_sizeof(type)) != REDISMODULE_OK) {
    rm_free(df->strval);
    return REDISMODULE_ERR;
  }
  df->unionType = FLD_VAR_T_CSTR;
  return REDISMODULE_OK;
}

int JSON_StoreMultiVectorInDocField(FieldSpec *fs, JSONIterable *itr, size_t len, struct DocumentField *df) {
  VecSimType type;
  size_t dim;
  bool multi;
  getJSONElementFunc getElement;
  RedisJSON element;

  switch (fs->vectorOpts.vecSimParams.algo) {
    case VecSimAlgo_HNSWLIB:
      type = fs->vectorOpts.vecSimParams.hnswParams.type;
      dim = fs->vectorOpts.vecSimParams.hnswParams.dim;
      multi = fs->vectorOpts.vecSimParams.hnswParams.multi;
      break;
    case VecSimAlgo_BF:
      type = fs->vectorOpts.vecSimParams.bfParams.type;
      dim = fs->vectorOpts.vecSimParams.bfParams.dim;
      multi = fs->vectorOpts.vecSimParams.bfParams.multi;
      break;
    default: return REDISMODULE_ERR;
  }

  if (!multi) {
    if (len == 1) { // TODO: should we accept?
      return JSON_StoreSingleVectorInDocField(fs, JSONIterable_Next(itr), df);
    } else {
      return REDISMODULE_ERR;
    }
  }

  getElement = VecSimGetJSONCallback(type);
  unsigned char step = VecSimType_sizeof(type);

  if (!(df->blobArr = rm_malloc(fs->vectorOpts.expBlobSize * len))) {
    return REDISMODULE_ERR;
  }
  df->blobSize = fs->vectorOpts.expBlobSize;
  size_t count = 0;

  while ((element = JSONIterable_Next(itr))) {
    JSONType jsonType = japi->getType(element);
    if (JSONType_Null == jsonType) {
      continue; // Skips Nulls.
    } else if (JSONType_Array != jsonType) {
      rm_free(df->blobArr);
      return REDISMODULE_ERR;
    }
    size_t cur_dim;
    if ((REDISMODULE_OK != japi->getLen(element, &cur_dim)) || (cur_dim != dim)) {
      rm_free(df->blobArr);
      return REDISMODULE_ERR;
    }
    if (REDISMODULE_OK != JSON_StoreVectorAt(element, cur_dim, getElement, df->blobArr + df->blobSize * count, step)) {
      rm_free(df->blobArr);
      return REDISMODULE_ERR;
    }
    count++;
  }
  df->blobArrLen = count;
  df->unionType = FLD_VAR_T_BLOB_ARRAY;
  return REDISMODULE_OK;
}

int JSON_StoreMultiVectorInDocFieldFromIter(FieldSpec *fs, JSONResultsIterator jsonIter, size_t len, struct DocumentField *df) {
  JSONIterable iter = (JSONIterable) {.type = ITERABLE_ITER,
                                      .iter = jsonIter};
  return JSON_StoreMultiVectorInDocField(fs, &iter, len, df);
}

int JSON_StoreMultiVectorInDocFieldFromArr(FieldSpec *fs, RedisJSON arr, size_t len, struct DocumentField *df) {
  JSONIterable iter = (JSONIterable) {.type = ITERABLE_ARRAY,
                                      .array.arr = arr,
                                      .array.index = 0};
  return JSON_StoreMultiVectorInDocField(fs, &iter, len, df);
}

int JSON_StoreVectorInDocField(FieldSpec *fs, RedisJSON arr, struct DocumentField *df) {
  size_t len;
  japi->getLen(arr, &len);
  if (len == 0)
    return REDISMODULE_ERR;

  RedisJSON el = japi->getAt(arr, 0); // We know there is at least one element in the array.
  switch (japi->getType(el)) {
    case JSONType_Int:
    case JSONType_Double:
      return JSON_StoreSingleVectorInDocField(fs, arr, df);
    case JSONType_Array:
      return JSON_StoreMultiVectorInDocFieldFromArr(fs, arr, len, df);
    default: return REDISMODULE_ERR;
  }
}

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
        // Handling multiple values as Text
        rv = JSON_StoreTextInDocFieldFromIter(len, jsonIter, df);
        break;
      case INDEXFLD_T_NUMERIC:
        // Handling multiple values as Numeric
        rv = JSON_StoreNumericInDocFieldFromIter(len, jsonIter, df);
        break;
      case INDEXFLD_T_VECTOR:;
        // Handling multiple values as Vector
        rv = JSON_StoreMultiVectorInDocFieldFromIter(fs, jsonIter, len, df);
        break;
      default:
        rv = REDISMODULE_ERR;
        break;
    }
  }

  return rv;
}
