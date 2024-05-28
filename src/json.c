/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


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
    RedisModule_Log(ctx, "warning", "Detected RedisJSON: failed to acquire ReJSON API");
  }
}

//---------------------------------------------------------------------------------------------

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange) {
    char ver[128];
    // Obtain the newest version of JSON API
    for (int i = RedisJSONAPI_LATEST_API_VER; i >= 1; --i) {
      sprintf(ver, "RedisJSON_V%d", i);
      japi = RedisModule_GetSharedAPI(ctx, ver);
      if (japi) {
        japi_ver = i;
        RedisModule_Log(ctx, "notice", "Acquired RedisJSON_V%d API", i);
        return 1;
      }
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

int FieldSpec_CheckJsonType(FieldType fieldType, JSONType type, QueryError *status) {
  int rv = REDISMODULE_ERR;
  switch (type) {
  // TEXT, TAG and GEO fields are represented as string
  // GEOMETRY field can be represented as WKT string
  case JSONType_String: {

    if (fieldType & (INDEXFLD_T_FULLTEXT | INDEXFLD_T_TAG | INDEXFLD_T_GEO | INDEXFLD_T_GEOMETRY)) {
      rv = REDISMODULE_OK;
    } else {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid JSON type: String type can represent only TEXT, TAG, GEO or GEOMETRY field");
    }
    break;
  }
  // NUMERIC field is represented as either integer or double
  case JSONType_Int:
  case JSONType_Double:
    if (fieldType == INDEXFLD_T_NUMERIC) {
      rv = REDISMODULE_OK;
    } else {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid JSON type: Numeric type can represent only NUMERIC field");
    }
    break;
  // Boolean values can be represented only as TAG
  case JSONType_Bool:
    if (fieldType == INDEXFLD_T_TAG) {
      rv = REDISMODULE_OK;
    } else {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid JSON type: Boolean type can be represent only TAG field");
    }
    break;
  case JSONType_Null:
    rv = REDISMODULE_OK;
    break;
  case JSONType_Array:
    if (!(fieldType & INDEXFLD_T_GEOMETRY)) { // TODO: GEOMETRY Handle multi-value geometry
      rv = REDISMODULE_OK;
    } else {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid JSON type: Array type cannot represent GEOMETRY field");
    }
    break;
  case JSONType_Object:
    if (fieldType == INDEXFLD_T_GEOMETRY) {
      // A GEOSHAPE field can be represented as GEOJSON "geoshape" object
      rv = REDISMODULE_OK;
    } else {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid JSON type: Object type can represent only GEOMETRY fields");
    }
    break;
  // null type is not supported
  case JSONType__EOF: {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid JSON type: Null type is not supported");
      break;
    }
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

#define BIT_CAST(type, value) ({ \
  __typeof__(value) value_var = (value); \
  static_assert(sizeof(type) == sizeof(value_var), "type and value must have the same size"); \
  const void *ptr = &(value_var); \
  *(type *)ptr; \
})

// Rounding the input to nearest even (float with 16 trailing zeros),
// and returning the 16 significant bits of the float as uint16_t.
// Calculation inspired by:
// https://gitlab.com/libeigen/eigen/-/blob/d626762e3ff6cdcdf65325e6edf27c995029786d/Eigen/src/Core/arch/Default/BFloat16.h#L403
static inline uint16_t floatToBF16bits(float input) {
  uint32_t f32 = BIT_CAST(uint32_t, input);
  uint32_t lsb = (f32 >> 16) & 1;
  uint32_t round = lsb + 0x7FFF;
  f32 += round;
  return (f32 >> 16);
}

static int JSON_getBFloat16(RedisJSON json, uint16_t *val) {
  double temp;
  int ret = japi->getDouble(json, &temp);
  if (REDISMODULE_OK == ret) {
    *val = floatToBF16bits((float)temp);
  }
  return ret;
}

// via Fabian "ryg" Giesen.
// https://gist.github.com/2156668
// Not handling INF or NaN (we don't expect them, and we don't handle them elsewhere)
static inline uint16_t floatToFP16bits(float input) {
  const uint32_t sign_mask = 1u << 31;
  const uint32_t f16infty = 31u << 23;
  const uint32_t round_mask = ~0xfffu;
  const uint32_t magic_bits = 15u << 23;
  const float magic = BIT_CAST(const float, magic_bits);

  uint32_t f32 = BIT_CAST(uint32_t, input);
  uint32_t sign = f32 & sign_mask;
  f32 ^= sign;

  float fscale = BIT_CAST(float, f32 & round_mask) * magic;
  uint32_t f16 = BIT_CAST(uint32_t, fscale) - round_mask;
  if (f16 > f16infty) f16 = f16infty; // Clamp to signed infinity if overflowed

  return ((f16 >> 13) | (sign >> 16));
}

static int JSON_getFloat16(RedisJSON json, uint16_t *val) {
  double temp;
  int ret = japi->getDouble(json, &temp);
  if (REDISMODULE_OK == ret) {
    *val = floatToFP16bits((float)temp);
  }
  return ret;
}

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
int JSON_StoreVectorAt(RedisJSON arr, size_t len, getJSONElementFunc getElement, char *target, unsigned char step, QueryError* status) {
  for (int i = 0; i < len; ++i) {
    RedisJSON json = japi->getAt(arr, i);
    if (getElement(json, target) != REDISMODULE_OK) {
      QueryError_SetErrorFmt(status, QUERY_EGENERIC, "Invalid vector element at index %d", i);
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
    case VecSimType_FLOAT16:
      return (getJSONElementFunc)JSON_getFloat16;
    case VecSimType_BFLOAT16:
      return (getJSONElementFunc)JSON_getBFloat16;
    // Uncomment when support for more types is added
    // case VecSimType_INT32:
    //   return (getJSONElementFunc)JSON_getInt32;
    // case VecSimType_INT64:
    //   return (getJSONElementFunc)japi->getInt;
  }
}

int JSON_StoreSingleVectorInDocField(FieldSpec *fs, RedisJSON arr, struct DocumentField *df, QueryError *status) {
  VecSimType type;
  size_t dim;
  getJSONElementFunc getElement;

  VecSimParams *params = &fs->vectorOpts.vecSimParams;
  if (params->algo == VecSimAlgo_TIERED) {
    params = params->algoParams.tieredParams.primaryIndexParams;
  }

  switch (params->algo) {
    case VecSimAlgo_HNSWLIB:
      type = params->algoParams.hnswParams.type;
      dim = params->algoParams.hnswParams.dim;
      break;
    case VecSimAlgo_BF:
      type = params->algoParams.bfParams.type;
      dim = params->algoParams.bfParams.dim;
      break;
    default: {
      QueryError_SetError(status, QUERY_EGENERIC, "Invalid vector similarity algorithm");
      return REDISMODULE_ERR;
    }
  }
  size_t arrLen;
  japi->getLen(arr, &arrLen);
  if (arrLen != dim) {
    QueryError_SetErrorFmt(status, QUERY_EGENERIC, "Invalid vector length. Expected %lu, got %lu", dim, arrLen);
    return REDISMODULE_ERR;
  }

  getElement = VecSimGetJSONCallback(type);

  if (!(df->strval = rm_malloc(fs->vectorOpts.expBlobSize))) {
    QueryError_SetError(status, QUERY_EGENERIC, "Failed to allocate memory for vector");
    return REDISMODULE_ERR;
  }
  df->strlen = fs->vectorOpts.expBlobSize;

  // At this point array length matches blob length
  if (JSON_StoreVectorAt(arr, arrLen, getElement, df->strval, VecSimType_sizeof(type), status) != REDISMODULE_OK) {
    rm_free(df->strval);
    return REDISMODULE_ERR;
  }
  df->unionType = FLD_VAR_T_CSTR;
  return REDISMODULE_OK;
}

int JSON_StoreMultiVectorInDocField(FieldSpec *fs, JSONIterable *itr, size_t len, struct DocumentField *df, QueryError *status) {
  VecSimType type;
  size_t dim;
  bool multi;
  getJSONElementFunc getElement;
  RedisJSON element;

  VecSimParams *params = &fs->vectorOpts.vecSimParams;
  if (params->algo == VecSimAlgo_TIERED) {
    params = params->algoParams.tieredParams.primaryIndexParams;
  }

switch (params->algo) {
    case VecSimAlgo_HNSWLIB:
      type = params->algoParams.hnswParams.type;
      dim = params->algoParams.hnswParams.dim;
      multi = params->algoParams.hnswParams.multi;
      break;
    case VecSimAlgo_BF:
      type = params->algoParams.bfParams.type;
      dim = params->algoParams.bfParams.dim;
      multi = params->algoParams.bfParams.multi;
      break;
    default: goto fail;
  }

  if (!multi)
    goto fail;

  getElement = VecSimGetJSONCallback(type);
  unsigned char step = VecSimType_sizeof(type);

  if (!(df->blobArr = rm_malloc(fs->vectorOpts.expBlobSize * len))) {
    goto fail;
  }
  df->blobSize = fs->vectorOpts.expBlobSize;
  size_t count = 0;

  while ((element = JSONIterable_Next(itr))) {
    JSONType jsonType = japi->getType(element);
    if (JSONType_Null == jsonType) {
      continue; // Skips Nulls.
    } else if (JSONType_Array != jsonType) {
      goto cleanup;
    }
    size_t cur_dim;
    if ((REDISMODULE_OK != japi->getLen(element, &cur_dim)) || (cur_dim != dim)) {
      goto cleanup;
    }
    if (REDISMODULE_OK != JSON_StoreVectorAt(element, cur_dim, getElement, df->blobArr + df->blobSize * count, step, status)) {
      goto cleanup;
    }
    count++; // counts only the valid non-null vectors, so we store only valid vectors continuously.
  }
  df->blobArrLen = count;
  df->unionType = FLD_VAR_T_BLOB_ARRAY;
  return REDISMODULE_OK;

cleanup:
  rm_free(df->blobArr);
fail:
  return REDISMODULE_ERR;
}

int JSON_StoreMultiVectorInDocFieldFromIter(FieldSpec *fs, JSONResultsIterator jsonIter, size_t len, struct DocumentField *df, QueryError *status) {
  JSONIterable iter = (JSONIterable) {.type = ITERABLE_ITER,
                                      .iter = jsonIter};
  return JSON_StoreMultiVectorInDocField(fs, &iter, len, df, status);
}

int JSON_StoreMultiVectorInDocFieldFromArr(FieldSpec *fs, RedisJSON arr, size_t len, struct DocumentField *df, QueryError *status) {
  JSONIterable iter = (JSONIterable) {.type = ITERABLE_ARRAY,
                                      .array.arr = arr,
                                      .array.index = 0};
  return JSON_StoreMultiVectorInDocField(fs, &iter, len, df, status);
}

int JSON_StoreVectorInDocField(FieldSpec *fs, RedisJSON arr, struct DocumentField *df, QueryError *status) {
  size_t len;
  japi->getLen(arr, &len);
  if (len == 0) {
    QueryError_SetError(status, QUERY_EBADVAL, "Empty array for vector field on JSON document");
    return REDISMODULE_ERR;
  }

  RedisJSON el = japi->getAt(arr, 0); // We know there is at least one element in the array.
  switch (japi->getType(el)) {
    case JSONType_Int:
    case JSONType_Double:
      return JSON_StoreSingleVectorInDocField(fs, arr, df, status);
    case JSONType_Array:
      return JSON_StoreMultiVectorInDocFieldFromArr(fs, arr, len, df, status);
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

int JSON_StoreTextInDocField(size_t len, JSONIterable *iterable, struct DocumentField *df, QueryError *status) {
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
      // Text/Tag fields can handle only strings or Nulls
      QueryError_SetError(status, QUERY_EBADVAL, "TEXT/TAG fields can only contain strings or nulls");
      goto error;
    }
  }
  RS_LOG_ASSERT ((i + nulls) == len, "TEXT/TAG iterator count and len must be equal");
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

int JSON_StoreTextInDocFieldFromIter(size_t len, JSONResultsIterator jsonIter, struct DocumentField *df, QueryError *status) {
  JSONIterable iter = (JSONIterable) {.type = ITERABLE_ITER,
                                      .iter = jsonIter};
  return JSON_StoreTextInDocField(len, &iter, df, status);
}

int JSON_StoreTextInDocFieldFromArr(RedisJSON arr, struct DocumentField *df, QueryError *status) {
  size_t len;
  japi->getLen(arr, &len);
  JSONIterable iter = (JSONIterable) {.type = ITERABLE_ARRAY,
                                      .array.arr = arr,
                                      .array.index = 0};
  return JSON_StoreTextInDocField(len, &iter, df, status);
}

int JSON_StoreNumericInDocField(size_t len, JSONIterable *iterable, struct DocumentField *df, QueryError *status) {
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
      QueryError_SetError(status, QUERY_EBADVAL, "NUMERIC fields can only contain numeric or nulls");
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

int JSON_StoreNumericInDocFieldFromIter(size_t len, JSONResultsIterator jsonIter, struct DocumentField *df, QueryError *status) {
  JSONIterable iter = (JSONIterable) {.type = ITERABLE_ITER,
                                      .iter = jsonIter};
  return JSON_StoreNumericInDocField(len, &iter, df, status);
}

int JSON_StoreNumericInDocFieldFromArr(RedisJSON arr, struct DocumentField *df, QueryError *status) {
  size_t len;
  japi->getLen(arr, &len);
  JSONIterable iter = (JSONIterable) {.type = ITERABLE_ARRAY,
                                      .array.arr = arr,
                                      .array.index = 0};
  return JSON_StoreNumericInDocField(len, &iter, df, status);
}


int JSON_StoreInDocField(RedisJSON json, JSONType jsonType, FieldSpec *fs, struct DocumentField *df, QueryError *status) {
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
        case INDEXFLD_T_TAG:
        case INDEXFLD_T_GEO:
          // (initially GEO is stored as TEXT)
          rv = JSON_StoreTextInDocFieldFromArr(json, df, status);
          break;
        case INDEXFLD_T_VECTOR:
          rv = JSON_StoreVectorInDocField(fs, json, df, status);
          break;
        case INDEXFLD_T_NUMERIC:
          rv = JSON_StoreNumericInDocFieldFromArr(json, df, status);
          break;
        case INDEXFLD_T_GEOMETRY:
          rv = REDISMODULE_ERR; // TODO: GEOMETRY = JSON_StoreGeometryInDocFieldFromArr(json, df);
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

int JSON_LoadDocumentField(JSONResultsIterator jsonIter, size_t len,
                              FieldSpec *fs, struct DocumentField *df, RedisModuleCtx *ctx, QueryError *status) {
  int rv = REDISMODULE_OK;

  if (len == 1) {
    RedisJSON json = japi->next(jsonIter);

    JSONType jsonType = japi->getType(json);
    if (FieldSpec_CheckJsonType(fs->types, jsonType, status) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }

    if (JSON_StoreInDocField(json, jsonType, fs, df, status) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  } else {
    switch (fs->types) {
      case INDEXFLD_T_TAG:
      case INDEXFLD_T_FULLTEXT:
      case INDEXFLD_T_GEO:
        // Handling multiple values as Text
        // (initially GEO is stored as TEXT)
        rv = JSON_StoreTextInDocFieldFromIter(len, jsonIter, df, status);
        break;
      case INDEXFLD_T_NUMERIC:
        // Handling multiple values as Numeric
        rv = JSON_StoreNumericInDocFieldFromIter(len, jsonIter, df, status);
        break;
      case INDEXFLD_T_VECTOR:;
        // Handling multiple values as Vector
        rv = JSON_StoreMultiVectorInDocFieldFromIter(fs, jsonIter, len, df, status);
        break;
      default:
        rv = REDISMODULE_ERR;
        break;
    }
  }

  df->multisv = NULL;
  // If all is successful up til here,
  // we check whether a multi value is needed to be calculated for SORTABLE (avoiding re-opening the key and re-parsing the path)
  // (requires some API V2 functions to be available)
  if (rv == REDISMODULE_OK && FieldSpec_IsSortable(fs) && df->unionType == FLD_VAR_T_ARRAY && japi_ver >= 3) {
    RSValue *rsv = NULL;
    japi->resetIter(jsonIter);
    // There is no api version (DIALECT) specified during ingestion,
    // So we need to prepare a value using newer api version,
    // in order to be able to handle a query later on with either old or new api version
    if (jsonIterToValue(ctx, jsonIter, APIVERSION_RETURN_MULTI_CMP_FIRST, &rsv) == REDISMODULE_OK) {
      df->multisv = rsv;
    } else {
      rv = REDISMODULE_ERR;
    }
  }
  return rv;
}
