/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "json.h"
#include "document.h"
#include "rmutil/rm_assert.h"
#include "vector_index.h"

#include <math.h>
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
    RedisModule_Log(ctx, "warning", "Detected RedisJSON: failed to acquire ReJSON API. Minimum required version is %d", RedisJSONAPI_MIN_API_VER);
  }
}

//---------------------------------------------------------------------------------------------

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange) {
    char ver[128];
    // Obtain the newest version of JSON API
    for (int i = RedisJSONAPI_LATEST_API_VER; i >= RedisJSONAPI_MIN_API_VER; --i) {
      snprintf(ver, sizeof(ver), "RedisJSON_V%d", i);
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

JSONPath pathParse(const HiddenString* path, RedisModuleString **err_msg) {
  return japi->pathParse(HiddenString_GetUnsafe(path, NULL), RSDummyContext, err_msg);
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
      QueryError_SetError(status, QUERY_ERROR_CODE_JSON_TYPE_BAD,
                         "Invalid JSON type: String type can represent only TEXT, TAG, GEO or GEOMETRY field");
    }
    break;
  }
  // NUMERIC field is represented as either integer or double
  case JSONType_Int:
  case JSONType_Double:
    if (fieldType == INDEXFLD_T_NUMERIC) {
      rv = REDISMODULE_OK;
    } else {
      QueryError_SetError(status, QUERY_ERROR_CODE_JSON_TYPE_BAD,
                         "Invalid JSON type: Numeric type can represent only NUMERIC field");
    }
    break;
  // Boolean values can be represented only as TAG
  case JSONType_Bool:
    if (fieldType == INDEXFLD_T_TAG) {
      rv = REDISMODULE_OK;
    } else {
      QueryError_SetError(status, QUERY_ERROR_CODE_JSON_TYPE_BAD,
                         "Invalid JSON type: Boolean type can be represent only TAG field");
    }
    break;
  case JSONType_Null:
    rv = REDISMODULE_OK;
    break;
  case JSONType_Array:
    if (!(fieldType & INDEXFLD_T_GEOMETRY)) { // TODO: GEOMETRY Handle multi-value geometry
      rv = REDISMODULE_OK;
    } else {
      QueryError_SetError(status, QUERY_ERROR_CODE_JSON_TYPE_BAD,
                         "Invalid JSON type: Array type cannot represent GEOMETRY field");
    }
    break;
  case JSONType_Object:
    if (fieldType == INDEXFLD_T_GEOMETRY) {
      // A GEOSHAPE field can be represented as GEOJSON "geoshape" object
      rv = REDISMODULE_OK;
    } else {
      QueryError_SetError(status, QUERY_ERROR_CODE_JSON_TYPE_BAD,
                         "Invalid JSON type: Object type can represent only GEOMETRY fields");
    }
    break;
  // null type is not supported
  case JSONType__EOF: {
      QueryError_SetError(status, QUERY_ERROR_CODE_JSON_TYPE_BAD,
                         "Invalid JSON type: Null type is not supported");
      break;
    }
  }

  return rv;
}

static JSONIterable JSONIterable_FromArr(RedisJSON arr) {
  return (JSONIterable) {
    .type = ITERABLE_ARRAY,
    .array.arr = arr,
    .array.index = 0,
    .array.value_ptr = japi->allocJson(),
  };
}

static JSONIterable JSONIterable_FromIter(JSONResultsIterator iter) {
  return (JSONIterable) {
    .type = ITERABLE_ITER,
    .iter = iter,
  };
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

// Inverse of `floatToFP16bits`; equivalent to VecSim's `FP16_to_FP32`.
static inline float FP16bitsToFloat(uint16_t input) {
  const uint32_t shifted_exp = 0x7c00u << 13;
  int32_t o = ((int32_t)(input & 0x7fffu)) << 13;
  int32_t exp = shifted_exp & o;
  o += (int32_t)(127 - 15) << 23;

  int32_t infnan_val = o + ((int32_t)(128 - 16) << 23);
  int32_t zerodenorm_val = BIT_CAST(int32_t,
      BIT_CAST(float, (uint32_t)(o + (1 << 23))) - BIT_CAST(float, 113u << 23));
  int32_t reg_val = (exp == 0) ? zerodenorm_val : o;

  int32_t sign_bit = ((int32_t)(input & 0x8000u)) << 16;
  int32_t bits = ((exp == shifted_exp) ? infnan_val : reg_val) | sign_bit;
  return BIT_CAST(float, bits);
}

// Reverse of `floatToBF16bits`: place the 16 bits in the upper half of a float32.
static inline float BF16bitsToFloat(uint16_t input) {
  uint32_t f32 = ((uint32_t)input) << 16;
  return BIT_CAST(float, f32);
}

static int JSON_getBFloat16(RedisJSON json, uint16_t *val) {
  double temp;
  int ret = japi->getDouble(json, &temp);
  *val = floatToBF16bits((float)temp);
  return ret;
}

static int JSON_getFloat16(RedisJSON json, uint16_t *val) {
  double temp;
  int ret = japi->getDouble(json, &temp);
  *val = floatToFP16bits((float)temp);
  return ret;
}

static int JSON_getFloat32(RedisJSON json, float *val) {
  double temp;
  int ret = japi->getDouble(json, &temp);
  *val = (float)temp;
  return ret;
}

static int JSON_getFloat64(RedisJSON json, double *val) {
  return japi->getDouble(json, val);
}

static int JSON_getUint8(RedisJSON json, uint8_t *val) {
  long long temp;
  int ret = japi->getInt(json, &temp);
  *val = (uint8_t)temp;
  return ret;
}

static int JSON_getInt8(RedisJSON json, int8_t *val) {
  long long temp;
  int ret = japi->getInt(json, &temp);
  *val = (int8_t)temp;
  return ret;
}

typedef int (*getJSONElementFunc)(RedisJSON, void *);
static getJSONElementFunc VecSimGetJSONCallback(VecSimType type);

// Returns true iff `t` tags a homogeneous buffer of a known numeric type we can
// read from. This is an explicit opt-in: any future JSONArrayType added upstream
// must be classified here, otherwise it stays safely routed to the V6 per-element
// fallback. The switch has no `default:` clause so `-Wswitch` flags new enumerators
// at compile time until they are handled.
static bool JSON_ArrayTypeIsNumeric(JSONArrayType t) {
  switch (t) {
    case JSONArrayType_I8:
    case JSONArrayType_U8:
    case JSONArrayType_I16:
    case JSONArrayType_U16:
    case JSONArrayType_F16:
    case JSONArrayType_BF16:
    case JSONArrayType_I32:
    case JSONArrayType_U32:
    case JSONArrayType_F32:
    case JSONArrayType_I64:
    case JSONArrayType_U64:
    case JSONArrayType_F64:
      return true;
    case JSONArrayType_Heterogeneous:
      return false;
  }
  // Unknown (future) tags fall through to V6 per-element handling.
  return false;
}

// Returns true iff a JSON numeric element of type `src` can be ingested into a vector
// field of type `target`. This mirrors the per-element accept matrix of the V6 path,
// where each element is read through `japi->getInt` (INT8/UINT8 target) or
// `japi->getDouble` (float targets):
//   - `getInt`    rejects JSON `Double` values (i.e. the F16/BF16/F32/F64 tags).
//   - `getDouble` accepts both JSON `Int` and `Double` values (any numeric tag).
//
// Non-numeric `src` (including Heterogeneous and any future unknown tag) returns
// false so the caller falls back to the V6 iterator. When it returns false for a
// known numeric `src`, the V6 loop would also reject every element of a homogeneous
// array, so the caller can safely short-circuit with an "invalid element at index 0"
// error instead of falling back.
static bool VecSim_AcceptsJSONArrayType(VecSimType target, JSONArrayType src) {
  if (!JSON_ArrayTypeIsNumeric(src)) return false;
  switch (target) {
    case VecSimType_FLOAT32:
    case VecSimType_FLOAT64:
    case VecSimType_FLOAT16:
    case VecSimType_BFLOAT16:
      // Any numeric tag: V6 `getDouble` accepts both Int and Double JSON values.
      return true;
    case VecSimType_INT8:
    case VecSimType_UINT8:
      // Integer tags only: V6 `getInt` rejects JSON Double values.
      switch (src) {
        case JSONArrayType_I8:
        case JSONArrayType_U8:
        case JSONArrayType_I16:
        case JSONArrayType_U16:
        case JSONArrayType_I32:
        case JSONArrayType_U32:
        case JSONArrayType_I64:
        case JSONArrayType_U64:
          return true;
        default:
          return false;
      }
    default:
      return false;
  }
}

// Mirrors the V6 `getDouble` path: promotes the i-th element of `src` to double.
static inline double VecSim_JSONArray_ReadAsDouble(const void *src, size_t i, JSONArrayType j) {
  switch (j) {
    case JSONArrayType_I8:   return (double)((const int8_t *)src)[i];
    case JSONArrayType_U8:   return (double)((const uint8_t *)src)[i];
    case JSONArrayType_I16:  return (double)((const int16_t *)src)[i];
    case JSONArrayType_U16:  return (double)((const uint16_t *)src)[i];
    case JSONArrayType_F16:  return (double)FP16bitsToFloat(((const uint16_t *)src)[i]);
    case JSONArrayType_BF16: return (double)BF16bitsToFloat(((const uint16_t *)src)[i]);
    case JSONArrayType_I32:  return (double)((const int32_t *)src)[i];
    case JSONArrayType_U32:  return (double)((const uint32_t *)src)[i];
    case JSONArrayType_I64:  return (double)((const int64_t *)src)[i];
    case JSONArrayType_U64:  return (double)((const uint64_t *)src)[i];
    case JSONArrayType_F32:  return (double)((const float *)src)[i];
    case JSONArrayType_F64:  return ((const double *)src)[i];
    default:
      RS_ABORT("unexpected JSONArrayType");
      return NAN;
  }
}

// Mirrors the V6 `getInt` path: reads the i-th element of `src` as `long long`.
// Only called for integer JSONArrayTypes (VecSim_AcceptsJSONArrayType enforces this
// for INT8/UINT8 targets).
static inline long long VecSim_JSONArray_ReadAsInt(const void *src, size_t i, JSONArrayType j) {
  switch (j) {
    case JSONArrayType_I8:  return ((const int8_t *)src)[i];
    case JSONArrayType_U8:  return ((const uint8_t *)src)[i];
    case JSONArrayType_I16: return ((const int16_t *)src)[i];
    case JSONArrayType_U16: return ((const uint16_t *)src)[i];
    case JSONArrayType_I32: return ((const int32_t *)src)[i];
    case JSONArrayType_U32: return (long long)((const uint32_t *)src)[i];
    case JSONArrayType_I64: return ((const int64_t *)src)[i];
    case JSONArrayType_U64: return (long long)((const uint64_t *)src)[i];
    default:
      RS_ABORT("unexpected JSONArrayType for integer target");
      return 0;
  }
}

// Writes `n` elements of `src` (tagged `jtype`) into the VecSim blob at `target`,
// converting scalar-by-scalar to match `target_type`. Preconditions:
//   VecSim_AcceptsJSONArrayType(target_type, jtype) == true.
// If source and target layouts are identical, a single `memcpy` is used.
static void VecSim_ConvertFromTypedBuffer(VecSimType target_type, JSONArrayType jtype,
                                          const void *src, size_t n, char *target) {
  if ((target_type == VecSimType_FLOAT32  && jtype == JSONArrayType_F32) ||
      (target_type == VecSimType_FLOAT64  && jtype == JSONArrayType_F64) ||
      (target_type == VecSimType_FLOAT16  && jtype == JSONArrayType_F16) ||
      (target_type == VecSimType_BFLOAT16 && jtype == JSONArrayType_BF16) ||
      (target_type == VecSimType_INT8     && jtype == JSONArrayType_I8) ||
      (target_type == VecSimType_UINT8    && jtype == JSONArrayType_U8)) {
    memcpy(target, src, n * VecSimType_sizeof(target_type));
    return;
  }

  switch (target_type) {
    case VecSimType_FLOAT32: {
      float *dst = (float *)target;
      for (size_t i = 0; i < n; ++i) dst[i] = (float)VecSim_JSONArray_ReadAsDouble(src, i, jtype);
      break;
    }
    case VecSimType_FLOAT64: {
      double *dst = (double *)target;
      for (size_t i = 0; i < n; ++i) dst[i] = VecSim_JSONArray_ReadAsDouble(src, i, jtype);
      break;
    }
    case VecSimType_FLOAT16: {
      uint16_t *dst = (uint16_t *)target;
      for (size_t i = 0; i < n; ++i) dst[i] = floatToFP16bits((float)VecSim_JSONArray_ReadAsDouble(src, i, jtype));
      break;
    }
    case VecSimType_BFLOAT16: {
      uint16_t *dst = (uint16_t *)target;
      for (size_t i = 0; i < n; ++i) dst[i] = floatToBF16bits((float)VecSim_JSONArray_ReadAsDouble(src, i, jtype));
      break;
    }
    case VecSimType_INT8: {
      int8_t *dst = (int8_t *)target;
      for (size_t i = 0; i < n; ++i) dst[i] = (int8_t)VecSim_JSONArray_ReadAsInt(src, i, jtype);
      break;
    }
    case VecSimType_UINT8: {
      uint8_t *dst = (uint8_t *)target;
      for (size_t i = 0; i < n; ++i) dst[i] = (uint8_t)VecSim_JSONArray_ReadAsInt(src, i, jtype);
      break;
    }
    default:
      RS_ABORT("unexpected VecSimType");
      break;
  }
}

// Stores `len` elements from the JSON array `arr` into the VecSim blob at `target`.
// For a homogeneous numeric array, uses the typed-buffer fast path (single `memcpy`
// or a typed conversion loop). Heterogeneous arrays fall back to the per-element
// loop using RedisJSON scalar accessors.
static int JSON_StoreVectorAt(RedisJSON arr, size_t len, VecSimType target_type,
                              char *target, QueryError *status) {
  // Fast path: homogeneous numeric buffer, single allocation-free copy/conversion.
  size_t buf_len = 0;
  JSONArrayType jtype = JSONArrayType_Heterogeneous;
  const void *buf = japi->getArray(arr, &buf_len, &jtype);
  RS_ASSERT(buf_len == len);
  if (buf && JSON_ArrayTypeIsNumeric(jtype)) {
    if (!VecSim_AcceptsJSONArrayType(target_type, jtype)) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_GENERIC, "Invalid vector element at index 0");
      return REDISMODULE_ERR;
    }
    VecSim_ConvertFromTypedBuffer(target_type, jtype, buf, len, target);
    return REDISMODULE_OK;
  }

  // Fallback: per-element conversion via RedisJSON scalar accessors. Covers
  // heterogeneous arrays and any future unknown JSONArrayType tag.
  getJSONElementFunc getElement = VecSimGetJSONCallback(target_type);
  unsigned char step = VecSimType_sizeof(target_type);
  RedisJSONPtr element = japi->allocJson();
  for (size_t i = 0; i < len; ++i) {
    if (japi->getAt(arr, i, element) != REDISMODULE_OK || getElement(*element, target) != REDISMODULE_OK) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_GENERIC, "Invalid vector element at index %zu", i);
      japi->freeJson(element);
      return REDISMODULE_ERR;
    }
    target += step;
  }
  japi->freeJson(element);
  return REDISMODULE_OK;
}

static getJSONElementFunc VecSimGetJSONCallback(VecSimType type) {
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
    case VecSimType_UINT8:
      return (getJSONElementFunc)JSON_getUint8;
    case VecSimType_INT8:
      return (getJSONElementFunc)JSON_getInt8;
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
    case VecSimAlgo_SVS:
      type = params->algoParams.svsParams.type;
      dim = params->algoParams.svsParams.dim;
      break;
    default: {
      QueryError_SetError(status, QUERY_ERROR_CODE_GENERIC, "Invalid vector similarity algorithm");
      return REDISMODULE_ERR;
    }
  }
  size_t arrLen;
  japi->getLen(arr, &arrLen);
  if (arrLen != dim) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_VECTOR_LEN_BAD,
                                     "Invalid vector length. Expected %lu, got %lu", dim, arrLen);
    return REDISMODULE_ERR;
  }

  if (!(df->strval = rm_malloc(fs->vectorOpts.expBlobSize))) {
    QueryError_SetError(status, QUERY_ERROR_CODE_GENERIC, "Failed to allocate memory for vector");
    return REDISMODULE_ERR;
  }
  df->strlen = fs->vectorOpts.expBlobSize;

  // At this point array length matches blob length
  if (JSON_StoreVectorAt(arr, arrLen, type, df->strval, status) != REDISMODULE_OK) {
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
  RedisJSON element;

  size_t count = 0;

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
    case VecSimAlgo_SVS:
      type = params->algoParams.svsParams.type;
      dim = params->algoParams.svsParams.dim;
      multi = params->algoParams.svsParams.multi;
      break;
  break;
    default: goto fail;
  }

  if (!multi)
    goto fail;

  if (!(df->blobArr = rm_malloc(fs->vectorOpts.expBlobSize * len))) {
    goto fail;
  }
  df->blobSize = fs->vectorOpts.expBlobSize;

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
    char *slot = df->blobArr + df->blobSize * count;
    if (REDISMODULE_OK != JSON_StoreVectorAt(element, cur_dim, type, slot, status)) {
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
  JSONIterable iter = JSONIterable_FromIter(jsonIter);
  int ret = JSON_StoreMultiVectorInDocField(fs, &iter, len, df, status);
  JSONIterable_Clean(&iter);
  return ret;
}

int JSON_StoreMultiVectorInDocFieldFromArr(FieldSpec *fs, RedisJSON arr, size_t len, struct DocumentField *df, QueryError *status) {
  JSONIterable iter = JSONIterable_FromArr(arr);
  int ret = JSON_StoreMultiVectorInDocField(fs, &iter, len, df, status);
  JSONIterable_Clean(&iter);
  return ret;
}

int JSON_StoreVectorInDocField(FieldSpec *fs, RedisJSON arr, struct DocumentField *df, QueryError *status) {
  size_t len;
  japi->getLen(arr, &len);
  if (len == 0) {
    QueryError_SetError(status, QUERY_ERROR_CODE_BAD_VAL, "Empty array for vector field on JSON document");
    return REDISMODULE_ERR;
  }

  // Fast probe: a known numeric tag implies a flat numeric array (single vector).
  // Heterogeneous (and any unknown future tag) falls through to the per-element
  // probe below, which also distinguishes single-vs-multi for arrays whose first
  // element is numeric but whose element types are mixed.
  size_t n;
  JSONArrayType jtype = JSONArrayType_Heterogeneous;
  japi->getArray(arr, &n, &jtype);
  if (JSON_ArrayTypeIsNumeric(jtype)) {
    return JSON_StoreSingleVectorInDocField(fs, arr, df, status);
  }

  RedisJSONPtr ptr = japi->allocJson();
  japi->getAt(arr, 0, ptr); // We know there is at least one element in the array.
  JSONType type = japi->getType(*ptr);
  japi->freeJson(ptr);

  switch (type) {
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
      japi->getAt(iterable->array.arr, iterable->array.index++, iterable->array.value_ptr);
      return *(iterable->array.value_ptr);

    default:
      return NULL;
  }
}

void JSONIterable_Clean(JSONIterable *iterable) {
  switch (iterable->type) {
    case ITERABLE_ARRAY:
      japi->freeJson(iterable->array.value_ptr);
      break;
    case ITERABLE_ITER:
      break;
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
      QueryError_SetError(status, QUERY_ERROR_CODE_BAD_VAL, "TEXT/TAG fields can only contain strings or nulls");
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
  JSONIterable iter = JSONIterable_FromIter(jsonIter);
  int ret = JSON_StoreTextInDocField(len, &iter, df, status);
  JSONIterable_Clean(&iter);
  return ret;
}

int JSON_StoreTextInDocFieldFromArr(RedisJSON arr, struct DocumentField *df, QueryError *status) {
  size_t len;
  japi->getLen(arr, &len);
  JSONIterable iter = JSONIterable_FromArr(arr);
  int ret = JSON_StoreTextInDocField(len, &iter, df, status);
  JSONIterable_Clean(&iter);
  return ret;
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
      QueryError_SetError(status, QUERY_ERROR_CODE_BAD_VAL, "NUMERIC fields can only contain numeric or nulls");
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
  JSONIterable iter = JSONIterable_FromIter(jsonIter);
  int ret = JSON_StoreNumericInDocField(len, &iter, df, status);
  JSONIterable_Clean(&iter);
  return ret;
}

int JSON_StoreNumericInDocFieldFromArr(RedisJSON arr, struct DocumentField *df, QueryError *status) {
  size_t len;
  japi->getLen(arr, &len);

  // Fast path: homogeneous numeric buffer -> one dispatch, tight typed conversion to
  // double (single memcpy when the source is already F64). Arrays containing nulls
  // are tagged Heterogeneous by RedisJSON and fall through to the per-element
  // iterator which preserves the null-skipping semantics; any unknown future tag
  // falls through for the same reason.
  size_t buf_len = 0;
  JSONArrayType jtype = JSONArrayType_Heterogeneous;
  const void *buf = japi->getArray(arr, &buf_len, &jtype);
  if (buf && JSON_ArrayTypeIsNumeric(jtype)) {
    RS_ASSERT(buf_len == len);
    arrayof(double) out = array_newlen(double, len);
    // VecSim_ConvertFromTypedBuffer accepts any known numeric jtype for a FLOAT64
    // target, so reusing it here covers every tag JSON_ArrayTypeIsNumeric admits.
    VecSim_ConvertFromTypedBuffer(VecSimType_FLOAT64, jtype, buf, len, (char *)out);
    df->arrNumval = out;
    df->unionType = FLD_VAR_T_ARRAY;
    return REDISMODULE_OK;
  }

  JSONIterable iter = JSONIterable_FromArr(arr);
  int ret = JSON_StoreNumericInDocField(len, &iter, df, status);
  JSONIterable_Clean(&iter);
  return ret;
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
          QueryError_SetError(status, QUERY_ERROR_CODE_GENERIC, "GEOMETRY field does not support array type");
          break;
        default:
          rv = REDISMODULE_ERR;
          QueryError_SetError(status, QUERY_ERROR_CODE_GENERIC, "Unsupported field type");
          break;
      }
      break;
    case JSONType_Object:
      rv = REDISMODULE_ERR;
      QueryError_SetError(status, QUERY_ERROR_CODE_GENERIC, "Object type is not supported");
      break;
    case JSONType__EOF:
      RS_ABORT("Should not happen");
      rv = REDISMODULE_ERR;
  }

  return rv;
}

static RSValue *jsonValToValue(RedisModuleCtx *ctx, RedisJSON json) {
  size_t len;
  char *str;
  const char *constStr;
  RedisModuleString *rstr;
  long long ll;
  double dd;
  int i;

  // Currently `getJSON` cannot fail here also the other japi APIs below
  switch (japi->getType(json)) {
    case JSONType_String:
      japi->getString(json, &constStr, &len);
      str = rm_strndup(constStr, len);
      return RSValue_NewString(str, len);
    case JSONType_Int:
      japi->getInt(json, &ll);
      return RSValue_NewNumberFromInt64(ll);
    case JSONType_Double:
      japi->getDouble(json, &dd);
      return RSValue_NewNumber(dd);
    case JSONType_Bool:
      japi->getBoolean(json, &i);
      return RSValue_NewNumberFromInt64(i);
    case JSONType_Array:
    case JSONType_Object:
      japi->getJSON(json, ctx, &rstr);
      return RSValue_NewRedisString(rstr);
    case JSONType_Null:
      return RSValue_NullStatic();
    case JSONType__EOF:
      break;
  }
  RS_ABORT("Cannot get here");
  return NULL;
}

// {"a":1, "b":[2, 3, {"c": "foo"}, 4], "d": null}
static RSValue *jsonValToValueExpanded(RedisModuleCtx *ctx, RedisJSON json) {

  RSValue *ret;
  size_t len;
  JSONType type = japi->getType(json);
  if (type == JSONType_Object) {
    // Object
    japi->getLen(json, &len);
    RSValue **pairs = NULL;
    if (len) {
      JSONKeyValuesIterator iter = japi->getKeyValues(json);
      RedisModuleString *keyName;
      size_t i = 0;
      RedisJSON value;
      RedisJSONPtr value_ptr = japi->allocJson();

      RSValueMapBuilder *map = RSValue_NewMapBuilder(len);
      for (; (japi->nextKeyValue(iter, &keyName, value_ptr) == REDISMODULE_OK); ++i) {
        value = *value_ptr;
        RSValue_MapBuilderSetEntry(map, i, RSValue_NewRedisString(keyName),
          jsonValToValueExpanded(ctx, value));
      }
      japi->freeJson(value_ptr);
      value_ptr = NULL;
      japi->freeKeyValuesIter(iter);
      RS_ASSERT(i == len);

      ret = RSValue_NewMapFromBuilder(map);
    } else {
      ret = RSValue_NewMapFromBuilder(RSValue_NewMapBuilder(0));
    }
  } else if (type == JSONType_Array) {
    // Array
    japi->getLen(json, &len);
    if (len) {
      RSValue **arr = RSValue_NewArrayBuilder(len);
      RedisJSONPtr value_ptr = japi->allocJson();
      for (size_t i = 0; i < len; ++i) {
        japi->getAt(json, i, value_ptr);
        RedisJSON value = *value_ptr;
        arr[i] = jsonValToValueExpanded(ctx, value);
      }
      japi->freeJson(value_ptr);
      ret = RSValue_NewArrayFromBuilder(arr, len);
    } else {
      // Empty array
      RSValue **arr = RSValue_NewArrayBuilder(0);
      ret = RSValue_NewArrayFromBuilder(arr, 0);
    }
  } else {
    // Scalar
    ret = jsonValToValue(ctx, json);
  }
  return ret;
}

// Return an array of expanded values from an iterator.
// The iterator is being reset and is not being freed.
static RSValue* jsonIterToValueExpanded(RedisModuleCtx *ctx, JSONResultsIterator iter) {
  RSValue *ret;
  RSValue **arr;
  size_t len = japi->len(iter);
  if (len) {
    japi->resetIter(iter);
    RedisJSON json;
    RSValue **arr = RSValue_NewArrayBuilder(len);
    for (size_t i = 0; (json = japi->next(iter)); ++i) {
      arr[i] = jsonValToValueExpanded(ctx, json);
    }
    ret = RSValue_NewArrayFromBuilder(arr, len);
  } else {
    // Empty array
    RSValue **arr = RSValue_NewArrayBuilder(0);
    ret = RSValue_NewArrayFromBuilder(arr, 0);
  }
  return ret;
}

// Get the value from an iterator and free the iterator
// Return REDISMODULE_OK, and set rsv to the value, if value exists
// Return REDISMODULE_ERR otherwise
//
// Multi value is supported with apiVersion >= APIVERSION_RETURN_MULTI_CMP_FIRST
int jsonIterToValue(RedisModuleCtx *ctx, JSONResultsIterator iter, unsigned int apiVersion, RSValue **rsv) {

  int res = REDISMODULE_ERR;
  RedisModuleString *serialized = NULL;
  size_t len = 0;

  if (apiVersion < APIVERSION_RETURN_MULTI_CMP_FIRST) {
    // Preserve single value behavior for backward compatibility
    RedisJSON json = japi->next(iter);
    if (!json) {
      goto done;
    }
    *rsv = jsonValToValue(ctx, json);
    res = REDISMODULE_OK;
    goto done;
  }

  len = japi->len(iter);
  if (len > 0) {
    // First get the JSON serialized value (since it does not consume the iterator)
    if (japi->getJSONFromIter(iter, ctx, &serialized) == REDISMODULE_ERR) {
      goto done;
    }

    // Second, get the first JSON value
    RedisJSON json = japi->next(iter);
    RedisJSONPtr json_alloc = NULL; // Used if we need to allocate a new JSON value (e.g if the value is an array)
    // If the value is an array, we currently try using the first element
    JSONType type = japi->getType(json);
    if (type == JSONType_Array) {
      json_alloc = japi->allocJson();
      // Empty array will return NULL
      if (japi->getAt(json, 0, json_alloc) == REDISMODULE_OK) {
        json = *json_alloc;
      } else {
        json = NULL;
      }
    }

    if (json) {
      RSValue *val = jsonValToValue(ctx, json);
      RSValue *otherval = RSValue_NewRedisString(serialized);
      RSValue *expand = jsonIterToValueExpanded(ctx, iter);
      *rsv = RSValue_NewTrio(val, otherval, expand);
      res = REDISMODULE_OK;
    } else if (serialized) {
      RedisModule_FreeString(ctx, serialized);
    }

    if (json_alloc) {
      japi->freeJson(json_alloc);
    }
  }

done:
  return res;
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
        QueryError_SetError(status, QUERY_ERROR_CODE_GENERIC, "Unsupported field type");
        break;
    }
  }

  df->multisv = NULL;
  // If all is successful up til here,
  // we check whether a multi value is needed to be calculated for SORTABLE (avoiding re-opening the key and re-parsing the path)
  // (requires some API V2 functions to be available)
  if (rv == REDISMODULE_OK && FieldSpec_IsSortable(fs) && df->unionType == FLD_VAR_T_ARRAY) {
    RSValue *rsv = NULL;
    japi->resetIter(jsonIter);
    // There is no api version (DIALECT) specified during ingestion,
    // So we need to prepare a value using newer api version,
    // in order to be able to handle a query later on with either old or new api version
    if (jsonIterToValue(ctx, jsonIter, APIVERSION_RETURN_MULTI_CMP_FIRST, &rsv) == REDISMODULE_OK) {
      df->multisv = rsv;
    } else {
      rv = REDISMODULE_ERR;
      QueryError_SetError(status, QUERY_ERROR_CODE_GENERIC, "Failed to get value from iterator");
    }
  }
  return rv;
}

void JSONParse_error(QueryError *status, RedisModuleString *err_msg, const HiddenString *path, const HiddenString *fieldName, const HiddenString *indexName) {
  QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_INVAL_PATH,
                         "Invalid JSONPath", " '%s' in attribute '%s' in index '%s'",
                         HiddenString_GetUnsafe(path, NULL), HiddenString_GetUnsafe(fieldName, NULL), HiddenString_GetUnsafe(indexName, NULL));
  RedisModule_FreeString(RSDummyContext, err_msg);
}


#ifdef ENABLE_ASSERT
#include "json_test_api.h"

bool JSONTest_AcceptsJSONArrayType(VecSimType target, JSONArrayType src) {
  return VecSim_AcceptsJSONArrayType(target, src);
}

void JSONTest_ConvertFromTypedBuffer(VecSimType target_type, JSONArrayType jtype,
                                     const void *src, size_t n, char *target) {
  VecSim_ConvertFromTypedBuffer(target_type, jtype, src, n, target);
}
#endif // ENABLE_ASSERT
