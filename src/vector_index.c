/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "vector_index.h"
#include "iterators/hybrid_reader.h"
#include "iterators/idlist_iterator.h"
#include "query_param.h"
#include "rdb.h"
#include "util/workers_pool.h"
#include "util/threadpool_api.h"
#include "redis_index.h"


#if defined(__x86_64__) && defined(__GLIBC__)
#include <cpuid.h>
#define CPUID_AVAILABLE 1
#endif

bool isLVQSupported() {

#if defined(CPUID_AVAILABLE) && BUILD_INTEL_SVS_OPT
  // Check if the machine is Intel based on the CPU vendor.
  unsigned int eax, ebx, ecx, edx;
  char vendor[13];

  // Get vendor string
  __cpuid(0, eax, ebx, ecx, edx);

  // Intel vendor string is "GenuineIntel"
  memcpy(vendor, &ebx, 4);
  memcpy(vendor + 4, &edx, 4);
  memcpy(vendor + 8, &ecx, 4);
  vendor[12] = '\0';

  return strcmp(vendor, "GenuineIntel") == 0;
#endif
  return false; // In which case we know that LVQ not supported.
}

VecSimIndex *openVectorIndex(IndexSpec *spec, RedisModuleString *keyName, bool create_if_index) {
  KeysDictValue *kdv = dictFetchValue(spec->keysDict, keyName);
  if (kdv) {
    return kdv->p;
  }
  if (!create_if_index) {
    return NULL;
  }

  size_t fieldLen;
  const char *fieldStr = RedisModule_StringPtrLen(keyName, &fieldLen);
  FieldSpec *fieldSpec = NULL;
  for (int i = 0; i < spec->numFields; ++i) {
    if (!HiddenString_CaseInsensitiveCompareC(spec->fields[i].fieldName, fieldStr, fieldLen)) {
      fieldSpec = &spec->fields[i];
      break;
    }
  }
  if (fieldSpec == NULL) {
    return NULL;
  }

  // create new vector data structure
  VecSimIndex* temp = VecSimIndex_New(&fieldSpec->vectorOpts.vecSimParams);
  if (!temp) {
    return NULL;
  }
  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->p = temp;
  kdv->dtor = (void (*)(void *))VecSimIndex_Free;
  dictAdd(spec->keysDict, keyName, kdv);
  return kdv->p;
}

QueryIterator *createMetricIteratorFromVectorQueryResults(VecSimQueryReply *reply, const bool yields_metric) {
  size_t res_num = VecSimQueryReply_Len(reply);
  if (res_num == 0) {
    VecSimQueryReply_Free(reply);
    return NULL;
  }
  t_docId *docIdsList = rm_malloc(sizeof(*docIdsList) * res_num);
  double *metricList = yields_metric ? rm_malloc(sizeof(*metricList) * res_num) : NULL;

  // Collect the results' id and distance and set it in the arrays.
  VecSimQueryReply_Iterator *iter = VecSimQueryReply_GetIterator(reply);
  for (size_t i = 0; i < res_num; i++) {
    VecSimQueryResult *res = VecSimQueryReply_IteratorNext(iter);
    docIdsList[i] = VecSimQueryResult_GetId(res);
    if (yields_metric) {
      metricList[i] = VecSimQueryResult_GetScore(res);
    }
  }
  VecSimQueryReply_IteratorFree(iter);
  VecSimQueryReply_Free(reply);

  // Move ownership on the arrays to the iterator.
  if (yields_metric) {
    return NewMetricIterator(docIdsList, metricList, res_num, VECTOR_DISTANCE);
  } else {
    return NewIdListIterator(docIdsList, res_num, 1.0);
  }
}

QueryIterator *NewVectorIterator(QueryEvalCtx *q, VectorQuery *vq, QueryIterator *child_it) {
  RedisSearchCtx *ctx = q->sctx;
  RedisModuleString *key = IndexSpec_GetFormattedKey(ctx->spec, vq->field, INDEXFLD_T_VECTOR);
  VecSimIndex *vecsim = openVectorIndex(ctx->spec, key, DONT_CREATE_INDEX);
  if (!vecsim) {
    return NULL;
  }

  VecSimIndexBasicInfo info = VecSimIndex_BasicInfo(vecsim);
  size_t dim = info.dim;
  VecSimType type = info.type;
  VecSimMetric metric = info.metric;

  VecSimQueryParams qParams = {0};
  FieldFilterContext filterCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = vq->field->index}, .predicate = FIELD_EXPIRATION_DEFAULT};
  switch (vq->type) {
    case VECSIM_QT_KNN: {
      if ((dim * VecSimType_sizeof(type)) != vq->knn.vecLen) {
        QueryError_SetWithUserDataFmt(q->status, QUERY_ERROR_CODE_INVAL,
                                      "Error parsing vector similarity query: query vector blob size",
                                      " (%zu) does not match index's expected size (%zu).",
                                      vq->knn.vecLen, (dim * VecSimType_sizeof(type)));
        return NULL;
      }
      VecsimQueryType queryType = child_it != NULL ? QUERY_TYPE_HYBRID : QUERY_TYPE_KNN;
      if (VecSim_ResolveQueryParams(vecsim, vq->params.params, array_len(vq->params.params),
                                    &qParams, queryType, q->status) != VecSim_OK)  {
        return NULL;
      }
      if (vq->knn.k > MAX_KNN_K) {
        QueryError_SetWithoutUserDataFmt(q->status, QUERY_ERROR_CODE_INVAL,
                                               "Error parsing vector similarity query: query " VECSIM_KNN_K_TOO_LARGE_ERR_MSG ", must not exceed %zu", MAX_KNN_K);
        return NULL;
      }
      HybridIteratorParams hParams = {.index = vecsim,
                                      .dim = dim,
                                      .elementType = type,
                                      .spaceMetric = metric,
                                      .query = vq->knn,
                                      .qParams = qParams,
                                      .vectorScoreField = vq->scoreField,
                                      .canTrimDeepResults = q->opts->flags & Search_CanSkipRichResults,
                                      .childIt = child_it,
                                      .timeout = q->sctx->time.timeout,
                                      .sctx = q->sctx,
                                      .filterCtx = &filterCtx,
      };
      return NewHybridVectorIterator(hParams, q->status);
    }
    case VECSIM_QT_RANGE: {
      if ((dim * VecSimType_sizeof(type)) != vq->range.vecLen) {
        QueryError_SetWithUserDataFmt(q->status, QUERY_ERROR_CODE_INVAL,
                               "Error parsing vector similarity query: query vector blob size",
                               " (%zu) does not match index's expected size (%zu).",
                               vq->range.vecLen, (dim * VecSimType_sizeof(type)));
        return NULL;
      }
      if (vq->range.radius < 0) {
        QueryError_SetWithoutUserDataFmt(q->status, QUERY_ERROR_CODE_INVAL,
                               "Error parsing vector similarity query: negative radius"
                               " (%g) given in a range query",
                               vq->range.radius);
        return NULL;
      }
      if (VecSim_ResolveQueryParams(vecsim, vq->params.params, array_len(vq->params.params),
                                    &qParams, QUERY_TYPE_RANGE, q->status) != VecSim_OK)  {
        return NULL;
      }
      qParams.timeoutCtx = &(TimeoutCtx){ .timeout = q->sctx->time.timeout, .counter = 0 };
      VecSimQueryReply *results =
          VecSimIndex_RangeQuery(vecsim, vq->range.vector, vq->range.radius,
                                 &qParams, vq->range.order);
      if (VecSimQueryReply_GetCode(results) == VecSim_QueryReply_TimedOut) {
        VecSimQueryReply_Free(results);
        QueryError_SetError(q->status, QUERY_ERROR_CODE_TIMED_OUT, NULL);
        return NULL;
      }
      bool yields_metric = vq->scoreField != NULL;
      return createMetricIteratorFromVectorQueryResults(results, yields_metric);
    }
  }
  return NULL;
}

int VectorQuery_EvalParams(dict *params, QueryNode *node, unsigned int dialectVersion, QueryError *status) {
  for (size_t i = 0; i < QueryNode_NumParams(node); i++) {
    int res = QueryParam_Resolve(&node->params[i], params, dialectVersion, status);
    if (res < 0) {
      return REDISMODULE_ERR;
    }
  }
  for (size_t i = 0; i < array_len(node->vn.vq->params.params); i++) {
    int res = VectorQuery_ParamResolve(node->vn.vq->params, i, params, status);
    if (res < 0) {
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;
}

int VectorQuery_ParamResolve(VectorQueryParams params, size_t index, dict *paramsDict, QueryError *status) {
  if (!params.needResolve[index]) {
    return 0;
  }
  size_t val_len;
  const char *val = Param_DictGet(paramsDict, params.params[index].value, &val_len, status);
  if (!val) {
    return -1;
  }
  rm_free((char *)params.params[index].value);
  params.params[index].value = rm_strndup(val, val_len);
  params.params[index].valLen = val_len;
  return 1;
}

char *VectorQuery_GetDefaultScoreFieldName(const char *fieldName, size_t fieldNameLen) {
  // Generate default scoreField name using vector field name
  char *scoreFieldName = NULL;
  int n_written = rm_asprintf(&scoreFieldName, "__%.*s_score", (int)fieldNameLen, fieldName);
  RS_ASSERT(n_written != -1);
  return scoreFieldName;
}

void VectorQuery_SetDefaultScoreField(VectorQuery *vq, const char *fieldName, size_t fieldNameLen) {
  // Set default scoreField using vector field name
  char *defaultName = VectorQuery_GetDefaultScoreFieldName(fieldName, fieldNameLen);
  vq->scoreField = defaultName;
}

void VectorQuery_Free(VectorQuery *vq) {
  if (vq->scoreField) rm_free((char *)vq->scoreField);
  switch (vq->type) {
    case VECSIM_QT_KNN: // no need to free the vector as we points to the query dictionary
    case VECSIM_QT_RANGE:
      break;
  }
  for (int i = 0; i < array_len(vq->params.params); i++) {
    rm_free((char *)vq->params.params[i].name);
    rm_free((char *)vq->params.params[i].value);
  }
  array_free(vq->params.params);
  array_free(vq->params.needResolve);
  rm_free(vq);
}

const char *VecSimType_ToString(VecSimType type) {
  switch (type) {
    case VecSimType_FLOAT32: return VECSIM_TYPE_FLOAT32;
    case VecSimType_FLOAT64: return VECSIM_TYPE_FLOAT64;
    case VecSimType_FLOAT16: return VECSIM_TYPE_FLOAT16;
    case VecSimType_BFLOAT16: return VECSIM_TYPE_BFLOAT16;
    case VecSimType_UINT8: return VECSIM_TYPE_UINT8;
    case VecSimType_INT8: return VECSIM_TYPE_INT8;
    case VecSimType_INT32: return VECSIM_TYPE_INT32;
    case VecSimType_INT64: return VECSIM_TYPE_INT64;
  }
  return NULL;
}

size_t VecSimType_sizeof(VecSimType type) {
    switch (type) {
        case VecSimType_FLOAT32: return sizeof(float);
        case VecSimType_FLOAT64: return sizeof(double);
        case VecSimType_FLOAT16: return sizeof(float)/2;
        case VecSimType_BFLOAT16: return sizeof(float)/2;
        case VecSimType_UINT8: return sizeof(uint8_t);
        case VecSimType_INT8: return sizeof(int8_t);
        case VecSimType_INT32: return sizeof(int32_t);
        case VecSimType_INT64: return sizeof(int64_t);
    }
    return 0;
}

const char *VecSimMetric_ToString(VecSimMetric metric) {
  switch (metric) {
    case VecSimMetric_IP: return VECSIM_METRIC_IP;
    case VecSimMetric_L2: return VECSIM_METRIC_L2;
    case VecSimMetric_Cosine: return VECSIM_METRIC_COSINE;
  }
  return NULL;
}

const char *VecSimAlgorithm_ToString(VecSimAlgo algo) {
  switch (algo) {
    case VecSimAlgo_BF: return VECSIM_ALGORITHM_BF;
    case VecSimAlgo_HNSWLIB: return VECSIM_ALGORITHM_HNSW;
    case VecSimAlgo_TIERED: return VECSIM_ALGORITHM_TIERED;
    case VecSimAlgo_SVS: return VECSIM_ALGORITHM_SVS;
  }
  return NULL;
}
const char *VecSimSearchMode_ToString(VecSearchMode vecsimSearchMode) {
    switch (vecsimSearchMode) {
    case EMPTY_MODE:
        return "EMPTY_MODE";
    case STANDARD_KNN:
        return "STANDARD_KNN";
    case HYBRID_ADHOC_BF:
        return "HYBRID_ADHOC_BF";
    case HYBRID_BATCHES:
        return "HYBRID_BATCHES";
    case HYBRID_BATCHES_TO_ADHOC_BF:
        return "HYBRID_BATCHES_TO_ADHOC_BF";
    case RANGE_QUERY:
        return "RANGE_QUERY";
    }
    return NULL;
}

bool VecSim_IsLeanVecCompressionType(VecSimSvsQuantBits quantBits) {
  return quantBits == VecSimSvsQuant_4x8_LeanVec || quantBits == VecSimSvsQuant_8x8_LeanVec;
}

const char *VecSimSvsCompression_ToString(VecSimSvsQuantBits quantBits) {
  // If quantBits is not NONE, We need to check if we are running on intel machine,  and if not, we
  // need to fall back to scalar quantization.
  if (quantBits == VecSimSvsQuant_NONE) {
    return VECSIM_NO_COMPRESSION;
  }
  // If we are running on non-intel machine, only scalar quantization is possible.
  if (!isLVQSupported()) {
    return VECSIM_LVQ_SCALAR;
  }
  // Otherwise, we are running on intel machine, and we return the appropriate quantization mode.
  switch (quantBits) {
    case VecSimSvsQuant_4: return VECSIM_LVQ_4;
    case VecSimSvsQuant_8: return VECSIM_LVQ_8;
    case VecSimSvsQuant_4x4: return VECSIM_LVQ_4X4;
    case VecSimSvsQuant_4x8: return VECSIM_LVQ_4X8;
    case VecSimSvsQuant_4x8_LeanVec: return VECSIM_LEANVEC_4X8;
    case VecSimSvsQuant_8x8_LeanVec: return VECSIM_LEANVEC_8X8;
    default:;
  }
  return NULL;

}

const char *VecSimSearchHistory_ToString(VecSimOptionMode option) {
    if (option == VecSimOption_ENABLE)
        return VECSIM_USE_SEARCH_HISTORY_ON;
    else if (option == VecSimOption_DISABLE)
        return VECSIM_USE_SEARCH_HISTORY_OFF;
    else if (option == VecSimOption_AUTO)
        return VECSIM_USE_SEARCH_HISTORY_DEFAULT;
    return NULL;
}

void VecSim_RdbSave(RedisModuleIO *rdb, VecSimParams *vecsimParams) {
  RedisModule_SaveUnsigned(rdb, vecsimParams->algo);

  switch (vecsimParams->algo) {
  case VecSimAlgo_BF:
    RedisModule_SaveUnsigned(rdb, vecsimParams->algoParams.bfParams.type);
    RedisModule_SaveUnsigned(rdb, vecsimParams->algoParams.bfParams.dim);
    RedisModule_SaveUnsigned(rdb, vecsimParams->algoParams.bfParams.metric);
    RedisModule_SaveUnsigned(rdb, vecsimParams->algoParams.bfParams.multi);
    break;
  case VecSimAlgo_TIERED:
    RedisModule_SaveUnsigned(rdb, vecsimParams->algoParams.tieredParams.primaryIndexParams->algo);
    if (vecsimParams->algoParams.tieredParams.primaryIndexParams->algo == VecSimAlgo_HNSWLIB) {
      RedisModule_SaveUnsigned(rdb, vecsimParams->algoParams.tieredParams.specificParams.tieredHnswParams.swapJobThreshold);
      HNSWParams *primaryParams = &vecsimParams->algoParams.tieredParams.primaryIndexParams->algoParams.hnswParams;

      RedisModule_SaveUnsigned(rdb, primaryParams->type);
      RedisModule_SaveUnsigned(rdb, primaryParams->dim);
      RedisModule_SaveUnsigned(rdb, primaryParams->metric);
      RedisModule_SaveUnsigned(rdb, primaryParams->multi);
      RedisModule_SaveUnsigned(rdb, primaryParams->M);
      RedisModule_SaveUnsigned(rdb, primaryParams->efConstruction);
      RedisModule_SaveUnsigned(rdb, primaryParams->efRuntime);
      RedisModule_SaveDouble(rdb, primaryParams->epsilon);
    } else if (vecsimParams->algoParams.tieredParams.primaryIndexParams->algo == VecSimAlgo_SVS) {
      RedisModule_SaveUnsigned(rdb, vecsimParams->algoParams.tieredParams.specificParams.tieredSVSParams.trainingTriggerThreshold);
      SVSParams *primaryParams = &vecsimParams->algoParams.tieredParams.primaryIndexParams->algoParams.svsParams;

      RedisModule_SaveUnsigned(rdb, primaryParams->type);
      RedisModule_SaveUnsigned(rdb, primaryParams->dim);
      RedisModule_SaveUnsigned(rdb, primaryParams->metric);
      RedisModule_SaveUnsigned(rdb, primaryParams->multi);
      RedisModule_SaveUnsigned(rdb, primaryParams->quantBits);
      RedisModule_SaveUnsigned(rdb, primaryParams->graph_max_degree);
      RedisModule_SaveUnsigned(rdb, primaryParams->construction_window_size);
      RedisModule_SaveUnsigned(rdb, primaryParams->leanvec_dim);
      RedisModule_SaveUnsigned(rdb, primaryParams->search_window_size);
      RedisModule_SaveDouble(rdb, primaryParams->epsilon);
    }
    break;
  case VecSimAlgo_HNSWLIB:
  case VecSimAlgo_SVS:
    return; // Should not get here anymore.
  }
}

static int VecSimIndex_validate_Rdb_parameters(RedisModuleIO *rdb, VecSimParams *vecsimParams) {
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  QueryError status = QueryError_Default();
  int rv;

  // Checking if the loaded parameters fits the current server limits.
  rv = VecSimIndex_validate_params(ctx, vecsimParams, &status);
  if (REDISMODULE_OK != rv) {
    RedisModule_LogIOError(rdb, REDISMODULE_LOGLEVEL_WARNING, "ERROR: loading vector index failed! %s", QueryError_GetDisplayableError(&status, RSGlobalConfig.hideUserDataFromLog));
  }
  QueryError_ClearError(&status);
  return rv;
}

int VecSim_RdbLoad_v4(RedisModuleIO *rdb, VecSimParams *vecsimParams, StrongRef sp_ref,
                      const char *field_name) {
  vecsimParams->algo = LoadUnsigned_IOError(rdb, goto fail);
  VecSimLogCtx *logCtx = rm_new(VecSimLogCtx);
  logCtx->index_field_name = field_name;
  vecsimParams->logCtx = logCtx;

  switch (vecsimParams->algo) {
  case VecSimAlgo_BF:
    vecsimParams->algoParams.bfParams.type = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.dim = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.metric = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.multi = LoadUnsigned_IOError(rdb, goto fail);
    break;
  case VecSimAlgo_TIERED:
    VecSim_TieredParams_Init(&vecsimParams->algoParams.tieredParams, sp_ref);
    VecSimParams *primaryParams = vecsimParams->algoParams.tieredParams.primaryIndexParams;
    primaryParams->logCtx = vecsimParams->logCtx;
    primaryParams->algo = LoadUnsigned_IOError(rdb, goto fail);

    if (primaryParams->algo == VecSimAlgo_HNSWLIB) {
      vecsimParams->algoParams.tieredParams.specificParams.tieredHnswParams.swapJobThreshold = LoadUnsigned_IOError(rdb, goto fail);

      primaryParams->algoParams.hnswParams.type = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.dim = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.metric = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.multi = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.M = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.efConstruction = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.efRuntime = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.epsilon = LoadDouble_IOError(rdb, goto fail);
    } else if (primaryParams->algo == VecSimAlgo_SVS) {
      vecsimParams->algoParams.tieredParams.specificParams.tieredSVSParams.trainingTriggerThreshold = LoadUnsigned_IOError(rdb, goto fail);

      primaryParams->algoParams.svsParams.type = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.svsParams.dim = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.svsParams.metric = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.svsParams.multi = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.svsParams.quantBits = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.svsParams.graph_max_degree = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.svsParams.construction_window_size = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.svsParams.leanvec_dim = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.svsParams.search_window_size = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.svsParams.epsilon = LoadDouble_IOError(rdb, goto fail);
    } else {
      goto fail; // Unsupported primary algorithm for tiered index
    }
    break;
  case VecSimAlgo_HNSWLIB:
  case VecSimAlgo_SVS:
    goto fail; // We dont expect to see an HNSW/SVS index without a tiered index
  }

  return VecSimIndex_validate_Rdb_parameters(rdb, vecsimParams);

fail:
  return REDISMODULE_ERR;
}

int VecSim_RdbLoad_v3(RedisModuleIO *rdb, VecSimParams *vecsimParams, StrongRef sp_ref,
                      const char *field_name) {
  vecsimParams->algo = LoadUnsigned_IOError(rdb, goto fail);
  VecSimLogCtx *logCtx = rm_new(VecSimLogCtx);
  logCtx->index_field_name = field_name;
  vecsimParams->logCtx = logCtx;

  switch (vecsimParams->algo) {
  case VecSimAlgo_BF:
    vecsimParams->algoParams.bfParams.type = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.dim = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.metric = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.multi = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.initialCapacity = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.blockSize = LoadUnsigned_IOError(rdb, goto fail);
    break;
  case VecSimAlgo_TIERED:
    VecSim_TieredParams_Init(&vecsimParams->algoParams.tieredParams, sp_ref);
    VecSimParams *primaryParams = vecsimParams->algoParams.tieredParams.primaryIndexParams;
    primaryParams->logCtx = vecsimParams->logCtx;
    primaryParams->algo = LoadUnsigned_IOError(rdb, goto fail);

    if (primaryParams->algo == VecSimAlgo_HNSWLIB) {
      vecsimParams->algoParams.tieredParams.specificParams.tieredHnswParams.swapJobThreshold = LoadUnsigned_IOError(rdb, goto fail);

      primaryParams->algoParams.hnswParams.type = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.dim = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.metric = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.multi = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.initialCapacity = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.M = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.efConstruction = LoadUnsigned_IOError(rdb, goto fail);
      primaryParams->algoParams.hnswParams.efRuntime = LoadUnsigned_IOError(rdb, goto fail);
    } else {
      RS_LOG_ASSERT(primaryParams->algo == VecSimAlgo_HNSWLIB,
              "Tiered index only supports HNSW as primary index in this version");
      goto fail;
    }
    break;
  case VecSimAlgo_HNSWLIB:
  case VecSimAlgo_SVS:
    goto fail; // We dont expect to see an HNSW index without a tiered index or SVS index.
  }

  return VecSimIndex_validate_Rdb_parameters(rdb, vecsimParams);

fail:
  return REDISMODULE_ERR;
}

int VecSim_RdbLoad_v2(RedisModuleIO *rdb, VecSimParams *vecsimParams) {

  vecsimParams->algo = LoadUnsigned_IOError(rdb, goto fail);

  switch (vecsimParams->algo) {
  case VecSimAlgo_BF:
    vecsimParams->algoParams.bfParams.type = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.dim = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.metric = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.multi = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.initialCapacity = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.blockSize = LoadUnsigned_IOError(rdb, goto fail);
    break;
  case VecSimAlgo_HNSWLIB:
    vecsimParams->algoParams.hnswParams.type = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.dim = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.metric = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.multi = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.initialCapacity = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.M = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.efConstruction = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.efRuntime = LoadUnsigned_IOError(rdb, goto fail);
    break;
  case VecSimAlgo_TIERED:
  case VecSimAlgo_SVS:
    goto fail; // Should not get here
  }

  return VecSimIndex_validate_Rdb_parameters(rdb, vecsimParams);

fail:
  return REDISMODULE_ERR;
}

// load for before multi-value vector field was supported
int VecSim_RdbLoad(RedisModuleIO *rdb, VecSimParams *vecsimParams) {

  vecsimParams->algo = LoadUnsigned_IOError(rdb, goto fail);

  switch (vecsimParams->algo) {
  case VecSimAlgo_BF:
    vecsimParams->algoParams.bfParams.type = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.dim = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.metric = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.multi = false;
    vecsimParams->algoParams.bfParams.initialCapacity = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.bfParams.blockSize = LoadUnsigned_IOError(rdb, goto fail);
    break;
  case VecSimAlgo_HNSWLIB:
    vecsimParams->algoParams.hnswParams.type = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.dim = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.metric = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.multi = false;
    vecsimParams->algoParams.hnswParams.initialCapacity = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.M = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.efConstruction = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->algoParams.hnswParams.efRuntime = LoadUnsigned_IOError(rdb, goto fail);
    break;
  case VecSimAlgo_TIERED:
  case VecSimAlgo_SVS:
    goto fail; // Should not get here
  }

  return VecSimIndex_validate_Rdb_parameters(rdb, vecsimParams);

fail:
  return REDISMODULE_ERR;
}

void VecSimParams_Cleanup(VecSimParams *params) {
  if (params->algo == VecSimAlgo_TIERED) {
    WeakRef spec_ref = {params->algoParams.tieredParams.jobQueueCtx};
    WeakRef_Release(spec_ref);
    rm_free(params->algoParams.tieredParams.primaryIndexParams);
  }
  // Note that for tiered index, this would free both params->logCtx and
  // params->tieredParams.primaryIndexParams->logCtx that point to the same object.
  rm_free(params->logCtx);
}

VecSimResolveCode VecSim_ResolveQueryParams(VecSimIndex *index, VecSimRawParam *params, size_t params_len,
                          VecSimQueryParams *qParams, VecsimQueryType queryType, QueryError *status) {

  VecSimResolveCode vecSimCode = VecSimIndex_ResolveParams(index, params, params_len, qParams, queryType);
  if (vecSimCode == VecSim_OK) {
    return vecSimCode;
  }

  QueryErrorCode RSErrorCode;
  switch (vecSimCode) {
    case VecSimParamResolverErr_AlreadySet: {
      RSErrorCode = QUERY_ERROR_CODE_DUP_PARAM;
      break;
    }
    case VecSimParamResolverErr_UnknownParam: {
      RSErrorCode = QUERY_ERROR_CODE_NO_OPTION;
      break;
    }
    case VecSimParamResolverErr_BadValue: {
      RSErrorCode = QUERY_ERROR_CODE_BAD_VAL;
      break;
    }
    case VecSimParamResolverErr_InvalidPolicy_NHybrid: {
      RSErrorCode = QUERY_ERROR_CODE_NON_HYBRID;
      break;
    }
    case VecSimParamResolverErr_InvalidPolicy_NExits: {
      RSErrorCode = QUERY_ERROR_CODE_HYBRID_NON_EXIST;
      break;
    }
    case VecSimParamResolverErr_InvalidPolicy_AdHoc_With_BatchSize: {
      RSErrorCode = QUERY_ERROR_CODE_ADHOC_WITH_BATCH_SIZE;
      break;
    }
    case VecSimParamResolverErr_InvalidPolicy_AdHoc_With_EfRuntime: {
      RSErrorCode = QUERY_ERROR_CODE_ADHOC_WITH_EF_RUNTIME;
      break;
    }
    case VecSimParamResolverErr_InvalidPolicy_NRange: {
      RSErrorCode = QUERY_ERROR_CODE_NON_RANGE;
      break;
    }
    default: {
      RSErrorCode = QUERY_ERROR_CODE_GENERIC;
    }
  }
  const char *error_msg = QueryError_Strerror(RSErrorCode);
  QueryError_SetWithUserDataFmt(status, RSErrorCode, "Error parsing vector similarity parameters", ": %s", error_msg);
  return vecSimCode;
}

void VecSim_TieredParams_Init(TieredIndexParams *params, StrongRef sp_ref) {
  params->primaryIndexParams = rm_calloc(1, sizeof(VecSimParams));
  // We expect the thread pool to be initialized from the module init function, and to stay constant
  // throughout the lifetime of the module. It can be initialized to NULL.
  params->jobQueue = _workers_thpool;
  params->flatBufferLimit = RSGlobalConfig.tieredVecSimIndexBufferLimit;
  params->jobQueueCtx = StrongRef_Demote(sp_ref).rm;
  params->submitCb = (SubmitCB)ThreadPoolAPI_SubmitIndexJobs;
}

void VecSimLogCallback(void *ctx, const char *level, const char *message) {
  VecSimLogCtx *log_ctx = (VecSimLogCtx *)ctx;
  RedisModule_Log(RSDummyContext, level, "vector index '%s' - %s", log_ctx->index_field_name, message);
}

int VecSim_CallTieredIndexesGC(WeakRef spRef) {
  // Get spec
  StrongRef strong = WeakRef_Promote(spRef);
  IndexSpec *sp = StrongRef_Get(strong);
  if (!sp) {
    // Index was deleted
    return 0;
  }
  // Lock the spec for reading
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(NULL, sp);
  RedisSearchCtx_LockSpecRead(&sctx);
  // Iterate over the fields and call the GC for each tiered index
  if (sp->flags & Index_HasVecSim) { // Early return if the spec doesn't have vector indexes
    for (size_t ii = 0; ii < sp->numFields; ++ii) {
      if (sp->fields[ii].types & INDEXFLD_T_VECTOR &&
          sp->fields[ii].vectorOpts.vecSimParams.algo == VecSimAlgo_TIERED) {
        // Get the vector index
        RedisModuleString *vecsim_name = IndexSpec_GetFormattedKey(sp, sp->fields + ii, INDEXFLD_T_VECTOR);
        VecSimIndex *vecsim = openVectorIndex(sp, vecsim_name, DONT_CREATE_INDEX);
        // Call the tiered index GC if the vector index is not empty
        if (vecsim) VecSimTieredIndex_GC(vecsim);
      }
    }
  }
  // Cleanup and return success
  RedisSearchCtx_UnlockSpec(&sctx);
  StrongRef_Release(strong);
  return 1;
}

VecSimMetric getVecSimMetricFromVectorField(const FieldSpec *vectorField) {
  RS_ASSERT(FIELD_IS(vectorField, INDEXFLD_T_VECTOR))
  VecSimParams vec_params = vectorField->vectorOpts.vecSimParams;

  VecSimAlgo field_algo = vec_params.algo;
  AlgoParams algo_params = vec_params.algoParams;

  switch (field_algo) {
    case VecSimAlgo_TIERED: {
      VecSimParams *primary_params = algo_params.tieredParams.primaryIndexParams;
      if (primary_params->algo == VecSimAlgo_HNSWLIB) {
        HNSWParams hnsw_params = primary_params->algoParams.hnswParams;
        return hnsw_params.metric;
      } else if (primary_params->algo == VecSimAlgo_SVS) {
        SVSParams svs_params = primary_params->algoParams.svsParams;
        return svs_params.metric;
      } else {
        // Unknown primary algorithm in tiered index
        char error_msg[256];
        snprintf(error_msg, sizeof(error_msg), "Unknown primary algorithm in tiered index: %s",
                 VecSimAlgorithm_ToString(primary_params->algo));
        RS_ABORT(error_msg);
      }
      break;
    }
    case VecSimAlgo_BF:
      return algo_params.bfParams.metric;
    default:
      // Unknown algorithm type
      RS_ABORT("Unknown algorithm in vector index");
  }
}
