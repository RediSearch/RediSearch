/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "vector_index.h"
#include "hybrid_reader.h"
#include "metric_iterator.h"
#include "query_param.h"
#include "rdb.h"

static VecSimIndex *openVectorKeysDict(IndexSpec *spec, RedisModuleString *keyName,
                                             int write) {
  KeysDictValue *kdv = dictFetchValue(spec->keysDict, keyName);
  if (kdv) {
    return kdv->p;
  }
  if (!write) {
    return NULL;
  }

  size_t fieldLen;
  const char *fieldStr = RedisModule_StringPtrLen(keyName, &fieldLen);
  FieldSpec *fieldSpec = NULL;
  for (int i = 0; i < spec->numFields; ++i) {
    if (!strcasecmp(fieldStr, spec->fields[i].name)) {
      fieldSpec = &spec->fields[i];
      break;
    }
  }
  if (fieldSpec == NULL) {
    return NULL;
  }

  // create new vector data structure
  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->p = VecSimIndex_New(&fieldSpec->vectorOpts.vecSimParams);
  VecSimIndexInfo indexInfo = VecSimIndex_Info(kdv->p);
  switch (indexInfo.algo)
  {
    case VecSimAlgo_BF:
      spec->stats.vectorIndexSize += indexInfo.bfInfo.memory;
      break;
    case VecSimAlgo_HNSWLIB:
      spec->stats.vectorIndexSize += indexInfo.hnswInfo.memory;
      break;
    default:
      break;
  }
  dictAdd(spec->keysDict, keyName, kdv);
  kdv->dtor = (void (*)(void *))VecSimIndex_Free;
  return kdv->p;
}

VecSimIndex *OpenVectorIndex(IndexSpec *sp, RedisModuleString *keyName) {
  return openVectorKeysDict(sp, keyName, 1);
}

IndexIterator *createMetricIteratorFromVectorQueryResults(VecSimQueryResult_List results,
                                                          bool yields_metric) {
  size_t res_num = VecSimQueryResult_Len(results);
  if (res_num == 0) {
    VecSimQueryResult_Free(results);
    return NULL;
  }
  t_docId *docIdsList = array_new(t_docId, res_num);
  double *metricList = array_new(double, res_num);

  // Collect the results' id and distance and set it in the arrays.
  VecSimQueryResult_Iterator *iter = VecSimQueryResult_List_GetIterator(results);
  while (VecSimQueryResult_IteratorHasNext(iter)) {
    VecSimQueryResult *res = VecSimQueryResult_IteratorNext(iter);
    docIdsList = array_append(docIdsList, VecSimQueryResult_GetId(res));
    metricList = array_append(metricList, VecSimQueryResult_GetScore(res));
  }
  VecSimQueryResult_IteratorFree(iter);
  VecSimQueryResult_Free(results);

  // Move ownership on the arrays to the MetricIterator.
  return NewMetricIterator(docIdsList, metricList, VECTOR_DISTANCE, yields_metric);
}

IndexIterator *NewVectorIterator(QueryEvalCtx *q, VectorQuery *vq, IndexIterator *child_it) {
  RedisSearchCtx *ctx = q->sctx;
  RedisModuleString *key = RedisModule_CreateStringPrintf(ctx->redisCtx, "%s", vq->property);
  VecSimIndex *vecsim = openVectorKeysDict(ctx->spec, key, 0);
  RedisModule_FreeString(ctx->redisCtx, key);
  if (!vecsim) {
    return NULL;
  }

  VecSimIndexInfo info = VecSimIndex_Info(vecsim);
  size_t dim = 0;
  VecSimType type = (VecSimType)0;
  VecSimMetric metric = (VecSimMetric)0;
  switch (info.algo) {
    case VecSimAlgo_HNSWLIB:
      dim = info.hnswInfo.dim;
      type = info.hnswInfo.type;
      metric = info.hnswInfo.metric;
      break;
    case VecSimAlgo_BF:
      dim = info.bfInfo.dim;
      type = info.bfInfo.type;
      metric = info.bfInfo.metric;
      break;
  }

  VecSimQueryParams qParams = {0};
  switch (vq->type) {
    case VECSIM_QT_KNN: {
      if ((dim * VecSimType_sizeof(type)) != vq->knn.vecLen) {
        QueryError_SetErrorFmt(q->status, QUERY_EINVAL,
                               "Error parsing vector similarity query: query vector blob size"
                               " (%zu) does not match index's expected size (%zu).",
                               vq->knn.vecLen, (dim * VecSimType_sizeof(type)));
        return NULL;
      }
      VecsimQueryType queryType = child_it != NULL ? QUERY_TYPE_HYBRID : QUERY_TYPE_KNN;
      if (VecSim_ResolveQueryParams(vecsim, vq->params.params, array_len(vq->params.params),
                                    &qParams, queryType, q->status) != VecSim_OK)  {
        return NULL;
      }
      HybridIteratorParams hParams = {.index = vecsim,
                                      .dim = dim,
                                      .elementType = type,
                                      .spaceMetric = metric,
                                      .query = vq->knn,
                                      .qParams = qParams,
                                      .vectorScoreField = vq->scoreField,
                                      .ignoreDocScore = q->opts->flags & Search_IgnoreScores,
                                      .childIt = child_it,
                                      .timeout = q->sctx->timeout,
      };
      return NewHybridVectorIterator(hParams, q->status);
    }
    case VECSIM_QT_RANGE: {
      if ((dim * VecSimType_sizeof(type)) != vq->range.vecLen) {
        QueryError_SetErrorFmt(q->status, QUERY_EINVAL,
                               "Error parsing vector similarity query: query vector blob size"
                               " (%zu) does not match index's expected size (%zu).",
                               vq->range.vecLen, (dim * VecSimType_sizeof(type)));
        return NULL;
      }
      if (vq->range.radius < 0) {
        QueryError_SetErrorFmt(q->status, QUERY_EINVAL,
                               "Error parsing vector similarity query: negative radius (%g) "
                               "given in a range query",
                               vq->range.radius);
        return NULL;
      }
      if (VecSim_ResolveQueryParams(vecsim, vq->params.params, array_len(vq->params.params),
                                    &qParams, QUERY_TYPE_RANGE, q->status) != VecSim_OK)  {
        return NULL;
      }
      qParams.timeoutCtx = &(TimeoutCtx){ .timeout = q->sctx->timeout, .counter = 0 };
      VecSimQueryResult_List results =
          VecSimIndex_RangeQuery(vecsim, vq->range.vector, vq->range.radius,
                                 &qParams, vq->range.order);
      if (results.code == VecSim_QueryResult_TimedOut) {
        VecSimQueryResult_Free(results);
        QueryError_SetError(q->status, QUERY_TIMEDOUT, NULL);
        return NULL;
      }
      bool yields_metric = vq->scoreField != NULL;
      return createMetricIteratorFromVectorQueryResults(results, yields_metric);
    }
  }
  return NULL;
}

int VectorQuery_EvalParams(dict *params, QueryNode *node, QueryError *status) {
  for (size_t i = 0; i < QueryNode_NumParams(node); i++) {
    int res = QueryParam_Resolve(&node->params[i], params, status);
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

void VectorQuery_Free(VectorQuery *vq) {
  if (vq->property) rm_free((char *)vq->property);
  if (vq->scoreField) rm_free((char *)vq->scoreField);
  switch (vq->type) {
    case VECSIM_QT_KNN: // no need to free the vector as we pointes to the query dictionary
    default:
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
    case VecSimType_INT32: return VECSIM_TYPE_INT32;
    case VecSimType_INT64: return VECSIM_TYPE_INT64;
  }
  return NULL;
}

size_t VecSimType_sizeof(VecSimType type) {
    switch (type) {
        case VecSimType_FLOAT32: return sizeof(float);
        case VecSimType_FLOAT64: return sizeof(double);
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
  }
  return NULL;
}

void VecSim_RdbSave(RedisModuleIO *rdb, VecSimParams *vecsimParams) {
  RedisModule_SaveUnsigned(rdb, vecsimParams->algo);

  switch (vecsimParams->algo) {
  case VecSimAlgo_BF:
    RedisModule_SaveUnsigned(rdb, vecsimParams->bfParams.type);
    RedisModule_SaveUnsigned(rdb, vecsimParams->bfParams.dim);
    RedisModule_SaveUnsigned(rdb, vecsimParams->bfParams.metric);
    RedisModule_SaveUnsigned(rdb, vecsimParams->bfParams.multi);
    RedisModule_SaveUnsigned(rdb, vecsimParams->bfParams.initialCapacity);
    RedisModule_SaveUnsigned(rdb, vecsimParams->bfParams.blockSize);
    break;
  case VecSimAlgo_HNSWLIB:
    RedisModule_SaveUnsigned(rdb, vecsimParams->hnswParams.type);
    RedisModule_SaveUnsigned(rdb, vecsimParams->hnswParams.dim);
    RedisModule_SaveUnsigned(rdb, vecsimParams->hnswParams.metric);
    RedisModule_SaveUnsigned(rdb, vecsimParams->hnswParams.multi);
    RedisModule_SaveUnsigned(rdb, vecsimParams->hnswParams.initialCapacity);
    RedisModule_SaveUnsigned(rdb, vecsimParams->hnswParams.M);
    RedisModule_SaveUnsigned(rdb, vecsimParams->hnswParams.efConstruction);
    RedisModule_SaveUnsigned(rdb, vecsimParams->hnswParams.efRuntime);
    break;
  }
}

static int VecSimIndex_validate_Rdb_parameters(RedisModuleIO *rdb, VecSimParams *vecsimParams) {
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  QueryError status = {0};
  int rv;

  // Checking if the loaded parameters fits the current server limits.
  rv = VecSimIndex_validate_params(ctx, vecsimParams, &status);
  if (REDISMODULE_OK != rv) {
    size_t old_block_size = 0;
    size_t old_initial_cap = 0;
    RedisModule_LogIOError(rdb, REDISMODULE_LOGLEVEL_WARNING, "ERROR: %s", QueryError_GetError(&status));
    // We change the initial size and block size to default and try again.
    switch (vecsimParams->algo) {
      case VecSimAlgo_BF:
        old_block_size = vecsimParams->bfParams.blockSize;
        old_initial_cap = vecsimParams->bfParams.initialCapacity;
        vecsimParams->bfParams.blockSize = 0;
        vecsimParams->bfParams.initialCapacity = SIZE_MAX;
        break;
      case VecSimAlgo_HNSWLIB:
        old_block_size = vecsimParams->hnswParams.blockSize;
        old_initial_cap = vecsimParams->hnswParams.initialCapacity;
        vecsimParams->hnswParams.blockSize = 0;
        vecsimParams->hnswParams.initialCapacity = SIZE_MAX;
        break;
    }
    // We don't know yet what the block size will be (default value can be effected by memory limit),
    // so we first validating the new parameters.
    QueryError_ClearError(&status);
    rv = VecSimIndex_validate_params(ctx, vecsimParams, &status);
    // Now default block size is set. we can log this change now.
    switch (vecsimParams->algo) {
      case VecSimAlgo_BF:
        if (vecsimParams->bfParams.initialCapacity != old_initial_cap)
          RedisModule_LogIOError(rdb, REDISMODULE_LOGLEVEL_WARNING, "WARNING: changing initial capacity from %zu to %zu", old_initial_cap, vecsimParams->bfParams.initialCapacity);
        if (vecsimParams->hnswParams.blockSize != old_block_size)
          RedisModule_LogIOError(rdb, REDISMODULE_LOGLEVEL_WARNING, "WARNING: changing block size from %zu to %zu", old_block_size, vecsimParams->bfParams.blockSize);
        break;
      case VecSimAlgo_HNSWLIB:
        if (vecsimParams->hnswParams.initialCapacity != old_initial_cap)
          RedisModule_LogIOError(rdb, REDISMODULE_LOGLEVEL_WARNING, "WARNING: changing initial capacity from %zu to %zu", old_initial_cap, vecsimParams->hnswParams.initialCapacity);
        if (vecsimParams->hnswParams.blockSize != old_block_size)
          RedisModule_LogIOError(rdb, REDISMODULE_LOGLEVEL_WARNING, "WARNING: changing block size from %zu to %zu", old_block_size, vecsimParams->hnswParams.blockSize);
        break;
    }
    // If the second validation failed, we fail.
    if (REDISMODULE_OK != rv) {
      RedisModule_LogIOError(rdb, REDISMODULE_LOGLEVEL_WARNING, "ERROR: second load with default parameters failed! %s", QueryError_GetError(&status));
    }
  }
  QueryError_ClearError(&status);
  return rv;
}

int VecSim_RdbLoad_v2(RedisModuleIO *rdb, VecSimParams *vecsimParams) {

  vecsimParams->algo = LoadUnsigned_IOError(rdb, goto fail);

  switch (vecsimParams->algo) {
  case VecSimAlgo_BF:
    vecsimParams->bfParams.type = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->bfParams.dim = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->bfParams.metric = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->bfParams.multi = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->bfParams.initialCapacity = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->bfParams.blockSize = LoadUnsigned_IOError(rdb, goto fail);
    break;
  case VecSimAlgo_HNSWLIB:
    vecsimParams->hnswParams.type = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.dim = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.metric = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.multi = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.initialCapacity = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.M = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.efConstruction = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.efRuntime = LoadUnsigned_IOError(rdb, goto fail);
    break;
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
    vecsimParams->bfParams.type = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->bfParams.dim = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->bfParams.metric = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->bfParams.multi = false;
    vecsimParams->bfParams.initialCapacity = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->bfParams.blockSize = LoadUnsigned_IOError(rdb, goto fail);
    break;
  case VecSimAlgo_HNSWLIB:
    vecsimParams->hnswParams.type = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.dim = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.metric = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.multi = false;
    vecsimParams->hnswParams.initialCapacity = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.M = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.efConstruction = LoadUnsigned_IOError(rdb, goto fail);
    vecsimParams->hnswParams.efRuntime = LoadUnsigned_IOError(rdb, goto fail);
    break;
  }

  return VecSimIndex_validate_Rdb_parameters(rdb, vecsimParams);

fail:
  return REDISMODULE_ERR;
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
      RSErrorCode = QUERY_EDUPPARAM;
      break;
    }
    case VecSimParamResolverErr_UnknownParam: {
      RSErrorCode = QUERY_ENOOPTION;
      break;
    }
    case VecSimParamResolverErr_BadValue: {
      RSErrorCode = QUERY_EBADVAL;
      break;
    }
    case VecSimParamResolverErr_InvalidPolicy_NHybrid: {
      RSErrorCode = QUERY_ENHYBRID;
      break;
    }
    case VecSimParamResolverErr_InvalidPolicy_NExits: {
      RSErrorCode = QUERY_EHYBRIDNEXIST;
      break;
    }
    case VecSimParamResolverErr_InvalidPolicy_AdHoc_With_BatchSize: {
      RSErrorCode = QUERY_EADHOCWBATCHSIZE;
      break;
    }
    case VecSimParamResolverErr_InvalidPolicy_AdHoc_With_EfRuntime: {
      RSErrorCode = QUERY_EADHOCWEFRUNTIME;
      break;
    }
    case VecSimParamResolverErr_InvalidPolicy_NRange: {
      RSErrorCode = QUERY_ENRANGE;
      break;
    }
    default: {
      RSErrorCode = QUERY_EGENERIC;
    }
  }
  const char *error_msg = QueryError_Strerror(RSErrorCode);
  QueryError_SetErrorFmt(status, RSErrorCode, "Error parsing vector similarity parameters: %s", error_msg);
  return vecSimCode;
}
