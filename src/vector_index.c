#include "vector_index.h"
#include "list_reader.h"
#include "query_param.h"

// taken from parser.c
void unescape(char *s, size_t *sz) {
  
  char *dst = s;
  char *src = dst;
  char *end = s + *sz;
  while (src < end) {
      // unescape 
      if (*src == '\\' && src + 1 < end &&
         (ispunct(*(src+1)) || isspace(*(src+1)))) {
          ++src;
          --*sz;
          continue;
      }
      *dst++ = *src++;
  }
}

static VecSimIndex *openVectorKeysDict(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                             int write) {
  IndexSpec *spec = ctx->spec;
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
    if (!strncasecmp(fieldStr, spec->fields[i].name, fieldLen)) {
      fieldSpec = &spec->fields[i];
    }
  }
  if (fieldSpec == NULL) {
    return NULL;
  }

  // create new vector data structure
  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->p = VecSimIndex_New(&fieldSpec->vecSimParams);
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

VecSimIndex *OpenVectorIndex(RedisSearchCtx *ctx,
                            RedisModuleString *keyName) {
  return openVectorKeysDict(ctx, keyName, 1);
}

static bool isFit (VecSimIndex *ind, size_t size) {
  VecSimIndexInfo info = VecSimIndex_Info(ind);
  size_t dim = 0;
  VecSimType type = 0;
  bool res = false;
  switch (info.algo) {
    case VecSimAlgo_HNSWLIB:
      dim = info.hnswInfo.dim;
      type = info.hnswInfo.type;
      break;
    case VecSimAlgo_BF:
      dim = info.bfInfo.dim;
      type = info.bfInfo.type;
      break;
  }
  switch (type) {
    case VecSimType_FLOAT32:
    case VecSimType_FLOAT64:
    case VecSimType_INT32:
    case VecSimType_INT64:
      res = ((dim * sizeof(float)) == size);
      break;
  }
  return res;
}

IndexIterator *NewVectorIterator(RedisSearchCtx *ctx, VectorQuery *vq, QueryError *status) {
  // TODO: change Dict to hold strings
  RedisModuleString *key = RedisModule_CreateStringPrintf(ctx->redisCtx, "%s", vq->property);
  VecSimIndex *vecsim = openVectorKeysDict(ctx, key, 0);
  RedisModule_FreeString(ctx->redisCtx, key);
  if (!vecsim) {
    return NULL;
  }

  switch (vq->type) {
    case VECSIM_QT_TOPK:;
      VecSimQueryParams qParams;
      int err;
      if ((err = VecSimIndex_ResolveParams(vecsim, vq->params.params, array_len(vq->params.params), &qParams)) != VecSim_OK) {
        err = VecSimResolveCode_to_QueryErrorCode(err);
        QueryError_SetErrorFmt(status, err, "Error parsing vector similarity parameters: %s", QueryError_Strerror(err));
        return NULL;
      }
      if (!isFit(vecsim, vq->topk.vecLen)) {
        QueryError_SetError(status, QUERY_EINVAL, "Error parsing vector similarity query: query vector does not match index's type or dimention.");
        return NULL;
      }
      vq->results = VecSimIndex_TopKQuery(vecsim, vq->topk.vector, vq->topk.k, &qParams, vq->topk.order );
      vq->resultsLen = VecSimQueryResult_Len(vq->results);
      break;
  }

  return NewListIterator(vq->results, vq->resultsLen);
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
    case VECSIM_QT_TOPK: // no need to free the vector as we pointes to the query dictionary
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

int VecSimResolveCode_to_QueryErrorCode(int code) {
  switch (code) {
    case VecSim_OK: return QUERY_OK;
    case VecSimParamResolverErr_AlreadySet: return QUERY_EDUPFIELD;
    case VecSimParamResolverErr_UnknownParam: return QUERY_ENOOPTION;
    case VecSimParamResolverErr_BadValue: return QUERY_EBADATTR;
  }
  return QUERY_EGENERIC;
}
