#include "vector_index.h"
#include "list_reader.h"
#include "dep/base64/base64.h"

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
  dictAdd(ctx->spec->keysDict, keyName, kdv);
  kdv->dtor = (void (*)(void *))VecSimIndex_Free;
  return kdv->p;
}

VecSimIndex *OpenVectorIndex(RedisSearchCtx *ctx,
                            RedisModuleString *keyName) {
  return openVectorKeysDict(ctx, keyName, 1);
}

IndexIterator *NewVectorIterator(RedisSearchCtx *ctx, VectorFilter *vf) {
  VecSimQueryResult *result;
  // TODO: change Dict to hold strings
  RedisModuleString *key = RedisModule_CreateStringPrintf(ctx->redisCtx, "%s", vf->property);
  VecSimIndex *vecsim = openVectorKeysDict(ctx, key, 0);
  RedisModule_FreeString(ctx->redisCtx, key);
  if (!vecsim) {
    return NULL;
  }

  size_t outLen;
  unsigned char *vector = vf->vector;
  switch (vf->type) {
    case VECTOR_TOPK:
      if (vf->isBase64) {
        unescape((char *)vector, &vf->vecLen);
        vector = base64_decode(vector, vf->vecLen, &outLen);
      }
      
      VecSimQueryParams qParams = {.hnswRuntimeParams.efRuntime = vf->efRuntime};
      vf->results = VecSimIndex_TopKQueryByID(vecsim, vector, vf->value, &qParams);
      if (vf->isBase64) {
        rm_free(vector);
      }
      break;
    case VECTOR_RANGE:
      RS_LOG_ASSERT(0, "isn't implemented yet");
      break;
  }

  return NewListIterator(vf->results, vf->value);
}

/* Create a vector filter from parsed strings and numbers */
// TODO: add property?
VectorFilter *NewVectorFilter(const void *vector, size_t len, char *type, double value) {
  VectorFilter *vf = rm_calloc(1, sizeof(*vf));
  // copy vector
  vf->vector = rm_malloc(len);
  memcpy(vf->vector, vector, len);
  vf->vecLen = len;
  vf->efRuntime = HNSW_DEFAULT_EF_RT;

  if (!strncasecmp(type, "TOPK", strlen("TOPK"))) {
    vf->type = VECTOR_TOPK;
  } else if (!strncasecmp(type, "RANGE", strlen("RANGE"))) {
    vf->type = VECTOR_TOPK;
    vf->isBase64 = true;
  } else {
    VectorFilter_Free(vf);
    return NULL;
  }

  vf->value = value;

  return vf;
}

void VectorFilter_Free(VectorFilter *vf) {
  if (vf->property) rm_free((char *)vf->property);
  if (vf->vector) rm_free(vf->vector);
  rm_free(vf);
}
