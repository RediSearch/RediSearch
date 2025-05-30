/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "param.h"
#include "rmalloc.h"

#include <assert.h>

void Param_FreeInternal(Param *param) {
  if (param->name) {
    rm_free((void *)param->name);
    param->name = NULL;
  }
}

dict *Param_DictCreate() {
  return dictCreate(&dictTypeHeapStrings, NULL);
}

int Param_DictAdd(dict *d, const char *name, const char *value, size_t value_len, QueryError *status) {
  RedisModuleString *rms_value = RedisModule_CreateString(NULL, value, value_len);
  int res = dictAdd(d, (void*)name, (void*)rms_value);
  if (res == DICT_ERR) {
    RedisModule_FreeString(NULL, rms_value);
    QueryError_SetWithUserDataFmt(status, QUERY_EADDARGS, "Duplicate parameter", " `%s`", name);
  }
  return res;
}

const char *Param_DictGet(dict *d, const char *name, size_t *value_len, QueryError *status) {
  RedisModuleString *rms_val = d ? dictFetchValue(d, name) : NULL;
  if (!rms_val) {
    QueryError_SetWithUserDataFmt(status, QUERY_ENOPARAM, "No such parameter", " `%s`", name);
    return NULL;
  }
  const char *val = RedisModule_StringPtrLen(rms_val, value_len);
  return val;
}

void Param_DictFree(dict *d) {
  dictIterator* iter = dictGetIterator(d);
  dictEntry* entry = NULL;
  while ((entry = dictNext(iter))) {
    RedisModuleString *data = dictGetVal(entry);
    RedisModule_FreeString(NULL, data);
  }
  dictReleaseIterator(iter);
  dictRelease(d);
}
