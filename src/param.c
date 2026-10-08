/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "param.h"

#include <string.h>

#include "query_error_ffi.h"
#include "rmalloc.h"
#include "redismodule.h"

// Keys and values are borrowed from the request's held argv, so the dict owns neither.
static dictType dictTypeBorrowedParams = {
  .hashFunction = stringsHashFunction,
  .keyDup = NULL,
  .valDup = NULL,
  .keyCompare = stringsKeyCompare,
  .keyDestructor = NULL,
  .valDestructor = NULL,
};

dict *Param_DictCreate() {
  return dictCreate(&dictTypeBorrowedParams, NULL);
}

int Param_DictAdd(dict *d, const char *name, RedisModuleString *value, QueryError *status) {
  int res = dictAdd(d, (void*)name, value);
  if (res == DICT_ERR) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_ADD_ARGS, "Duplicate parameter", " `%s`", name);
  }
  return res;
}

const char *Param_DictGet(dict *d, const char *name, size_t name_len, size_t *value_len, QueryError *status) {
  RedisModuleString *rms_val = NULL;
  if (d) {
    // Dict keys are NUL-terminated, while names usually point into the middle of the query.
    char stackKey[64];
    char *key = name_len < sizeof(stackKey) ? stackKey : rm_malloc(name_len + 1);
    memcpy(key, name, name_len);
    key[name_len] = '\0';
    rms_val = dictFetchValue(d, key);
    if (key != stackKey) {
      rm_free(key);
    }
  }
  if (!rms_val) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_NO_PARAM, "Parameter not found", " `%.*s`", (int)name_len, name);
    return NULL;
  }
  const char *val = RedisModule_StringPtrLen(rms_val, value_len);
  return val;
}

void Param_DictFree(dict *d) {
  dictRelease(d);
}

dict *Param_DictClone(dict *source) {
  if (!source) {
    return NULL;
  }

  dict *clone = Param_DictCreate();
  dictExpand(clone, dictSize(source));
  dictIterator *iter = dictGetIterator(source);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    dictAdd(clone, dictGetKey(entry), dictGetVal(entry));
  }
  dictReleaseIterator(iter);

  return clone;
}
