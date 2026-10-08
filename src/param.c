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
#include "spec.h"

// The dict owns only the CharBuf of each key; its bytes are borrowed like the values.
static void *borrowedKeyDup(void *privdata, const void *key) {
  CharBuf *cb = rm_malloc(sizeof(*cb));
  *cb = *(const CharBuf *)key;
  return cb;
}

static void borrowedKeyDestructor(void *privdata, void *key) {
  rm_free(key);
}

// CharBuf keys let lookups use names that point into the query, which are not NUL-terminated.
static dictType dictTypeBorrowedParams = {
  .hashFunction = CharBuf_HashFunction,
  .keyDup = borrowedKeyDup,
  .valDup = NULL,
  .keyCompare = CharBuf_KeyCompare,
  .keyDestructor = borrowedKeyDestructor,
  .valDestructor = NULL,
};

dict *Param_DictCreate() {
  return dictCreate(&dictTypeBorrowedParams, NULL);
}

int Param_DictAdd(dict *d, const char *name, RedisModuleString *value, QueryError *status) {
  CharBuf key = {.buf = (char *)name, .len = strlen(name)};
  int res = dictAdd(d, &key, value);
  if (res == DICT_ERR) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_ADD_ARGS, "Duplicate parameter", " `%s`", name);
  }
  return res;
}

const char *Param_DictGet(dict *d, const char *name, size_t name_len, size_t *value_len, QueryError *status) {
  CharBuf key = {.buf = (char *)name, .len = name_len};
  RedisModuleString *rms_val = d ? dictFetchValue(d, &key) : NULL;
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
