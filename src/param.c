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
#include "rmutil/rm_assert.h"
#include "util/charbuf.h"

// The keys' CharBufs live in one array sized up front, handed to the dict as its private data;
// their bytes are borrowed like the values.
typedef struct {
  size_t len;
  size_t cap;
  CharBuf keys[];
} ParamKeys;

static void *borrowedKeyDup(void *privdata, const void *key) {
  ParamKeys *keys = privdata;
  RS_ASSERT(keys->len < keys->cap);
  CharBuf *cb = &keys->keys[keys->len++];
  *cb = *(const CharBuf *)key;
  return cb;
}

// CharBuf keys let lookups use names that point into the query, which are not NUL-terminated.
static dictType dictTypeBorrowedParams = {
  .hashFunction = CharBuf_HashFunction,
  .keyDup = borrowedKeyDup,
  .valDup = NULL,
  .keyCompare = CharBuf_KeyCompare,
  .keyDestructor = NULL,
  .valDestructor = NULL,
};

dict *Param_DictCreate(size_t capacity) {
  ParamKeys *keys = rm_malloc(sizeof(*keys) + capacity * sizeof(*keys->keys));
  keys->len = 0;
  keys->cap = capacity;
  dict *d = dictCreate(&dictTypeBorrowedParams, keys);
  dictExpand(d, capacity);
  return d;
}

int Param_DictAdd(dict *d, const char *name, size_t name_len, RedisModuleString *value, QueryError *status) {
  CharBuf key = {.buf = (char *)name, .len = name_len};
  int res = dictAdd(d, &key, value);
  if (res == DICT_ERR) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_ADD_ARGS, "Duplicate parameter", " `%.*s`", (int)name_len, name);
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
  void *keys = d->privdata;
  dictRelease(d);
  rm_free(keys);
}

dict *Param_DictClone(dict *source) {
  if (!source) {
    return NULL;
  }

  dict *clone = Param_DictCreate(dictSize(source));
  dictIterator *iter = dictGetIterator(source);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    dictAdd(clone, dictGetKey(entry), dictGetVal(entry));
  }
  dictReleaseIterator(iter);

  return clone;
}
