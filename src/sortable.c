/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <stdlib.h>
#include <stdio.h>
#include "rmutil/rm_assert.h"
#include "libnu/libnu.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmalloc.h"
#include "sortable.h"
#include "buffer.h"

/* Create a sorting vector of a given length for a document */
RSSortingVector *NewSortingVector(size_t len) {
  if (len > RS_SORTABLES_MAX) {
    return NULL;
  }
  RSSortingVector *ret = rm_malloc(sizeof(RSSortingVector) + (len * sizeof(RSValue*)));
  ret->len = len;
  // set all values to NIL
  for (int i = 0; i < len; i++) {
    ret->values[i] = RS_NullVal();
  }
  return ret;
}

/* Normalize sorting string for storage. This folds everything to unicode equivalent strings. The
 * allocated return string needs to be freed later */
char *normalizeStr(const char *str) {

  size_t buflen = 2 * strlen(str) + 1;
  char *lower_buffer = rm_calloc(buflen, 1);
  char *lower = lower_buffer;
  char *end = lower + buflen;

  const char *p = str;
  size_t off = 0;
  while (*p != 0 && lower < end) {
    uint32_t in = 0;
    p = nu_utf8_read(p, &in);
    const char *lo = nu_tofold(in);

    if (lo != 0) {
      uint32_t u = 0;
      do {
        lo = nu_casemap_read(lo, &u);
        if (u == 0) break;
        lower = nu_utf8_write(u, lower);
      } while (u != 0 && lower < end);
    } else {
      lower = nu_utf8_write(in, lower);
    }
  }

  return lower_buffer;
}

#define RSPUT_SANITY_CHECK \
if (idx > vec->len) {\
  return;\
}\
if (vec->values[idx]) {\
  RSValue_Decref(vec->values[idx]);\
}

void RSSortingVector_PutNum(RSSortingVector *vec, size_t idx, double num) {
  RSPUT_SANITY_CHECK
  vec->values[idx] = RS_NumVal(num);
}

void RSSortingVector_PutStr(RSSortingVector* vec, size_t idx, const char* str) {
  RSPUT_SANITY_CHECK
  vec->values[idx] = RS_StringValT(str, strlen(str), RSString_RMAlloc);
}

void RSSortingVector_PutRSVal(RSSortingVector* vec, size_t idx, RSValue* val) {
  RSPUT_SANITY_CHECK
  vec->values[idx] = val;
}

/* Free a sorting vector */
void SortingVector_Free(RSSortingVector *v) {
  for (size_t i = 0; i < v->len; i++) {
    RSValue_Decref(v->values[i]);
  }
  rm_free(v);
}

/* Load a sorting vector from RDB */
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb) {

  int len = (int)RedisModule_LoadUnsigned(rdb);
  if (len > RS_SORTABLES_MAX || len <= 0) {
    return NULL;
  }
  RSSortingVector *vec = NewSortingVector(len);
  for (int i = 0; i < len; i++) {
    RSValueType t = RedisModule_LoadUnsigned(rdb);

    switch (t) {
      case RSValue_String: {
        size_t len;
        // strings include an extra character for null terminator. we set it to zero just in case
        char *s = RedisModule_LoadStringBuffer(rdb, &len);
        s[len - 1] = '\0';
        vec->values[i] = RS_StringValT(rm_strdup(s), len - 1, RSString_RMAlloc);
        RedisModule_Free(s);
        break;
      }
      case RS_SORTABLE_NUM:
        // load numeric value
        vec->values[i] = RS_NumVal(RedisModule_LoadDouble(rdb));
        break;
      // for nil we read nothing
      case RS_SORTABLE_NIL:
      default:
        vec->values[i] = RS_NullVal();
        break;
    }
  }
  return vec;
}

size_t RSSortingVector_GetMemorySize(RSSortingVector *v) {
  if (!v) return 0;

  size_t sum = v->len * sizeof(RSValue *);
  for (int i = 0; i < v->len; i++) {
    if (!v->values[i] || v->values[i] == RS_NullVal()) continue;
    sum += sizeof(RSValue);

    RSValue *val = RSValue_Dereference(v->values[i]);
    if (RSValue_IsString(val)) {
      size_t sz;
      RSValue_StringPtrLen(val, &sz);
      sum += sz;
    }
  }
  return sum;
}
