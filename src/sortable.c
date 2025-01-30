/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
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
RSSortingVector *NewSortingVector(int len) {
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

/* Internal compare function between members of the sorting vectors, sorted by sk */
int RSSortingVector_Cmp(RSSortingVector *self, RSSortingVector *other, RSSortingKey *sk,
                        QueryError *qerr) {
  RSValue *v1 = self->values[sk->index];
  RSValue *v2 = other->values[sk->index];
  int rc = RSValue_Cmp(v1, v2, qerr);
  return sk->ascending ? rc : -rc;
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

/* Put a value in the sorting vector */
void RSSortingVector_Put(RSSortingVector *tbl, int idx, const void *p, int type, int unf) {
  if (idx > RS_SORTABLES_MAX) {
    return;
  }
  if (tbl->values[idx]) {
    RSValue_Decref(tbl->values[idx]);
    tbl->values[idx] = NULL;
  }
  switch (type) {
    case RS_SORTABLE_NUM:
      tbl->values[idx] = RS_NumVal(*(double *)p);

      break;
    case RS_SORTABLE_STR: {
      char *str = unf ? rm_strdup(p) : normalizeStr((const char *)p);
      tbl->values[idx] = RS_StringValT(str, strlen(str), RSString_RMAlloc);
      break;
    }
    case RS_SORTABLE_RSVAL:
      tbl->values[idx] = (RSValue*)p;
      break;
    case RS_SORTABLE_NIL:
    default:
      tbl->values[idx] = RS_NullVal();
      break;
  }
}

/* Free a sorting vector */
void SortingVector_Free(RSSortingVector *v) {
  for (size_t i = 0; i < v->len; i++) {
    RSValue_Decref(v->values[i]);
  }
  rm_free(v);
}

/* Save a sorting vector to rdb. This is called from the doc table */
void SortingVector_RdbSave(RedisModuleIO *rdb, RSSortingVector *v) {
  if (!v) {
    RedisModule_SaveUnsigned(rdb, 0);
    return;
  }
  RedisModule_SaveUnsigned(rdb, v->len);
  for (int i = 0; i < v->len; i++) {
    RSValue *val = v->values[i];
    if (!val) {
      RedisModule_SaveUnsigned(rdb, RSValue_Null);
      continue;
    }
    RedisModule_SaveUnsigned(rdb, val->t);
    switch (val->t) {
      case RSValue_String:
        // save string - one extra byte for null terminator
        RedisModule_SaveStringBuffer(rdb, val->strval.str, val->strval.len + 1);
        break;

      case RSValue_Number:
        // save numeric value
        RedisModule_SaveDouble(rdb, val->numval);
        break;
      // for nil we write nothing
      default:
        break;
    }
  }
}

/* Load a sorting vector from RDB */
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb, int encver) {

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
    if (!v->values[i]) continue;
    sum += sizeof(RSValue);

    RSValue *val = RSValue_Dereference(v->values[i]);
    if (val && RSValue_IsString(val)) {
      size_t sz;
      RSValue_StringPtrLen(val, &sz);
      sum += sz;
    }
  }
  return sum;
}

/* Create a new sorting table of a given length */
RSSortingTable *NewSortingTable(void) {
  RSSortingTable *tbl = rm_calloc(1, sizeof(*tbl));
  tbl->cap = 1;
  return tbl;
}

void SortingTable_Free(RSSortingTable *t) {
  rm_free(t);
}

int RSSortingTable_Add(RSSortingTable **tbl, const char *name, RSValueType t) {
  if ((*tbl)->len == RS_SORTABLES_MAX) return -1;

  if ((*tbl)->len == (*tbl)->cap) {
    (*tbl)->cap += 8;
    *tbl = rm_realloc(*tbl, sizeof(RSSortingTable) + ((*tbl)->cap) * sizeof(RSSortField));
  }

  (*tbl)->fields[(*tbl)->len].name = name;
  (*tbl)->fields[(*tbl)->len].type = t;
  return (*tbl)->len++;
}

/* Get the field index by name from the sorting table. Returns -1 if the field was not found */
int RSSortingTable_GetFieldIdx(RSSortingTable *tbl, const char *field) {

  if (!tbl) return -1;
  for (int i = 0; i < tbl->len; i++) {
    if (!strcasecmp(tbl->fields[i].name, field)) {
      return i;
    }
  }
  return -1;
}
