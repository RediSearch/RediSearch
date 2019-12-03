#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "dep/libnu/libnu.h"
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
  RSSortingVector *ret = rm_calloc(1, sizeof(RSSortingVector) + len * (sizeof(*ret->values)));
  ret->len = len;
  // set all values to NIL
  for (int i = 0; i < len; i++) {
    ret->values[i] = RSValue_IncrRef(RS_NullVal());
  }
  return ret;
}

/* Internal compare function between members of the sorting vectors, sorted by sk */
inline int RSSortingVector_Cmp(RSSortingVector *self, RSSortingVector *other, RSSortingKey *sk) {

  RSValue *v1 = self->values[sk->index];
  RSValue *v2 = other->values[sk->index];
  int rc = RSValue_Cmp(v1, v2);
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
void RSSortingVector_Put(RSSortingVector *tbl, int idx, void *p, int type) {
  //  if(tbl->values[idx] && tbl->values[idx]->t != RSValue_Null){
  //    RSValue_Free(tbl->values[idx]);
  //  }
  if (idx <= RS_SORTABLES_MAX) {
    switch (type) {
      case RS_SORTABLE_NUM:
        tbl->values[idx] = RSValue_IncrRef(RS_NumVal(*(double *)p));

        break;
      case RS_SORTABLE_STR: {
        char *ns = normalizeStr((char *)p);
        tbl->values[idx] = RSValue_IncrRef(RS_StringValT(ns, strlen(ns), RSString_RMAlloc));
        break;
      }
      case RS_SORTABLE_NIL:
      default:
        tbl->values[idx] = RSValue_IncrRef(RS_NullVal());
        break;
    }
  }
}
RSValue *RSSortingVector_Get(RSSortingVector *v, RSSortingKey *k) {
  if (!v || !k) return NULL;
  if (k->index >= 0 && k->index < v->len) {
    return v->values[k->index];
  }
  return NULL;
}

/* Free a sorting vector */
void SortingVector_Free(RSSortingVector *v) {
  for (int i = 0; i < v->len; i++) {
    RSValue_Free(v->values[i]);
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
        char *s1 = rm_strdup(s);
        RedisModule_Free(s);
        vec->values[i] = RSValue_IncrRef(RS_StringValT(s1, len - 1, RSString_RMAlloc));
        break;
      }
      case RS_SORTABLE_NUM:
        // load numeric value
        vec->values[i] = RSValue_IncrRef(RS_NumVal(RedisModule_LoadDouble(rdb)));
        break;
      // for nil we read nothing
      case RS_SORTABLE_NIL:
      default:
        vec->values[i] = RSValue_IncrRef(RS_NullVal());
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
  return tbl;
}

void SortingTable_Free(RSSortingTable *t) {
  rm_free(t);
}

int RSSortingTable_Add(RSSortingTable *tbl, const char *name, RSValueType t) {
  assert(tbl->len < RS_SORTABLES_MAX);
  tbl->fields[tbl->len].name = name;
  tbl->fields[tbl->len].type = t;
  return tbl->len++;
}

RSValueType SortingTable_GetFieldType(RSSortingTable *tbl, const char *name, RSValueType deflt) {
  if (tbl) {

    for (int i = 0; i < tbl->len; i++) {
      if (!strcasecmp(tbl->fields[i].name, name)) {
        return tbl->fields[i].type;
      }
    }
  }
  return deflt;
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

/* Parse the sorting key of a query from redis arguments. We expect SORTBY {filed} [ASC/DESC]. The
 * default is ASC if not specified.*/
int RSSortingTable_ParseKey(RSSortingTable *tbl, RSSortingKey *k, RedisModuleString **argv,
                            int argc, size_t *offset) {
  const char *field = NULL;
  k->index = -1;
  k->ascending = 1;
  if (!tbl) return REDISMODULE_ERR;

  if (argc == *offset) {
    return REDISMODULE_ERR;
  }

  // Parse the field
  const char *fieldName = RedisModule_StringPtrLen(argv[*offset], NULL);
  k->index = RSSortingTable_GetFieldIdx(tbl, fieldName);
  if (k->index == -1) {
    return REDISMODULE_ERR;
  }

  if (++*offset == argc) {
    return REDISMODULE_OK;
  }

  if (RMUtil_StringEqualsCaseC(argv[*offset], "ASC")) {
    k->ascending = 1;
    ++*offset;
  } else if (RMUtil_StringEqualsCaseC(argv[*offset], "DESC")) {
    k->ascending = 0;
    ++*offset;
  }
  return REDISMODULE_OK;
}

void RSSortingKey_Free(RSSortingKey *k) {
  rm_free(k);
}
