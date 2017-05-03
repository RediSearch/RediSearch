#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "rmalloc.h"
#include "sortable.h"

RSSortingVector *NewSortingVector(int len) {
  if (len > 255) {
    return NULL;
  }
  RSSortingVector *ret = rm_calloc(1, sizeof(RSSortingVector) + len * (sizeof(RSSortableValue)));
  ret->len = len;
  // set all values to NIL
  for (int i = 0; i < len; i++) {
    ret->values[i].type = RS_SORTABLE_NIL;
  }
  return ret;
}

void RSSortingVector_Free(RSSortingVector *tbl) {
  rm_free(tbl);
  // TODO: Free all values if needed
}

int RSSortingVector_Cmp(RSSortingVector *self, RSSortingVector *other, int idx) {

  RSSortableValue v1 = self->values[idx];
  RSSortableValue v2 = other->values[idx];
  if (v2.type == RS_SORTABLE_NIL) {
    printf("v2 is nil\n");
    return v1.type == RS_SORTABLE_NIL ? 0 : 1;
  }

  assert(v1.type == v2.type || v1.type == RS_SORTABLE_NIL);
  switch (v1.type) {
    case RS_SORTABLE_NUM: {
      int rc = v1.num < v2.num ? 1 : (v2.num < v1.num ? -1 : 0);
      printf("v1: %f, v2: %f, rc: %d\n", v1.num, v2.num, rc);
      return rc;
    }
    case RS_SORTABLE_STR: {
      int rc = -1 * strcmp(v1.str, v2.str);
      printf("v1: %s, v2: %s, rc: %d\n", v1.str, v2.str, rc);
      return rc;
    }

    case RS_SORTABLE_NIL:
      printf("v1 is nul, returning -1\n");

      // we've already checked that v2 is not nil so v1 must be smaller than it
      return -1;
  }
  return 0;
}

void RSSortingVector_Put(RSSortingVector *tbl, int idx, void *p, int type) {
  if (idx < 255) {
    switch (type) {
      case RS_SORTABLE_NUM:
        tbl->values[idx].num = *(double *)p;

        break;
      case RS_SORTABLE_STR:
        tbl->values[idx].str = rm_strdup((char *)p);
      case RS_SORTABLE_NIL:
      default:
        break;
    }
    tbl->values[idx].type = type;
  }
}

void SortingVector_Free(RSSortingVector *v) {
  for (int i = 0; i < v->len; i++) {
    if (v->values[i].type == RS_SORTABLE_STR) {
      rm_free(v->values[i].str);
    }
  }
  rm_free(v);
}
void SortingVector_RdbSave(RedisModuleIO *rdb, RSSortingVector *v) {
  RedisModule_SaveUnsigned(rdb, v->len);
  for (int i = 0; i < v->len; i++) {
    RSSortableValue *val = &v->values[i];
    RedisModule_SaveUnsigned(rdb, val->type);
    switch (val->type) {
      case RS_SORTABLE_STR:
        // save string - one extra byte for null terminator
        RedisModule_SaveStringBuffer(rdb, val->str, strlen(val->str) + 1);
        break;

      case RS_SORTABLE_NUM:
        // save numeric value
        RedisModule_SaveDouble(rdb, val->num);
        break;
      // for nil we write nothing
      case RS_SORTABLE_NIL:
      default:
        break;
    }
  }
}
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb, int encver) {

  int len = (int)RedisModule_LoadUnsigned(rdb);
  if (len > 255 || len <= 0) {
    return NULL;
  }
  RSSortingVector *vec = NewSortingVector(len);
  for (int i = 0; i < len; i++) {
    vec->values[i].type = RedisModule_LoadUnsigned(rdb);
    switch (vec->values[i].type) {
      case RS_SORTABLE_STR: {
        size_t len;
        // strings include an extra character for null terminator. we set it to zero just in case
        vec->values[i].str = RedisModule_LoadStringBuffer(rdb, &len);
        vec->values[i].str[len - 1] = '\0';

        break;
      }
      case RS_SORTABLE_NUM:
        // load numeric value
        vec->values[i].num = RedisModule_LoadDouble(rdb);
        break;
      // for nil we read nothing
      case RS_SORTABLE_NIL:
      default:
        break;
    }
  }
  return vec;
}

RSSortingTable *NewSortingTable(int len) {
  RSSortingTable *tbl = rm_calloc(1, sizeof(RSSortingTable) + len * sizeof(const char *));
  tbl->len = len;
  return tbl;
}

void SortingTable_Free(RSSortingTable *t) {
  rm_free(t);
}

void SortingTable_SetFieldName(RSSortingTable *tbl, int idx, const char *name) {
  if (idx >= tbl->len) {
    return;
  }
  tbl->fields[idx] = name;
}

int RSSortingTable_GetFieldIdx(RSSortingTable *tbl, const char *field) {
  for (int i = 0; i < tbl->len; i++) {
    if (!strcasecmp(tbl->fields[i], field)) {
      return i;
    }
  }
  return -1;
}
