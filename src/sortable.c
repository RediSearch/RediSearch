#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmalloc.h"
#include "sortable.h"

/* Create a sorting vector of a given length for a document */
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

/* Internal compare function between members of the sorting vectors, sorted by sk */
inline int RSSortingVector_Cmp(RSSortingVector *self, RSSortingVector *other, RSSortingKey *sk) {

  RSSortableValue v1 = self->values[sk->index];
  RSSortableValue v2 = other->values[sk->index];
  int rc = 0;
  if (v2.type == RS_SORTABLE_NIL) {
    rc = v1.type == RS_SORTABLE_NIL ? 1 : 0;
  } else {

    assert(v1.type == v2.type || v1.type == RS_SORTABLE_NIL);
    switch (v1.type) {
      case RS_SORTABLE_NUM: {
        rc = v1.num < v2.num ? -1 : (v2.num < v1.num ? 1 : 0);
        break;
      }
      case RS_SORTABLE_STR: {
        rc = strcmp(v1.str, v2.str);
        break;
      }

      case RS_SORTABLE_NIL:
        rc = 1;
        break;
    }
  }
  return sk->ascending ? rc : -rc;
}

/* Put a value in the sorting vector */
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

/* Free a sorting vector */
void SortingVector_Free(RSSortingVector *v) {
  for (int i = 0; i < v->len; i++) {
    if (v->values[i].type == RS_SORTABLE_STR) {
      rm_free(v->values[i].str);
    }
  }
  rm_free(v);
}

/* Save a sorting vector to rdb. This is called from the doc table */
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

/* Load a sorting vector from RDB */
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

/* Create a new sortin table of a given length */
RSSortingTable *NewSortingTable(int len) {
  RSSortingTable *tbl = rm_calloc(1, sizeof(RSSortingTable) + len * sizeof(const char *));
  tbl->len = len;
  return tbl;
}

void SortingTable_Free(RSSortingTable *t) {
  rm_free(t);
}

/* Set a field in the table by index. This is called during the schema parsing */
void SortingTable_SetFieldName(RSSortingTable *tbl, int idx, const char *name) {
  if (idx >= tbl->len) {
    return;
  }
  tbl->fields[idx] = name;
}

/* Get the field index by name from the sorting table. Returns -1 if the field was not found */
int RSSortingTable_GetFieldIdx(RSSortingTable *tbl, const char *field) {
  for (int i = 0; i < tbl->len; i++) {
    if (!strcasecmp(tbl->fields[i], field)) {
      return i;
    }
  }
  return -1;
}

/* Parse the sorting key of a query from redis arguments. We expect SORTBY {filed} [ASC/DESC]. The
 * default is ASC if not specified.  This function returns 1 if we found sorting args, they are
 * valid and the field name exists */
int RSSortingTable_ParseKey(RSSortingTable *tbl, RSSortingKey *k, RedisModuleString **argv,
                            int argc) {
  k->field = NULL;
  k->index = -1;
  k->ascending = 1;
  int sortPos = RMUtil_ArgIndex("SORTBY", argv, argc);
  if (sortPos >= 0 && sortPos + 1 < argc) {

    // parse the sorting field
    RMUtil_ParseArgs(argv, argc, sortPos + 1, "c", &k->field);

    // if we've found a field...
    if (k->field) {
      // parse optional ASC/DESC
      if (sortPos + 2 < argc) {

        if (RMUtil_StringEqualsCaseC(argv[sortPos + 2], "ASC")) {
          k->ascending = 1;
        } else if (RMUtil_StringEqualsCaseC(argv[sortPos + 2], "DESC")) {
          k->ascending = 0;
        }
      }
      // Get the actual field index from the descriptor
      k->index = RSSortingTable_GetFieldIdx(tbl, k->field);
    }
  }
  // return 1 on successful parse, 0 if not found or no sorting key
  return k->index == -1 ? 0 : 1;
}
