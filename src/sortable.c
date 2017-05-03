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
    return v1.type == RS_SORTABLE_NIL ? 0 : 1;
  }

  assert(v1.type == v2.type || v1.type == RS_SORTABLE_NIL);
  switch (v1.type) {
    case RS_SORTABLE_NUM:
      return v1.num > v2.num ? 1 : (v2.num > v1.num ? -1 : 0);
    case RS_SORTABLE_STR:
      return strcmp(v1.str, v2.str);
    case RS_SORTABLE_NIL:
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
        tbl->values[idx].str = strdup((char *)p);
      case RS_SORTABLE_NIL:
      default:
        break;
    }
    tbl->values[idx].type = type;
  }
}

RSSortingTable *NewSortingTable(int len) {
  RSSortingTable *tbl = rm_calloc(1, sizeof(RSSortingTable) + len * sizeof(const char *));
  tbl->len = len;
  return tbl;
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