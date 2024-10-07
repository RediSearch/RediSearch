/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "alias.h"
#include "spec.h"
#include "util/dict.h"
#include "rmutil/rm_assert.h"

AliasTable *AliasTable_g = NULL;

AliasTable *AliasTable_New(void) {
  AliasTable *t = rm_calloc(1, sizeof(*t));
  t->d = dictCreate(&dictTypeHeapStrings, NULL);
  return t;
}

void IndexAlias_InitGlobal(void) {
  AliasTable_g = AliasTable_New();
}

void IndexAlias_DestroyGlobal(AliasTable **t) {
  if (!*t) {
    return;
  }
  dictRelease((*t)->d);
  rm_free(*t);
  *t = NULL;
}

static int AliasTable_Add(AliasTable *table, HiddenName *alias, StrongRef spec_ref, int options, QueryError *error) {
  // look up and see if it exists:
  dictEntry *e, *existing = NULL;
  IndexSpec *spec = StrongRef_Get(spec_ref);
  e = dictAddRaw(table->d, (void *)alias, &existing);
  if (existing) {
    QueryError_SetError(error, QUERY_EINDEXEXISTS, "Alias already exists");
    return REDISMODULE_ERR;
  }
  RS_LOG_ASSERT(e->key != alias, "Alias should be different than key");
  // Dictionary holds a pointer tho the spec manager. Its the same reference owned by the specs dictionary.
  e->v.val = spec_ref.rm;
  if (!(options & INDEXALIAS_NO_BACKREF)) {
    spec->aliases = array_ensure_append(spec->aliases, alias, 1, HiddenName *);
  }
  if (table->on_add) {
    table->on_add(alias, spec);
  }
  return REDISMODULE_OK;
}

static int AliasTable_Del(AliasTable *table, HiddenName *alias, StrongRef spec_ref, int options,
                          QueryError *error) {
  IndexSpec *spec = StrongRef_Get(spec_ref);
  HiddenName *toFree = NULL;

  ssize_t idx = -1;
  for (size_t ii = 0; ii < array_len(spec->aliases); ++ii) {
    // note, NULL might be here if we're clearing the spec's aliases
    if (spec->aliases[ii] && !HiddenName_CaseSensitiveCompare(spec->aliases[ii], alias)) {
      idx = ii;
      break;
    }
  }
  if (idx == -1) {
    QueryError_SetError(error, QUERY_ENOINDEX, "Alias does not belong to provided spec");
    return REDISMODULE_ERR;
  }

  if (!(options & INDEXALIAS_NO_BACKREF)) {
    toFree = spec->aliases[idx];
    spec->aliases = array_del_fast(spec->aliases, idx);
  }
  int rc = dictDelete(table->d, alias);
  RS_LOG_ASSERT(rc == DICT_OK, "Dictionary delete failed");
  if (table->on_del) {
    table->on_del(alias, spec);
  }

  if (toFree) {
    HiddenName_Free(toFree, true);
  }
  return REDISMODULE_OK;
}

StrongRef AliasTable_Get(AliasTable *tbl, const char *alias) {
  StrongRef ret = {0};
  dictEntry *e = dictFind(tbl->d, alias);
  if (e) {
    ret.rm = e->v.val;
  }
  return ret;
}

int IndexAlias_Add(HiddenName *alias, StrongRef spec_ref, int options, QueryError *status) {
  return AliasTable_Add(AliasTable_g, alias, spec_ref, options, status);
}

int IndexAlias_Del(HiddenName *alias, StrongRef spec_ref, int options, QueryError *status) {
  return AliasTable_Del(AliasTable_g, alias, spec_ref, options, status);
}

StrongRef IndexAlias_Get(const char *alias) {
  return AliasTable_Get(AliasTable_g, alias);
}

void IndexSpec_ClearAliases(StrongRef spec_ref) {
  IndexSpec *sp = StrongRef_Get(spec_ref);
  for (size_t ii = 0; ii < array_len(sp->aliases); ++ii) {
    HiddenName **pp = sp->aliases + ii;
    QueryError e = {0};
    int rc = IndexAlias_Del(*pp, spec_ref, INDEXALIAS_NO_BACKREF, &e);
    RS_LOG_ASSERT(rc == REDISMODULE_OK, "Alias delete has failed");
    rm_free(*pp);
    // set to NULL so IndexAlias_Del skips over this
    *pp = NULL;
  }
  array_free(sp->aliases);
}
