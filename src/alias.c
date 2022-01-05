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

int AliasTable_Add(AliasTable *table, const char *alias, IndexSpec *spec, int options,
                   QueryError *error) {
  // look up and see if it exists:
  dictEntry *e, *existing = NULL;
  e = dictAddRaw(table->d, (void *)alias, &existing);
  if (existing) {
    QueryError_SetError(error, QUERY_EINDEXEXISTS, "Alias already exists");
    return REDISMODULE_ERR;
  }
  RS_LOG_ASSERT(e->key != alias, "Alias should be different than key");
  e->v.val = spec;
  if (!(options & INDEXALIAS_NO_BACKREF)) {
    char *duped = rm_strdup(alias);
    spec->aliases = array_ensure_append(spec->aliases, &duped, 1, char *);
  }
  if (table->on_add) {
    table->on_add(alias, spec);
  }
  return REDISMODULE_OK;
}

int AliasTable_Del(AliasTable *table, const char *alias, IndexSpec *spec, int options,
                   QueryError *error) {
  char *toFree = NULL;
  // ensure that the item exists in the list
  if (!spec->aliases) {
    QueryError_SetError(error, QUERY_ENOINDEX, "Alias does not belong to provided spec");
    return REDISMODULE_ERR;
  }

  ssize_t idx = -1;
  for (size_t ii = 0; ii < array_len(spec->aliases); ++ii) {
    // note, NULL might be here if we're clearing the spec's aliases
    if (spec->aliases[ii] && !strcasecmp(spec->aliases[ii], alias)) {
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
    rm_free(toFree);
  }
  return REDISMODULE_OK;
}

IndexSpec *AliasTable_Get(AliasTable *tbl, const char *alias) {
  dictEntry *e = dictFind(tbl->d, alias);
  if (e) {
    return e->v.val;
  } else {
    return NULL;
  }
}

int IndexAlias_Add(const char *alias, IndexSpec *spec, int options, QueryError *status) {
  return AliasTable_Add(AliasTable_g, alias, spec, options, status);
}

int IndexAlias_Del(const char *alias, IndexSpec *spec, int options, QueryError *status) {
  return AliasTable_Del(AliasTable_g, alias, spec, options, status);
}

IndexSpec *IndexAlias_Get(const char *alias) {
  return AliasTable_Get(AliasTable_g, alias);
}

void IndexSpec_ClearAliases(IndexSpec *sp) {
  if (!sp->aliases) {
    return;
  }
  for (size_t ii = 0; ii < array_len(sp->aliases); ++ii) {
    char **pp = sp->aliases + ii;
    QueryError e = {0};
    int rc = IndexAlias_Del(*pp, sp, INDEXALIAS_NO_BACKREF, &e);
    RS_LOG_ASSERT(rc == REDISMODULE_OK, "Alias delete has failed");
    rm_free(*pp);
    // set to NULL so IndexAlias_Del skips over this
    *pp = NULL;
  }
  array_free(sp->aliases);
}