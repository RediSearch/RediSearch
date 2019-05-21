#include "alias.h"
#include "util/dict.h"

AliasTable *AliasTable_g = NULL;

AliasTable *AliasTable_New(void) {
  return dictCreate(&dictTypeHeapStrings, NULL);
}

void IndexAlias_InitGlobal(void) {
  AliasTable_g = AliasTable_New();
}

void IndexAlias_DestroyGlobal(void) {
  if (!AliasTable_g) {
    return;
  }
  dictRelease(AliasTable_g);
  AliasTable_g = NULL;
}

int AliasTable_Add(AliasTable *table, const char *alias, IndexSpec *spec, int options,
                   QueryError *error) {
  // look up and see if it exists:
  dictEntry *e, *existing = NULL;
  e = dictAddRaw(table, (void *)alias, &existing);
  if (existing) {
    QueryError_SetError(error, QUERY_EINDEXEXISTS, "Alias already exists");
    return REDISMODULE_ERR;
  }
  assert(e->key != alias);
  e->v.val = spec;
  if (!(options & INDEXALIAS_NO_BACKREF)) {
    char *duped = rm_strdup(alias);
    spec->aliases = array_ensure_append(spec->aliases, &duped, 1, char *);
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
    size_t oldLen = array_len(spec->aliases);
    spec->aliases = array_del(spec->aliases, idx);
  }

  int rc = REDISMODULE_OK;
  if (dictDelete(table, alias) == DICT_ERR) {
    QueryError_SetError(error, QUERY_ENOINDEX, "Alias does not belong to index");
    rc = REDISMODULE_ERR;
  }

  if (toFree) {
    rm_free(toFree);
  }
  return REDISMODULE_OK;
}

IndexSpec *AliasTable_Get(AliasTable *tbl, const char *alias) {
  dictEntry *e = dictFind(tbl, alias);
  if (e) {
    return e->v.val;
  } else {
    return NULL;
  }
}

#define ENSURE_INIT()

int IndexAlias_Add(const char *alias, IndexSpec *spec, int options, QueryError *status) {
  if (!AliasTable_g) {
    IndexAlias_InitGlobal();
  }
  return AliasTable_Add(AliasTable_g, alias, spec, options, status);
}
int IndexAlias_Del(const char *alias, IndexSpec *spec, int options, QueryError *status) {
  if (!AliasTable_g) {
    QueryError_SetError(status, QUERY_ENOINDEX, "No alias have been created");
    return REDISMODULE_ERR;
  }
  return AliasTable_Del(AliasTable_g, alias, spec, options, status);
}
IndexSpec *IndexAlias_Get(const char *alias) {
  if (!AliasTable_g) {
    return NULL;
  }
  return AliasTable_Get(AliasTable_g, alias);
}

void IndexSpec_ClearAliases(IndexSpec *sp) {
  if (!sp->aliases) {
    return;
  }
  for (size_t ii = 0; ii < array_len(sp->aliases); ++ii) {
    char **pp = sp->aliases + ii;
    if (!*pp) {
      continue;
    }
    QueryError e = {0};
    int rc = IndexAlias_Del(*pp, sp, INDEXALIAS_NO_BACKREF, &e);
    if (rc != REDISMODULE_OK) {
      fprintf(stderr, "redisearch: alias %s in list for %s but does not exist: %s\n", *pp, sp->name,
              QueryError_GetError(&e));
      QueryError_ClearError(&e);
    }
    rm_free(*pp);
    *pp = NULL;
  }
  array_free(sp->aliases);
}