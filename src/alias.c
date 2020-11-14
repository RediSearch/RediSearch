#include "alias.h"
#include "util/dict.h"
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

AliasTable::AliasTable() {
  d = dictCreate(&dictTypeHeapStrings, NULL);
}

//---------------------------------------------------------------------------------------------

AliasTable::~AliasTable() {
  dictRelease(d);
}

//---------------------------------------------------------------------------------------------

int AliasTable::Add(const char *alias, IndexSpec *spec, int options, QueryError *error) {
  // look up and see if it exists:
  dictEntry *e, *existing = NULL;
  e = dictAddRaw(d, (void *)alias, &existing);
  if (existing) {
    error->SetError(QUERY_EINDEXEXISTS, "Alias already exists");
    return REDISMODULE_ERR;
  }
  RS_LOG_ASSERT(e->key != alias, "Alias should be different than key");
  e->v.val = spec;
  if (!(options & INDEXALIAS_NO_BACKREF)) {
    char *duped = rm_strdup(alias);
    spec->aliases = array_ensure_append(spec->aliases, &duped, 1, char *);
  }
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

int AliasTable::Del(const char *alias, IndexSpec *spec, int options, QueryError *error) {
  char *toFree = NULL;
  // ensure that the item exists in the list
  if (!spec->aliases) {
    error->SetError(QUERY_ENOINDEX, "Alias does not belong to provided spec");
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
    error->SetError(QUERY_ENOINDEX, "Alias does not belong to provided spec");
    return REDISMODULE_ERR;
  }

  if (!(options & INDEXALIAS_NO_BACKREF)) {
    toFree = spec->aliases[idx];
    spec->aliases = array_del_fast(spec->aliases, idx);
  }
  int rc = dictDelete(d, alias);
  RS_LOG_ASSERT(rc == DICT_OK, "Dictionary delete failed");

  if (toFree) {
    rm_free(toFree);
  }
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

IndexSpec *AliasTable::Get(const char *alias) {
  dictEntry *e = dictFind(d, alias);
  if (e) {
    return e->v.val;
  } else {
    return NULL;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

AliasTable *IndexAlias::AliasTable_g = NULL;

//---------------------------------------------------------------------------------------------

void IndexAlias::InitGlobal() {
  AliasTable_g = new AliasTable();
}

//---------------------------------------------------------------------------------------------

void IndexAlias_DestroyGlobal() {
  if (!AliasTable_g) {
    return;
  }
  
  delete AliasTable_g;
  AliasTable_g = NULL;
}

int IndexAlias_Add(const char *alias, IndexSpec *spec, int options, QueryError *status) {
  return AliasTable_g->Add(alias, spec, options, status);
}

//---------------------------------------------------------------------------------------------

int IndexAlias_Del(const char *alias, IndexSpec *spec, int options, QueryError *status) {
  return AliasTable_g->Del(alias, spec, options, status);
}

//---------------------------------------------------------------------------------------------

IndexSpec *IndexAlias_Get(const char *alias) {
  return AliasTable_g->Get(alias);
}

///////////////////////////////////////////////////////////////////////////////////////////////

void IndexSpec::ClearAliases(IndexSpec *sp) {
  if (!aliases) {
    return;
  }
  for (size_t i = 0; i < array_len(aliases); ++i) {
    char **pp = &aliases[i];
    QueryError e = {0};
    int rc = IndexAlias::Del(*pp, sp, INDEXALIAS_NO_BACKREF, &e);
    RS_LOG_ASSERT(rc == REDISMODULE_OK, "Alias delete has failed");
    rm_free(*pp);
    // set to NULL so IndexAlias_Del skips over this
    *pp = NULL;
  }
  array_free(sp->aliases);
}

///////////////////////////////////////////////////////////////////////////////////////////////
