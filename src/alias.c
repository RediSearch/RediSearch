#include "alias.h"
#include "util/dict.h"
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

void IndexAlias::InitGlobal() {
  AliasTable_g = new AliasTable();
}

//---------------------------------------------------------------------------------------------

void IndexAlias::DestroyGlobal() {
  if (!AliasTable_g) {
    return;
  }

  delete AliasTable_g;
  AliasTable_g = NULL;
}


//---------------------------------------------------------------------------------------------

int AliasTable::Add(const char *alias, IndexSpec *spec, int options, QueryError *error) {
  bool success = d.insert({alias, spec}).second;
  if (!success) {
    error->SetError(QUERY_EINDEXEXISTS, "Alias already exists");
    return REDISMODULE_ERR;
  }
  if (!(options & INDEXALIAS_NO_BACKREF)) {
    spec->addAlias(alias);
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
    spec->delAlias(idx);
  }

  int rc = d.erase(alias);
  if (!rc) throw Error("Alias '%s' delete failed", alias);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

IndexSpec *AliasTable::Get(const char *alias) {
  return d[alias];
}

///////////////////////////////////////////////////////////////////////////////////////////////

AliasTable *IndexAlias::AliasTable_g = NULL;

//---------------------------------------------------------------------------------------------
int IndexAlias::Add(const char *alias, IndexSpec *spec, int options, QueryError *status) {
  return AliasTable_g->Add(alias, spec, options, status);
}

//---------------------------------------------------------------------------------------------

int IndexAlias::Del(const char *alias, IndexSpec *spec, int options, QueryError *status) {
  return AliasTable_g->Del(alias, spec, options, status);
}

//---------------------------------------------------------------------------------------------

IndexSpec *IndexAlias::Get(const char *alias) {
  return AliasTable_g->Get(alias);
}

//---------------------------------------------------------------------------------------------

void IndexSpec::ClearAliases() {
  if (!aliases) {
    return;
  }
  for (size_t i = 0; i < array_len(aliases); ++i) {
    char **pp = &aliases[i];
    QueryError e;
    int rc = IndexAlias::Del(*pp, this, INDEXALIAS_NO_BACKREF, &e);
    if (rc != REDISMODULE_OK) throw Error("Alias delete has failed");
    rm_free(*pp);
    // set to NULL so IndexAlias::Del skips over this
    *pp = NULL;
  }
  array_free(aliases);
}

///////////////////////////////////////////////////////////////////////////////////////////////
