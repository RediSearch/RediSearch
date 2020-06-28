
#pragma once

#ifndef RULES_RULES_H
#define RULES_RULES_H

#include "query_error.h"
#include "dep/triemap/triemap.h"
#include "stemmer.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct RSExpr;
struct IndexSpec;

typedef struct {
  const char **prefixes;
  int nprefixes;
  char *lang;
} SchemaRuleArgs;

typedef struct SchemaRule {
  const char *type; // HASH, JSON, etc.
  const char **prefixes; // util_arr
  char *filter;
  struct IndexSpec *spec;
  struct RSExpr *expression;
  RSLanguage lang;
  double score;
  char *payload;
} SchemaRule;

extern SchemaRule **SchemaRules_g; // util_arr

SchemaRule *SchemaRule_Create(SchemaRule *rule0, SchemaRuleArgs *ags,
  struct IndexSpec *spec, QueryError *status);
void SchemaRule_Free(SchemaRule *);
void SchemaRules_RemoveSpecRules(struct IndexSpec *spec);

//---------------------------------------------------------------------------------------------

extern TrieMap *ScemaPrefixes_g;

void SchemaPrefixes_Create();
void SchemaPrefixes_Free();
void SchemaPrefixes_Add(const char *prefix, struct IndexSpec *index);
void SchemaPrefixes_RemoveSpec(struct IndexSpec *spec);

typedef struct {
  const char *prefix;
  struct IndexSpec **index_specs; // util_arr
} SchemaPrefixNode;

SchemaPrefixNode *SchemaPrefixNode_Create(const char *prefix, struct IndexSpec *index);
void SchemaPrefixNode_Free(SchemaPrefixNode*);

///////////////////////////////////////////////////////////////////////////////////////////////

#endif // RULES_RULES_H
