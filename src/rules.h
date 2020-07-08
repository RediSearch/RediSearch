
#pragma once

#include "query_error.h"
#include "dep/triemap/triemap.h"
#include "stemmer.h"
#include "util/arr.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct RSExpr;
struct IndexSpec;

typedef enum {
  SchameRuleType_Any,
  SchemaRuleType_Hash
} SchemaRuleType;

//---------------------------------------------------------------------------------------------

typedef struct {
  const char *type; // HASH, JSON, etc.
  const char **prefixes;
  int nprefixes;
  char *filter_exp_str;
  char *lang_field;
  char *score_field;
  char *payload_field;
} SchemaRuleArgs;

typedef struct SchemaRule {
  SchemaRuleType type;
  struct IndexSpec *spec;
  arrayof(const char *) prefixes;
  char *filter_exp_str;
  struct RSExpr *filter_exp;
  char *lang_field;
  char *score_field;
  char *payload_field;
} SchemaRule;

extern arrayof(SchemaRule*) SchemaRules_g;

SchemaRule *SchemaRule_Create(SchemaRuleArgs *ags, struct IndexSpec *spec, QueryError *status);
void SchemaRule_Free(SchemaRule *);
void SchemaRules_RemoveSpecRules(struct IndexSpec *spec);

RSLanguage SchemaRule_HashLang(const SchemaRule *rule, RedisModuleKey *key, const char *kname);
double SchemaRule_HashScore(const SchemaRule *rule, RedisModuleKey *key, const char *kname);
RedisModuleString *SchemaRule_HashPayload(const SchemaRule *rule, RedisModuleKey *key, const char *kname);

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
