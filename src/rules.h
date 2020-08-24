
#pragma once

#include "query_error.h"
#include "dep/triemap/triemap.h"
#include "stemmer.h"
#include "util/arr.h"
#ifdef __cplusplus
extern "C" {
#endif
///////////////////////////////////////////////////////////////////////////////////////////////

#define RULE_TYPE_HASH "HASH"

struct RSExpr;
struct IndexSpec;

typedef enum { SchameRuleType_Any, SchemaRuleType_Hash } SchemaRuleType;

const char *SchemaRuleType_ToString(SchemaRuleType type);
int SchemaRuleType_Parse(const char *type_str, SchemaRuleType *type, QueryError *status);

//---------------------------------------------------------------------------------------------

typedef struct {
  const char *type;  // HASH, JSON, etc.
  const char **prefixes;
  int nprefixes;
  char *filter_exp_str;
  char *lang_field;
  char *score_field;
  char *payload_field;
  char *lang_default;
  char *score_default;
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
  double score_default;
  RSLanguage lang_default;
} SchemaRule;

extern arrayof(SchemaRule *) SchemaRules_g;

/*
 * Free SchemaRuleArgs structure, use this function
 * only if the entire SchemaRuleArgs is heap allocated.
 */
void SchemaRuleArgs_Free(SchemaRuleArgs *args);

SchemaRule *SchemaRule_Create(SchemaRuleArgs *args, struct IndexSpec *spec, QueryError *status);
void SchemaRule_Free(SchemaRule *);

void SchemaRules_Create();
void SchemaRules_RemoveSpecRules(struct IndexSpec *spec);

RSLanguage SchemaRule_HashLang(RedisModuleCtx *rctx, const SchemaRule *rule, RedisModuleKey *key,
                               const char *kname);
double SchemaRule_HashScore(RedisModuleCtx *rctx, const SchemaRule *rule, RedisModuleKey *key,
                            const char *kname);
RedisModuleString *SchemaRule_HashPayload(RedisModuleCtx *rctx, const SchemaRule *rule,
                                          RedisModuleKey *key, const char *kname);

void SchemaRule_RdbSave(SchemaRule *rule, RedisModuleIO *rdb);
int SchemaRule_RdbLoad(struct IndexSpec *sp, RedisModuleIO *rdb, int encver);

//---------------------------------------------------------------------------------------------

extern TrieMap *ScemaPrefixes_g;

void SchemaPrefixes_Create();
void SchemaPrefixes_Free();
void SchemaPrefixes_Add(const char *prefix, struct IndexSpec *index);
void SchemaPrefixes_RemoveSpec(struct IndexSpec *spec);

typedef struct {
  char *prefix;
  arrayof(struct IndexSpec *) index_specs;
} SchemaPrefixNode;

#ifdef __cplusplus
}
#endif
///////////////////////////////////////////////////////////////////////////////////////////////
