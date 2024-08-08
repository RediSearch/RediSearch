/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "query_error.h"
#include "triemap/triemap.h"
#include "stemmer.h"
#include "util/arr.h"
#include "json.h"
#include "spec.h"
#include "redisearch.h"
#include "util/references.h"

#ifdef __cplusplus
extern "C" {
#endif

///////////////////////////////////////////////////////////////////////////////////////////////

#define RULE_TYPE_HASH "HASH"
#define RULE_TYPE_JSON "JSON"

struct RSExpr;
struct IndexSpec;

const char *DocumentType_ToString(DocumentType type);
int DocumentType_Parse(const char *type_str, DocumentType *type, QueryError *status);

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
  char *index_all;
} SchemaRuleArgs;

typedef struct SchemaRule {
  DocumentType type;
  arrayof(sds) prefixes;
  char *filter_exp_str;
  struct RSExpr *filter_exp;
  char **filter_fields;
  int *filter_fields_index;
  char *lang_field;
  char *score_field;
  char *payload_field;
  double score_default;
  RSLanguage lang_default;
  bool index_all;
} SchemaRule;

/*
 * Free SchemaRuleArgs structure, use this function
 * only if the entire SchemaRuleArgs is heap allocated.
 */
void SchemaRuleArgs_Free(SchemaRuleArgs *args);
void LegacySchemaRulesArgs_Free(RedisModuleCtx *ctx);

SchemaRule *SchemaRule_Create(SchemaRuleArgs *args, StrongRef spec_ref, QueryError *status);
void SchemaRule_FilterFields(struct IndexSpec *sp);
void SchemaRule_Free(SchemaRule *);

RSLanguage SchemaRule_HashLang(RedisModuleCtx *rctx, const SchemaRule *rule, RedisModuleKey *key,
                               const char *kname);
RSLanguage SchemaRule_JsonLang(RedisModuleCtx *ctx, const SchemaRule *rule,
                               RedisJSON jsonKey, const char *keyName);
double SchemaRule_HashScore(RedisModuleCtx *rctx, const SchemaRule *rule, RedisModuleKey *key,
                            const char *kname);
double SchemaRule_JsonScore(RedisModuleCtx *ctx, const SchemaRule *rule,
                                RedisJSON jsonKey, const char *keyName);
RedisModuleString *SchemaRule_HashPayload(RedisModuleCtx *rctx, const SchemaRule *rule,
                                          RedisModuleKey *key, const char *kname);

void SchemaRule_RdbSave(SchemaRule *rule, RedisModuleIO *rdb);
int SchemaRule_RdbLoad(StrongRef spec_ref, RedisModuleIO *rdb, int encver);

bool SchemaRule_ShouldIndex(struct IndexSpec *sp, RedisModuleString *keyname, DocumentType type);

//---------------------------------------------------------------------------------------------

extern TrieMap *SchemaPrefixes_g;

void SchemaPrefixes_Create();
void SchemaPrefixes_Free(TrieMap *t);
void SchemaPrefixes_Add(const char *prefix, size_t len, StrongRef spec);
void SchemaPrefixes_RemoveSpec(StrongRef spec);

typedef struct {
  char *prefix;
  arrayof(StrongRef) index_specs;
} SchemaPrefixNode;

///////////////////////////////////////////////////////////////////////////////////////////////

#ifdef __cplusplus
}
#endif
