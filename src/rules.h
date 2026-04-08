/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stdbool.h>                     // for bool

#include "query_error.h"                 // for QueryError
#include "triemap.h"                     // for TrieMap
#include "stemmer.h"
#include "util/arr.h"
#include "json.h"
#include "redisearch.h"
#include "util/references.h"             // for StrongRef
#include "obfuscation/hidden_unicode.h"  // for HiddenUnicodeString
#include "document_rs.h"                 // for DocumentType
#include "language.h"                    // for RSLanguage
#include "obfuscation/hidden.h"          // for HiddenString
#include "redismodule.h"                 // for RedisModuleCtx, RedisModuleKey
#include "rejson_api.h"                  // for RedisJSON
#include "util/arr/arr.h"                // for arrayof

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
  arrayof(HiddenUnicodeString*) prefixes;
  HiddenString *filter_exp_str;
  struct RSExpr *filter_exp;
  arrayof(char*) filter_fields;
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
int SchemaRule_RdbLoad(StrongRef spec_ref, RedisModuleIO *rdb, int encver, QueryError *status);

bool SchemaRule_ShouldIndex(struct IndexSpec *sp, RedisModuleString *keyname, DocumentType type);

struct EvalCtx;

/**
 * Evaluate the filter expression for a schema rule.
 * @param r The evaluation context (must be initialized with RLookup_LoadRuleFields)
 * @param filter_exp The filter expression to evaluate
 * @return true if the document passes the filter (should be indexed), false otherwise
 */
bool SchemaRule_FilterPasses(struct EvalCtx *r, struct RSExpr *filter_exp);

//---------------------------------------------------------------------------------------------

extern TrieMap *SchemaPrefixes_g;

void SchemaPrefixes_Create();
void SchemaPrefixes_Free(TrieMap *t);
void SchemaPrefixes_Add(HiddenUnicodeString *prefix, StrongRef spec);
void SchemaPrefixes_RemoveSpec(StrongRef spec);

typedef struct {
  char *prefix;
  arrayof(StrongRef) index_specs;
} SchemaPrefixNode;

///////////////////////////////////////////////////////////////////////////////////////////////

#ifdef __cplusplus
}
#endif
