/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "stemmer.h"
#include "util/arr.h"
#include "json.h"
#include "redisearch.h"
#include "util/references.h"
#include "obfuscation/hidden_unicode.h"
#include "rmutil/args.h"

typedef struct TrieMap TrieMap;

typedef struct QueryError QueryError;

#ifdef __cplusplus
extern "C" {
#endif

///////////////////////////////////////////////////////////////////////////////////////////////

#define RULE_TYPE_HASH "HASH"
#define RULE_TYPE_JSON "JSON"

#define MAX_SCHEMA_PREFIXES 1000000

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

/* Same as SchemaRule_Create, but the prefixes are read from an ArgsCursor
 * instead of `args->prefixes`/`args->nprefixes`. The cursor must contain at
 * least one prefix. The cursor is consumed (advanced) by this function. */
SchemaRule *SchemaRule_CreateWithPrefixesAC(SchemaRuleArgs *args, ArgsCursor *prefixes_ac,
                                            StrongRef spec_ref, QueryError *status);
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

// Decide whether `keyname` (of `type`) belongs to `sp`: matches its type, a prefix, and the
// optional FILTER expression.
//
// `ctx` is used to load the document's fields when evaluating the schema FILTER expression. It
// must be selected to the DB the index lives on (sp->dbid) so the filter reads the right document;
// passing a DB-0 context (e.g. RSDummyContext) would evaluate filters against the wrong keyspace
// for indexes on non-zero DBs.
//
// `openKey`, when non-NULL, is an already-open pinned handle for `keyname` (e.g. the value pinned
// for an AsyncScan callback) that the FILTER evaluation reuses to load fields instead of reopening
// by name; pass NULL when no such handle is available.
bool SchemaRule_ShouldIndex(RedisModuleCtx *ctx, struct IndexSpec *sp, RedisModuleString *keyname,
                            DocumentType type, RedisModuleKey *openKey);

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
