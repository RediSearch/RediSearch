/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rules.h"
#include "aggregate/expr/expression.h"
#include "aggregate/expr/exprast.h"
#include "json.h"
#include "rdb.h"

TrieMap *SchemaPrefixes_g;

///////////////////////////////////////////////////////////////////////////////////////////////

const char *DocumentType_ToString(DocumentType type) {
  switch (type) {
    case DocumentType_Hash:
      return "HASH";
    case DocumentType_Json:
      return "JSON";
    case DocumentType_Unsupported:
    default:
      RS_LOG_ASSERT(true, "SchameRuleType_Any is not supported");
      return "";
  }
}

int DocumentType_Parse(const char *type_str, DocumentType *type, QueryError *status) {
  if (!type_str || !strcasecmp(type_str, RULE_TYPE_HASH)) {
    *type = DocumentType_Hash;
    return REDISMODULE_OK;
  } else if (japi && !strcasecmp(type_str, RULE_TYPE_JSON)) {
    *type = DocumentType_Json;
    return REDISMODULE_OK;
  }
  QueryError_SetErrorFmt(status, QUERY_EADDARGS, "Invalid rule type: %s", type_str);
  return REDISMODULE_ERR;
}

///////////////////////////////////////////////////////////////////////////////////////////////

void SchemaRuleArgs_Free(SchemaRuleArgs *rule_args) {
  // free rule_args
  if (!rule_args) return;
#define FREE_IF_NEEDED(arg) \
  if (arg) rm_free(arg)
  FREE_IF_NEEDED(rule_args->filter_exp_str);
  FREE_IF_NEEDED(rule_args->lang_default);
  FREE_IF_NEEDED(rule_args->lang_field);
  FREE_IF_NEEDED(rule_args->payload_field);
  FREE_IF_NEEDED(rule_args->score_default);
  FREE_IF_NEEDED(rule_args->score_field);
  FREE_IF_NEEDED((char *)rule_args->type);
  for (size_t i = 0; i < rule_args->nprefixes; ++i) {
    rm_free((char *)rule_args->prefixes[i]);
  }
  rm_free(rule_args->prefixes);
  rm_free(rule_args);
}

void LegacySchemaRulesArgs_Free(RedisModuleCtx *ctx) {
  if (!legacySpecRules) return;
  dictIterator *iter = dictGetIterator(legacySpecRules);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    char *indexName = dictGetKey(entry);
    SchemaRuleArgs *rule_args = dictGetVal(entry);
    RedisModule_Log(ctx, "warning", "Index %s was defined for upgrade but was not found", indexName);
    SchemaRuleArgs_Free(rule_args);
  }
  dictReleaseIterator(iter);
  dictEmpty(legacySpecRules, NULL);
  dictRelease(legacySpecRules);
  legacySpecRules = NULL;
}

SchemaRule *SchemaRule_Create(SchemaRuleArgs *args, StrongRef ref, QueryError *status) {
  SchemaRule *rule = rm_calloc(1, sizeof(*rule));

  if (DocumentType_Parse(args->type, &rule->type, status) == REDISMODULE_ERR) {
    goto error;
  }

  rule->filter_exp_str = args->filter_exp_str ? rm_strdup(args->filter_exp_str) : NULL;
  rule->lang_field = args->lang_field ? rm_strdup(args->lang_field) : NULL;
  rule->score_field = args->score_field ? rm_strdup(args->score_field) : NULL;
  rule->payload_field = args->payload_field ? rm_strdup(args->payload_field) : NULL;

  if (args->score_default) {
    double score;
    char *endptr = {0};
    score = strtod(args->score_default, &endptr);
    if (args->score_default == endptr || score < 0 || score > 1) {
      QueryError_SetError(status, QUERY_EADDARGS, "Invalid score");
      goto error;
    }
    rule->score_default = score;
  } else {
    rule->score_default = DEFAULT_SCORE;
  }

  if (args->lang_default) {
    RSLanguage lang = RSLanguage_Find(args->lang_default, 0);
    if (lang == RS_LANG_UNSUPPORTED) {
      QueryError_SetError(status, QUERY_EADDARGS, "Invalid language");
      goto error;
    }
    rule->lang_default = lang;
  } else {
    rule->lang_default = DEFAULT_LANGUAGE;
  }

  rule->prefixes = array_new(sds, args->nprefixes);
  for (int i = 0; i < args->nprefixes; ++i) {
    sds p = sdsnew(args->prefixes[i]);
    array_append(rule->prefixes, p);
  }

  if (rule->filter_exp_str) {
    rule->filter_exp = ExprAST_Parse(rule->filter_exp_str, strlen(rule->filter_exp_str), status);
    if (!rule->filter_exp) {
      QueryError_SetError(status, QUERY_EADDARGS, "Invalid expression");
      goto error;
    }
  }

  if (args->index_all) {
    // Validate the arg (if it's not ENABLE or DISABLE -> throw an error)
    if (!strcasecmp(args->index_all, "enable")) {
      rule->index_all = true;
    } else if (!strcasecmp(args->index_all, "disable")) {
      rule->index_all = false;
    } else {
      QueryError_SetError(status, QUERY_EADDARGS, "Invalid argument for `INDEX_ALL`");
      goto error;
    }
  }

  for (int i = 0; i < array_len(rule->prefixes); ++i) {
    SchemaPrefixes_Add(rule->prefixes[i], sdslen(rule->prefixes[i]), ref);
  }

  return rule;

error:
  SchemaRule_Free(rule);
  return NULL;
}

/*.
 * RSExpr_GetProperties receives from the rule filter a list off all fields within it.
 *
 * The fields within the list are compared to the list of fieldSpecs and find
 * the index for each field.
 *
 * At documentation, the field index is used to load required fields instead of
 * expensive comparisons.
 */
void SchemaRule_FilterFields(IndexSpec *spec) {
  char **properties = array_new(char *, 8);
  SchemaRule *rule = spec->rule;
  RSExpr_GetProperties(rule->filter_exp, &properties);
  int propLen = array_len(properties);
  if (array_len(properties) > 0) {
    rule->filter_fields = properties;
    rule->filter_fields_index = rm_calloc(propLen, sizeof(int));
    for (int i = 0; i < propLen; ++i) {
      for (int j = 0; j < spec->numFields; ++j) {
        // a match. save the field index for fast access
        FieldSpec *fs = spec->fields + j;
        if (!strcmp(properties[i], fs->name) || !strcmp(properties[i], fs->path)) {
          rule->filter_fields_index[i] = j;
          break;
        }
        // no match was found we will load the field by the name provided.
        rule->filter_fields_index[i] = -1;
      }
    }
  } else {
    array_free(properties);
  }
}

void SchemaRule_Free(SchemaRule *rule) {
  rm_free((void *)rule->lang_field);
  rm_free((void *)rule->score_field);
  rm_free((void *)rule->payload_field);
  rm_free((void *)rule->filter_exp_str);
  if (rule->filter_exp) {
    ExprAST_Free((RSExpr *)rule->filter_exp);
  }
  array_free_ex(rule->prefixes, sdsfree(*(sds *)ptr));
  array_free_ex(rule->filter_fields, rm_free(*(char **)ptr));
  rm_free(rule->filter_fields_index);
  rm_free((void *)rule);
}

//---------------------------------------------------------------------------------------------

static SchemaPrefixNode *SchemaPrefixNode_Create(const char *prefix, StrongRef ref) {
  SchemaPrefixNode *node = rm_calloc(1, sizeof(*node));
  node->prefix = rm_strdup(prefix);
  node->index_specs = array_new(StrongRef, 1);
  array_append(node->index_specs, ref);
  return node;
}

static void SchemaPrefixNode_Free(SchemaPrefixNode *node) {
  array_free(node->index_specs);
  rm_free(node->prefix);
  rm_free(node);
}

//---------------------------------------------------------------------------------------------

RSLanguage SchemaRule_HashLang(RedisModuleCtx *rctx, const SchemaRule *rule, RedisModuleKey *key,
                               const char *kname) {
  RSLanguage lang = rule->lang_default;
  RedisModuleString *lang_rms = NULL;
  if (!rule->lang_field) {
    goto done;
  }
  int rv = RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, rule->lang_field, &lang_rms, NULL);
  if (rv != REDISMODULE_OK) {
    goto done;
  }
  if (lang_rms == NULL) {
    goto done;
  }
  size_t len;
  const char *lang_s = RedisModule_StringPtrLen(lang_rms, &len);
  lang = RSLanguage_Find(lang_s, len);
  if (lang == RS_LANG_UNSUPPORTED) {
    RedisModule_Log(NULL, "warning", "invalid language for key %s", kname);
    lang = rule->lang_default;
  }
done:
  if (lang_rms) {
    RedisModule_FreeString(rctx, lang_rms);
  }
  return lang;
}

RSLanguage SchemaRule_JsonLang(RedisModuleCtx *ctx, const SchemaRule *rule,
                               RedisJSON jsonRoot, const char *kname) {
  int rv = REDISMODULE_ERR;
  JSONResultsIterator jsonIter = NULL;
  RSLanguage lang = rule->lang_default;
  if (!rule->lang_field) {
    goto done;
  }

  assert(japi);
  if (!japi) {
    goto done;
  }

  jsonIter = japi->get(jsonRoot, rule->lang_field);
  if (!jsonIter) {
    goto done;
  }

  const char *langStr;
  size_t len;
  RedisJSON langJson = japi->next(jsonIter);
  if (!langJson || japi->getString(langJson, &langStr, &len) != REDISMODULE_OK) {
    RedisModule_Log(NULL, "warning", "invalid field %s for key %s: not a string", rule->lang_field, kname);
    goto done;
  }

  lang = RSLanguage_Find(langStr, len);
  if (lang == RS_LANG_UNSUPPORTED) {
    RedisModule_Log(NULL, "warning", "invalid language for key %s", kname);
    lang = rule->lang_default;
    goto done;
  }

done:
  if (jsonIter) {
    japi->freeIter(jsonIter);
  }
  return lang;
}

double SchemaRule_HashScore(RedisModuleCtx *rctx, const SchemaRule *rule, RedisModuleKey *key,
                            const char *kname) {
  double score = rule->score_default;
  RedisModuleString *score_rms = NULL;
  if (!rule->score_field) {
    goto done;
  }
  int rv = RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, rule->score_field, &score_rms, NULL);
  if (rv != REDISMODULE_OK) {
    goto done;
  }
  // score of 1.0 is not saved in hash
  if (score_rms == NULL) {
    goto done;
  }

  rv = RedisModule_StringToDouble(score_rms, &score);
  if (rv != REDISMODULE_OK) {
    RedisModule_Log(NULL, "warning", "invalid score for key %s", kname);
    score = rule->score_default;
  }
done:
  if (score_rms) {
    RedisModule_FreeString(rctx, score_rms);
  }
  return score;
}

double SchemaRule_JsonScore(RedisModuleCtx *ctx, const SchemaRule *rule,
                                RedisJSON jsonRoot, const char *kname) {
  double score = rule->score_default;
  JSONResultsIterator jsonIter = NULL;
  if (!rule->score_field) {
    goto done;
  }

  assert(japi);
  if (!japi) {
    goto done;
  }

  jsonIter = japi->get(jsonRoot, rule->score_field);
  if (jsonIter == NULL) {
    goto done;
  }

  RedisJSON scoreJson = japi->next(jsonIter);
  if (!scoreJson || japi->getDouble(scoreJson, &score) != REDISMODULE_OK) {
    RedisModule_Log(NULL, "warning", "invalid field %s for key %s", rule->score_field, kname);
  }

done:
  if (jsonIter) {
    japi->freeIter(jsonIter);
  }
  return score;
}

RedisModuleString *SchemaRule_HashPayload(RedisModuleCtx *rctx, const SchemaRule *rule,
                                          RedisModuleKey *key, const char *kname) {
  RedisModuleString *payload_rms = NULL;
  if (!rule->payload_field) {
    return NULL;
  }
  const char *payload_field = rule->payload_field ? rule->payload_field : UNDERSCORE_PAYLOAD;
  int rv = RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, payload_field, &payload_rms, NULL);
  if (rv != REDISMODULE_OK) {
    if (payload_rms != NULL) RedisModule_FreeString(rctx, payload_rms);
    return NULL;
  }
  return payload_rms;
}

//---------------------------------------------------------------------------------------------

int SchemaRule_RdbLoad(StrongRef ref, RedisModuleIO *rdb, int encver) {
  SchemaRuleArgs args = {0};
  size_t len;
#define RULEARGS_INITIAL_NUM_PREFIXES_ON_STACK 32
  char *prefixes[RULEARGS_INITIAL_NUM_PREFIXES_ON_STACK];

  int ret = REDISMODULE_OK;
  args.type = LoadStringBuffer_IOError(rdb, &len, goto cleanup);

  args.nprefixes = LoadUnsigned_IOError(rdb, goto cleanup);
  if (args.nprefixes <= RULEARGS_INITIAL_NUM_PREFIXES_ON_STACK) {
    args.prefixes = (const char **)prefixes;
    memset(args.prefixes, 0, args.nprefixes * sizeof(*args.prefixes));
  } else {
    args.prefixes = rm_calloc(args.nprefixes, sizeof(*args.prefixes));
  }

  for (size_t i = 0; i < args.nprefixes; ++i) {
    args.prefixes[i] = LoadStringBuffer_IOError(rdb, &len, goto cleanup);
  }

  uint64_t exist = LoadUnsigned_IOError(rdb, goto cleanup);
  if (exist) {
    args.filter_exp_str = LoadStringBuffer_IOError(rdb, &len, goto cleanup);
  }
  exist = LoadUnsigned_IOError(rdb, goto cleanup);
  if (exist) {
    args.lang_field = LoadStringBuffer_IOError(rdb, &len, goto cleanup);
  }
  exist = LoadUnsigned_IOError(rdb, goto cleanup);
  if (exist) {
    args.score_field = LoadStringBuffer_IOError(rdb, &len, goto cleanup);
  }
  exist = LoadUnsigned_IOError(rdb, goto cleanup);
  if (exist) {
    args.payload_field = LoadStringBuffer_IOError(rdb, &len, goto cleanup);
  }
  double score_default = LoadDouble_IOError(rdb, goto cleanup);
  RSLanguage lang_default = LoadUnsigned_IOError(rdb, goto cleanup);
  bool index_all = false;
  if (encver >= INDEX_INDEXALL_VERSION) {
    index_all = LoadUnsigned_IOError(rdb, goto cleanup);
  }

  QueryError status = {0};
  SchemaRule *rule = SchemaRule_Create(&args, ref, &status);
  if (!rule) {
    RedisModule_LogIOError(rdb, "warning", "%s", QueryError_GetError(&status));
    RedisModule_Assert(rule);
  }
  rule->score_default = score_default;
  rule->lang_default = lang_default;
  rule->index_all = index_all;

  // No need to validate the reference here, since we are loading it from the RDB
  IndexSpec *sp = StrongRef_Get(ref);
  sp->rule = rule;
  SchemaRule_FilterFields(sp);

cleanup:
  if (args.type) {
    RedisModule_Free((char *)args.type);
  }
  for (size_t i = 0; i < args.nprefixes; ++i) {
    if (args.prefixes[i]) {
      RedisModule_Free((char *)args.prefixes[i]);
    }
  }
  if (args.nprefixes > RULEARGS_INITIAL_NUM_PREFIXES_ON_STACK)
    rm_free(args.prefixes);
  if (args.filter_exp_str) {
    RedisModule_Free(args.filter_exp_str);
  }
  if (args.lang_field) {
    RedisModule_Free(args.lang_field);
  }
  if (args.score_field) {
    RedisModule_Free(args.score_field);
  }
  if (args.payload_field) {
    RedisModule_Free(args.payload_field);
  }
  if (!RedisModule_IsIOError(rdb))
    return ret;
  return REDISMODULE_ERR;
}

void SchemaRule_RdbSave(SchemaRule *rule, RedisModuleIO *rdb) {
  // the +1 is so we will save the \0
  const char *ruleTypeStr = DocumentType_ToString(rule->type);
  RedisModule_SaveStringBuffer(rdb, ruleTypeStr, strlen(ruleTypeStr) + 1);
  RedisModule_SaveUnsigned(rdb, array_len(rule->prefixes));
  for (size_t i = 0; i < array_len(rule->prefixes); ++i) {
    RedisModule_SaveStringBuffer(rdb, rule->prefixes[i], sdslen(rule->prefixes[i]) + 1);
  }
  if (rule->filter_exp_str) {
    RedisModule_SaveUnsigned(rdb, 1);
    RedisModule_SaveStringBuffer(rdb, rule->filter_exp_str, strlen(rule->filter_exp_str) + 1);
  } else {
    RedisModule_SaveUnsigned(rdb, 0);
  }
  if (rule->lang_field) {
    RedisModule_SaveUnsigned(rdb, 1);
    RedisModule_SaveStringBuffer(rdb, rule->lang_field, strlen(rule->lang_field) + 1);
  } else {
    RedisModule_SaveUnsigned(rdb, 0);
  }
  if (rule->score_field) {
    RedisModule_SaveUnsigned(rdb, 1);
    RedisModule_SaveStringBuffer(rdb, rule->score_field, strlen(rule->score_field) + 1);
  } else {
    RedisModule_SaveUnsigned(rdb, 0);
  }
  if (rule->payload_field) {
    RedisModule_SaveUnsigned(rdb, 1);
    RedisModule_SaveStringBuffer(rdb, rule->payload_field, strlen(rule->payload_field) + 1);
  } else {
    RedisModule_SaveUnsigned(rdb, 0);
  }
  RedisModule_SaveDouble(rdb, rule->score_default);
  RedisModule_SaveUnsigned(rdb, rule->lang_default);
  RedisModule_SaveUnsigned(rdb, rule->index_all);
}

bool SchemaRule_ShouldIndex(struct IndexSpec *sp, RedisModuleString *keyname, DocumentType type) {
  // check type
  if (type != sp->rule->type) {
    return false;
  }

  const char *keyCstr = RedisModule_StringPtrLen(keyname, NULL);

  // check prefixes (always found for an index with no prefixes)
  bool match = false;
  sds *prefixes = sp->rule->prefixes;
  for (int i = 0; i < array_len(prefixes); ++i) {
    // Using `strncmp` to compare the prefix, since the key might be longer than the prefix
    if (!strncmp(keyCstr, prefixes[i], sdslen(prefixes[i]))) {
      match = true;
      break;
    }
  }
  if (!match) {
    return false;
  }

  // check filters
  int ret = true;
  SchemaRule *rule = sp->rule;
  if (rule->filter_exp) {
    EvalCtx *r = NULL;
    // load hash only if required
    r = EvalCtx_Create();

    RLookup_LoadRuleFields(RSDummyContext, &r->lk, &r->row, sp, keyCstr);

    if (EvalCtx_EvalExpr(r, rule->filter_exp) != EXPR_EVAL_OK ||
        !RSValue_BoolTest(&r->res)) {
      ret = false;
    }
    QueryError_ClearError(r->ee.err);
    EvalCtx_Destroy(r);
  }

  return ret;
}


///////////////////////////////////////////////////////////////////////////////////////////////

void SchemaPrefixes_Create() {
  SchemaPrefixes_g = NewTrieMap();
}

static void freePrefixNode(void *ctx) {
  SchemaPrefixNode_Free(ctx);
}

void SchemaPrefixes_Free(TrieMap *t) {
  TrieMap_Free(t, freePrefixNode);
}

void SchemaPrefixes_Add(const char *prefix, size_t len, StrongRef ref) {
  void *p = TrieMap_Find(SchemaPrefixes_g, prefix, len);
  if (p == TRIEMAP_NOTFOUND) {
    SchemaPrefixNode *node = SchemaPrefixNode_Create(prefix, ref);
    TrieMap_Add(SchemaPrefixes_g, prefix, len, node, NULL);
  } else {
    SchemaPrefixNode *node = (SchemaPrefixNode *)p;
    array_append(node->index_specs, ref);
  }
}

void SchemaPrefixes_RemoveSpec(StrongRef ref) {
  IndexSpec *spec = StrongRef_Get(ref);
  if (!spec || !spec->rule || !spec->rule->prefixes) return;

  sds *prefixes = spec->rule->prefixes;
  for (int i = 0; i < array_len(prefixes); ++i) {
    // retrieve list of specs matching the prefix
    SchemaPrefixNode *node = TrieMap_Find(SchemaPrefixes_g, prefixes[i], sdslen(prefixes[i]));
    if (node == TRIEMAP_NOTFOUND) {
      continue;
    }
    // iterate over specs list and remove
    for (int j = 0; j < array_len(node->index_specs); ++j) {
      if (StrongRef_Equals(node->index_specs[j], ref)) {
        array_del_fast(node->index_specs, j);
        if (array_len(node->index_specs) == 0) {
          // if all specs were deleted, remove the node
          TrieMap_Delete(SchemaPrefixes_g, prefixes[i], sdslen(prefixes[i]), (freeCB)SchemaPrefixNode_Free);
        }
        break;
      }
    }
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
