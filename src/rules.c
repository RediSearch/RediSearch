#include "rules.h"
#include "aggregate/expr/expression.h"
#include "spec.h"
#include "json.h"
#include "rdb.h"

TrieMap *ScemaPrefixes_g;

///////////////////////////////////////////////////////////////////////////////////////////////

const char *DocumentType_ToString(DocumentType type) {
  switch (type) {
    case DocumentType_Hash:
      return "HASH";
    case DocumentType_Json:
      return "JSON";
    case DocumentType_None:
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

SchemaRule *SchemaRule_Create(SchemaRuleArgs *args, IndexSpec *spec, QueryError *status) {
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

  rule->prefixes = array_new(const char *, 1);
  for (int i = 0; i < args->nprefixes; ++i) {
    const char *p = rm_strdup(args->prefixes[i]);
    rule->prefixes = array_append(rule->prefixes, p);
  }

  rule->spec = spec;

  if (rule->filter_exp_str) {
    rule->filter_exp = ExprAST_Parse(rule->filter_exp_str, strlen(rule->filter_exp_str), status);
    if (!rule->filter_exp) {
      QueryError_SetError(status, QUERY_EADDARGS, "Invalid expression");
      goto error;
    }
  }

  for (int i = 0; i < array_len(rule->prefixes); ++i) {
    SchemaPrefixes_Add(rule->prefixes[i], spec);
  }

  return rule;

error:
  SchemaRule_Free(rule);
  return NULL;
}

void SchemaRule_Free(SchemaRule *rule) {
  SchemaPrefixes_RemoveSpec(rule->spec);

  rm_free((void *)rule->lang_field);
  rm_free((void *)rule->score_field);
  rm_free((void *)rule->payload_field);
  rm_free((void *)rule->filter_exp_str);
  if (rule->filter_exp) {
    ExprAST_Free((RSExpr *)rule->filter_exp);
  }
  array_free_ex(rule->prefixes, rm_free(*(char **)ptr));
  rm_free((void *)rule);
}

//---------------------------------------------------------------------------------------------

static SchemaPrefixNode *SchemaPrefixNode_Create(const char *prefix, IndexSpec *index) {
  SchemaPrefixNode *node = rm_calloc(1, sizeof(*node));
  node->prefix = rm_strdup(prefix);
  node->index_specs = array_new(IndexSpec *, 1);
  node->index_specs = array_append(node->index_specs, index);
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
  const char *lang_s = (const char *)RedisModule_StringPtrLen(lang_rms, NULL);
  lang = RSLanguage_Find(lang_s, 0);
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
  rv = japi->getString(langJson, &langStr, &len) ;
  if (rv != REDISMODULE_OK) {
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

RSLanguage SchemaRule_JsonScore(RedisModuleCtx *ctx, const SchemaRule *rule,
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
  if (japi->getDouble(scoreJson, &score) != REDISMODULE_OK) {
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

int SchemaRule_RdbLoad(IndexSpec *sp, RedisModuleIO *rdb, int encver) {
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

  QueryError status = {0};
  SchemaRule *rule = SchemaRule_Create(&args, sp, &status);
  if (!rule) {
    RedisModule_LogIOError(rdb, "warning", "%s", QueryError_GetError(&status));
    RedisModule_Assert(rule);
  } else {
    rule->score_default = score_default;
    rule->lang_default = lang_default;
    sp->rule = rule;
  }

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
    RedisModule_SaveStringBuffer(rdb, rule->prefixes[i], strlen(rule->prefixes[i]) + 1);
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
}

///////////////////////////////////////////////////////////////////////////////////////////////

void SchemaPrefixes_Create() {
  ScemaPrefixes_g = NewTrieMap();
}

static void freePrefixNode(void *ctx) {
  SchemaPrefixNode_Free(ctx);
}

void SchemaPrefixes_Free(TrieMap *t) {
  TrieMap_Free(t, freePrefixNode);
}

void SchemaPrefixes_Add(const char *prefix, IndexSpec *spec) {
  size_t nprefix = strlen(prefix);
  void *p = TrieMap_Find(ScemaPrefixes_g, (char *)prefix, nprefix);
  if (p == TRIEMAP_NOTFOUND) {
    SchemaPrefixNode *node = SchemaPrefixNode_Create(prefix, spec);
    TrieMap_Add(ScemaPrefixes_g, (char *)prefix, nprefix, node, NULL);
  } else {
    SchemaPrefixNode *node = (SchemaPrefixNode *)p;
    node->index_specs = array_append(node->index_specs, spec);
  }
}

void SchemaPrefixes_RemoveSpec(IndexSpec *spec) {
  TrieMapIterator *it = TrieMap_Iterate(ScemaPrefixes_g, "", 0);
  while (true) {
    char *p;
    tm_len_t len;
    SchemaPrefixNode *node = NULL;
    if (!TrieMapIterator_Next(it, &p, &len, (void **)&node)) {
      break;
    }
    if (!node) {
      return;
    }
    for (int i = 0; i < array_len(node->index_specs); ++i) {
      if (node->index_specs[i] == spec) {
        array_del_fast(node->index_specs, i);
        break;
      }
    }
  }
  TrieMapIterator_Free(it);
}

///////////////////////////////////////////////////////////////////////////////////////////////
