#include "rules.h"
#include "aggregate/expr/expression.h"
#include "spec.h"

arrayof(SchemaRule *) SchemaRules_g;
TrieMap *ScemaPrefixes_g;

///////////////////////////////////////////////////////////////////////////////////////////////

const char *SchemaRuleType_ToString(SchemaRuleType type) {
  switch (type) {
    case SchemaRuleType_Hash:
      return "HASH";
    case SchameRuleType_Any:
    default:
      RS_LOG_ASSERT(true, "SchameRuleType_Any is not supported");
      return "";
  }
}

int SchemaRuleType_Parse(const char *type_str, SchemaRuleType *type, QueryError *status) {
  if (!type_str || !strcasecmp(type_str, "HASH")) {
    *type = SchemaRuleType_Hash;
    return REDISMODULE_OK;
  }
  QueryError_SetError(status, QUERY_EADDARGS, "Invalid rule type");
  return REDISMODULE_ERR;
}

///////////////////////////////////////////////////////////////////////////////////////////////

SchemaRule *SchemaRule_Create(SchemaRuleArgs *args, IndexSpec *spec, QueryError *status) {
  SchemaRule *rule = rm_calloc(1, sizeof(*rule));

  if (SchemaRuleType_Parse(args->type, &rule->type, status) == REDISMODULE_ERR) {
    goto error;
  }

  rule->filter_exp_str = args->filter_exp_str ? rm_strdup(args->filter_exp_str) : NULL;
  rule->lang_field = rm_strdup(args->lang_field ? args->lang_field : "__language");
  rule->score_field = rm_strdup(args->score_field ? args->score_field : "__score");
  rule->payload_field = rm_strdup(args->payload_field ? args->payload_field : "__payload");

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

  SchemaRules_g = array_append(SchemaRules_g, rule);
  return rule;

error:
  SchemaRule_Free(rule);
  return NULL;
}

void SchemaRule_Free(SchemaRule *rule) {
  SchemaPrefixes_RemoveSpec(rule->spec);
  SchemaRules_RemoveSpecRules(rule->spec);

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
  RSLanguage lang = DEFAULT_LANGUAGE;
  RedisModuleString *lang_rms = NULL;
  if (!rule->lang_field) {
    goto done;
  }
  int rv = RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, rule->lang_field, &lang_rms, NULL);
  if (rv != REDISMODULE_OK) {
    RedisModule_Log(NULL, "warning", "invalid field %s for key %s", rule->lang_field, kname);
    goto done;
  }
  if (lang_rms == NULL) {
    goto done;
  }
  const char *lang_s = (const char *)RedisModule_StringPtrLen(lang_rms, NULL);
  lang = RSLanguage_Find(lang_s);
  if (lang == RS_LANG_UNSUPPORTED) {
    RedisModule_Log(NULL, "warning", "invalid language for key %s", kname);
    lang = DEFAULT_LANGUAGE;
  }
done:
  if (lang_rms) {
    RedisModule_FreeString(rctx, lang_rms);
  }
  return lang;
}

double SchemaRule_HashScore(RedisModuleCtx *rctx, const SchemaRule *rule, RedisModuleKey *key,
                            const char *kname) {
  double score = 1.0;
  RedisModuleString *score_rms = NULL;
  if (!rule->score_field) {
    goto done;
  }
  int rv = RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, rule->score_field, &score_rms, NULL);
  if (rv != REDISMODULE_OK) {
    RedisModule_Log(NULL, "warning", "invalid field %s for key %s", rule->score_field, kname);
    goto done;
  }
  // score of 1.0 is not saved in hash
  if (score_rms == NULL) {
    goto done;
  }

  rv = RedisModule_StringToDouble(score_rms, &score);
  if (rv != REDISMODULE_OK) {
    RedisModule_Log(NULL, "warning", "invalid score for key %s", kname);
    score = 1.0;
  }
done:
  if (score_rms) {
    RedisModule_FreeString(rctx, score_rms);
  }
  return score;
}

RedisModuleString *SchemaRule_HashPayload(RedisModuleCtx *rctx, const SchemaRule *rule,
                                          RedisModuleKey *key, const char *kname) {
  RedisModuleString *payload_rms = NULL;
  const char *payload_field = rule->payload_field ? rule->payload_field : "__payload";
  int rv = RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, payload_field, &payload_rms, NULL);
  if (rv != REDISMODULE_OK) {
    RedisModule_Log(NULL, "warning", "invalid field %s for key %s", rule->payload_field, kname);
    if (payload_rms != NULL) RedisModule_FreeString(rctx, payload_rms);
    return NULL;
  }
  return payload_rms;
}

//---------------------------------------------------------------------------------------------

void SchemaRules_Create() {
  SchemaRules_g = array_new(SchemaRule *, 1);
}

void SchemaRules_RemoveSpecRules(IndexSpec *spec) {
  for (size_t i = 0; i < array_len(SchemaRules_g); ++i) {
    SchemaRule *rule = SchemaRules_g[i];
    if (spec == rule->spec) {
      array_del_fast(SchemaRules_g, i);
      return;
    }
  }
}

int SchemaRule_RdbLoad(IndexSpec *sp, RedisModuleIO *rdb, int encver) {
  SchemaRuleArgs args = {0};
  size_t len;
  int ret = REDISMODULE_OK;
  args.type = RedisModule_LoadStringBuffer(rdb, &len);
  args.nprefixes = RedisModule_LoadUnsigned(rdb);
  char *prefixes[args.nprefixes];
  for (size_t i = 0; i < args.nprefixes; ++i) {
    prefixes[i] = RedisModule_LoadStringBuffer(rdb, &len);
  }
  args.prefixes = (const char **)prefixes;
  if (RedisModule_LoadUnsigned(rdb)) {
    args.filter_exp_str = RedisModule_LoadStringBuffer(rdb, &len);
  }
  if (RedisModule_LoadUnsigned(rdb)) {
    args.lang_field = RedisModule_LoadStringBuffer(rdb, &len);
  }
  if (RedisModule_LoadUnsigned(rdb)) {
    args.score_field = RedisModule_LoadStringBuffer(rdb, &len);
  }
  if (RedisModule_LoadUnsigned(rdb)) {
    args.payload_field = RedisModule_LoadStringBuffer(rdb, &len);
  }

  QueryError status = {0};
  SchemaRule *rule = SchemaRule_Create(&args, sp, &status);
  if (!rule) {
    RedisModule_LogIOError(rdb, "warning", "%s", QueryError_GetError(&status));
    QueryError_ClearError(&status);
    ret = REDISMODULE_ERR;
  } else {
    sp->rule = rule;
  }

  RedisModule_Free((char *)args.type);
  for (size_t i = 0; i < args.nprefixes; ++i) {
    RedisModule_Free((char *)args.prefixes[i]);
  }
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

  return ret;
}

void SchemaRule_RdbSave(SchemaRule *rule, RedisModuleIO *rdb) {
  // the +1 is so we will save the \0
  const char *ruleTypeStr = SchemaRuleType_ToString(rule->type);
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
}

///////////////////////////////////////////////////////////////////////////////////////////////

void SchemaPrefixes_Create() {
  ScemaPrefixes_g = NewTrieMap();
}

static void freePrefixNode(void *ctx) {
  SchemaPrefixNode_Free(ctx);
}

void SchemaPrefixes_Free() {
  TrieMap_Free(ScemaPrefixes_g, freePrefixNode);
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
