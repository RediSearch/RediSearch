#include "rules.h"
#include "aggregate/expr/expression.h"
#include "spec.h"

arrayof(SchemaRule*) SchemaRules_g;
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
  if (!type_str) {
    QueryError_SetError(status, QUERY_EADDARGS, "No rule type given");
    return REDISMODULE_ERR;
  }
  if (!strcasecmp(type_str, "HASH")) {
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
  rule->lang_field = args->lang_field ? rm_strdup(args->lang_field) : NULL;
  rule->score_field = args->score_field ? rm_strdup(args->score_field) : NULL;
  rule->payload_field = args->payload_field ? rm_strdup(args->payload_field) : NULL;

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

  rm_free((void*) rule->filter_exp_str);
  if (rule->filter_exp) {
    ExprAST_Free((RSExpr *) rule->filter_exp);
  }
  array_free_ex(rule->prefixes, rm_free(*(char **)ptr));
  rm_free((void*) rule);
}

//---------------------------------------------------------------------------------------------

RSLanguage SchemaRule_HashLang(const SchemaRule *rule, RedisModuleKey *key, const char *kname) {
  if (!rule->lang_field) {
    return DEFAULT_LANGUAGE;
  }
  RedisModuleString *lang_rms = NULL;
  int rv = RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, rule->lang_field, &lang_rms, NULL);
  if (rv != REDISMODULE_OK) {
    RedisModule_Log(NULL, "warning", "invalid field %s for key %s", rule->lang_field, kname);
    return DEFAULT_LANGUAGE;
  }
  const char *lang_s = (const char *) RedisModule_StringPtrLen(lang_rms, NULL); 
  RSLanguage lang = RSLanguage_Find(lang_s);
  if (lang == RS_LANG_UNSUPPORTED) {
    RedisModule_Log(NULL, "warning", "invalid language for for key %s", kname);
    return DEFAULT_LANGUAGE;
  }
  return lang;
}

double SchemaRule_HashScore(const SchemaRule *rule, RedisModuleKey *key, const char *kname) {
  double _default = 1.0;
  if (!rule->score_field) {
    return _default;
  }
  RedisModuleString *score_rms = NULL;
  int rv = RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, rule->score_field, &score_rms, NULL);
  if (rv != REDISMODULE_OK) {
    RedisModule_Log(NULL, "warning", "invalid field %s for key %s", rule->lang_field, kname);
    return _default;
  }
  // score of 1.0 is not saved in hash
  if (score_rms == NULL) {
    return _default;
  }
  double score;
  rv = RedisModule_StringToDouble(score_rms, &score);
  if (rv != REDISMODULE_OK) {
    RedisModule_Log(NULL, "warning", "invalid score for for key %s", kname);
    return _default;
  }
  return score;
}

RedisModuleString *SchemaRule_HashPayload(const SchemaRule *rule, RedisModuleKey *key, const char *kname) {
  const char *payload_field = rule->payload_field ? rule->payload_field : "__payload";
  RedisModuleString *payload_rms = NULL;
  int rv = RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, payload_field, &payload_rms, NULL);
  if (rv != REDISMODULE_OK) {
    RedisModule_Log(NULL, "warning", "invalid field %s for key %s", rule->lang_field, kname);
    return NULL;
  }
  return payload_rms;
}

//---------------------------------------------------------------------------------------------

void SchemaRules_Create() {
  SchemaRules_g = array_new(SchemaRule*, 1);
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

///////////////////////////////////////////////////////////////////////////////////////////////

void SchemaPrefixes_Create() {
  ScemaPrefixes_g = NewTrieMap();
}

void SchemaPrefixes_Free() {
  TrieMap_Free(ScemaPrefixes_g, NULL);
}

void SchemaPrefixes_Add(const char *prefix, IndexSpec *spec) {
  size_t nprefix = strlen(prefix);
  void *p = TrieMap_Find(ScemaPrefixes_g, (char *) prefix, nprefix);
  if (p == TRIEMAP_NOTFOUND) {
    SchemaPrefixNode *node = SchemaPrefixNode_Create(prefix, spec);
    TrieMap_Add(ScemaPrefixes_g, (char *) prefix, nprefix, node, NULL);
  } else {
    SchemaPrefixNode *node = (SchemaPrefixNode *) p;
    node->index_specs = array_append(node->index_specs, spec);
  }
}

void SchemaPrefixes_RemoveSpec(IndexSpec *spec) {
  TrieMapIterator *it = TrieMap_Iterate(ScemaPrefixes_g, "", 0);
  while (true) {
    char *p;
    tm_len_t len;
    SchemaPrefixNode *node = NULL;
    if (!TrieMapIterator_Next(it, &p, &len, (void **) &node)) {
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

//---------------------------------------------------------------------------------------------

SchemaPrefixNode *SchemaPrefixNode_Create(const char *prefix, IndexSpec *index) {
  SchemaPrefixNode *node = rm_calloc(1, sizeof(*node));
  node->prefix = prefix;
  node->index_specs = array_new(IndexSpec*, 1);
  node->index_specs = array_append(node->index_specs, index);
  return node;
}

void SchemaPrefixNode_Free(SchemaPrefixNode *node) {
  array_free(node->index_specs);
}

///////////////////////////////////////////////////////////////////////////////////////////////
