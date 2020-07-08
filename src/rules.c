#include "rules.h"
#include "aggregate/expr/expression.h"
#include "spec.h"

arrayof(SchemaRule*) SchemaRules_g;
TrieMap *ScemaPrefixes_g;

///////////////////////////////////////////////////////////////////////////////////////////////

SchemaRule *SchemaRule_Create(SchemaRuleArgs *args, IndexSpec *spec, QueryError *status) {
  SchemaRule *rule = rm_calloc(1, sizeof(*rule));

  rule->type = rm_strdup(args->type);
  rule->filter_exp_str = args->filter_exp_str ? rm_strdup(args->filter_exp_str) : NULL;
  rule->lang_field = args->lang_field ? rm_strdup(args->lang_field) : NULL;
  rule->score_field = args->score_field ? rm_strdup(args->score_field) : NULL;
  rule->payload_field = args->payload_field ? rm_strdup(args->payload_field) : NULL;

  for (int i = 0; i < args->nprefixes; ++i) {
    const char *p = rm_strdup(args->prefixes[i]);
    rule->prefixes = array_ensure_append(rule->prefixes, &p, 1, const char*);
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

  SchemaRules_g = array_ensure_append_1(SchemaRules_g, rule);
  return rule;

error:
  SchemaRule_Free(rule);
  return NULL;
}

void SchemaRule_Free(SchemaRule *rule) {
  SchemaPrefixes_RemoveSpec(rule->spec);
  SchemaRules_RemoveSpecRules(rule->spec);

  rm_free((void*) rule->type);
  rm_free((void*) rule->lang_field);
  rm_free((void*) rule->score_field);
  rm_free((void*) rule->payload_field);
  rm_free((void*) rule->filter_exp_str);
  if (rule->filter_exp) {
    ExprAST_Free((RSExpr *) rule->filter_exp);
  }
  array_free_ex(rule->prefixes, rm_free(*(char **)ptr));
  rm_free((void*) rule);
}

//---------------------------------------------------------------------------------------------

void SchemaRules_RemoveSpecRules(IndexSpec *spec) {
  for (size_t i = 0; i < array_len(SchemaRules_g); ++i) {
	  SchemaRule *rule = SchemaRules_g[i];
    if (spec == rule->spec) {
      array_del_fast(SchemaRules_g, i);
      return;
    }
  }
}

//---------------------------------------------------------------------------------------------

RSLanguage SchemaRule_HashLang(RedisModuleCtx *rctx, const SchemaRule *rule, RedisModuleKey *key, const char *kname) {
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
  const char *lang_s = (const char *) RedisModule_StringPtrLen(lang_rms, NULL); 
  lang = RSLanguage_Find(lang_s);
  if (lang == RS_LANG_UNSUPPORTED) {
    RedisModule_Log(NULL, "warning", "invalid language for for key %s", kname);
    lang = DEFAULT_LANGUAGE;
  }
done:
  if (lang_rms) {
    RedisModule_FreeString(rctx, lang_rms);
  }
  return lang;
}

double SchemaRule_HashScore(RedisModuleCtx *rctx, const SchemaRule *rule, RedisModuleKey *key, const char *kname) {
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
    RedisModule_Log(NULL, "warning", "invalid score for for key %s", kname);
    score = 1.0;
  }
done:
  if (score_rms) {
    RedisModule_FreeString(rctx, score_rms);
  }
  return score;
}

RedisModuleString *SchemaRule_HashPayload(RedisModuleCtx *rctx, const SchemaRule *rule, RedisModuleKey *key, const char *kname) {
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

///////////////////////////////////////////////////////////////////////////////////////////////

void SchemaPrefixes_Create() {
  ScemaPrefixes_g = NewTrieMap();
}

static void freePrefixNode(void *ctx) {
  SchemaPrefixNode *node = ctx;
  array_free(node->index_specs);
  rm_free(node);
}

void SchemaPrefixes_Free() {
  TrieMap_Free(ScemaPrefixes_g, freePrefixNode);
}

void SchemaPrefixes_Add(const char *prefix, IndexSpec *spec) {
  size_t nprefix = strlen(prefix);
  void *p = TrieMap_Find(ScemaPrefixes_g, (char *) prefix, nprefix);
  if (p == TRIEMAP_NOTFOUND) {
    SchemaPrefixNode *node = SchemaPrefixNode_Create(prefix, spec);
    TrieMap_Add(ScemaPrefixes_g, (char *) prefix, nprefix, node, NULL);
  } else {
    SchemaPrefixNode *node = (SchemaPrefixNode *) p;
    node->index_specs = array_ensure_append_1(node->index_specs, spec);
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
  node->index_specs = array_ensure_append_1(node->index_specs, index);
  return node;
}

///////////////////////////////////////////////////////////////////////////////////////////////
