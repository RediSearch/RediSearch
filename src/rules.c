#include "rules.h"
#include "aggregate/expr/expression.h"
#include "spec.h"

SchemaRule **SchemaRules_g;
TrieMap *ScemaPrefixes_g;

///////////////////////////////////////////////////////////////////////////////////////////////

SchemaRule *SchemaRule_Create(SchemaRule *rule0, SchemaRuleArgs *args, IndexSpec *spec, QueryError *status) {
  SchemaRule *rule = rm_calloc(1, sizeof(*rule));
  memcpy(rule, rule0, sizeof(*rule));

  rule->type = rm_strdup(rule->type);
  rule->filter = rule0->filter ? rm_strdup(rule->filter) : NULL;
  rule->payload = rule0->payload ? rm_strdup(rule->payload) : NULL;

  for (int i = 0; i < args->nprefixes; ++i) {
    const char *p = rm_strdup(args->prefixes[i]);
    rule->prefixes = array_ensure_append(rule->prefixes, &p, 1, const char*);
  }

  rule->spec = spec;

  if (rule->filter) {
    RSExpr *e = ExprAST_Parse(rule->filter, strlen(rule->filter), status);
    if (!e) {
      QueryError_SetError(status, QUERY_EADDARGS, "Invalid expression");
      goto error;
    }
  }

  rule->lang = RSLanguage_Find(args->lang);
  if (rule->lang == RS_LANG_UNSUPPORTED) {
    QueryError_SetError(status, QUERY_EADDARGS, "Unsupported language");
    goto error;
  }

  for (int i = 0; i < array_len(rule->prefixes); ++i) {
    SchemaPrefixes_Add(rule->prefixes[i], spec);
  }

  SchemaRules_g = array_ensure_append(SchemaRules_g, &rule, 1, SchemaRule*);
  return rule;

error:
  SchemaRule_Free(rule);
  return NULL;
}

void SchemaRule_Free(SchemaRule *rule) {
  BB;
  SchemaPrefixes_RemoveSpec(rule->spec);
  SchemaRules_RemoveSpecRules(rule->spec);

  rm_free((void*) rule->type);
  rm_free((void*) rule->filter);
  array_free_ex(rule->prefixes, rm_free(*(char **)ptr));
  ExprAST_Free(rule->expression);
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

///////////////////////////////////////////////////////////////////////////////////////////////

void SchemaPrefixes_Create() {
  ScemaPrefixes_g = NewTrieMap();
}

void SchemaPrefixes_Free() {
  TrieMap_Free(ScemaPrefixes_g, NULL);
}

void SchemaPrefixes_Add(const char *prefix, IndexSpec *index) {
  void *p = TrieMap_Find(ScemaPrefixes_g, (char *)prefix, strlen(prefix));
  if (p == TRIEMAP_NOTFOUND) {
    SchemaPrefixNode *node = SchemaPrefixNode_Create(prefix, index);
    TrieMap_Add(ScemaPrefixes_g, (char *) prefix, strlen(prefix), node, NULL);
  } else {
    SchemaPrefixNode *node = (SchemaPrefixNode *) p;
    node->index_specs = array_ensure_append(node->index_specs, &index, 1, IndexSpec*);
  }
}

void SchemaPrefixes_RemoveSpec(IndexSpec *spec) {
  TrieMapIterator *it = TrieMap_Iterate(ScemaPrefixes_g, "", 0);
  while (true) {
    char *p;
    tm_len_t len;
    SchemaPrefixNode *node;
    if (!TrieMapIterator_Next(it, &p, &len, (void **) &node)) {
      break;
    }
    int j = -1;
    for (int i = 0; i < array_len(node->index_specs); ++i) {
      if (node->index_specs[i] == spec) {
        j = i;
        break;
      }
    }
    if (j != -1) {
      array_del_fast(node->index_specs, j);
    }
  }
  TrieMapIterator_Free(it); 
}

//---------------------------------------------------------------------------------------------

SchemaPrefixNode *SchemaPrefixNode_Create(const char *prefix, IndexSpec *index) {
  SchemaPrefixNode *node = rm_calloc(1, sizeof(*node));
  node->prefix = prefix;
  node->index_specs = NULL;
  node->index_specs = array_ensure_append(node->index_specs, &index, 1, IndexSpec*);
  return node;
}

void SchemaPrefixNode_Free(SchemaPrefixNode *node) {
  array_free(node->index_specs);
}

///////////////////////////////////////////////////////////////////////////////////////////////
