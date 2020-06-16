#include "rules.h"
#include "aggregate/expr/expression.h"

IndexSpec **SchemaRules_g;

SchemaRule *Rule_Create(ruleSettings *rulesopts, QueryError *status) {
    RSExpr *e = ExprAST_Parse(rulesopts->expr, strlen(rulesopts->expr), status);
    if (!e) {
      return NULL;
    }

    SchemaRule *rule = rm_calloc(1, sizeof(*rule));
    rule->expression = e;
    rule->setting.expr = rm_strdup(rulesopts->expr);

    if (rulesopts->score) {
      rule->setting.score = rm_strdup(rulesopts->score);
    }
    
    if (rulesopts->lang) {
      rule->setting.lang = rm_strdup(rulesopts->lang);
    }

    if (rulesopts->payload) {
      rule->setting.payload = rm_strdup(rulesopts->payload);
    }

    return rule;
}

void Rule_free(SchemaRule *rule) {
  ExprAST_Free(rule->expression);
  rm_free(rule->setting.expr);

  if (rule->setting.score) {
    rm_free(rule->setting.score);
  }  
  if (rule->setting.lang) {
    rm_free(rule->setting.lang);
  }
  if (rule->setting.payload) {
    rm_free(rule->setting.payload);
  }
  rm_free(rule);
  // TODO: Remove from global list
}
