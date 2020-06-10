#include "rules.h"

int Rule_EvalExpression(IndexSpec *spec, ruleSettings *rulesopts, QueryError *status) {
    RSExpr *e = ExprAST_Parse(rulesopts->expr, strlen(rulesopts->expr), status);
    if (!e) {
      return REDISMODULE_ERR;
    }

    SchemaRule *rule = rm_calloc(1, sizeof(*rule));
    rule->spec = spec;
    rule->expression = e;
    rule->setting.expr = rm_strdup(rulesopts->expr);

    if (rule->setting.score) {
      rule->setting.score = rm_strdup(rulesopts->score);
    }
    
    if (rule->setting.lang) {
      rule->setting.lang = rm_strdup(rulesopts->lang);
    }

    if (rule->setting.payload) {
      rule->setting.payload = rm_strdup(rulesopts->payload);
    }

    SchemaRules_g = array_ensure_append(SchemaRules_g, &rule, 1, SchemaRule);
    return REDISMODULE_OK;
}