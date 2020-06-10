#ifndef RULES_RULES_H
#define RULES_RULES_H

#include "aggregate/expr/expression.h"
//#include "spec.h"

typedef struct {
  char *expr;
  char *score;
  char *lang;
  char *payload;
} ruleSettings;

typedef struct {
  IndexSpec *spec;
  RSExpr *expression;
  ruleSettings setting;
} SchemaRule;

SchemaRule *SchemaRules_g;

int Rule_EvalExpression(IndexSpec *spec, ruleSettings *rulesopts, QueryError *status);

#endif // RULES_RULES_H