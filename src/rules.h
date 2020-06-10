#ifndef RULES_RULES_H
#define RULES_RULES_H

#include "query_error.h"
typedef struct RSExpr RSExpr;
typedef struct IndexSpec IndexSpec;

typedef struct {
  char *expr;
  char *score;
  char *lang;
  char *payload;
} ruleSettings;

typedef struct {
  RSExpr *expression;
  ruleSettings setting;
} SchemaRule;

extern IndexSpec *SchemaRules_g;

SchemaRule *Rule_Create(ruleSettings *rulesopts, QueryError *status);
void Rule_free(SchemaRule *);

#endif // RULES_RULES_H