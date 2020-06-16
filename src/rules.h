
#pragma once

#ifndef RULES_RULES_H
#define RULES_RULES_H

#include "query_error.h"

struct RSExpr;
struct IndexSpec;

typedef struct {
  char *expr;
  char *score;
  char *lang;
  char *payload;
} ruleSettings;

typedef struct {
  struct RSExpr *expression;
  ruleSettings setting;
} SchemaRule;

extern struct IndexSpec **SchemaRules_g;

SchemaRule *Rule_Create(ruleSettings *rulesopts, QueryError *status);
void Rule_free(SchemaRule *);

#endif // RULES_RULES_H