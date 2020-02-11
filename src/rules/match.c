#include "rules.h"
#include "ruledefs.h"

static SchemaRule *parsePrefixRule(ArgsCursor *ac, QueryError *err) {
  const char *prefix;
  if (AC_GetString(ac, &prefix, NULL, 0) != AC_OK) {
    QueryError_SetError(err, QUERY_EPARSEARGS, "Missing prefix");
    return NULL;
  }
  SchemaPrefixRule *ret = rm_calloc(1, sizeof(*ret));
  ret->rtype = SCRULE_TYPE_KEYPREFIX;
  ret->prefix = rm_strdup(prefix);
  ret->nprefix = strlen(ret->prefix);
  return (SchemaRule *)ret;
}

static SchemaRule *parseExprRule(ArgsCursor *ac, QueryError *err) {
  const char *expr;
  SchemaExprRule *erule = NULL;
  if (AC_GetString(ac, &expr, NULL, 0) != AC_OK) {
    QueryError_SetError(err, QUERY_EPARSEARGS, "Missing expression");
    goto expr_err;
  }

  erule = rm_calloc(1, sizeof(*erule));
  erule->rtype = SCRULE_TYPE_EXPRESSION;
  if (!(erule->exprobj = ExprAST_Parse(expr, strlen(expr), err))) {
    goto expr_err;
  }
  erule->exprstr = rm_strdup(expr);
  RLookup_Init(&erule->lk, NULL);
  erule->lk.options |= RLOOKUP_OPT_UNRESOLVED_OK;
  if (ExprAST_GetLookupKeys(erule->exprobj, &erule->lk, err) != EXPR_EVAL_OK) {
    goto expr_err;
  }
  for (RLookupKey *kk = erule->lk.head; kk; kk = kk->next) {
    kk->flags |= RLOOKUP_F_DOCSRC;
  }
  return (SchemaRule *)erule;

expr_err:
  if (erule) {
    if (erule->exprobj) {
      RSExpr_Free(erule->exprobj);
    }
    rm_free(erule->exprstr);
    RLookup_Cleanup(&erule->lk);
    rm_free(erule);
  }
  return NULL;
}

static SchemaAction *extractAction(const char *atype, ArgsCursor *ac, QueryError *err) {
  SchemaAction *ret = rm_calloc(1, sizeof(*ret));
  if (!strcasecmp(atype, "INDEX")) {
    ret->atype = SCACTION_TYPE_INDEX;
  } else if (!strcasecmp(atype, "ABORT")) {
    ret->atype = SCACTION_TYPE_ABORT;
  } else if (!strcasecmp(atype, "GOTO")) {
    const char *target;
    if (AC_GetString(ac, &target, NULL, 0) != AC_OK) {
      QueryError_SetError(err, QUERY_EPARSEARGS, "Missing GOTO target");
      rm_free(ret);
      return NULL;
    }
    ret->atype = SCACTION_TYPE_GOTO;
    ret->u.goto_ = rm_strdup(target);
  } else if (!strcasecmp(atype, "SETATTRS")) {
    // pairwise name/value
    ArgsCursor sub_ac = {0};
    if (AC_GetVarArgs(ac, &sub_ac) != AC_OK) {
      QueryError_SetErrorFmt(err, QUERY_EPARSEARGS, "Missing attributes for action");
      rm_free(ret);
      return NULL;
    }
    if (sub_ac.argc % 2) {
      QueryError_SetError(err, QUERY_EPARSEARGS, "Attributes must be specified in k/v pairs");
      // TODO parse actions
    }
    ret->atype = SCACTION_TYPE_SETATTR;
  } else {
    QueryError_SetErrorFmt(err, QUERY_EPARSEARGS, "Unknown action type `%s`", atype);
    rm_free(ret);
    return NULL;
  }
  return ret;
}

static int matchExpression(const SchemaRule *r, RedisModuleCtx *ctx, RuleKeyItem *item) {
  SchemaExprRule *e = (SchemaExprRule *)r;
  int rc = 0;
  RLookupRow row = {0};
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, NULL);
  QueryError status = {0};
  RLookupLoadOptions loadopts = {
      .sctx = &sctx, .status = &status, .mode = RLOOKUP_LOAD_LKKEYS, .noSortables = 1};
  if (item->kobj) {
    loadopts.ktype = RLOOKUP_KEY_OBJ;
    loadopts.key.kobj = item->kobj;
  } else {
    loadopts.ktype = RLOOKUP_KEY_RSTR;
    loadopts.key.rstr = item->kstr;
  }
  RSValue rsv = RSVALUE_STATIC;

  if (RLookup_LoadDocument(&e->lk, &row, &loadopts) != REDISMODULE_OK) {
    // printf("Couldn't load document: %s\n", QueryError_GetError(&status));
    goto done;
  }

  ExprEval eval = {.err = &status, .lookup = &e->lk, .srcrow = &row, .root = e->exprobj};
  if (ExprEval_Eval(&eval, &rsv) != EXPR_EVAL_OK) {
    // printf("Couldn't evaluate expression: %s\n", QueryError_GetError(&status));
    goto done;
  }

  rc = RSValue_BoolTest(&rsv);

done:
  RLookupRow_Cleanup(&row);
  QueryError_ClearError(&status);
  RSValue_Clear(&rsv);
  return rc;
}

static int matchPrefix(const SchemaRule *r, RedisModuleCtx *ctx, RuleKeyItem *item) {
  SchemaPrefixRule *prule = (SchemaPrefixRule *)r;
  size_t n;
  const char *s = RedisModule_StringPtrLen(item->kstr, &n);
  if (prule->nprefix > n) {
    return 0;
  }
  int ret = strncmp(prule->prefix, s, prule->nprefix) == 0;
  return ret;
}

int SchemaRules_AddArgs(SchemaRules *rules, const char *index, const char *name, ArgsCursor *ac,
                        QueryError *err) {
  // First argument is the name...
  const char *rtype = NULL;
  int rc = AC_GetString(ac, &rtype, NULL, 0);
  if (rc != AC_OK) {
    QueryError_SetError(err, QUERY_EPARSEARGS, "Missing type for rule");
    return REDISMODULE_ERR;
  }

  SchemaRule *r = NULL;
  if (!strcasecmp(rtype, "PREFIX")) {
    r = parsePrefixRule(ac, err);
  } else if (!strcasecmp(rtype, "EXPR")) {
    r = parseExprRule(ac, err);
  } else {
    QueryError_SetErrorFmt(err, QUERY_ENOOPTION, "No such match type `%s`\n", rtype);
    return REDISMODULE_ERR;
  }

  if (!r) {
    return REDISMODULE_ERR;
  }
  const char *astr = NULL;
  if (AC_GetString(ac, &astr, NULL, 0) != AC_OK) {
    QueryError_SetError(err, QUERY_EPARSEARGS, "Missing action!");
    // todo: free r
    return REDISMODULE_ERR;
  }
  r->action = extractAction(astr, ac, err);
  if (r->action == NULL) {
    return REDISMODULE_ERR;
  }
  r->index = rm_strdup(index);
  r->name = rm_strdup(name);
  dllist_append(&rules->rules, &r->llnode);
  return REDISMODULE_OK;
}

/**
 * The idea here is to allow multiple rule matching types, and to have a dynamic
 * function table for each rule type
 */
typedef int (*scruleMatchFn)(const SchemaRule *, RedisModuleCtx *, RuleKeyItem *);

static scruleMatchFn matchfuncs_g[] = {[SCRULE_TYPE_KEYPREFIX] = matchPrefix,
                                       [SCRULE_TYPE_EXPRESSION] = matchExpression};

int SchemaRules_Check(const SchemaRules *rules, RedisModuleCtx *ctx, RuleKeyItem *item,
                      MatchAction **results, size_t *nresults) {
  array_clear(rules->actions);
  *results = rules->actions;

  DLLIST_FOREACH(it, &rules->rules) {
    SchemaRule *rule = DLLIST_ITEM(it, SchemaRule, llnode);
    scruleMatchFn fn = matchfuncs_g[rule->rtype];
    if (!fn(rule, ctx, item)) {
      continue;
    }

    // MatchActions contain the settings, per-index; we first find the result for
    // the given index, and if we can't find it, we create it.

    MatchAction *curAction = NULL;
    for (size_t ii = 0; ii < *nresults; ++ii) {
      if (!strcmp((*results)[ii].index, rule->index)) {
        curAction = (*results) + ii;
      }
    }
    if (!curAction) {
      curAction = array_ensure_tail(results, MatchAction);
      curAction->index = rule->index;
      curAction->attrs.language = NULL;
      curAction->attrs.score = 0;
      curAction->attrs.npayload = 0;
    }
    assert(rule->action->atype == SCACTION_TYPE_INDEX);
  }
  *nresults = array_len(*results);
  return *nresults;
}
