#include "expression.h"
#include "result_processor.h"

static int evalInternal(ExprEval *eval, const RSExpr *e, RSValue *res);

static void setReferenceValue(RSValue *dst, RSValue *src) {
  RSValue_MakeReference(dst, src);
}
static int evalFunc(ExprEval *eval, const RSFunctionExpr *f, RSValue *result) {
  int rc = EXPR_EVAL_ERR;

  /** First, evaluate every argument */
  size_t nusedargs = 0;
  size_t nargs = f->args->len;
  RSValue *argspp[nargs];
  RSValue args[nargs];

  for (size_t ii = 0; ii < nargs; ii++) {
    args[ii] = (RSValue)RSVALUE_STATIC;
    argspp[ii] = &args[ii];
    if (evalInternal(eval, f->args->args[ii], &args[ii]) == EXPR_EVAL_ERR) {
      // TODO: Free other results
      rc = EXPR_EVAL_ERR;
      goto cleanup;
    }
    nusedargs++;
  }

  /** We pass an RSValue**, not an RSValue*, as the arguments */
  rc = f->Call(eval, result, argspp, nargs, eval->err);

cleanup:
  for (size_t ii = 0; ii < nusedargs; ii++) {
    RSValue_Clear(&args[ii]);
  }
  return rc;
}

static int evalOp(ExprEval *eval, const RSExprOp *op, RSValue *result) {
  RSValue l = RSVALUE_STATIC, r = RSVALUE_STATIC;
  int rc = EXPR_EVAL_ERR;

  if (evalInternal(eval, op->left, &l) == EXPR_EVAL_ERR) {
    goto cleanup;
  }
  if (evalInternal(eval, op->right, &r) == EXPR_EVAL_ERR) {
    goto cleanup;
  }

  double n1, n2;
  if (!RSValue_ToNumber(&l, &n1) || !RSValue_ToNumber(&r, &n2)) {

    QueryError_SetError(eval->err, QUERY_ENOTNUMERIC, NULL);
    rc = EXPR_EVAL_ERR;
    goto cleanup;
  }

  double res;
  switch (op->op) {
    case '+':
      res = n1 + n2;
      break;
    case '/':
      res = n1 / n2;
      break;
    case '-':
      res = n1 - n2;
      break;
    case '*':
      res = n1 * n2;
      break;
    case '%':
      res = (long long)n1 % (long long)n2;
      break;
    case '^':
      res = pow(n1, n2);
      break;
    default:
      res = NAN;  // todo : we can not really reach here
  }

  result->numval = res;
  result->t = RSValue_Number;
  rc = EXPR_EVAL_OK;

cleanup:
  RSValue_Clear(&l);
  RSValue_Clear(&r);
  return rc;
}

static int getPredicateBoolean(ExprEval *eval, const RSValue *l, const RSValue *r, RSCondition op) {
  QueryError *qerr = eval ? eval->err : NULL;
  switch (op) {
    case RSCondition_Eq:
      return RSValue_Equal(l, r, qerr);

    case RSCondition_Lt:
      return RSValue_Cmp(l, r, qerr) < 0;

    /* Less than or equal, <= */
    case RSCondition_Le:
      return RSValue_Cmp(l, r, qerr) <= 0;

      /* Greater than, > */
    case RSCondition_Gt:
      return RSValue_Cmp(l, r, qerr) > 0;

    /* Greater than or equal, >= */
    case RSCondition_Ge:
      return RSValue_Cmp(l, r, qerr) >= 0;

    /* Not equal, != */
    case RSCondition_Ne:
      return !RSValue_Equal(l, r, qerr);

      /* Logical AND of 2 expressions, && */
    case RSCondition_And:
      return RSValue_BoolTest(l) && RSValue_BoolTest(r);

    /* Logical OR of 2 expressions, || */
    case RSCondition_Or:
      return RSValue_BoolTest(l) || RSValue_BoolTest(r);

    default:
      assert("Unknown predicate received" && 0);
      return 0;
  }
}

static int evalInverted(ExprEval *eval, const RSInverted *vv, RSValue *result) {
  RSValue tmpval = RSVALUE_STATIC;
  if (evalInternal(eval, vv->child, &tmpval) != EXPR_EVAL_OK) {
    return EXPR_EVAL_ERR;
  }

  result->numval = !RSValue_BoolTest(&tmpval);
  result->t = RSValue_Number;

  RSValue_Clear(&tmpval);
  return EXPR_EVAL_OK;
}

static int evalPredicate(ExprEval *eval, const RSPredicate *pred, RSValue *result) {
  int res;
  RSValue l = RSVALUE_STATIC, r = RSVALUE_STATIC;
  int rc = EXPR_EVAL_ERR;
  if (evalInternal(eval, pred->left, &l) == EXPR_EVAL_ERR) {
    goto cleanup;
  } else if (pred->cond == RSCondition_Or && RSValue_BoolTest(&l)) {
    res = 1;
    goto success;
  } else if (pred->cond == RSCondition_And && !RSValue_BoolTest(&l)) {
    res = 0;
    goto success;
  } else if (evalInternal(eval, pred->right, &r) == EXPR_EVAL_ERR) {
    goto cleanup;
  }

  res = getPredicateBoolean(eval, &l, &r, pred->cond);

success:
  if (!eval->err || eval->err->code == QUERY_OK) {
    result->numval = res;
    result->t = RSValue_Number;
    rc = EXPR_EVAL_OK;
  } else {
    result->t = RSValue_Undef;
  }

cleanup:
  RSValue_Clear(&l);
  RSValue_Clear(&r);
  return rc;
}

static int evalProperty(ExprEval *eval, const RSLookupExpr *e, RSValue *res) {
  if (!e->lookupObj) {
    // todo : this can not happened
    // No lookup object. This means that the key does not exist
    // Note: Because this is evaluated for each row potentially, do not assume
    // that query error is present:
    if (eval->err) {
      QueryError_SetError(eval->err, QUERY_ENOPROPKEY, NULL);
    }
    return 0;
  }

  /** Find the actual value */
  RSValue *value = RLookup_GetItem(e->lookupObj, eval->srcrow);
  if (!value) {
    if (eval->err) {
      QueryError_SetError(eval->err, QUERY_ENOPROPVAL, NULL);
    }
    return 0;
  }

  setReferenceValue(res, value);
  return 1;
}

static int evalInternal(ExprEval *eval, const RSExpr *e, RSValue *res) {
  RSValue_Clear(res);
  switch (e->t) {
    case RSExpr_Property:
      return evalProperty(eval, &e->property, res);
    case RSExpr_Literal:
      RSValue_MakeReference(res, (RSValue *)&e->literal);
      return EXPR_EVAL_OK;
    case RSExpr_Function:
      return evalFunc(eval, &e->func, res);
    case RSExpr_Op:
      return evalOp(eval, &e->op, res);
    case RSExpr_Predicate:
      return evalPredicate(eval, &e->pred, res);
    case RSExpr_Inverted:
      return evalInverted(eval, &e->inverted, res);
  }
  return EXPR_EVAL_ERR;  // todo: this can not happened
}

int ExprEval_Eval(ExprEval *evaluator, RSValue *result) {
  return evalInternal(evaluator, evaluator->root, result);
}

int ExprAST_GetLookupKeys(RSExpr *expr, RLookup *lookup, QueryError *err) {
#define RECURSE(v)                                                                             \
  if (!v) {                                                                                    \
    QueryError_SetErrorFmt(err, QUERY_EEXPR, "Missing (or badly formatted) value for %s", #v); \
    return EXPR_EVAL_ERR;                                                                      \
  }                                                                                            \
  if (ExprAST_GetLookupKeys(v, lookup, err) != EXPR_EVAL_OK) {                                 \
    return EXPR_EVAL_ERR;                                                                      \
  }

  switch (expr->t) {
    case RSExpr_Property:
      expr->property.lookupObj = RLookup_GetKey(lookup, expr->property.key, RLOOKUP_F_NOINCREF);
      if (!expr->property.lookupObj) {
        QueryError_SetErrorFmt(err, QUERY_ENOPROPKEY, "Property `%s` not loaded in pipeline",
                               expr->property.key);
        return EXPR_EVAL_ERR;
      }
      break;
    case RSExpr_Function:
      for (size_t ii = 0; ii < expr->func.args->len; ii++) {
        RECURSE(expr->func.args->args[ii])
      }
      break;
    case RSExpr_Op:
      RECURSE(expr->op.left);
      RECURSE(expr->op.right);
      break;
    case RSExpr_Predicate:
      RECURSE(expr->pred.left);
      RECURSE(expr->pred.right);
      break;
    case RSExpr_Inverted:
      RECURSE(expr->inverted.child);
      break;
    default:
      break;
  }
  return EXPR_EVAL_OK;
}

/* Allocate some memory for a function that can be freed automatically when the execution is done */
void *ExprEval_UnalignedAlloc(ExprEval *ctx, size_t sz) {
  return BlkAlloc_Alloc(&ctx->stralloc, sz, MAX(sz, 1024));
}

char *ExprEval_Strndup(ExprEval *ctx, const char *str, size_t len) {
  char *ret = ExprEval_UnalignedAlloc(ctx, len + 1);
  memcpy(ret, str, len);
  ret[len] = '\0';
  return ret;
}

/**
 * ResultProcessor type which evaluates expressions
 */
typedef struct RPEvaluator RPEvaluator;
struct RPEvaluator {
  ResultProcessor base;
  ExprEval eval;
  RSValue *val;
  const RLookupKey *outkey;
  int isFilter;
};

#define RESULT_EVAL_ERR RS_RESULT_MAX + 1

static int rpevalCommon(RPEvaluator *pc, SearchResult *r) {
  /** Get the upstream result */
  int rc = pc->base.upstream->Next(pc->base.upstream, r);
  if (rc != RS_RESULT_OK) {
    return rc;
  }

  pc->eval.res = r;
  pc->eval.srcrow = &r->rowdata;

  // TODO: Set this once only
  pc->eval.err = pc->base.parent->err;

  if (!pc->val) {
    pc->val = RS_NewValue(RSValue_Undef);
  }

  rc = ExprEval_Eval(&pc->eval, pc->val);
  if (rc == EXPR_EVAL_ERR) {
    return RS_RESULT_ERROR;
  }
  return RS_RESULT_OK;
}

static int rpevalNext_project(ResultProcessor *rp, SearchResult *r) {
  RPEvaluator *pc = (RPEvaluator *)rp;
  int rc = rpevalCommon(pc, r);

  if (rc != RS_RESULT_OK) {
    return rc;
  }
  RLookup_WriteOwnKey(pc->outkey, &r->rowdata, pc->val);
  pc->val = NULL;
  return RS_RESULT_OK;
}

static int rpevalNext_filter(ResultProcessor *rp, SearchResult *r) {
  RPEvaluator *pc = (RPEvaluator *)rp;
  int rc;
  while ((rc = rpevalCommon(pc, r)) == RS_RESULT_OK) {
    // Check if it's a boolean result!
    int boolrv = RSValue_BoolTest(pc->val);
    RSValue_Clear(pc->val);

    if (boolrv) {
      return RS_RESULT_OK;
    }

    // Otherwise, the result must be filtered out.
    SearchResult_Clear(r);
  }
  return rc;
}

static void rpevalFree(ResultProcessor *rp) {
  RPEvaluator *ee = (RPEvaluator *)rp;
  if (ee->val) {
    RSValue_Decref(ee->val);
  }
  BlkAlloc_FreeAll(&ee->eval.stralloc, NULL, NULL, 0);
  rm_free(ee);
}
static ResultProcessor *RPEvaluator_NewCommon(const RSExpr *ast, const RLookup *lookup,
                                              const RLookupKey *dstkey, int isFilter) {
  RPEvaluator *rp = rm_calloc(1, sizeof(*rp));
  rp->base.Next = isFilter ? rpevalNext_filter : rpevalNext_project;
  rp->base.Free = rpevalFree;
  rp->base.name = isFilter ? "Filter" : "Projector";
  rp->eval.lookup = lookup;
  rp->eval.root = ast;
  rp->outkey = dstkey;
  BlkAlloc_Init(&rp->eval.stralloc);
  return &rp->base;
}

ResultProcessor *RPEvaluator_NewProjector(const RSExpr *ast, const RLookup *lookup,
                                          const RLookupKey *dstkey) {
  return RPEvaluator_NewCommon(ast, lookup, dstkey, 0);
}

ResultProcessor *RPEvaluator_NewFilter(const RSExpr *ast, const RLookup *lookup) {
  return RPEvaluator_NewCommon(ast, lookup, NULL, 1);
}
