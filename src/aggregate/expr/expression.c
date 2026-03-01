/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "expression.h"
#include "result_processor.h"
#include "rlookup.h"
#include "profile/profile.h"

///////////////////////////////////////////////////////////////////////////////////////////////

static int evalInternal(ExprEval *eval, const RSExpr *e, RSValue *res);

static void setReferenceValue(RSValue *dst, RSValue *src) {
  RSValue_MakeReference(dst, src);
}

extern int func_exists(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result);
extern int func_case(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result);

static int evalFuncCase(ExprEval *eval, const RSFunctionExpr *f, RSValue *result) {
  // Evaluate the condition
  RSValue *condVal = RSValue_NewUndefined();
  int rc = evalInternal(eval, f->args->args[0], condVal);
  if (rc != EXPR_EVAL_OK) {
    RSValue_DecrRef(condVal);
    return rc;
  }

  // Determine which branch to evaluate based on the condition
  int condition = RSValue_BoolTest(condVal);
  RSValue_DecrRef(condVal);

  // Evaluate only the branch we need
  int branchIndex = condition ? 1 : 2;
  rc = evalInternal(eval, f->args->args[branchIndex], result);
  return rc;
}

static int evalFunc(ExprEval *eval, const RSFunctionExpr *f, RSValue *result) {
  int rc = EXPR_EVAL_ERR;

  // Special handling for func_case. The condition is evaluated to determine
  // which branch to take and only that branch is evaluated.
  // For other functions, we evaluate all arguments first.
  if (f->Call == func_case) {
    return evalFuncCase(eval, f, result);
  }

  /** First, evaluate every argument */
  size_t nallocdargs = 0;
  size_t nargs = f->args->len;
  RSValue *args[nargs];

  // Normal function evaluation
  for (size_t ii = 0; ii < nargs; ii++) {
    args[ii] = RSValue_NewUndefined();
    nallocdargs++;

    int internalRes = evalInternal(eval, f->args->args[ii], args[ii]);

    // Handle NULL values:
    // 1. For func_exists, always allow NULL values
    // 2. For all other functions, NULL values are errors
    if (internalRes == EXPR_EVAL_ERR ||
       (internalRes == EXPR_EVAL_NULL && f->Call != func_exists)) {
      goto cleanup;
    }
  }

  rc = f->Call(eval, args, nargs, result);

cleanup:
  for (size_t ii = 0; ii < nallocdargs; ii++) {
    RSValue_DecrRef(args[ii]);
  }
  return rc;
}

static int evalOp(ExprEval *eval, const RSExprOp *op, RSValue *result) {
  RSValue *l = RSValue_NewUndefined(), *r = RSValue_NewUndefined();
  int rc = EXPR_EVAL_ERR;

  if (evalInternal(eval, op->left, l) != EXPR_EVAL_OK) {
    goto cleanup;
  }
  if (evalInternal(eval, op->right, r) != EXPR_EVAL_OK) {
    goto cleanup;
  }

  double n1, n2;
  if (!RSValue_ToNumber(l, &n1) || !RSValue_ToNumber(r, &n2)) {

    QueryError_SetError(eval->err, QUERY_ERROR_CODE_NUMERIC_VALUE_INVALID, NULL);
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
      res = fmod(n1, n2);
      break;
    case '^':
      res = pow(n1, n2);
      break;
    default: RS_LOG_ASSERT_FMT(0, "Invalid operator %c", op->op);
  }

  RSValue_SetNumber(result, res);
  rc = EXPR_EVAL_OK;

cleanup:
  RSValue_DecrRef(l);
  RSValue_DecrRef(r);
  return rc;
}

static int getPredicateBoolean(ExprEval *eval, const RSValue *l, const RSValue *r, RSCondition op) {
  QueryError *qerr = eval ? eval->err : NULL;

  l = RSValue_Dereference(l);
  r = RSValue_Dereference(r);

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
      RS_ABORT("invalid RSCondition");
      return 0;
  }
}

static int evalInverted(ExprEval *eval, const RSInverted *vv, RSValue *result) {
  RSValue *tmpval = RSValue_NewUndefined();
  if (evalInternal(eval, vv->child, tmpval) != EXPR_EVAL_OK) {
    RSValue_DecrRef(tmpval);
    return EXPR_EVAL_ERR;
  }

  RSValue_SetNumber(result, !RSValue_BoolTest(tmpval));

  RSValue_DecrRef(tmpval);
  return EXPR_EVAL_OK;
}

static int evalPredicate(ExprEval *eval, const RSPredicate *pred, RSValue *result) {
  int res;
  RSValue *l = RSValue_NewUndefined(), *r = RSValue_NewUndefined();
  int rc = EXPR_EVAL_ERR;
  if (evalInternal(eval, pred->left, l) != EXPR_EVAL_OK) {
    goto cleanup;
  } else if (pred->cond == RSCondition_Or && RSValue_BoolTest(l)) {
    res = 1;
    goto success;
  } else if (pred->cond == RSCondition_And && !RSValue_BoolTest(l)) {
    res = 0;
    goto success;
  } else if (evalInternal(eval, pred->right, r) != EXPR_EVAL_OK) {
    goto cleanup;
  }

  res = getPredicateBoolean(eval, l, r, pred->cond);

success:
  if (!eval->err || QueryError_IsOk(eval->err)) {
    RSValue_SetNumber(result, res);
    rc = EXPR_EVAL_OK;
  } else {
    RSValue_Clear(result);
  }

cleanup:
  RSValue_DecrRef(l);
  RSValue_DecrRef(r);
  return rc;
}

static int evalProperty(ExprEval *eval, const RSLookupExpr *e, RSValue *res) {
  if (!e->lookupObj) {
    // todo : this can not happened
    // No lookup object. This means that the key does not exist
    // Note: Because this is evaluated for each row potentially, do not assume
    // that query error is present:
    if (eval->err) {
      QueryError_SetError(eval->err, QUERY_ERROR_CODE_NO_PROP_KEY, NULL);
    }
    return EXPR_EVAL_ERR;
  }

  /** Find the actual value */
  RSValue *value = RLookupRow_Get(e->lookupObj, eval->srcrow);
  if (!value) {
    if (eval->err) {
      QueryError_SetWithUserDataFmt(eval->err, QUERY_ERROR_CODE_NO_PROP_VAL, "Could not find the value for a parameter name, consider using EXISTS if applicable", " for %s", RLookupKey_GetName(e->lookupObj));
    }
    RSValue_SetNull(res);
    return EXPR_EVAL_NULL;
  }

  setReferenceValue(res, value);
  return EXPR_EVAL_OK;
}

static int evalInternal(ExprEval *eval, const RSExpr *e, RSValue *res) {
  RSValue_Clear(res);
  switch (e->t) {
    case RSExpr_Property:
      return evalProperty(eval, &e->property, res);
    case RSExpr_Literal:
      RSValue_MakeReference(res, e->literal);
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
#define RECURSE(v)                                                                                 \
  if (!v) {                                                                                        \
    QueryError_SetWithUserDataFmt(err, QUERY_ERROR_CODE_EXPR, "Missing (or badly formatted) value for", " %s", #v); \
    return EXPR_EVAL_ERR;                                                                          \
  }                                                                                                \
  if (ExprAST_GetLookupKeys(v, lookup, err) != EXPR_EVAL_OK) {                                     \
    return EXPR_EVAL_ERR;                                                                          \
  }

  switch (expr->t) {
    case RSExpr_Property:
      expr->property.lookupObj = RLookup_GetKey_Read(lookup, expr->property.key, RLOOKUP_F_NOFLAGS);
      if (!expr->property.lookupObj) {
        QueryError_SetWithUserDataFmt(err, QUERY_ERROR_CODE_NO_PROP_KEY,
                                      "Property not loaded nor in pipeline",
                                      ": `%s`",
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
    case RSExpr_Literal:
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

///////////////////////////////////////////////////////////////////////////////////////////////

EvalCtx *EvalCtx_Create() {
  EvalCtx *r = rm_calloc(1, sizeof(EvalCtx));

  RLookup _lk = RLookup_New();
  r->lk = _lk;
  QueryError _status = QueryError_Default();
  r->status = _status;

  r->ee.lookup = &r->lk;
  r->ee.srcrow = &r->row;
  r->ee.err = &r->status;

  r->res = RSValue_NewNull();

  r->_expr = NULL;

  return r;
}

EvalCtx *EvalCtx_FromExpr(RSExpr *expr) {
  EvalCtx *r = EvalCtx_Create();
  r->_expr = expr;
  r->_own_expr = false;
  return r;
}

EvalCtx *EvalCtx_FromString(const HiddenString *expr) {
  EvalCtx *r = EvalCtx_Create();
  if (!expr) {
  	r->ee.root = NULL;
  } else {
    r->_expr = ExprAST_Parse(expr, r->ee.err);
    if (r->ee.root == NULL) {
  	  goto error;
    }
    r->_own_expr = true;
  }
  return r;

error:
  EvalCtx_Destroy(r);
  return NULL;
}

void EvalCtx_Destroy(EvalCtx *r) {
  RSValue_DecrRef(r->res);
  if (r->_expr && r->_own_expr) {
    ExprAST_Free(r->_expr);
  }
  RLookupRow_Reset(&r->row);
  RLookup_Cleanup(&r->lk);
  QueryError_ClearError(&r->status);
  rm_free(r);
}

//---------------------------------------------------------------------------------------------

int EvalCtx_Eval(EvalCtx *r) {
  if (!r->_expr) {
    return EXPR_EVAL_ERR;
  }
  r->ee.root = r->_expr;
  if (ExprAST_GetLookupKeys(r->ee.root, (RLookup *) r->ee.lookup, r->ee.err) != EXPR_EVAL_OK) {
    return EXPR_EVAL_ERR;
  }
  return ExprEval_Eval(&r->ee, r->res);
}

int EvalCtx_EvalExpr(EvalCtx *r, RSExpr *expr) {
  if (r->_expr && r->_own_expr) {
    ExprAST_Free(r->_expr);
  }
  r->_expr = expr;
  r->_own_expr = false;

  return EvalCtx_Eval(r);
}

int EvalCtx_EvalExprStr(EvalCtx *r, const HiddenString *expr) {
  if (r->_expr && r->_own_expr) {
    ExprAST_Free(r->_expr);
  }
  r->_expr = ExprAST_Parse(expr, r->ee.err);
  r->_own_expr = true;

  return EvalCtx_Eval(r);
}

///////////////////////////////////////////////////////////////////////////////////////////////

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
  pc->eval.srcrow = SearchResult_GetRowData(r);

  // TODO: Set this once only
  pc->eval.err = pc->base.parent->err;

  if (!pc->val) {
    pc->val = RSValue_NewUndefined();
  }

  rc = ExprEval_Eval(&pc->eval, pc->val);
  if (rc != EXPR_EVAL_OK) {
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
  RLookup_WriteOwnKey(pc->outkey, SearchResult_GetRowDataMut(r), pc->val);
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

    // Reduce the total number of results
    RS_ASSERT(rp->parent->totalResults > 0);
    rp->parent->totalResults--;
    // Otherwise, the result must be filtered out.
    SearchResult_Clear(r);
  }
  return rc;
}

static void rpevalFree(ResultProcessor *rp) {
  RPEvaluator *ee = (RPEvaluator *)rp;
  if (ee->val) {
    RSValue_DecrRef(ee->val);
  }
  BlkAlloc_FreeAll(&ee->eval.stralloc, NULL, NULL, 0);
  rm_free(ee);
}
static ResultProcessor *RPEvaluator_NewCommon(RSExpr *ast, const RLookup *lookup,
                                              const RLookupKey *dstkey, int isFilter) {
  RPEvaluator *rp = rm_calloc(1, sizeof(*rp));
  rp->base.Next = isFilter ? rpevalNext_filter : rpevalNext_project;
  rp->base.Free = rpevalFree;
  rp->base.type = isFilter ? RP_FILTER : RP_PROJECTOR;
  rp->eval.lookup = lookup;
  rp->eval.root = ast;
  rp->outkey = dstkey;
  BlkAlloc_Init(&rp->eval.stralloc);
  return &rp->base;
}

ResultProcessor *RPEvaluator_NewProjector(RSExpr *ast, const RLookup *lookup,
                                          const RLookupKey *dstkey) {
  return RPEvaluator_NewCommon(ast, lookup, dstkey, 0);
}

ResultProcessor *RPEvaluator_NewFilter(RSExpr *ast, const RLookup *lookup) {
  return RPEvaluator_NewCommon(ast, lookup, NULL, 1);
}

void RPEvaluator_Reply(RedisModule_Reply *reply, const char *title, const ResultProcessor *rp) {
  if (title) {
    RedisModule_Reply_SimpleString(reply, title);
  }

  ResultProcessorType type = rp->type;
  const char *typeStr = RPTypeToString(rp->type);
  RS_LOG_ASSERT (type == RP_PROJECTOR || type == RP_FILTER, "Error");

  char buf[32];
  size_t len;
  const char *literal;
  RPEvaluator *rpEval = (RPEvaluator *)rp;
  const RSExpr *expr = rpEval->eval.root;
  switch (expr->t) {
    case RSExpr_Literal:
      literal = RSValue_ConvertStringPtrLen(expr->literal, &len, buf, sizeof(buf));
      RedisModule_Reply_SimpleStringf(reply, "%s - Literal %s", typeStr, literal);
      break;
    case RSExpr_Property:
      RedisModule_Reply_SimpleStringf(reply, "%s - Property %s", typeStr, expr->property.key);
      break;
    case RSExpr_Op:
      RedisModule_Reply_SimpleStringf(reply, "%s - Operator %c", typeStr, expr->op.op);
      break;
    case RSExpr_Function:
      RedisModule_Reply_SimpleStringf(reply, "%s - Function %s", typeStr, expr->func.name);
      break;
    case RSExpr_Predicate:
      RedisModule_Reply_SimpleStringf(reply, "%s - Predicate %s", typeStr, getRSConditionStrings(expr->pred.cond));
      break;
    case RSExpr_Inverted:
      RedisModule_Reply_SimpleStringf(reply, "%s - Inverted", typeStr);
      break;
    default:
      RS_ABORT("error");
      break;
  }
}
