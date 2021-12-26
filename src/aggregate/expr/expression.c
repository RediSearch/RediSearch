
#include "expression.h"
#include "result_processor.h"

int func_exists(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc, QueryError *err);

///////////////////////////////////////////////////////////////////////////////////////////////

static void setReferenceValue(RSValue *dst, RSValue *src) {
  RSValue_MakeReference(dst, src);
}

//---------------------------------------------------------------------------------------------

int ExprEval::evalFunc(const RSFunctionExpr *f, RSValue *result) {
  int rc = EXPR_EVAL_ERR;

  // First, evaluate every argument
  size_t nusedargs = 0;
  size_t nargs = f->args->len;
  RSValue *argspp[nargs];
  RSValue args[nargs];

  for (size_t ii = 0; ii < nargs; ii++) {
    args[ii] = RSValue();
    argspp[ii] = &args[ii];
    int internalRes = evalInternal(f->args->args[ii], &args[ii]);
    if (internalRes == EXPR_EVAL_ERR ||
        (internalRes == EXPR_EVAL_NULL && f->Call != func_exists)) {
      // TODO: Free other results
      goto cleanup;
    }
    nusedargs++;
  }

  // We pass an RSValue**, not an RSValue*, as the arguments
  rc = f->Call(this, result, argspp, nargs, err);

cleanup:
  for (size_t ii = 0; ii < nusedargs; ii++) {
    args[ii].Clear();
  }
  return rc;
}

//---------------------------------------------------------------------------------------------

int ExprEval::evalOp(const RSExprOp *op, RSValue *result) {
  RSValue l, r;
  int rc = EXPR_EVAL_ERR;

  if (evalInternal(op->left, &l) != EXPR_EVAL_OK) {
    goto cleanup;
  }
  if (evalInternal(op->right, &r) != EXPR_EVAL_OK) {
    goto cleanup;
  }

  double n1, n2;
  if (!RSValue_ToNumber(&l, &n1) || !RSValue_ToNumber(&r, &n2)) {

    err->SetError(QUERY_ENOTNUMERIC, NULL);
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
  l.Clear();
  r.Clear();
  return rc;
}

//---------------------------------------------------------------------------------------------

int ExprEval::getPredicateBoolean(const RSValue *l, const RSValue *r, RSCondition op) {
  switch (op) {
    case RSCondition_Eq:
      return RSValue_Equal(l, r, err);

    case RSCondition_Lt:
      return RSValue_Cmp(l, r, err) < 0;

    // Less than or equal, <=
    case RSCondition_Le:
      return RSValue_Cmp(l, r, err) <= 0;

    // Greater than, >
    case RSCondition_Gt:
      return RSValue_Cmp(l, r, err) > 0;

    // Greater than or equal, >=
    case RSCondition_Ge:
      return RSValue_Cmp(l, r, err) >= 0;

    // Not equal, !=
    case RSCondition_Ne:
      return !RSValue_Equal(l, r, err);

    // Logical AND of 2 expressions, &&
    case RSCondition_And:
      return l->BoolTest() && r->BoolTest();

    // Logical OR of 2 expressions, ||
    case RSCondition_Or:
      return l->BoolTest() || r->BoolTest();

    default:
      RS_LOG_ASSERT(0, "invalid RSCondition");
      return 0;
  }
}

//---------------------------------------------------------------------------------------------

int ExprEval::evalInverted(const RSInverted *vv, RSValue *result) {
  RSValue tmpval;
  if (evalInternal(vv->child, &tmpval) != EXPR_EVAL_OK) {
    return EXPR_EVAL_ERR;
  }

  result->numval = !tmpval.BoolTest();
  result->t = RSValue_Number;

  tmpval.Clear();
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

int ExprEval::evalPredicate(const RSPredicate *pred, RSValue *result) {
  int res;
  RSValue l, r;
  int rc = EXPR_EVAL_ERR;
  if (evalInternal(pred->left, &l) != EXPR_EVAL_OK) {
    goto cleanup;
  } else if (pred->cond == RSCondition_Or && l.BoolTest()) {
    res = 1;
    goto success;
  } else if (pred->cond == RSCondition_And && !l.BoolTest()) {
    res = 0;
    goto success;
  } else if (evalInternal(pred->right, &r) != EXPR_EVAL_OK) {
    goto cleanup;
  }

  res = getPredicateBoolean(&l, &r, pred->cond);

success:
  if (!err || err->code == QUERY_OK) {
    result->numval = res;
    result->t = RSValue_Number;
    rc = EXPR_EVAL_OK;
  } else {
    result->t = RSValue_Undef;
  }

cleanup:
  l.Clear();
  r.Clear();
  return rc;
}

//---------------------------------------------------------------------------------------------

int ExprEval::evalProperty(const RSLookupExpr *e, RSValue *res) {
  if (!e->lookupKey) {
    // todo : this can not happened
    // No lookup object. This means that the key does not exist
    // Note: Because this is evaluated for each row potentially, do not assume
    // that query error is present:
    if (err) {
      err->SetError(QUERY_ENOPROPKEY, NULL);
    }
    return EXPR_EVAL_ERR;
  }

  // Find the actual value
  RSValue *value = srcrow->GetItem(e->lookupKey);
  if (!value) {
    if (err) {
      err->SetError(QUERY_ENOPROPVAL, NULL);
    }
    res->t = RSValue_Null;
    return EXPR_EVAL_NULL;
  }

  setReferenceValue(res, value);
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

int ExprEval::evalInternal(const RSExpr *e, RSValue *res) {
  res->Clear();
  switch (e->t) {
    case RSExpr_Property:
      return evalProperty(&e->property, res);
    case RSExpr_Literal:
      RSValue_MakeReference(res, (RSValue *)&e->literal);
      return EXPR_EVAL_OK;
    case RSExpr_Function:
      return evalFunc(&e->func, res);
    case RSExpr_Op:
      return evalOp(&e->op, res);
    case RSExpr_Predicate:
      return evalPredicate(&e->pred, res);
    case RSExpr_Inverted:
      return evalInverted(&e->inverted, res);
  }
  return EXPR_EVAL_ERR;  // todo: this can not happened
}

//---------------------------------------------------------------------------------------------

int ExprEval::Eval(RSValue *result) {
  return evalInternal(root, result);
}

//---------------------------------------------------------------------------------------------

/**
 * Scan through the expression and generate any required lookups for the keys.
 * @param root Root iterator for scan start
 * @param lookup The lookup registry which will store the keys
 * @param err If this fails, EXPR_EVAL_ERR is returned, and this variable contains
 *  the error.
 */

int ExprAST_GetLookupKeys(RSExpr *expr, RLookup *lookup, QueryError *err) {

#define RECURSE(v)                                                                    \
  do {                                                                                \
    if (!v) {                                                                         \
      err->SetErrorFmt(QUERY_EEXPR, "Missing (or badly formatted) value for %s", #v); \
      return EXPR_EVAL_ERR;                                                           \
    }                                                                                 \
    if (ExprAST_GetLookupKeys(v, lookup, err) != EXPR_EVAL_OK) {                      \
      return EXPR_EVAL_ERR;                                                           \
    }                                                                                 \
  } while (0)

  switch (expr->t) {
    case RSExpr_Property:
      expr->property.lookupKey = lookup->GetKey(expr->property.key, RLOOKUP_F_NOINCREF);
      if (!expr->property.lookupKey) {
        err->SetErrorFmt(QUERY_ENOPROPKEY, "Property `%s` not loaded in pipeline", expr->property.key);
        return EXPR_EVAL_ERR;
      }
      break;
    case RSExpr_Function:
      for (size_t ii = 0; ii < expr->func.args->len; ii++) {
        RECURSE(expr->func.args->args[ii]);
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

//---------------------------------------------------------------------------------------------

// Allocate some memory for a function that can be freed automatically when the execution is done

void *ExprEval::UnalignedAlloc(size_t sz) {
  return stralloc.Alloc(sz, MAX(sz, 1024));
}

//---------------------------------------------------------------------------------------------

char *ExprEval::Strndup(const char *str, size_t len) {
  char *ret = UnalignedAlloc(len + 1);
  memcpy(ret, str, len);
  ret[len] = '\0';
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////

#define RESULT_EVAL_ERR (RS_RESULT_MAX + 1)

//---------------------------------------------------------------------------------------------

int RPEvaluator::Next(SearchResult *r) {
  // Get the upstream result
  int rc = upstream->Next(r);
  if (rc != RS_RESULT_OK) {
    return rc;
  }

  eval.res = r;
  eval.srcrow = &r->rowdata;

  // TODO: Set this once only
  eval.err = parent->err;

  if (!val) {
    val = RS_NewValue(RSValue_Undef);
  }

  rc = eval.Eval(val);
  return rc == EXPR_EVAL_OK ? RS_RESULT_OK : RS_RESULT_ERROR;
}

//---------------------------------------------------------------------------------------------

int RPProjector::Next(SearchResult *r) {
  int rc = RPEvaluator::Next(r);
  if (rc != RS_RESULT_OK) {
    return rc;
  }
  r->rowdata.WriteOwnKey(outkey, val);
  val = NULL;
  return RS_RESULT_OK;
}

//---------------------------------------------------------------------------------------------

int RPFilter::Next(SearchResult *r) {
  int rc;
  while ((rc = RPEvaluator::Next(r)) == RS_RESULT_OK) {
    // Check if it's a boolean result!
    int boolrv = val->BoolTest();
    val->Clear();

    if (boolrv) {
      return RS_RESULT_OK;
    }

    // Otherwise, the result must be filtered out.
    r->Clear();
  }
  return rc;
}

//---------------------------------------------------------------------------------------------

RPEvaluator::~RPEvaluator() {
  if (val) {
    val->Decref();
  }
}

//---------------------------------------------------------------------------------------------

RPEvaluator::RPEvaluator(const char *name, const RSExpr *ast, const RLookup *lookup, 
    const RLookupKey *dstkey) : ResultProcessor(name) {
  eval.lookup = lookup;
  eval.root = ast;
  outkey = dstkey;
}

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a new result processor in the form of a projector. The projector will
 * execute the expression in `ast` and write the result of that expression to the
 * appropriate place.
 * 
 * @param ast the parsed expression
 * @param lookup the lookup registry that contains the keys to search for
 * @param dstkey the target key (in lookup) to store the result.
 * 
 * @note The ast needs to be paired with the appropriate RLookupKey objects. This
 * can be done by calling EXPR_GetLookupKeys()
 */

RPProjector::RPProjector(const RSExpr *ast, const RLookup *lookup, const RLookupKey *dstkey) :
    RPEvaluator("Projector", ast, lookup, dstkey) {
}

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a new result processor in the form of a filter. The filter will
 * execute the expression in `ast` on each upstream result. If the expression
 * evaluates to false, the result will not be propagated to the next processor.
 * 
 * @param ast the parsed expression
 * @param lookup lookup used to find the key for the value
 * 
 * See notes for RPProjector.
 */

RPFilter::RPFilter(const RSExpr *ast, const RLookup *lookup) :
    RPEvaluator("Filter", ast, lookup, NULL) {
}

///////////////////////////////////////////////////////////////////////////////////////////////
