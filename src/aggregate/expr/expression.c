
#include "expression.h"
#include "result_processor.h"

int func_exists(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc, QueryError *err);

///////////////////////////////////////////////////////////////////////////////////////////////

int RSFunctionExpr::Eval(ExprEval &eval, RSValue *res) {
  res->Clear();
  int rc = EXPR_EVAL_ERR;

  // First, evaluate every argument
  size_t nusedargs = 0;
  size_t nargs = _args->length();
  RSValue *argspp[nargs];
  RSValue args[nargs];

  for (size_t ii = 0; ii < nargs; ii++) {
    args[ii] = RSValue();
    argspp[ii] = &args[ii];
    int internalRes = _args->args[ii]->Eval(eval, &args[ii]);
    if (internalRes == EXPR_EVAL_ERR ||
        (internalRes == EXPR_EVAL_NULL && Call != func_exists)) {
      // TODO: Free other results
      goto cleanup;
    }
    nusedargs++;
  }

  // We pass an RSValue**, not an RSValue*, as the arguments
  rc = Call(&eval, res, argspp, nargs, eval.err);

cleanup:
  for (size_t ii = 0; ii < nusedargs; ii++) {
    args[ii].Clear();
  }
  return rc;
}

int RSFunctionExpr::GetLookupKeys(RLookup *lookup, QueryError *err) {
  for (size_t ii = 0; ii < _args->length(); ii++) {
    auto rc = _args->args[ii]->GetLookupKeys(lookup, err);
    if (rc != EXPR_EVAL_OK) return rc;
  }
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

int RSExprOp::Eval(ExprEval &eval, RSValue *result) {
  result->Clear();

  RSValue l, r;
  int rc = EXPR_EVAL_ERR;

  if (eval.eval(left, &l) != EXPR_EVAL_OK) {
    goto cleanup;
  }
  if (eval.eval(right, &r) != EXPR_EVAL_OK) {
    goto cleanup;
  }

  double n1, n2;
  if (!l.ToNumber(&n1) || !r.ToNumber(&n2)) {
    eval.err->SetError(QUERY_ENOTNUMERIC, NULL);
    rc = EXPR_EVAL_ERR;
    goto cleanup;
  }

  double res;
  switch (op) {
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

int RSExprOp::GetLookupKeys(RLookup *lookup, QueryError *err) {
  auto rc = left->GetLookupKeys(lookup, err);
  if (rc != EXPR_EVAL_OK) return rc;
  return right->GetLookupKeys(lookup, err);
}

//---------------------------------------------------------------------------------------------

int ExprEval::getPredicateBoolean(const RSValue *l, const RSValue *r, RSCondition op) {
  switch (op) {
    case RSCondition_Eq:
      return RSValue::Equal(l, r, err);

    case RSCondition_Lt:
      return RSValue::Cmp(l, r, err) < 0;

    // Less than or equal, <=
    case RSCondition_Le:
      return RSValue::Cmp(l, r, err) <= 0;

    // Greater than, >
    case RSCondition_Gt:
      return RSValue::Cmp(l, r, err) > 0;

    // Greater than or equal, >=
    case RSCondition_Ge:
      return RSValue::Cmp(l, r, err) >= 0;

    // Not equal, !=
    case RSCondition_Ne:
      return !RSValue::Equal(l, r, err);

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

int RSInverted::Eval(ExprEval &eval, RSValue *result) {
  result->Clear();
  RSValue tmpval;
  if (child->Eval(eval, &tmpval) != EXPR_EVAL_OK) {
    return EXPR_EVAL_ERR;
  }

  result->numval = !tmpval.BoolTest();
  result->t = RSValue_Number;

  tmpval.Clear();
  return EXPR_EVAL_OK;
}

int RSInverted::GetLookupKeys(RLookup *lookup, QueryError *err) {
  return child->GetLookupKeys(lookup, err);
}

//---------------------------------------------------------------------------------------------

int RSPredicate::Eval(ExprEval &eval, RSValue *result) {
  result->Clear();
  int res;
  RSValue l, r;
  int rc = EXPR_EVAL_ERR;
  if (left->Eval(eval, &l) != EXPR_EVAL_OK) {
    goto cleanup;
  } else if (cond == RSCondition_Or && l.BoolTest()) {
    res = 1;
    goto success;
  } else if (cond == RSCondition_And && !l.BoolTest()) {
    res = 0;
    goto success;
  } else if (right->Eval(eval, &r) != EXPR_EVAL_OK) {
    goto cleanup;
  }

  res = eval.getPredicateBoolean(&l, &r, cond);

success:
  if (!eval.err || eval.err->code == QUERY_OK) {
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

int RSPredicate::GetLookupKeys(RLookup *lookup, QueryError *err) {
  auto rc = left->GetLookupKeys(lookup, err);
  if (rc != EXPR_EVAL_OK) return rc;
  return right->GetLookupKeys(lookup, err);
}

//---------------------------------------------------------------------------------------------

int RSLookupExpr::Eval(ExprEval &eval, RSValue *res) {
  res->Clear();
  if (!eval.lookup) {
    // todo : this can not happened
    // No lookup object. This means that the key does not exist
    // Note: Because this is evaluated for each row potentially, do not assume
    // that query error is present:
    if (eval.err) {
      eval.err->SetError(QUERY_ENOPROPKEY, NULL);
    }
    return EXPR_EVAL_ERR;
  }

  // Find the actual value
  RSValue *value = eval.srcrow->GetItem(lookupKey);
  if (!value) {
    if (eval.err) {
      eval.err->SetError(QUERY_ENOPROPVAL, NULL);
    }
    res->t = RSValue_Null;
    return EXPR_EVAL_NULL;
  }

  res->MakeReference(value);
  return EXPR_EVAL_OK;
}

int RSLookupExpr::GetLookupKeys(RLookup *lookup, QueryError *err) {
  lookupKey = lookup->GetKey(key, RLOOKUP_F_NOINCREF);
  if (!lookupKey) {
    err->SetErrorFmt(QUERY_ENOPROPKEY, "Property `%s` not loaded in pipeline", key);
    return EXPR_EVAL_ERR;
  }
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

int RSLiteral::Eval(ExprEval &eval, RSValue *res) {
  res->Clear();
  res->MakeReference(&literal);
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

/**
 * Scan through the expression and generate any required lookups for the keys.
 * @param root Root iterator for scan start
 * @param lookup The lookup registry which will store the keys
 * @param err If this fails, EXPR_EVAL_ERR is returned, and this variable contains
 *  the error.
 */

int RSExpr::GetLookupKeys(RLookup *lookup, QueryError *err) {
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
