#include <ctype.h>
#include "expression.h"
#include "result_processor.h"
#include "util/arr.h"

#define arglist_sizeof(l) (sizeof(RSArgList) + ((l) * sizeof(RSExpr *)))

RSArgList *RS_NewArgList(RSExpr *e) {
  RSArgList *ret = rm_malloc(arglist_sizeof(e ? 1 : 0));
  ret->len = e ? 1 : 0;
  if (e) ret->args[0] = e;
  return ret;
}

RSArgList *RSArgList_Append(RSArgList *l, RSExpr *e) {
  l = rm_realloc(l, arglist_sizeof(l->len + 1));
  l->args[l->len++] = e;
  return l;
}

static RSExpr *newExpr(RSExprType t) {
  RSExpr *e = rm_malloc(sizeof(*e));
  e->t = t;
  return e;
}

// unquote and unescape a stirng literal, and return a cleaned copy of it
char *unescpeStringDup(const char *s, size_t sz) {

  char *dst = rm_malloc(sz);
  char *dstStart = dst;
  char *src = (char *)s + 1;       // we start after the first quote
  char *end = (char *)s + sz - 1;  // we end at the last quote
  while (src < end) {
    // unescape
    if (*src == '\\' && src + 1 < end && (ispunct(*(src + 1)) || isspace(*(src + 1)))) {
      ++src;
      continue;
    }
    *dst++ = *src++;
  }
  *dst = '\0';
  return dstStart;
}
RSExpr *RS_NewStringLiteral(const char *str, size_t len) {
  RSExpr *e = newExpr(RSExpr_Literal);
  e->literal = RS_StaticValue(RSValue_String);
  e->literal.strval.str = unescpeStringDup(str, len);
  e->literal.strval.len = strlen(e->literal.strval.str);
  e->literal.strval.stype = RSString_Malloc;
  return e;
}

RSExpr *RS_NewNullLiteral() {
  RSExpr *e = newExpr(RSExpr_Literal);
  RSValue_MakeReference(&e->literal, RS_NullVal());
  return e;
}

RSExpr *RS_NewNumberLiteral(double n) {
  RSExpr *e = newExpr(RSExpr_Literal);

  e->literal = RS_StaticValue(RSValue_Number);
  e->literal.numval = n;
  return e;
}

RSExpr *RS_NewOp(unsigned char op, RSExpr *left, RSExpr *right) {
  RSExpr *e = newExpr(RSExpr_Op);
  e->op.op = op;
  e->op.left = left;
  e->op.right = right;
  return e;
}

RSExpr *RS_NewPredicate(RSCondition cond, RSExpr *left, RSExpr *right) {
  RSExpr *e = newExpr(RSExpr_Predicate);
  e->pred = (RSPredicate){
      .cond = cond,
      .left = left,
      .right = right,
  };
  return e;
}

RSExpr *RS_NewFunc(const char *str, size_t len, RSArgList *args, RSFunction cb) {
  RSExpr *e = newExpr(RSExpr_Function);
  e->func.args = args;
  e->func.name = rm_strndup(str, len);
  e->func.Call = cb;
  return e;
}

RSExpr *RS_NewProp(const char *str, size_t len) {
  RSExpr *e = newExpr(RSExpr_Property);
  e->property.key = rm_strndup(str, len);
  e->property.sortableIdx = RSKEY_UNCACHED;
  e->property.fieldIdx = RSKEY_UNCACHED;
  return e;
}
void RSArgList_Free(RSArgList *l) {
  if (!l) return;
  for (size_t i = 0; i < l->len; i++) {
    RSExpr_Free(l->args[i]);
  }
  rm_free(l);
}
void RSExpr_Free(RSExpr *e) {
  if (!e) return;
  switch (e->t) {
    case RSExpr_Literal:
      RSValue_Free(&e->literal);
      break;
    case RSExpr_Function:
      rm_free((char *)e->func.name);
      RSArgList_Free(e->func.args);
      break;
    case RSExpr_Op:
      RSExpr_Free(e->op.left);
      RSExpr_Free(e->op.right);
      break;
    case RSExpr_Predicate:
      RSExpr_Free(e->pred.left);
      RSExpr_Free(e->pred.right);
      break;
    case RSExpr_Property:
      rm_free((char *)e->property.key);
      break;
  }
  rm_free(e);
}

void RSExpr_Print(RSExpr *e) {
  if (!e) {
    printf("NULL");
    return;
  }
  switch (e->t) {
    case RSExpr_Literal:
      RSValue_Print(&e->literal);
      break;
    case RSExpr_Function:
      printf("%s(", e->func.name);
      for (size_t i = 0; e->func.args != NULL && i < e->func.args->len; i++) {
        RSExpr_Print(e->func.args->args[i]);
        if (i < e->func.args->len - 1) printf(", ");
      }
      printf(")");
      break;
    case RSExpr_Op:
      printf("(");
      RSExpr_Print(e->op.left);
      printf(" %c ", e->op.op);
      RSExpr_Print(e->op.right);
      printf(")");
      break;

    case RSExpr_Predicate:
      // NOT of a single predicate
      if (e->pred.cond == RSCondition_Not) {
        printf("!");
        RSExpr_Print(e->pred.left);
        return;
      }
      printf("(");
      RSExpr_Print(e->pred.left);
      printf(" %s ", RSConditionStrings[e->pred.cond]);
      RSExpr_Print(e->pred.right);
      printf(")");

      break;
    case RSExpr_Property:
      printf("@%s", e->property.key);
  }
}

static inline int evalFunc(RSExprEvalCtx *ctx, RSFunctionExpr *f, RSValue *result, char **err) {

  RSValue args[f->args->len];
  for (size_t i = 0; i < f->args->len; i++) {
    args[i] = RSVALUE_STATIC;
    if (RSExpr_Eval(ctx, f->args->args[i], &args[i], err) == EXPR_EVAL_ERR) {
      // TODO: Free other results
      return EXPR_EVAL_ERR;
    }
  }

  int rc = f->Call(ctx->fctx, result, args, f->args->len, err);
  for (size_t i = 0; i < f->args->len; i++) {
    RSValue_Free(&args[i]);
  }
  return rc;
}

static int evalOp(RSExprEvalCtx *ctx, RSExprOp *op, RSValue *result, char **err) {

  RSValue l = RSVALUE_STATIC, r = RSVALUE_STATIC;
  if (RSExpr_Eval(ctx, op->left, &l, err) == EXPR_EVAL_ERR) {
    return EXPR_EVAL_ERR;
  }
  if (RSExpr_Eval(ctx, op->right, &r, err) == EXPR_EVAL_ERR) {
    return EXPR_EVAL_ERR;
  }

  double n1, n2;
  int rc = EXPR_EVAL_OK;
  if (!RSValue_ToNumber(&l, &n1) || !RSValue_ToNumber(&r, &n2)) {

    // asprintf(err, "Invalid values for op '%c'", op->op);
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
      res = NAN;
  }

  result->numval = res;
  result->t = RSValue_Number;

cleanup:
  RSValue_Free(&l);
  RSValue_Free(&r);
  return rc;
}

static int evalPredicate(RSExprEvalCtx *ctx, RSPredicate *pred, RSValue *result, char **err) {
  int res;
  RSValue l = RSVALUE_STATIC, r = RSVALUE_STATIC;
  if (RSExpr_Eval(ctx, pred->left, &l, err) == EXPR_EVAL_ERR) {
    return EXPR_EVAL_ERR;
  }
  if (pred->cond == RSCondition_Or && RSValue_BoolTest(&l)) {
    res = 1;
    goto success;
  }
  if (pred->cond == RSCondition_And && !RSValue_BoolTest(&l)) {
    res = 0;
    goto success;
  }
  if (pred->right && RSExpr_Eval(ctx, pred->right, &r, err) == EXPR_EVAL_ERR) {
    return EXPR_EVAL_ERR;
  }

  RSValue *l_ptr = RSValue_Dereference(&l);
  RSValue *r_ptr = RSValue_Dereference(&r);

  if (l_ptr->t == RSValue_Null || r_ptr->t == RSValue_Null) {
    // NULL are not comparable
    res = 0;
  } else
    switch (pred->cond) {
      case RSCondition_Eq:
        res = RSValue_Equal(&l, &r);
        break;
      case RSCondition_Lt:
        res = RSValue_Cmp(&l, &r) < 0;
        break;
      /* Less than or equal, <= */
      case RSCondition_Le:
        res = RSValue_Cmp(&l, &r) <= 0;

        break;
        /* Greater than, > */
      case RSCondition_Gt:
        res = RSValue_Cmp(&l, &r) > 0;

        break;

      /* Greater than or equal, >= */
      case RSCondition_Ge:
        res = RSValue_Cmp(&l, &r) >= 0;

        break;

      /* Not equal, != */
      case RSCondition_Ne:
        res = !RSValue_Equal(&l, &r);
        break;
        /* Logical AND of 2 expressions, && */
      case RSCondition_And:
        res = RSValue_BoolTest(&l) && RSValue_BoolTest(&r);
        break;

      /* Logical OR of 2 expressions, || */
      case RSCondition_Or:
        res = RSValue_BoolTest(&l) || RSValue_BoolTest(&r);

        break;

      case RSCondition_Not:
        res = RSValue_BoolTest(&l) == 0;
        break;
    }

success:
  result->numval = res;
  result->t = RSValue_Number;

  RSValue_Free(&l);
  RSValue_Free(&r);
  return EXPR_EVAL_OK;
}
static inline int evalProperty(RSExprEvalCtx *ctx, RSKey *k, RSValue *result, char **err) {
  RSValue_MakeReference(result, SearchResult_GetValue(ctx->r, ctx->sortables, k));
  return EXPR_EVAL_OK;
}

int RSExpr_Eval(RSExprEvalCtx *ctx, RSExpr *e, RSValue *result, char **err) {
  if (!e) {
    return EXPR_EVAL_ERR;
  }
  switch (e->t) {
    case RSExpr_Property:
      return evalProperty(ctx, &e->property, result, err);
    case RSExpr_Literal:
      RSValue_MakeReference(result, &e->literal);
      return EXPR_EVAL_OK;
    case RSExpr_Function:
      return evalFunc(ctx, &e->func, result, err);

    case RSExpr_Op:
      return evalOp(ctx, &e->op, result, err);
    case RSExpr_Predicate:
      return evalPredicate(ctx, &e->pred, result, err);
  }
  return EXPR_EVAL_ERR;
}

void expr_GetFieldsInternal(RSExpr *expr, const char ***arr) {
  if (!expr) {
    return;
  }
  switch (expr->t) {
    case RSExpr_Property:
      *arr = array_append(*arr, expr->property.key);
      break;
    case RSExpr_Function:
      for (size_t i = 0; i < expr->func.args->len; i++) {
        expr_GetFieldsInternal(expr->func.args->args[i], arr);
      }
      break;
    case RSExpr_Op:
      expr_GetFieldsInternal(expr->op.left, arr);
      expr_GetFieldsInternal(expr->op.right, arr);
      break;
    case RSExpr_Predicate:
      expr_GetFieldsInternal(expr->pred.left, arr);
      expr_GetFieldsInternal(expr->pred.right, arr);
      break;
    default:
      break;
  }
}

/* Return all the field names needed by the expression. Returns an array that should be freed with
 * array_free */
const char **Expr_GetRequiredFields(RSExpr *expr) {
  const char **ret = array_new(const char *, 2);
  expr_GetFieldsInternal(expr, &ret);
  return ret;
}

/* Get the return type of an expression. In the case of a property we do not try to guess but
 * rather just return String */
RSValueType GetExprType(RSExpr *expr, RSSortingTable *tbl) {
  if (!expr) return RSValue_Null;
  switch (expr->t) {
    case RSExpr_Function:
      return RSFunctionRegistry_GetType(expr->func.name, strlen(expr->func.name));
    case RSExpr_Op:
      return RSValue_Number;
    case RSExpr_Predicate:
      return RSValue_Number;
    case RSExpr_Literal:
      return expr->literal.t;
    case RSExpr_Property: {
      // best effort based on sorting table, default to string
      // safe if tbl is null
      return SortingTable_GetFieldType(tbl, RSKEY(expr->property.key), RSValue_String);
    default:
      assert(0);
    }
  }
}
