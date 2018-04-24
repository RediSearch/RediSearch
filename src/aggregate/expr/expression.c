#include <ctype.h>
#include "expression.h"
#include "result_processor.h"

#define arglist_sizeof(l) (sizeof(RSArgList) + ((l) * sizeof(RSExpr *)))

RSArgList *RS_NewArgList(RSExpr *e) {
  RSArgList *ret = malloc(arglist_sizeof(e ? 1 : 0));
  ret->len = e ? 1 : 0;
  if (e) ret->args[0] = e;
  return ret;
}

RSArgList *RSArgList_Append(RSArgList *l, RSExpr *e) {
  l = realloc(l, arglist_sizeof(l->len + 1));
  l->args[l->len++] = e;
  return l;
}

static RSExpr *newExpr(RSExprType t) {
  RSExpr *e = malloc(sizeof(*e));
  e->t = t;
  return e;
}

// unquote and unescape a stirng literal, and return a cleaned copy of it
char *unescpeStringDup(const char *s, size_t sz) {

  char *dst = malloc(sz);
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

RSExpr *RS_NewFunc(const char *str, size_t len, RSArgList *args, RSFunction cb) {
  RSExpr *e = newExpr(RSExpr_Function);
  e->func.args = args;
  e->func.name = strndup(str, len);
  e->func.Call = cb;
  return e;
}

RSExpr *RS_NewProp(const char *str, size_t len) {
  RSExpr *e = newExpr(RSExpr_Property);
  e->property.key = strndup(str, len);
  e->property.sortableIdx = RSKEY_UNCACHED;
  e->property.fieldIdx = RSKEY_UNCACHED;
  return e;
}
void RSArgList_Free(RSArgList *l) {
  if (!l) return;
  for (size_t i = 0; i < l->len; i++) {
    RSExpr_Free(l->args[i]);
  }
  free(l);
}
void RSExpr_Free(RSExpr *e) {
  switch (e->t) {
    case RSExpr_Literal:
      RSValue_Free(&e->literal);
      break;
    case RSExpr_Function:
      free((char *)e->func.name);
      RSArgList_Free(e->func.args);
      break;
    case RSExpr_Op:
      RSExpr_Free(e->op.left);
      RSExpr_Free(e->op.right);
      break;
    case RSExpr_Property:
      free((char *)e->property.key);
  }
  free(e);
}

void RSExpr_Print(RSExpr *e) {
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

static inline int evalProperty(RSExprEvalCtx *ctx, RSKey *k, RSValue *result, char **err) {
  RSValue_MakeReference(result, SearchResult_GetValue(ctx->r, ctx->sortables, k));
  return EXPR_EVAL_OK;
}

int RSExpr_Eval(RSExprEvalCtx *ctx, RSExpr *e, RSValue *result, char **err) {
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
  }
  return EXPR_EVAL_ERR;
}

/* Get the return type of an expression. In the case of a property we do not try to guess but rather
 * just return String */
RSValueType GetExprType(RSExpr *expr, RSSortingTable *tbl) {
  if (!expr) return RSValue_Null;
  switch (expr->t) {
    case RSExpr_Function:
      return RSFunctionRegistry_GetType(expr->func.name, strlen(expr->func.name));
      break;
    case RSExpr_Op:
      return RSValue_Number;
    case RSExpr_Literal:
      return expr->literal.t;
    case RSExpr_Property: {
      // best effort based on sorting table, default to string
      // safe if tbl is null
      return SortingTable_GetFieldType(tbl, RSKEY(expr->property.key), RSValue_String);
    }
  }
}