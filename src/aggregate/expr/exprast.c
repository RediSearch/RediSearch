/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "exprast.h"
#include <ctype.h>
#include "obfuscation/obfuscation_api.h"

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
  RSExpr *e = rm_calloc(1, sizeof(*e));
  e->t = t;
  return e;
}

// unquote and unescape a stirng literal, and return a cleaned copy of it
char *unescpeStringDup(const char *s, size_t sz) {
  if (!s || !sz) {
    return rm_strdup("");
  }
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
  e->pred.cond = cond;
  e->pred.left = left;
  e->pred.right = right;
  // e->pred = (RSPredicate){
  //     .cond = cond,
  //     .left = left,
  //     .right = right,
  // };
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
  e->property.lookupObj = NULL;
  return e;
}

RSExpr *RS_NewInverted(RSExpr *child) {
  RSExpr *e = newExpr(RSExpr_Inverted);
  e->inverted.child = child;
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
      RSValue_Clear(&e->literal);
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
    case RSExpr_Inverted:
      RSExpr_Free(e->inverted.child);
  }
  rm_free(e);
}

// Extract all field names from an RSExpr tree recursively 
void RSExpr_GetProperties(RSExpr *e, char ***props) {
  if (!e) return;
  switch (e->t) {
    case RSExpr_Property:
      *props = array_append(*props, rm_strdup(e->property.key));
      break;
    case RSExpr_Literal:
      break;
    case RSExpr_Function:
      for (size_t ii = 0; ii < e->func.args->len; ii++) {
        RSExpr_GetProperties(e->func.args->args[ii], props);
      }
      break;
    case RSExpr_Op:
      RSExpr_GetProperties(e->op.left, props);
      RSExpr_GetProperties(e->op.right, props);
      break;
    case RSExpr_Predicate:
      RSExpr_GetProperties(e->pred.left, props);
      RSExpr_GetProperties(e->pred.right, props);
      break;
    case RSExpr_Inverted:
      RSExpr_GetProperties(e->inverted.child, props);
  }
}

sds RSExpr_DumpSds(const RSExpr *e, sds s, bool obfuscate) {
  if (!e) {
    return sdscat(s, "NULL");
  }
  switch (e->t) {
    case RSExpr_Literal:
      s = RSValue_DumpSds(&e->literal, s, obfuscate);
      break;
    case RSExpr_Function:
      s = sdscatfmt(s, "%s(", e->func.name);
      for (size_t i = 0; e->func.args != NULL && i < e->func.args->len; i++) {
        s = RSExpr_DumpSds(e->func.args->args[i], s, obfuscate);
        if (i < e->func.args->len - 1) s = sdscat(s, ", ");
      }
      s = sdscat(s, ")");
      break;
    case RSExpr_Op:
      s = sdscat(s, "(");
      s = RSExpr_DumpSds(e->op.left, s, obfuscate);
      const char buffer[2] = {e->op.op, 0};
      s = sdscatfmt(s, " %s ", buffer);
      s = RSExpr_DumpSds(e->op.right, s, obfuscate);
      s = sdscat(s, ")");
      break;

    case RSExpr_Predicate:
      s = sdscat(s, "(");
      s = RSExpr_DumpSds(e->pred.left, s, obfuscate);
      s = sdscatfmt(s, " %s ", getRSConditionStrings(e->pred.cond));
      s = RSExpr_DumpSds(e->pred.right, s, obfuscate);
      s = sdscat(s, ")");
      break;
    case RSExpr_Property:
      if (obfuscate) {
        s = sdscatfmt(s, "@%s", Obfuscate_Text(e->property.key));
      } else {
        s = sdscatfmt(s, "@%s", e->property.key);
      }
      break;
    case RSExpr_Inverted:
      s = sdscat(s, "!");
      s = RSExpr_DumpSds(e->inverted.child, s, obfuscate);
      break;
  }
  return s;
}

void ExprAST_Free(RSExpr *e) {
  RSExpr_Free(e);
}

char *ExprAST_Dump(const RSExpr *e, bool obfuscate) {
  sds s = sdsempty();
  s = RSExpr_DumpSds(e, s, obfuscate);
  char *ret = rm_strdup(s);
  sdsfree(s);
  return ret;
}

RSExpr *ExprAST_Parse(const char *e, size_t n, QueryError *status) {
  char *errtmp = NULL;
  RS_LOG_ASSERT(!QueryError_HasError(status), "Query has error")

  RSExpr *ret = RSExpr_Parse(e, n, &errtmp);
  if (!ret) {
    QueryError_SetError(status, QUERY_EEXPR, errtmp);
  }
  rm_free(errtmp);
  return ret;
}
