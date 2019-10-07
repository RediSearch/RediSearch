#include <util/minmax.h>
#include <rmutil/sds.h>
#include <util/block_alloc.h>
#include <aggregate/expr/expression.h>
#include <ctype.h>
#include <util/arr.h>
#include "function.h"
#include <err.h>

#define STRING_BLOCK_SIZE 512

static int func_matchedTerms(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc,
                             QueryError *err) {
  int maxTerms = 0;
  if (argc == 1) {
    double d;
    if (RSValue_ToNumber(argv[0], &d)) {
      if (d > 0) {
        maxTerms = (int)d;
      }
    }
  }

  if (maxTerms == 0) maxTerms = 100;
  maxTerms = MIN(100, maxTerms);
  const SearchResult *res = ctx->res;

  // fprintf(stderr, "res %p, indexresult %p\n", res, res ? res->indexResult : NULL);
  if (res && res->indexResult) {
    RSQueryTerm *terms[maxTerms];
    size_t n = IndexResult_GetMatchedTerms(ctx->res->indexResult, terms, maxTerms);
    if (n) {
      RSValue **arr = rm_calloc(n, sizeof(RSValue *));
      for (size_t i = 0; i < n; i++) {
        arr[i] = RS_ConstStringVal(terms[i]->str, terms[i]->len);
      }
      RSValue *v = RSValue_NewArrayEx(arr, n, RSVAL_ARRAY_ALLOC | RSVAL_ARRAY_NOINCREF);
      RSValue_MakeOwnReference(result, v);
      return EXPR_EVAL_OK;
    }
  }
  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

/* lower(str) */
static int stringfunc_tolower(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc,
                              QueryError *err) {

  VALIDATE_ARGS("lower", 1, 1, err);
  RSValue *val = RSValue_Dereference(argv[0]);
  if (!RSValue_IsString(val)) {
    RSValue_MakeReference(result, RS_NullVal());
    return EXPR_EVAL_OK;
  }

  size_t sz = 0;
  char *p = (char *)RSValue_StringPtrLen(val, &sz);
  char *np = ExprEval_UnalignedAlloc(ctx, sz + 1);
  for (size_t i = 0; i < sz; i++) {
    np[i] = tolower(p[i]);
  }
  np[sz] = '\0';
  RSValue_SetConstString(result, np, sz);
  return EXPR_EVAL_OK;
}

/* upper(str) */
static int stringfunc_toupper(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc,
                              QueryError *err) {
  VALIDATE_ARGS("upper", 1, 1, err);

  RSValue *val = RSValue_Dereference(argv[0]);
  if (!RSValue_IsString(val)) {
    RSValue_MakeReference(result, RS_NullVal());
    return EXPR_EVAL_OK;
  }

  size_t sz = 0;
  char *p = (char *)RSValue_StringPtrLen(val, &sz);
  char *np = ExprEval_UnalignedAlloc(ctx, sz + 1);
  for (size_t i = 0; i < sz; i++) {
    np[i] = toupper(p[i]);
  }
  np[sz] = '\0';
  RSValue_SetConstString(result, np, sz);
  return EXPR_EVAL_OK;
}

/* substr(str, offset, len) */
static int stringfunc_substr(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc,
                             QueryError *err) {
  VALIDATE_ARGS("substr", 3, 3, err);

  VALIDATE_ARG_TYPE("substr", argv, 1, RSValue_Number);
  VALIDATE_ARG_TYPE("substr", argv, 2, RSValue_Number);

  size_t sz;
  const char *str = RSValue_StringPtrLen(argv[0], &sz);
  if (!str) {
    QueryError_SetError(err, QUERY_EPARSEARGS, "Invalid type for substr. Expected string");
    return EXPR_EVAL_ERR;
  }

  int offset = (int)RSValue_Dereference(argv[1])->numval;
  int len = (int)RSValue_Dereference(argv[2])->numval;

  // for negative offsets we count from the end of the string
  if (offset < 0) {
    offset = (int)sz + offset;
  }
  offset = MAX(0, MIN(offset, sz));
  // len < 0 means read until the end of the string
  if (len < 0) {
    len = MAX(0, ((int)sz - offset) + len);
  }
  if (offset + len > sz) {
    len = sz - offset;
  }

  char *dup = ExprEval_Strndup(ctx, &str[offset], len);
  RSValue_SetConstString(result, dup, len);
  return EXPR_EVAL_OK;
}

int func_to_number(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc, QueryError *err) {
  VALIDATE_ARGS("to_number", 1, 1, err);

  double n;
  if (!RSValue_ToNumber(argv[0], &n)) {
    size_t sz = 0;
    const char *p = RSValue_StringPtrLen(argv[0], &sz);
    QueryError_SetErrorFmt(err, QUERY_EPARSEARGS, "to_number: cannot convert string '%s'", p);
    return EXPR_EVAL_ERR;
  }

  RSValue_SetNumber(result, n);
  return EXPR_EVAL_OK;
}

int func_to_str(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc, QueryError *err) {
  VALIDATE_ARGS("to_str", 1, 1, err);

  RSValue_ToString(result, argv[0]);
  return EXPR_EVAL_OK;
}

static int stringfunc_format(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc,
                             QueryError *err) {
  if (argc < 1) {
    QERR_MKBADARGS_FMT(err, "Need at least one argument for format");
    return EXPR_EVAL_ERR;
  }
  VALIDATE_ARG_ISSTRING("format", argv, 0);

  size_t argix = 1;
  size_t fmtsz = 0;
  const char *fmt = RSValue_StringPtrLen(argv[0], &fmtsz);
  const char *last = fmt, *end = fmt + fmtsz;
  sds out = sdsMakeRoomFor(sdsnew(""), fmtsz);

  for (size_t ii = 0; ii < fmtsz; ++ii) {
    if (fmt[ii] != '%') {
      continue;
    }

    if (ii == fmtsz - 1) {
      // ... %"
      QERR_MKBADARGS_FMT(err, "Bad format string!");
      goto error;
    }

    // Detected a format string. Write from 'last' up to 'fmt'
    out = sdscatlen(out, last, (fmt + ii) - last);
    last = fmt + ii + 2;

    char type = fmt[++ii];
    if (type == '%') {
      // Append literal '%'
      out = sdscat(out, "%");
      continue;
    }

    if (argix == argc) {
      QERR_MKBADARGS_FMT(err, "Not enough arguments for format");
      goto error;
    }

    RSValue *arg = RSValue_Dereference(argv[argix++]);
    if (type == 's') {
      if (arg->t == RSValue_Null) {
        // write null value
        out = sdscat(out, "(null)");
        continue;
      } else if (!RSValue_IsString(arg)) {

        RSValue strval = RSVALUE_STATIC;
        RSValue_ToString(&strval, arg);
        size_t sz;
        const char *str = RSValue_StringPtrLen(&strval, &sz);
        if (!str) {
          out = sdscat(out, "(null)");
        } else {
          out = sdscatlen(out, str, sz);
        }
        RSValue_Free(&strval);
      } else {
        size_t sz;
        const char *str = RSValue_StringPtrLen(arg, &sz);
        out = sdscatlen(out, str, sz);
      }
    } else {
      QERR_MKBADARGS_FMT(err, "Unknown format specifier passed");
      goto error;
    }
  }

  if (last && last < end) {
    out = sdscatlen(out, last, end - last);
  }

  RSValue_SetSDS(result, out);
  return EXPR_EVAL_OK;

error:
  assert(QueryError_HasError(err));
  sdsfree(out);
  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_ERR;
}

char *strtrim(char *s, size_t sl, size_t *outlen, const char *cset) {
  char *start, *end, *sp, *ep;

  sp = start = s;
  ep = end = s + sl - 1;
  while (sp <= end && strchr(cset, *sp)) sp++;
  while (ep > sp && strchr(cset, *ep)) ep--;
  *outlen = (sp > ep) ? 0 : ((ep - sp) + 1);

  return sp;
}
static int stringfunc_split(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc,
                            QueryError *err) {
  if (argc < 1 || argc > 3) {
    QERR_MKBADARGS_FMT(err, "Invalid number of arguments for split");
    return EXPR_EVAL_ERR;
  }
  VALIDATE_ARG_ISSTRING("format", argv, 0);
  const char *sep = ",";
  const char *strp = " ";
  if (argc >= 2) {
    VALIDATE_ARG_ISSTRING("format", argv, 1);
    sep = RSValue_StringPtrLen(argv[1], NULL);
  }
  if (argc == 3) {
    VALIDATE_ARG_ISSTRING("format", argv, 2);
    strp = RSValue_StringPtrLen(argv[2], NULL);
  }

  size_t len;
  char *str = (char *)RSValue_StringPtrLen(argv[0], &len);
  char *ep = str + len;
  size_t l = 0;
  char *next;
  char *tok = str;

  // extract at most 1024 values
  static RSValue *tmp[1024];
  while (l < 1024 && tok < ep) {
    next = strpbrk(tok, sep);
    size_t sl = next ? (next - tok) : ep - tok;

    if (sl > 0) {
      size_t outlen;
      // trim the strip set
      char *s = strtrim(tok, sl, &outlen, strp);
      if (outlen) {
        tmp[l++] = RS_NewCopiedString(s, outlen);
      }
    }

    // advance tok while it's not in the sep
    if (!next) break;

    tok = next + 1;
  }

  // if (len > 0) {
  //   tmp[l++] = RS_ConstStringVal(tok, len);
  // }

  RSValue **vals = rm_calloc(l, sizeof(*vals));
  for (size_t i = 0; i < l; i++) {
    vals[i] = tmp[i];
  }
  RSValue *ret = RSValue_NewArrayEx(vals, l, RSVAL_ARRAY_ALLOC | RSVAL_ARRAY_NOINCREF);
  RSValue_MakeOwnReference(result, ret);
  return EXPR_EVAL_OK;
}

void RegisterStringFunctions() {
  RSFunctionRegistry_RegisterFunction("lower", stringfunc_tolower, RSValue_String);
  RSFunctionRegistry_RegisterFunction("upper", stringfunc_toupper, RSValue_String);
  RSFunctionRegistry_RegisterFunction("substr", stringfunc_substr, RSValue_String);
  RSFunctionRegistry_RegisterFunction("format", stringfunc_format, RSValue_String);
  RSFunctionRegistry_RegisterFunction("split", stringfunc_split, RSValue_Array);
  RSFunctionRegistry_RegisterFunction("matched_terms", func_matchedTerms, RSValue_Array);
  RSFunctionRegistry_RegisterFunction("to_number", func_to_number, RSValue_Number);
  RSFunctionRegistry_RegisterFunction("to_str", func_to_str, RSValue_String);
}
