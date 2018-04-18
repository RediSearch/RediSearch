#include <util/minmax.h>
#include <rmutil/sds.h>
#include <util/block_alloc.h>
#include <aggregate/expr/expression.h>
#include <ctype.h>
#include <util/arr.h>
#include "function.h"
#include <err.h>

#define STRING_BLOCK_SIZE 512

static int func_matchedTerms(RSFunctionEvalCtx *ctx, RSValue *result, RSValue *argv, int argc,
                             char **err) {
  int maxTerms = 0;
  if (argc == 1) {
    double d;
    if (RSValue_ToNumber(&argv[0], &d)) {
      if (d > 0) {
        maxTerms = (int)d;
      }
    }
  }

  SearchResult *res = ctx->res;
  if (maxTerms == 0) maxTerms = 100;
  maxTerms = MIN(100, maxTerms);

  // fprintf(stderr, "res %p, indexresult %p\n", res, res ? res->indexResult : NULL);
  if (res && res->indexResult) {
    RSQueryTerm *terms[maxTerms];
    size_t n = IndexResult_GetMatchedTerms(ctx->res->indexResult, terms, maxTerms);
    if (n) {
      RSValue **arr = calloc(n, sizeof(RSValue *));
      for (size_t i = 0; i < n; i++) {
        arr[i] = RS_ConstStringVal(terms[i]->str, terms[i]->len);
      }
      RSValue *v = RS_ArrVal(arr, n);
      RSValue_MakeReference(result, v);
      return EXPR_EVAL_OK;
    }
  }
  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

/* lower(str) */
static int stringfunc_tolower(RSFunctionEvalCtx *ctx, RSValue *result, RSValue *argv, int argc,
                              char **err) {

  VALIDATE_ARGS("lower", 1, 1, err);
  if (!RSValue_IsString(&argv[0])) {
    RSValue_MakeReference(result, RS_NullVal());
    return EXPR_EVAL_OK;
  }

  size_t sz = 0;
  char *p = (char *)RSValue_StringPtrLen(&argv[0], &sz);
  char *np = RSFunction_Alloc(ctx, sz + 1);
  for (size_t i = 0; i < sz; i++) {
    np[i] = tolower(p[i]);
  }
  np[sz] = '\0';
  RSValue_SetConstString(result, np, sz);
  return EXPR_EVAL_OK;
}

/* upper(str) */
static int stringfunc_toupper(RSFunctionEvalCtx *ctx, RSValue *result, RSValue *argv, int argc,
                              char **err) {
  VALIDATE_ARGS("upper", 1, 1, err);

  if (!RSValue_IsString(&argv[0])) {
    RSValue_MakeReference(result, RS_NullVal());
    return EXPR_EVAL_OK;
  }

  size_t sz = 0;
  char *p = (char *)RSValue_StringPtrLen(&argv[0], &sz);
  char *np = RSFunction_Alloc(ctx, sz + 1);
  for (size_t i = 0; i < sz; i++) {
    np[i] = toupper(p[i]);
  }
  np[sz] = '\0';
  RSValue_SetConstString(result, np, sz);
  return EXPR_EVAL_OK;
}

/* substr(str, offset, len) */
static int stringfunc_substr(RSFunctionEvalCtx *ctx, RSValue *result, RSValue *argv, int argc,
                             char **err) {
  VALIDATE_ARGS("substr", 3, 3, err);

  VALIDATE_ARG_TYPE("substr", argv, 1, RSValue_Number);
  VALIDATE_ARG_TYPE("substr", argv, 2, RSValue_Number);

  size_t sz;
  const char *str = RSValue_StringPtrLen(&argv[0], &sz);
  if (!str) {
    SET_ERR(err, "Invalid type for substr, expected string");
    return EXPR_EVAL_ERR;
  }

  int offset = (int)RSValue_Dereference(&argv[1])->numval;
  int len = (int)RSValue_Dereference(&argv[2])->numval;

  // for negative offsets we count from the end of the string
  if (offset < 0) {
    offset = (int)sz + offset;
  }
  offset = MAX(0, MIN(offset, sz));
  // len < 0 means read until the end of the string
  if (len < 0) {
    len = MAX(0, (sz - offset) + len);
  }
  if (offset + len > sz) {
    len = sz - offset;
  }

  char *dup = RSFunction_Strndup(ctx, &str[offset], len);
  RSValue_SetConstString(result, dup, len);
  return EXPR_EVAL_OK;
}

static int stringfunc_format(RSFunctionEvalCtx *ctx, RSValue *result, RSValue *argv, int argc,
                             char **err) {
  if (argc < 1) {
    SET_ERR(err, "Need at least one argument for format");
    return EXPR_EVAL_ERR;
  }
  VALIDATE_ARG_ISSTRING("format", argv, 0);

  size_t argix = 1;
  size_t fmtsz = 0;
  const char *fmt = RSValue_StringPtrLen(&argv[0], &fmtsz);
  const char *last = fmt, *end = fmt + fmtsz;
  sds out = sdsMakeRoomFor(sdsnew(""), fmtsz);

  for (size_t ii = 0; ii < fmtsz; ++ii) {
    if (fmt[ii] != '%') {
      continue;
    }

    if (fmt[ii] == fmtsz - 1) {
      // ... %"
      SET_ERR(err, "Bad format string!");
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
      SET_ERR(err, "Not enough arguments for format");
      goto error;
    }

    RSValue *arg = RSValue_Dereference(&argv[argix++]);
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
      SET_ERR(err, "Unknown format specifier passed");
      goto error;
    }
  }

  if (last && last < end) {
    out = sdscatlen(out, last, end - last);
  }

  RSValue_SetSDS(result, out);
  return EXPR_EVAL_OK;

error:
  if (!*err) {
    SET_ERR(err, "Error in format");
  }
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
static int stringfunc_split(RSFunctionEvalCtx *ctx, RSValue *result, RSValue *argv, int argc,
                            char **err) {
  if (argc < 1 || argc > 3) {
    SET_ERR(err, "Invalid number of arguments for split");
    return EXPR_EVAL_ERR;
  }
  VALIDATE_ARG_ISSTRING("format", argv, 0);
  const char *sep = ",";
  const char *strp = " ";
  if (argc >= 2) {
    VALIDATE_ARG_ISSTRING("format", argv, 1);
    sep = RSValue_StringPtrLen(&argv[1], NULL);
  }
  if (argc == 3) {
    VALIDATE_ARG_ISSTRING("format", argv, 2);
    strp = RSValue_StringPtrLen(&argv[2], NULL);
  }

  size_t len;
  char *str = (char *)RSValue_StringPtrLen(&argv[0], &len);
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
        // we mark the strings as volatile so they'll be copied if persisted
        // otherwise we'd have to copy them...
        tmp[l++] = RS_StringValT(s, outlen, RSString_Volatile);
      }
    }

    // advance tok while it's not in the sep
    if (!next) break;

    tok = next + 1;
  }

  // if (len > 0) {
  //   tmp[l++] = RS_ConstStringVal(tok, len);
  // }

  RSValue **vals = calloc(l, sizeof(*vals));
  for (size_t i = 0; i < l; i++) {
    vals[i] = tmp[i];
  }

  RSValue *ret = RS_ArrVal(vals, l);
  RSValue_MakeReference(result, ret);
  return EXPR_EVAL_OK;
}

void RegisterStringFunctions() {
  RSFunctionRegistry_RegisterFunction("lower", stringfunc_tolower, RSValue_String);
  RSFunctionRegistry_RegisterFunction("upper", stringfunc_toupper, RSValue_String);
  RSFunctionRegistry_RegisterFunction("substr", stringfunc_substr, RSValue_String);
  RSFunctionRegistry_RegisterFunction("format", stringfunc_format, RSValue_String);
  RSFunctionRegistry_RegisterFunction("split", stringfunc_split, RSValue_Array);
  RSFunctionRegistry_RegisterFunction("matched_terms", func_matchedTerms, RSValue_Array);
}
