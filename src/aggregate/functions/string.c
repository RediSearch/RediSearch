/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "rmutil/rm_assert.h"
#include "util/minmax.h"
#include "util/block_alloc.h"
#include "aggregate/expr/expression.h"
#include "util/arr.h"
#include "function.h"
#include "rmalloc.h"

#include "value.h"

#include <ctype.h>

#define STRING_BLOCK_SIZE 512
#define FMT_OUT_STR_MIN_PREALLOC 8
#define FMT_OUT_STR_MAX_PREALLOC (1024 * 1024)
#define MAX(i, j) (((i) > (j)) ? (i) : (j))

static int func_matchedTerms(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  int maxTerms = 100;
  if (argc == 1) {
    double d;
    if (RSValue_ToNumber(argv[0], &d) && 1 <= d && d <= 100) {
      maxTerms = (int)d;
    }
  }

  const SearchResult *res = ctx->res;

  if (res && SearchResult_HasIndexResult(res)) {
    RSQueryTerm *terms[maxTerms];
    size_t n = IndexResult_GetMatchedTerms(SearchResult_GetIndexResult(ctx->res), terms, maxTerms);
    if (n) {
      RSValue **arr = RSValue_NewArrayBuilder(n);
      for (size_t i = 0; i < n; i++) {
        size_t len;
        const char *str = QueryTerm_GetStrAndLen(terms[i], &len);
        arr[i] = RSValue_NewConstString(str, len);
      }
      RSValue *v = RSValue_NewArrayFromBuilder(arr, n);
      RSValue_MakeOwnReference(result, v);
      return EXPR_EVAL_OK;
    }
  }
  RSValue_MakeReference(result, RSValue_NullStatic());
  return EXPR_EVAL_OK;
}

#define stringfunc_to_generic(func)                      \
  size_t sz;                                             \
  const char *p;                                         \
  if (!(p = RSValue_StringPtrLen(argv[0], &sz))) {      \
    RSValue_MakeReference(result, RSValue_NullStatic()); \
    return EXPR_EVAL_OK;                                 \
  }                                                      \
  char *np = ExprEval_UnalignedAlloc(ctx, sz + 1);       \
  for (size_t i = 0; i < sz; i++) {                      \
    np[i] = func(p[i]);                                  \
  }                                                      \
  np[sz] = '\0';                                         \
  RSValue_SetConstString(result, np, sz);                \
  return EXPR_EVAL_OK

/* lower(str) */
static int stringfunc_tolower(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  stringfunc_to_generic(tolower);
}

/* upper(str) */
static int stringfunc_toupper(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  stringfunc_to_generic(toupper);
}

/* substr(str, offset, len) */
static int stringfunc_substr(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  VALIDATE_ARG_TYPE("substr", argv, 1, RSValueType_Number);
  VALIDATE_ARG_TYPE("substr", argv, 2, RSValueType_Number);

  size_t sz;
  const char *str = RSValue_StringPtrLen(argv[0], &sz);
  if (!str) {
    QueryError_SetError(ctx->err, QUERY_ERROR_CODE_PARSE_ARGS,
                        "Invalid type for substr. Expected string");
    return EXPR_EVAL_ERR;
  }

  int offset = (int)RSValue_Number_Get(RSValue_Dereference(argv[1]));
  int len = (int)RSValue_Number_Get(RSValue_Dereference(argv[2]));

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

int func_to_number(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {

  double n;
  if (!RSValue_ToNumber(argv[0], &n)) {
    size_t sz = 0;
    const char *p = RSValue_StringPtrLen(argv[0], &sz);
    QueryError_SetWithUserDataFmt(ctx->err, QUERY_ERROR_CODE_PARSE_ARGS,
                                  "to_number: cannot convert string", " '%s'", p);
    return EXPR_EVAL_ERR;
  }

  RSValue_SetNumber(result, n);
  return EXPR_EVAL_OK;
}

int func_to_str(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  RSValue_ToString(result, argv[0]);
  return EXPR_EVAL_OK;
}

// Helper for stringfunc_format that appends `src`
// to `dst`, keeping track of capacity and reallocating
// when needed.
void append_to_string(char **dst, char **dst_tail, size_t *dst_cap, const char *src,
                      size_t src_len) {
  size_t dst_len = *dst_tail - *dst;
  size_t dst_free = *dst_cap - dst_len;

  if (src_len > dst_free) {
    size_t new_cap = *dst_cap + src_len;
    if (new_cap < FMT_OUT_STR_MIN_PREALLOC) {
      new_cap = FMT_OUT_STR_MIN_PREALLOC;
    } else if (new_cap >= FMT_OUT_STR_MAX_PREALLOC) {
      new_cap += FMT_OUT_STR_MAX_PREALLOC;
    } else {
      new_cap *= 2;
    }

    *dst = rm_realloc(*dst, new_cap);
    *dst_cap = new_cap;
    RS_ASSERT(*dst != NULL);
    *dst_tail = *dst + dst_len;
  }
  memcpy(*dst_tail, src, src_len);
  *dst_tail += src_len;
}

static int stringfunc_format(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  VALIDATE_ARG_ISSTRING("format", argv, 0);

  size_t argix = 1;
  size_t fmtsz = 0;
  const char *fmt = RSValue_StringPtrLen(argv[0], &fmtsz);
  const char *last = fmt, *end = fmt + fmtsz;

  size_t out_cap = fmtsz;
  char *out = rm_malloc(fmtsz);
  char *out_tail = out;

  for (size_t ii = 0; ii < fmtsz; ++ii) {
    if (fmt[ii] != '%') {
      continue;
    }

    if (ii == fmtsz - 1) {
      // ... %"
      QueryError_SetError(ctx->err, QUERY_ERROR_CODE_PARSE_ARGS, "Bad format string!");
      goto error;
    }

    // Detected a format string. Write from 'last' up to 'fmt'
    size_t len = (fmt + ii) - last;
    append_to_string(&out, &out_tail, &out_cap, last, len);
    last = fmt + ii + 2;

    char type = fmt[++ii];
    if (type == '%') {
      // Append literal '%'
      append_to_string(&out, &out_tail, &out_cap, "%", 1);
      continue;
    }

    if (argix == argc) {
      QueryError_SetError(ctx->err, QUERY_ERROR_CODE_PARSE_ARGS, "Not enough arguments for format");
      goto error;
    }

    RSValue *arg = RSValue_Dereference(argv[argix++]);
    if (type == 's') {
      if (arg == RSValue_NullStatic()) {
        // write null value
        append_to_string(&out, &out_tail, &out_cap, "(null)", 6);
        continue;
      } else if (!RSValue_IsString(arg)) {

        RSValue *strval = RSValue_NewUndefined();
        RSValue_ToString(strval, arg);
        size_t sz;
        const char *str = RSValue_StringPtrLen(strval, &sz);
        if (!str) {
          append_to_string(&out, &out_tail, &out_cap, "(null)", 6);
        } else {
          append_to_string(&out, &out_tail, &out_cap, str, sz);
        }
        RSValue_DecrRef(strval);
      } else {
        size_t sz;
        const char *str = RSValue_StringPtrLen(arg, &sz);
        append_to_string(&out, &out_tail, &out_cap, str, sz);
      }
    } else {
      QueryError_SetError(ctx->err, QUERY_ERROR_CODE_PARSE_ARGS, "Unknown format specifier passed");
      goto error;
    }
  }

  if (last && last < end) {
    append_to_string(&out, &out_tail, &out_cap, last, end - last);
  }
  append_to_string(&out, &out_tail, &out_cap, "\0", 1);

  // Don't count the null terminator
  RSValue_SetString(result, out, out_tail - out - 1);
  return EXPR_EVAL_OK;

error:
  RS_ASSERT(QueryError_HasError(ctx->err));
  rm_free(out);
  RSValue_MakeReference(result, RSValue_NullStatic());
  return EXPR_EVAL_ERR;
}

static char *str_trim(char *s, size_t sl, const char *cset, size_t *outlen) {
  char *start, *end, *sp, *ep;

  sp = start = s;
  ep = end = s + sl - 1;
  while (sp <= end && strchr(cset, *sp)) sp++;
  while (ep > sp && strchr(cset, *ep)) ep--;
  *outlen = (sp > ep) ? 0 : ((ep - sp) + 1);

  return sp;
}
static int stringfunc_split(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  VALIDATE_ARG_ISSTRING("split", argv, 0);
  const char *sep = ",";
  const char *strp = " ";
  if (argc >= 2) {
    VALIDATE_ARG_ISSTRING("split", argv, 1);
    sep = RSValue_StringPtrLen(argv[1], NULL);
  }
  if (argc == 3) {
    VALIDATE_ARG_ISSTRING("split", argv, 2);
    strp = RSValue_StringPtrLen(argv[2], NULL);
  }

  size_t len;
  char *str = (char *)RSValue_StringPtrLen(argv[0], &len);
  char *ep = str + len;
  size_t l = 0;
  char *next;
  char *tok = str;

  // extract at most 1024 values
  RSValue *tmp[1024];
  while (l < 1024 && tok < ep) {
    next = strpbrk(tok, sep);
    size_t sl = next ? (next - tok) : ep - tok;

    if (sl > 0) {
      size_t outlen;
      // trim the strip set
      char *s = str_trim(tok, sl, strp, &outlen);
      if (outlen) {
        tmp[l++] = RSValue_NewCopiedString(s, outlen);
      }
    }

    // advance tok while it's not in the sep
    if (!next) break;

    tok = next + 1;
  }

  RSValue **vals = RSValue_NewArrayBuilder(l);
  memcpy(vals, tmp, l * sizeof(*vals));

  RSValue *ret = RSValue_NewArrayFromBuilder(vals, l);
  RSValue_MakeOwnReference(result, ret);
  return EXPR_EVAL_OK;
}

int func_exists(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  if (RSValue_Type(argv[0]) != RSValueType_Null) {
    RSValue_SetNumber(result, 1);
  } else {
    QueryError_ClearError(ctx->err);
    RSValue_SetNumber(result, 0);
  }
  return EXPR_EVAL_OK;
}

int func_case(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  // This function is never directly called for CASE expressions
  // The actual implementation is in evalFuncCase in expression.c
  // This is just a placeholder for function registration

  return EXPR_EVAL_OK;
}

static int stringfunc_startswith(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  VALIDATE_ARG_ISSTRING("startswith", argv, 0);
  VALIDATE_ARG_ISSTRING("startswith", argv, 1);

  RSValue *str = RSValue_Dereference(argv[0]);
  RSValue *pref = RSValue_Dereference(argv[1]);

  const char *p_str = RSValue_StringPtrLen(str, NULL);
  size_t n;
  const char *p_pref = RSValue_StringPtrLen(pref, &n);
  RSValue_SetNumber(result, strncmp(p_pref, p_str, n) == 0);
  return EXPR_EVAL_OK;
}

static int stringfunc_contains(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  VALIDATE_ARG_ISSTRING("contains", argv, 0);
  VALIDATE_ARG_ISSTRING("contains", argv, 1);

  RSValue *str = RSValue_Dereference(argv[0]);
  RSValue *pref = RSValue_Dereference(argv[1]);

  size_t p_str_size;
  char *p_str = (char *)RSValue_StringPtrLen(str, &p_str_size);
  size_t p_pref_size;
  const char *p_pref = (char *)RSValue_StringPtrLen(pref, &p_pref_size);

  size_t num;
  if (p_pref_size > 0) {
    num = 0;
    while ((p_str = strstr(p_str, p_pref)) != NULL) {
      num++;
      p_str++;
    }
  } else {
    num = p_str_size + 1;
  }
  RSValue_SetNumber(result, num);
  return EXPR_EVAL_OK;
}

static int stringfunc_strlen(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  VALIDATE_ARG_ISSTRING("strlen", argv, 0);

  RSValue *str = RSValue_Dereference(argv[0]);

  size_t n;
  const char *p_pref = (char *)RSValue_StringPtrLen(str, &n);
  RSValue_SetNumber(result, n);
  return EXPR_EVAL_OK;
}

void RegisterStringFunctions() {
  RSFunctionRegistry_RegisterFunction("lower", stringfunc_tolower, RSValueType_String, 1, 1);
  RSFunctionRegistry_RegisterFunction("upper", stringfunc_toupper, RSValueType_String, 1, 1);
  RSFunctionRegistry_RegisterFunction("substr", stringfunc_substr, RSValueType_String, 3, 3);
  RSFunctionRegistry_RegisterFunction("format", stringfunc_format, RSValueType_String, 1, -1);
  RSFunctionRegistry_RegisterFunction("split", stringfunc_split, RSValueType_Array, 1, 3);
  RSFunctionRegistry_RegisterFunction("matched_terms", func_matchedTerms, RSValueType_Array, 0, 1);
  RSFunctionRegistry_RegisterFunction("to_number", func_to_number, RSValueType_Number, 1, 1);
  RSFunctionRegistry_RegisterFunction("to_str", func_to_str, RSValueType_String, 1, 1);
  RSFunctionRegistry_RegisterFunction("exists", func_exists, RSValueType_Number, 1, 1);
  RSFunctionRegistry_RegisterFunction("case", func_case, RSValueType_Undef, 3, 3);
  RSFunctionRegistry_RegisterFunction("startswith", stringfunc_startswith, RSValueType_Number, 2,
                                      2);
  RSFunctionRegistry_RegisterFunction("contains", stringfunc_contains, RSValueType_Number, 2, 2);
  RSFunctionRegistry_RegisterFunction("strlen", stringfunc_strlen, RSValueType_Number, 1, 1);
}
