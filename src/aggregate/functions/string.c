#include <util/minmax.h>
#include <util/array.h>
#include <util/block_alloc.h>
#include <aggregate/expr/expression.h>
#include <ctype.h>
#include "function.h"
#define STRING_BLOCK_SIZE 512

/* lower(str) */
static int stringfunc_tolower(RSValue *result, RSValue *argv, int argc, char **err) {

  VALIDATE_ARGS("lower", 1, 1, err);
  RSValue *v = RSValue_IsString(&argv[0]) ? &argv[0] : RSValue_ToString(&argv[0]);

  size_t sz;
  char *p = (char *)RSValue_StringPtrLen(v, &sz);
  for (size_t i = 0; i < sz; i++) {
    p[i] = tolower(p[i]);
  }
  RSValue_MakeReference(result, v);
  return EXPR_EVAL_OK;
}

/* uppert(str) */
static int stringfunc_toupper(RSValue *result, RSValue *argv, int argc, char **err) {
  VALIDATE_ARGS("upper", 1, 1, err);

  RSValue *v = RSValue_IsString(&argv[0]) ? &argv[0] : RSValue_ToString(&argv[0]);

  size_t sz;
  char *p = (char *)RSValue_StringPtrLen(v, &sz);
  for (size_t i = 0; i < sz; i++) {
    p[i] = toupper(p[i]);
  }
  RSValue_MakeReference(result, v);
  return EXPR_EVAL_OK;
}

/* substr(str, offset, len) */
static int stringfunc_substr(RSValue *result, RSValue *argv, int argc, char **err) {
  VALIDATE_ARGS("substr", 3, 3, err);
  VALIDATE_ARG_TYPE("substr", argv, 1, RSValue_Number);
  VALIDATE_ARG_TYPE("substr", argv, 2, RSValue_Number);

  size_t sz;
  const char *str = RSValue_StringPtrLen(&argv[0], &sz);
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

  printf("sz %zd, offset %d, len %d\n", sz, offset, len);
  char *dup = strndup(&str[offset], len);
  RSValue_SetString(result, dup, len);
  return EXPR_EVAL_OK;
}

static void write_strvalue(Array *arr, RSValue *val, int steal) {
  size_t argsz;
  const char *arg = RSValue_StringPtrLen(val, &argsz);
  Array_Write(arr, arg, argsz);
  if (steal) {
    RSValue_DecrRef(val);
  }
}

static int stringfunc_format(RSValue *result, RSValue *argv, int argc, char **err) {
  if (argc < 1) {
    *err = strdup("Need at least one argument for format");
    return EXPR_EVAL_ERR;
  }
  VALIDATE_ARG_ISSTRING("format", argv, 0);

  Array arr = {NULL};
  Array_InitEx(&arr, ArrayAlloc_LibC);
  size_t argix = 1;
  size_t fmtsz;
  const char *fmt = RSValue_StringPtrLen(&argv[0], &fmtsz);
  const char *last = fmt, *end = fmt + fmtsz;

  for (size_t ii = 0; ii < fmtsz; ++ii) {
    if (fmt[ii] != '%') {
      continue;
    }

    if (fmt[ii] == fmtsz - 1) {
      // ... %"
      *err = strdup("Bad format string!");
      goto error;
    }

    // Detected a format string. Write from 'last' up to 'fmt'
    Array_Write(&arr, last, (fmt + ii) - last);
    last = fmt + ii + 2;

    char type = fmt[++ii];
    if (type == '%') {
      // Append literal '%'
      Array_Write(&arr, &type, 1);
      continue;
    }

    if (argix == argc) {
      *err = strdup("Not enough arguments for format");
      goto error;
    }

    RSValue *arg = RSValue_Dereference(&argv[argix++]);
    if (type == 's') {
      if (arg->t == RSValue_Null) {
        // Don't do anything
        continue;
      } else if (!RSValue_IsString(arg)) {
        *err = strdup("argument for %s is not a string");
        goto error;
      } else {
        write_strvalue(&arr, arg, 0);
      }
    } else if (type == 'd') {
      if (arg->t == RSValue_Number) {
        char numbuf[64] = {0};
        size_t n = snprintf(numbuf, sizeof numbuf, "%lld", (long long)arg->numval);
        Array_Write(&arr, numbuf, n);
      } else if (arg->t == RSValue_Null) {
        Array_Write(&arr, "0", 1);
      } else {
        *err = strdup("argument for %d is not a number");
        goto error;
      }
    } else if (type == 'f') {
      if (arg->t == RSValue_Null) {
        write_strvalue(&arr, RSValue_ToString(arg), 1);
      } else if (arg->t == RSValue_Null) {
        Array_Write(&arr, "0.0", 3);
      } else {
        *err = strdup("argument for %f is not a number");
        goto error;
      }
    } else if (type == 'v') {
      write_strvalue(&arr, RSValue_ToString(arg), 1);
    } else {
      *err = strdup("Unknown format specifier passed");
      goto error;
    }
  }

  if (last && last < end) {
    Array_Write(&arr, last, end - last);
  }

  Array_ShrinkToSize(&arr);  // We're no longer allocating more memory for this string
  RSValue_SetString(result, arr.data, arr.len);
  // No need to clear the array object. Its content has been 'stolen'!
  return EXPR_EVAL_OK;

error:
  if (!*err) {
    *err = strdup("Error in format");
  }
  Array_Free(&arr);
  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_ERR;
}

void RegisterStringFunctions(RSFunctionRegistry *reg) {
  RSFunctionRegistry_RegisterFunction(reg, "lower", stringfunc_tolower);
  RSFunctionRegistry_RegisterFunction(reg, "upper", stringfunc_toupper);
  RSFunctionRegistry_RegisterFunction(reg, "substr", stringfunc_substr);
  RSFunctionRegistry_RegisterFunction(reg, "format", stringfunc_format);
}