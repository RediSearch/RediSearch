/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef RS_STRCONV_H_
#define RS_STRCONV_H_
#include <stdlib.h>
#include <limits.h>
#include <sys/errno.h>
#include <math.h>
#include <string.h>
#include <ctype.h>
/* Strconv - common simple string conversion utils */

// Case insensitive string equal
#define STR_EQCASE(str, len, other) (len == strlen(other) && !strncasecmp(str, other, len))

// Case sensitive string equal
#define STR_EQ(str, len, other) (len == strlen(other) && !strncmp(str, other, len))

/* Parse string into int, returning 1 on success, 0 otherwise */
static int ParseInteger(const char *arg, long long *val) {

  char *e = NULL;
  errno = 0;
  *val = strtoll(arg, &e, 10);
  if ((errno == ERANGE && (*val == LONG_MAX || *val == LONG_MIN)) || (errno != 0 && *val == 0) ||
      *e != '\0') {
    *val = -1;
    return 0;
  }

  return 1;
}

/* Parse string into double, returning 1 on success, 0 otherwise */
static int ParseDouble(const char *arg, double *d) {
  char *e;
  errno = 0;
  *d = strtod(arg, &e);

  if ((errno == ERANGE && (*d == HUGE_VAL || *d == -HUGE_VAL)) || (errno != 0 && *d == 0) ||
      *e != '\0') {
    return 0;
  }

  return 1;
}

static int ParseBoolean(const char *arg, int *res) {
  if (STR_EQCASE(arg, strlen(arg), "true") || STR_EQCASE(arg, strlen(arg), "1")) {
    *res = 1;
    return 1;
  }

  if (STR_EQCASE(arg, strlen(arg), "false") || STR_EQCASE(arg, strlen(arg), "0")) {
    *res = 0;
    return 1;
  }

  return 0;
}

static char *strtolower(char *str) {
  char *p = str;
  while (*p) {
    *p = tolower(*p);
    p++;
  }
  return str;
}

static char *rm_strndup_unescape(alloc_context *ctx, const char *s, size_t len) {
  char *ret = rm_strndup(ctx, s, len);
  char *dst = ret;
  char *src = (char *)s;
  while (*src && len) {
    // unescape
    if (*src == '\\' && (ispunct(*(src+1)) || isspace(*(src+1)))) {
      ++src;
      --len;
      continue;
    }
    *dst = *src;
    ++dst;
    ++src;
    --len;
  }
  *dst = '\0';

  return ret;
}

// strndup + lowercase in one pass!
static char *rm_strdupcase(const char *s, size_t len) {
  char *ret = rm_strndup(s, len);
  char *dst = ret;
  char *src = dst;
  while (*src) {
    // unescape
    if (*src == '\\' && (ispunct(*(src+1)) || isspace(*(src+1)))) {
      ++src;
      continue;
    }
    *dst = tolower(*src);
    ++dst;
    ++src;

  }
  *dst = '\0';

  return ret;
}

#endif