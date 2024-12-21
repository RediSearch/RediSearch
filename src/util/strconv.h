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
#include <wctype.h>
#include <wchar.h>
#include <locale.h>
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
static int ParseDouble(const char *arg, double *d, int sign) {
  char *e;
  errno = 0;

  // Simulate the behavior of glibc's strtod
  #if !defined(__GLIBC__)
  if (strcmp(arg, "") == 0) {
    *d = 0;
    return 1;
  }
  #endif

  *d = strtod(arg, &e);

  if ((errno == ERANGE && (*d == HUGE_VAL || *d == -HUGE_VAL)) || (errno != 0 && *d == 0) ||
      *e != '\0') {
    return 0;
  }
  
  if(sign == -1) {
    *d = -(*d);
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

static char *rm_strndup_unescape(const char *s, size_t len) {
  char *ret = rm_strndup(s, len);
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

static char *rm_strdupcase_utf8(const char *s, size_t len) {
  // TODO: set locale depending on the index language?
  setlocale(LC_ALL, "en_US.UTF-8");

  // Allocate memory for the destination string
  char *ret = rm_strndup(s, len);
  if (!ret) {
    return NULL;
  }

  char *dst = ret;
  char *src = dst;
  mbstate_t state;
  memset(&state, 0, sizeof(state));
  wchar_t wc;
  size_t len_wc;

  while (*src) {
    len_wc = mbrtowc(&wc, src, MB_CUR_MAX, &state);
    if (len_wc == (size_t)-1 || len_wc == (size_t)-2) {
      // Handle invalid multi-byte sequence
      src++;
      continue;
    }

    // Unescape
    if (wc == L'\\' && (iswpunct(*(src + len_wc)) || iswspace(*(src + len_wc)))) {
      src++;
      continue;
    }

    wc = towlower(wc);
    len_wc = wcrtomb(dst, wc, &state);
    if (len_wc == (size_t)-1) {
      // Handle conversion error
      src++;
      continue;
    }

    dst += len_wc;
    src += len_wc;
  }

  *dst = '\0';

  return ret;
}

#endif
