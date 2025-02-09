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
#include "libnu/libnu.h"
/* Strconv - common simple string conversion utils */

// Case insensitive string equal
#define STR_EQCASE(str, len, other) (len == strlen(other) && !strncasecmp(str, other, len))

// Case sensitive string equal
#define STR_EQ(str, len, other) (len == strlen(other) && !strncmp(str, other, len))

// Threshold for Small String Optimization (SSO)
#define SSO_MAX_LENGTH 128

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

// transform utf8 string to lower case using nunicode library
// encoded: the utf8 string to transform, if the transformation is successful
//          the transformed string will be written back to this buffer
// in_len: the length of the utf8 string
// returns the bytes written to encoded, or 0 if the length of the transformed
// string is greater than in_len and no transformation was done
static size_t unicode_tolower(char *encoded, size_t in_len) {
  uint32_t u_stack_buffer[SSO_MAX_LENGTH];
  uint32_t *u_buffer = u_stack_buffer;

  if (in_len == 0) {
    return 0;
  }

  const char *encoded_char = encoded;
  ssize_t u_len = nu_strtransformnlen(encoded, in_len, nu_utf8_read,
                                              nu_tolower, nu_casemap_read);

  if (u_len >= (SSO_MAX_LENGTH - 1)) {
    u_buffer = (uint32_t *)rm_malloc(sizeof(*u_buffer) * (u_len + 1));
  }

  // Decode utf8 string into Unicode codepoints and transform to lower
  uint32_t codepoint;
  unsigned i = 0;
  for (ssize_t j = 0; j < u_len; j++) {
    // Read unicode codepoint from utf8 string
    encoded_char = nu_utf8_read(encoded_char, &codepoint);
    // Transform unicode codepoint to lower case
    const char *map = nu_tolower(codepoint);

    // Read the transformed codepoint and store it in the unicode buffer
    if (map != 0) {
      uint32_t mu;
      while (1) {
        map = nu_casemap_read(map, &mu);
        if (mu == 0) {
          break;
        }
        u_buffer[i] = mu;
        ++i;
      }
    }
    else {
      // If no transformation is needed, just copy the unicode codepoint
      u_buffer[i] = codepoint;
      ++i;
    }
  }

  // Encode Unicode codepoints back to utf8 string
  ssize_t reencoded_len = nu_bytenlen(u_buffer, u_len, nu_utf8_write);
  if (reencoded_len > 0 && reencoded_len <= in_len) {
    nu_writenstr(u_buffer, u_len, encoded, nu_utf8_write);
  } else {
    reencoded_len = 0;
  }

  // Free heap-allocated memory if needed
  if (u_buffer != u_stack_buffer) {
    rm_free(u_buffer);
  }

  return reencoded_len;
}


// strndup + unescape + fold
static char *rm_normalize(const char *s, size_t len) {
  char *ret = rm_strndup(s, len);
  char *dst = ret;
  char *src = ret;
  while (*src) {
    // unescape
    if (*src == '\\' && (ispunct(*(src+1)) || isspace(*(src+1)))) {
      ++src;
      continue;
    }
    *dst = *src;
    ++dst;
    ++src;

  }
  *dst = '\0';

  // convert to lower case
  size_t newLen = unicode_tolower(ret, len);
  if (newLen && newLen <= len) {
    ret[newLen] = '\0';
  }

  return ret;
}

#endif
