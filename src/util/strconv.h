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
#include <../trie/rune_util.h>
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

// transform utf8 string to lowercase using nunicode library
// returns the transformed string
// the length of the transformed string is stored in len
static char* nunicode_tolower(const char *encoded, size_t in_len, size_t *len) {
	ssize_t unicode_len = nu_strtransformlen(encoded, nu_utf8_read, nu_tolower, nu_casemap_read);
	uint32_t *unicode_buffer = (uint32_t *)rm_malloc(sizeof(*unicode_buffer) * (unicode_len + 1));

  // Decode utf8 string into Unicode codepoints and convert to lowercase
	uint32_t unicode;
	unsigned i = 0;
	while (1) {
    // Read unicode codepoint from utf8 string
		encoded = nu_utf8_read(encoded, &unicode);
    // Transform unicode codepoint to lowercase
		const char *map = nu_tolower(unicode);

    // Read the transformed codepoint and store it in the unicode buffer
		if (map != 0) {
			uint32_t mu;
			while (1) {
				map = nu_casemap_read(map, &mu);
				if (mu == 0) {
					break;
				}
				unicode_buffer[i] = mu;
				++i;
			}
		}
		else {
      // If no transformation is needed, just copy the unicode codepoint
			unicode_buffer[i] = unicode;
			++i;
		}

    // Break if the end of the string is reached
		if (unicode == 0) {
			break;
		}
	}

  // Encode Unicode codepoints back to utf8 string
  char *reencoded = NULL;
  ssize_t reencoded_len = nu_bytelen(unicode_buffer, nu_utf8_write);
  if (reencoded_len > 0 && reencoded_len <= in_len) {
    reencoded = (char*)rm_malloc(reencoded_len + 1);
    nu_writenstr(unicode_buffer, reencoded_len, reencoded, nu_utf8_write);
  } else {
    reencoded_len = 0;
  }

	rm_free(unicode_buffer);
  if (len) {
    *len = reencoded_len;
  }
	return reencoded;
}


// strndup + lowercase in one pass!
static char *rm_strdupcase(const char *s, size_t len) {
  char *ret = rm_strndup(s, len);
  char *dst = ret;
  char *src = ret;
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

  // convert multibyte-chars to lowercase
  size_t lower_len = 0;
  char *lowerCase = nunicode_tolower(ret, len, &lower_len);
  if (lowerCase) {
    rm_free(ret);
    return lowerCase;
  }
  return ret;
}

#endif
