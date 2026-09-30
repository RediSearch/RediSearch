/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef RS_STRCONV_H_
#define RS_STRCONV_H_
#include <limits.h>
#include <errno.h>
#include <math.h>
#include <string.h>
#include <ctype.h>
#include "fast_float/fast_float_strtod.h"
#include "libnu/libnu.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"
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
  if (strcmp(arg, "") == 0) {
    *d = 0;
    return 1;
  }

  *d = fast_float_strtod(arg, &e);

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
// encoded: the utf8 string to transform
// inout_len: input/output parameter, on input contains the length of the input
// string in bytes, on output will be set to the length of the output string in
// bytes. If the input string is not modified, it will be set to the same
// length as the input.
// Returns a newly allocated string with the transformed content, or NULL if no
// new memory was allocated (i.e., the output fits in the input buffer).
static char* unicode_tolower(char *encoded, size_t *inout_len) {
  if (*inout_len == 0) {
    return NULL;
  }

  size_t in_len = *inout_len;

  // ASCII fast-path: when no byte has the high bit set, every byte is a
  // single-codepoint ASCII character. Standard ASCII case folding (A-Z -> a-z)
  // is byte-length preserving and matches what the nunicode pipeline would
  // produce for codepoints < 0x80, so we can lowercase in place and skip the
  // multi-pass transform entirely. The scan also stops at an embedded NUL,
  // because nu_utf8_read treats codepoint 0 as end-of-string and truncates
  // the output there. Multibyte inputs fall through to the slow path at the
  // first byte with bit 7 set.
  {
    size_t j = 0;
    while (j < in_len) {
      unsigned char c = (unsigned char)encoded[j];
      if (c == 0 || c >= 0x80) break;
      j++;
    }
    if (j == in_len || encoded[j] == 0) {
      for (size_t k = 0; k < j; k++) {
        unsigned char c = (unsigned char)encoded[k];
        if (c >= 'A' && c <= 'Z') {
          encoded[k] = (char)(c + ('a' - 'A'));
        }
      }
      if (j < in_len && j > 0) {
        *inout_len = j;
      }
      return NULL;
    }
  }

  // `nu_utf8_read` (deps/libnu/utf8.h) is documented to take "a pointer
  // to UTF-8 encoded string". `encoded` here is caller-supplied and not
  // guaranteed to be valid UTF-8 (this is reachable from raw query
  // tokens), so we're passing it input against its safety docs -- it has
  // no bounds checking of its own to fall back on, so the result is
  // undefined behavior: for a lead byte whose declared sequence length
  // extends past `in_len`, it reads past the allocation. Work around
  // that below.
  //
  // Walk the string exactly as the decode loop below will, one declared
  // sequence at a time from the start, and stop `safe_len` at the first
  // position whose sequence would extend past `in_len`. A dangling
  // sequence earlier in the string is just as unsafe as one at the very
  // end (e.g. a lead byte followed by unrelated trailing bytes, not just
  // one at the tail), so this must walk forward from the start rather
  // than inspect only the string's last few bytes. Everything from
  // `safe_len` to `in_len` is excluded from decoding and appended to the
  // output unchanged instead, since it cannot be decoded without reading
  // past the allocation.
  //
  // A sequence that decodes to codepoint 0 is a separate case, checked
  // only once its length is already known to fit: both the measuring
  // pass and the decode loop below treat codepoint 0 as end-of-string
  // and stop there, the same truncate-at-NUL contract the ASCII fast
  // path above applies. Everything past it is beyond what this function
  // processes at all -- unlike a dangling sequence, it is not preserved,
  // only dropped, since splicing it back in as a tail would resurrect
  // bytes the decode loop never sees past it (and, if a dangling
  // sequence follows further along, silently skip whatever sits between
  // the two). This is not limited to a literal 0x00 byte: `nu_utf8_read`
  // has no overlong-encoding check either, so e.g. 0xC0 0x80 -- an
  // overlong 2-byte encoding of NUL -- decodes to codepoint 0 exactly
  // like a bare 0x00 does, via `utf8_2b`'s bit math on two all-zero
  // payload bytes. Reading the codepoint is only safe once the sequence
  // is already confirmed to fit, hence checking it after the length
  // guard below rather than folding it into the same pass.
  size_t safe_len = in_len;
  size_t tail_len = 0;
  {
    size_t p = 0;
    while (p < in_len) {
      unsigned char c = (unsigned char)encoded[p];
      size_t seq_len = (c < 0x80) ? 1 : (c < 0xE0) ? 2 : (c < 0xF0) ? 3 : 4;
      if (p + seq_len > in_len) {
        safe_len = p;
        tail_len = in_len - p;
        break;
      }
      uint32_t codepoint;
      nu_utf8_read(encoded + p, &codepoint);
      if (codepoint == 0) {
        safe_len = p;
        break;
      }
      p += seq_len;
    }
  }

  uint32_t u_stack_buffer[SSO_MAX_LENGTH];
  uint32_t *u_buffer = u_stack_buffer;
  char *longer_dst = NULL;

  ssize_t u_len = nu_strtransformnlen(encoded, safe_len, nu_utf8_read,
                                              nu_tolower, nu_casemap_read);

  if (u_len > (SSO_MAX_LENGTH - 1)) {
    u_buffer = (uint32_t *)rm_malloc(sizeof(*u_buffer) * (u_len + 1));
  }

  // Decode utf8 string into Unicode codepoints and transform to lower
  const char *encoded_char = encoded;
  unsigned i = 0;
  while (encoded_char < encoded + safe_len) {
    uint32_t codepoint = 0;
    // Read unicode codepoint from utf8 string
    // This might read more than one char.
    encoded_char = nu_utf8_read(encoded_char, &codepoint);
    if (codepoint == 0) {
      // If we reach the end of the string, break
      break;
    }

    // Transform unicode codepoint to lower case
    const char *map = nu_tolower(codepoint);

    // Read the transformed codepoint and store it in the unicode buffer
    // map would be NULL if no transformation is needed,
    // i.e.: lower case is the same as the original, emoji, etc.
    if (map != NULL) {
      uint32_t mu;
      while (1) {
        map = nu_casemap_read(map, &mu);
        if (mu == 0) {
          break;
        }
        u_buffer[i++] = mu;
      }
    } else {
      // If no transformation is needed, just copy the unicode codepoint
      u_buffer[i++] = codepoint;
    }
  }
  RS_LOG_ASSERT_FMT(i == u_len, "i (%u) should be equal to u_len (%zd)", i, u_len);
  // Encode Unicode codepoints back to utf8 string
  ssize_t reencoded_len = nu_bytenlen(u_buffer, i, nu_utf8_write);
  if (reencoded_len > 0 || tail_len > 0) {
    size_t head_len = reencoded_len > 0 ? (size_t)reencoded_len : 0;
    size_t total_len = head_len + tail_len;
    if (total_len <= in_len) {
      // If the reencoded length is less than or equal to the original length,
      // we can write directly to the original buffer
      // Write the reencoded string back to the original buffer
      // Note: nu_writenstr does not null-terminate the string, so we handle that separately
      // it should be updated by the caller if needed
      // `total_len <= in_len` and `total_len = head_len + tail_len` (with
      // `tail_len = in_len - safe_len`) together guarantee `head_len <=
      // safe_len`, so the head write above never touches the excluded
      // suffix still sitting at its original offset. `head_len` can still
      // be less than `safe_len` (the head shrank), so the source and
      // destination ranges here can overlap -- memmove, not memcpy.
      if (head_len > 0) {
        nu_writenstr(u_buffer, i, encoded, nu_utf8_write);
      }
      if (tail_len > 0) {
        memmove(encoded + head_len, encoded + safe_len, tail_len);
      }
    } else {
      longer_dst = (char *)rm_malloc((total_len + 1) * sizeof(*longer_dst));
      if (head_len > 0) {
        nu_writenstr(u_buffer, i, longer_dst, nu_utf8_write);
      }
      if (tail_len > 0) {
        memcpy(longer_dst + head_len, encoded + safe_len, tail_len);
      }
      longer_dst[total_len] = '\0';
    }
    *inout_len = total_len;
  }

  // Free heap-allocated memory if needed
  if (u_buffer != u_stack_buffer) {
    rm_free(u_buffer);
  }
  return longer_dst;
}

// strndup + unescape + tolower
static char *rm_normalize(const char *s, size_t len) {
  char *ret = rm_strndup(s, len);
  char *dst = ret;
  char *src = ret;
  while (*src) {
    // unescape
    if (*src == '\\' && (ispunct(*(src+1)) || isspace(*(src+1)))) {
      ++src;
      --len;
      continue;
    }
    *dst = *src;
    ++dst;
    ++src;
  }
  *dst = '\0';

  // convert to lower case
  char *longerDst = unicode_tolower(ret, &len);
  if (longerDst) {
      rm_free(ret);
      ret = longerDst;
  } else {
    // No memory allocation, just ensure null termination
    ret[len] = '\0';
  }

  return ret;
}

// Non-static wrapper around unicode_tolower for FFI testing.
char *unicode_tolower_fn(char *encoded, size_t *inout_len);

#endif
