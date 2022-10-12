#pragma once

#include <stdlib.h>
#include <limits.h>
#include <sys/errno.h>
#include <math.h>
#include <string.h>

// Strconv - common simple string conversion utils

///////////////////////////////////////////////////////////////////////////////////////////////

// Case insensitive string equal
#define STR_EQCASE(str, len, other) (len == strlen(other) && !strncasecmp(str, other, len))

inline int str_casecmp(const std::string_view &ss, const char *s, size_t len) {
  return strncasecmp(ss.data(), s, len);
}

inline int str_casecmp(const std::string_view &a, const std::string_view &b) {
  return strncasecmp(a.data(), b.data(), a.length());
}

// Case sensitive string equal
#define STR_EQ(str, len, other) (len == strlen(other) && !strncmp(str, other, len))

inline int str_caseeq(const std::string_view &ss, const char *s, size_t len) {
  return ss == std::string_view(s, len);
}

inline int str_caseeq(const std::string_view &a, const std::string_view &b) {
  return a.length() == b.length() && !strncasecmp(a.data(), b.data(), a.length());
}

//---------------------------------------------------------------------------------------------

// Parse string into int, returning 1 on success, 0 otherwise

static bool ParseInteger(const char *arg, long long &n) {
  char *e = NULL;
  n = strtoll(arg, &e, 10);
  errno = 0;
  if ((errno == ERANGE && (n == LONG_MAX || n == LONG_MIN)) || (errno != 0 && n == 0) ||
      *e != '\0') {
    n = -1;
    return false;
  }

  return true;
}

//---------------------------------------------------------------------------------------------

// Parse string into double, returning 1 on success, 0 otherwise

static bool ParseDouble(const char *arg, double &d) {
  char *e;
  d = strtod(arg, &e);
  errno = 0;

  if ((errno == ERANGE && (d == HUGE_VAL || d == -HUGE_VAL)) || (errno != 0 && d == 0) || *e != '\0') {
    return false;
  }

  return true;
}

//---------------------------------------------------------------------------------------------

static bool ParseBoolean(const char *s, bool &b) {
  size_t len = strlen(s);
  if (STR_EQCASE(s, len, "true") || STR_EQCASE(s, len, "1")) {
    b = true;
    return true;
  }

  if (STR_EQCASE(s, len, "false") || STR_EQCASE(s, len, "0")) {
    b = false;
    return true;
  }

  return false;
}

///////////////////////////////////////////////////////////////////////////////////////////////
