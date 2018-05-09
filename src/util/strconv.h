#ifndef RS_STRCONV_H_
#define RS_STRCONV_H_
#include <stdlib.h>
#include <limits.h>
#include <sys/errno.h>
#include <math.h>
#include <string.h>
/* Strconv - common simple string conversion utils */

// Case insensitive string equal
#define STR_EQCASE(str, len, other) (len == strlen(other) && !strncasecmp(str, other, len))

// Case sensitive string equal
#define STR_EQ(str, len, other) (len == strlen(other) && !strncmp(str, other, len))

/* Parse string into int, returning 1 on success, 0 otherwise */
static int ParseInteger(const char *arg, long long *val) {

  char *e = NULL;
  *val = strtoll(arg, &e, 10);
  errno = 0;
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
  *d = strtod(arg, &e);
  errno = 0;

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

#endif