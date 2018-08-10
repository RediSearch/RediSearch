#include "args.h"
#include "redismodule.h"
#include <float.h>
#include <math.h>
#include <string.h>

int AC_Advance(ArgsCursor *ac) {
  return AC_AdvanceBy(ac, 1);
}

int AC_AdvanceBy(ArgsCursor *ac, size_t by) {
  if (ac->offset + by >= ac->argc) {
    return AC_ERR_NOARG;
  } else {
    ac->offset += by;
  }
  return 0;
}

#define MAYBE_ADVANCE()            \
  if (!(flags & AC_F_NOADVANCE)) { \
    AC_Advance(ac);                \
  }

static int tryReadAsDouble(ArgsCursor *ac, long long *ll, int flags) {
  double dTmp = 0.0;
  if (AC_GetDouble(ac, &dTmp, flags | AC_F_NOADVANCE)) {
    return AC_ERR_PARSE;
  }
  if (flags & AC_F_COALESCE) {
    *ll = dTmp;
    return 0;
  }

  if ((double)(long long)dTmp != dTmp) {
    return AC_ERR_PARSE;
  } else {
    *ll = dTmp;
    return 0;
  }
}

int AC_GetLongLong(ArgsCursor *ac, long long *ll, int flags) {
  if (ac->offset == ac->argc) {
    return AC_ERR_NOARG;
  }
  if (ac->type == AC_TYPE_RSTRING) {
    if (RedisModule_StringToLongLong(AC_CURRENT(ac), ll) == REDISMODULE_ERR &&
        tryReadAsDouble(ac, ll, flags)) {
    }
  } else {
    char *endptr = AC_CURRENT(ac);
    *ll = strtoll(AC_CURRENT(ac), &endptr, 10);
    if (*endptr != '\0' || *ll == LLONG_MIN || *ll == LLONG_MAX) {
      if (tryReadAsDouble(ac, ll, flags) != 0) {
        return AC_ERR_PARSE;
      }
    }
  }
  if ((flags & AC_F_GE0) && *ll < 0) {
    return AC_ERR_ELIMIT;
  }
  // Do validation
  if ((flags & AC_F_GE1) && *ll < 1) {
    return AC_ERR_ELIMIT;
  }
  MAYBE_ADVANCE();
  return 0;
}

#define GEN_AC_FUNC(name, T, minVal, maxVal, isUnsigned)      \
  int name(ArgsCursor *ac, T *p, int flags) {                 \
    if (isUnsigned) {                                         \
      flags |= AC_F_GE0;                                      \
    }                                                         \
    long long ll;                                             \
    int rv = AC_GetLongLong(ac, &ll, flags | AC_F_NOADVANCE); \
    if (rv) {                                                 \
      return rv;                                              \
    }                                                         \
    if (ll > maxVal || ll < minVal) {                         \
      return AC_ERR_ELIMIT;                                   \
    }                                                         \
    *p = ll;                                                  \
    MAYBE_ADVANCE();                                          \
    return 0;                                                 \
  }

GEN_AC_FUNC(AC_GetUnsignedLongLong, unsigned long long, 0, LLONG_MAX, 1)
GEN_AC_FUNC(AC_GetUnsigned, unsigned, 0, UINT_MAX, 1)
GEN_AC_FUNC(AC_GetInt, int, INT_MIN, INT_MAX, 0)

int AC_GetDouble(ArgsCursor *ac, double *d, int flags) {
  if (ac->type == AC_TYPE_RSTRING) {
    if (RedisModule_StringToDouble(ac->objs[ac->offset], d) != REDISMODULE_OK) {
      return AC_ERR_PARSE;
    }
  } else {
    char *endptr = AC_CURRENT(ac);
    *d = strtod(AC_CURRENT(ac), &endptr);
    if (*endptr != '\0' || *d == HUGE_VAL || *d == -HUGE_VAL) {
      return AC_ERR_PARSE;
    }
  }
  if ((flags & AC_F_GE0) && *d < 0.0) {
    return AC_ERR_ELIMIT;
  }
  if ((flags & AC_F_GE1) && *d < 1.0) {
    return AC_ERR_ELIMIT;
  }
  MAYBE_ADVANCE();
  return 0;
}

int AC_GetString(ArgsCursor *ac, const char **s, size_t *n, int flags) {
  if (ac->offset == ac->argc) {
    return AC_ERR_NOARG;
  }
  if (ac->type == AC_TYPE_RSTRING) {
    *s = RedisModule_StringPtrLen(AC_CURRENT(ac), n);
  } else {
    *s = AC_CURRENT(ac);
    if (n) {
      *n = strlen(*s);
    }
  }
  MAYBE_ADVANCE();
  return 0;
}

const char *AC_GetStringNC(ArgsCursor *ac, size_t *len) {
  const char *s = NULL;
  if (AC_GetString(ac, &s, len, 0) != 0) {
    return NULL;
  }
  return s;
}