
#include "args.h"
#include "redismodule.h"

#include <float.h>
#include <math.h>
#include <string.h>
#include <assert.h>

int ArgsCursor::Advance() {
  return AdvanceBy(1);
}

int ArgsCursor::AdvanceBy(size_t by) {
  if (offset + by > argc) {
    return AC_ERR_NOARG;
  } else {
    offset += by;
  }
  return AC_OK;
}

// Advances the cursor if the next argument matches the given string. This
// will swallow it up.

int ArgsCursor::AdvanceIfMatch(const char *s) {
  const char *cur;
  if (IsAtEnd()) {
    return 0;
  }

  int rv = GetString(&cur, NULL, AC_F_NOADVANCE);
  assert(rv == AC_OK);
  rv = !strcasecmp(s, cur);
  if (rv) {
    Advance();
  }
  return rv;
}

#define MAYBE_ADVANCE()            \
  if (!(flags & AC_F_NOADVANCE)) { \
    this->Advance();               \
  }

static int tryReadAsDouble(ArgsCursor *ac, long long *ll, int flags) {
  double dTmp = 0.0;
  if (ac->GetDouble(&dTmp, flags | AC_F_NOADVANCE) != AC_OK) {
    return AC_ERR_PARSE;
  }
  if (flags & AC_F_COALESCE) {
    *ll = dTmp;
    return AC_OK;
  }

  if ((double)(long long)dTmp != dTmp) {
    return AC_ERR_PARSE;
  } else {
    *ll = dTmp;
    return AC_OK;
  }
}

int ArgsCursor::GetLongLong(long long *ll, int flags) {
  long long tmpll = 0;
  if (offset == argc) {
    return AC_ERR_NOARG;
  }

  int hasErr = 0;
  // Try to parse the number as a normal integer first. If that fails, try
  // to parse it as a double. This will work if the number is in the format of
  // 3.00, OR if the number is in the format of 3.14 *AND* AC_F_COALESCE is set.
  if (type == AC_TYPE_RSTRING) {
    if (RedisModule_StringToLongLong(CURRENT(), &tmpll) == REDISMODULE_ERR) {
      hasErr = 1;
    }
  } else {
    char *endptr = CURRENT();
    tmpll = strtoll(CURRENT(), &endptr, 10);
    if (*endptr != '\0' || tmpll == LLONG_MIN || tmpll == LLONG_MAX) {
      hasErr = 1;
    }
  }

  if (hasErr && tryReadAsDouble(this, &tmpll, flags) != AC_OK) {
    return AC_ERR_PARSE;
  }

  if ((flags & AC_F_GE0) && tmpll < 0) {
    return AC_ERR_ELIMIT;
  }
  // Do validation
  if ((flags & AC_F_GE1) && tmpll < 1) {
    return AC_ERR_ELIMIT;
  }
  MAYBE_ADVANCE();
  *ll = tmpll;
  return AC_OK;
}

// לשאול את רפי
#define GEN_AC_FUNC(name, T, minVal, maxVal, isUnsigned)      \
  int name(ArgsCursor *ac, T *p, int flags) {                 \
    if (isUnsigned) {                                         \
      flags |= AC_F_GE0;                                      \
    }                                                         \
    long long ll;                                             \
    int rv = ac->GetLongLong( &ll, flags | AC_F_NOADVANCE); \
    if (rv) {                                                 \
      return rv;                                              \
    }                                                         \
    if (ll > maxVal || ll < minVal) {                         \
      return AC_ERR_ELIMIT;                                   \
    }                                                         \
    *p = ll;                                                  \
    MAYBE_ADVANCE();                                          \
    return AC_OK;                                             \
  }

GEN_AC_FUNC(GetUnsignedLongLong, unsigned long long, 0, LLONG_MAX, 1)
GEN_AC_FUNC(GetUnsigned, unsigned, 0, UINT_MAX, 1)
GEN_AC_FUNC(GetInt, int, INT_MIN, INT_MAX, 0)
GEN_AC_FUNC(GetU32, uint32_t, 0, UINT32_MAX, 1)
GEN_AC_FUNC(GetU64, uint64_t, 0, UINT64_MAX, 1)
GEN_AC_FUNC(GetSize, size_t, 0, SIZE_MAX, 1)

int ArgsCursor::GetDouble(double *d, int flags) {
  double tmpd = 0;
  if (type == AC_TYPE_RSTRING) {
    if (RedisModule_StringToDouble(objs[offset], &tmpd) != REDISMODULE_OK) {
      return AC_ERR_PARSE;
    }
  } else {
    char *endptr = CURRENT();
    tmpd = strtod(CURRENT(), &endptr);
    if (*endptr != '\0' || tmpd == HUGE_VAL || tmpd == -HUGE_VAL) {
      return AC_ERR_PARSE;
    }
  }
  if ((flags & AC_F_GE0) && tmpd < 0.0) {
    return AC_ERR_ELIMIT;
  }
  if ((flags & AC_F_GE1) && tmpd < 1.0) {
    return AC_ERR_ELIMIT;
  }
  MAYBE_ADVANCE();
  *d = tmpd;
  return AC_OK;
}

int ArgsCursor::GetRString(RedisModuleString **s, int flags) {
  assert(type == AC_TYPE_RSTRING);
  if (offset == argc) {
    return AC_ERR_NOARG;
  }
  *s = CURRENT();
  MAYBE_ADVANCE();
  return AC_OK;
}

int ArgsCursor::GetString(const char **s, size_t *n, int flags) {
  if (offset == argc) {
    return AC_ERR_NOARG;
  }
  if (type == AC_TYPE_RSTRING) {
    *s = RedisModule_StringPtrLen(CURRENT(), n);
  } else {
    *s = CURRENT();
    if (n) {
      if (type == AC_TYPE_SDS) {
        *n = sdslen((const sds)*s);
      } else {
        *n = strlen(*s);
      }
    }
  }
  MAYBE_ADVANCE();
  return AC_OK;
}

// Gets the string (and optionally the length). If the string does not exist,
// it returns NULL. Used when caller is sure the arg exists

const char *ArgsCursor::GetStringNC(size_t *len) {
  const char *s = NULL;
  if (GetString(&s, len, 0) != AC_OK) {
    return NULL;
  }
  return s;
}

// Read the argument list in the format of
// <NUM_OF_ARGS> <ARG[1]> <ARG[2]> .. <ARG[NUM_OF_ARGS]>
// The output is stored in dest which contains a sub-array of argv/argc

int ArgsCursor::GetVarArgs(ArgsCursor *dst) {
  unsigned nargs;
  int rv = GetUnsigned(&nargs, 0);
  if (rv != AC_OK) {
    return rv;
  }
  return GetSlice(dst, nargs);
}

// Consume the next <n> arguments and place them in <dest>

int ArgsCursor::GetSlice(ArgsCursor *ac, ArgsCursor *dst, size_t n) {
  if (n > ac->NumRemaining()) {
    return AC_ERR_NOARG;
  }

  dst->objs = ac->objs + ac->offset;
  dst->argc = n;
  dst->offset = 0;
  dst->type = ac->type;
  ac->AdvanceBy(n);
  return 0;
}

// למה זה סטטי
static int parseSingleSpec(ArgsCursor *ac, ACArgSpec *spec) {
  switch (spec->type) {
    case AC_ARGTYPE_BOOLFLAG:
      *(int *)spec->target = 1;
      return AC_OK;
    case AC_ARGTYPE_BITFLAG:
      *(uint32_t *)(spec->target) |= spec->slicelen;
      return AC_OK;
    case AC_ARGTYPE_UNFLAG:
      *(uint32_t *)spec->target &= ~spec->slicelen;
      return AC_OK;
    case AC_ARGTYPE_DOUBLE:
      return ac->GetDouble(spec->target, spec->intflags);
    case AC_ARGTYPE_INT:
      return ac->GetInt(spec->target, spec->intflags);
    case AC_ARGTYPE_LLONG:
      return ac->GetLongLong( spec->target, spec->intflags);
    case AC_ARGTYPE_ULLONG:
      return ac->GetUnsignedLongLong( spec->target, spec->intflags);
    case AC_ARGTYPE_UINT:
      return ac->GetUnsigned(spec->target, spec->intflags);
    case AC_ARGTYPE_STRING:
      return ac->GetString(spec->target, spec->len, 0);
    case AC_ARGTYPE_RSTRING:
      return ac->GetRString(spec->target, 0);
    case AC_ARGTYPE_SUBARGS:
      return ac->GetVarArgs(spec->target);
    case AC_ARGTYPE_SUBARGS_N:
      return ac->GetSlice(spec->target, spec->slicelen);
    default:
      fprintf(stderr, "Unknown type");
      abort();
  }
}

/**
 * Utilizes the argument cursor to traverse a list of known argument specs. This
 * function will return:
 * - AC_OK if the argument parsed successfully
 * - AC_ERR_ENOENT if an argument not mentioned in `specs` is encountered.
 * - Any other error is assumed to be a parser error, in which the argument exists
 *   but did not meet constraints of the type
 *
 * Note that ENOENT is not a 'hard' error. It simply means that the argument
 * was not provided within the list. This may be intentional if, for example,
 * it requires complex processing.
 */
int ArgsCursor::ParseArgSpec(ACArgSpec *specs, ACArgSpec **errSpec) {
  const char *s = NULL;
  size_t n;
  int rv;

  if (errSpec) {
    *errSpec = NULL;
  }

  while (!IsAtEnd()) {
    if ((rv = GetString(&s, &n, AC_F_NOADVANCE) != AC_OK)) {
      return rv;
    }
    ACArgSpec *cur = specs;

    for (; cur->name != NULL; cur++) {
      if (n != strlen(cur->name)) {
        continue;
      }
      if (!strncasecmp(cur->name, s, n)) {
        break;
      }
    }

    if (cur->name == NULL) {
      return AC_ERR_ENOENT;
    }

    Advance();
    if ((rv = parseSingleSpec(this, cur)) != AC_OK) {
      if (errSpec) {
        *errSpec = cur;
      }
      return rv;
    }
  }
  return AC_OK;
}
