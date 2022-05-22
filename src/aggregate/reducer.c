#include "reducer.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct FuncEntry {
  const char *name;
  ReducerFactory fn;
};

static FuncEntry *globalRegistry = NULL;

//---------------------------------------------------------------------------------------------

void RDCR_RegisterFactory(const char *name, ReducerFactory factory) {
  FuncEntry ent = {.name = name, .fn = factory};
  FuncEntry *tail = array_ensure_tail(&globalRegistry, FuncEntry);
  *tail = ent;
}

//---------------------------------------------------------------------------------------------

static bool isBuiltinsRegistered = false;

ReducerFactory RDCR_GetFactory(const char *name) {
  if (!isBuiltinsRegistered) {
    isBuiltinsRegistered = true;
    RDCR_RegisterBuiltins();
  }
  size_t n = array_len(globalRegistry);
  for (size_t ii = 0; ii < n; ++ii) {
    if (!strcasecmp(globalRegistry[ii].name, name)) {
      return globalRegistry[ii].fn;
    }
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

#define RDCR_XBUILTIN(X)                       \
  X(RDCRCount, "COUNT")                        \
  X(RDCRSum, "SUM")                            \
  X(RDCRToList, "TOLIST")                      \
  X(RDCRMin, "MIN")                            \
  X(RDCRMax, "MAX")                            \
  X(RDCRAvg, "AVG")                            \
  X(RDCRCountDistinct, "COUNT_DISTINCT")       \
  X(RDCRCountDistinctish, "COUNT_DISTINCTISH") \
  X(RDCRQuantile, "QUANTILE")                  \
  X(RDCRStdDev, "STDDEV")                      \
  X(RDCRFirstValue, "FIRST_VALUE")             \
  X(RDCRRandomSample, "RANDOM_SAMPLE")         \
  X(RDCRHLL, "HLL")                            \
  X(RDCRHLLSum, "HLL_SUM")

//---------------------------------------------------------------------------------------------

void RDCR_RegisterBuiltins() {
#define X(fn, n) RDCR_RegisterFactory(n, fn);
  RDCR_XBUILTIN(X);
#undef X
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Utility function to read the next argument as a lookup key.
// This advances the args variable (ReducerOptions::options) by one.
// If the lookup fails, the appropriate error code is stored in the status within the options.

bool ReducerOptions::GetKey(const RLookupKey **out) const {
  ArgsCursor *ac = args;
  const char *s;
  if (AC_GetString(ac, &s, NULL, 0) != AC_OK) {
    QERR_MKBADARGS_FMT(status, "Missing arguments for %s", name);
    return false;
  }

  // Get the input key
  if (*s == '@') {
    s++;
  }
  *out = srclookup->GetKey(s, RLOOKUP_F_HIDDEN);
  if (!*out) {
    status->SetErrorFmt(QUERY_ENOPROPKEY, "Property `%s` not present in document or pipeline", s);
    return false;
  }
  return true;
}

//---------------------------------------------------------------------------------------------

bool ReducerOptions::EnsureArgsConsumed() const {
  if (AC_NumRemaining(args)) {
    status->FmtUnknownArg(args, name);
    return false;
  }
  return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// This helper function ensures that all of a reducer's arguments are consumed.
// Otherwise, an error is raised to the user.

void *Reducer::BlkAlloc(size_t elemsz, size_t blksz) {
  return alloc.Alloc(elemsz, blksz);
}

//---------------------------------------------------------------------------------------------

#if 0

// Format a function name in the form of s(arg). Returns a pointer for use with 'free'
static inline char *FormatAggAlias(const char *alias, const char *fname, const char *propname) {
  if (alias) {
    return rm_strdup(alias);
  }

  if (!propname || *propname == 0) {
    return rm_strdup(fname);
  }

  char *s = NULL;
  rm_asprintf(&s, "%s(%s)", fname, propname);
  return s;
}

#endif // 0

///////////////////////////////////////////////////////////////////////////////////////////////
