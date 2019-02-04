#include "reducer.h"

typedef struct {
  const char *name;
  ReducerFactory fn;
} FuncEntry;

static FuncEntry *globalRegistry = NULL;

void RDCR_RegisterFactory(const char *name, ReducerFactory factory) {
  FuncEntry ent = {.name = name, .fn = factory};
  FuncEntry *tail = array_ensure_tail(&globalRegistry, FuncEntry);
  *tail = ent;
}

static int isBuiltinsRegistered = 0;

ReducerFactory RDCR_GetFactory(const char *name) {
  if (!isBuiltinsRegistered) {
    isBuiltinsRegistered = 1;
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

#define RDCR_XBUILTIN(X)                           \
  X(RDCRCount_New, "COUNT")                        \
  X(RDCRSum_New, "SUM")                            \
  X(RDCRToList_New, "TOLIST")                      \
  X(RDCRMin_New, "MIN")                            \
  X(RDCRMax_New, "MAX")                            \
  X(RDCRAvg_New, "AVG")                            \
  X(RDCRCountDistinct_New, "COUNT_DISTINCT")       \
  X(RDCRCountDistinctish_New, "COUNT_DISTINCTISH") \
  X(RDCRQuantile_New, "QUANTILE")                  \
  X(RDCRStdDev_New, "STDDEV")                      \
  X(RDCRFirstValue_New, "FIRST_VALUE")             \
  X(RDCRRandomSample_New, "RANDOM_SAMPLE")         \
  X(RDCRHLL_New, "HLL")                            \
  X(RDCRHLLSum_New, "HLL_SUM")

void RDCR_RegisterBuiltins(void) {
#define X(fn, n) RDCR_RegisterFactory(n, fn);
  RDCR_XBUILTIN(X);
#undef X
}

int ReducerOpts_GetKey(const ReducerOptions *options, const RLookupKey **out) {
  ArgsCursor *ac = options->args;
  const char *s;
  if (AC_GetString(ac, &s, NULL, 0) != AC_OK) {
    QERR_MKBADARGS_FMT(options->status, "Missing arguments for %s", options->name);
    return 0;
  }

  // Get the input key..
  if (*s == '@') {
    s++;
  }
  *out = RLookup_GetKey(options->srclookup, s, RLOOKUP_F_HIDDEN);
  if (!*out) {
    QueryError_SetErrorFmt(options->status, QUERY_ENOPROPKEY,
                           "Property `%s` not present in document or pipeline", s);
    return 0;
  }
  return 1;
}

int ReducerOpts_EnsureArgsConsumed(const ReducerOptions *options) {
  if (AC_NumRemaining(options->args)) {
    QueryError_FmtUnknownArg(options->status, options->args, options->name);
    return 0;
  }
  return 1;
}

void *Reducer_BlkAlloc(Reducer *r, size_t elemsz, size_t blksz) {
  return BlkAlloc_Alloc(&r->alloc, elemsz, blksz);
}