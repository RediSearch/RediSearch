/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "reducer.h"

typedef struct {
  const char *name;
  ReducerFactory fn;
} FuncEntry;

// Static registry of all builtin reducers - no runtime registration needed
static const FuncEntry globalRegistry[] = {
  {"COUNT", RDCRCount_New},
  {"SUM", RDCRSum_New},
  {"TOLIST", RDCRToList_New},
  {"MIN", RDCRMin_New},
  {"MAX", RDCRMax_New},
  {"AVG", RDCRAvg_New},
  {"COUNT_DISTINCT", RDCRCountDistinct_New},
  {"COUNT_DISTINCTISH", RDCRCountDistinctish_New},
  {"QUANTILE", RDCRQuantile_New},
  {"STDDEV", RDCRStdDev_New},
  {"FIRST_VALUE", RDCRFirstValue_New},
  {"RANDOM_SAMPLE", RDCRRandomSample_New},
  {"HLL", RDCRHLL_New},
  {"HLL_SUM", RDCRHLLSum_New}
};

#define REGISTRY_SIZE 14
static_assert(sizeof(globalRegistry) == sizeof(FuncEntry) * REGISTRY_SIZE);

ReducerFactory RDCR_GetFactory(const char *name) {
  for (size_t ii = 0; ii < REGISTRY_SIZE; ++ii) {
    if (!strcasecmp(globalRegistry[ii].name, name)) {
      return globalRegistry[ii].fn;
    }
  }
  return NULL;
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