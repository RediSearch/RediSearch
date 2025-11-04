/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "reducer.h"
#include "util/misc.h"

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
  size_t len;
  if (AC_GetString(ac, &s, &len, 0) != AC_OK) {
    QueryError_SetWithUserDataFmt(options->status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing arguments", " for %s", options->name);
    return 0;
  }

  // Get the input key..
  const char *keyName = ExtractKeyName(s, &len, options->status, options->strictPrefix, options->name);
  if (!keyName) {
    return 0;
  }
  *out = RLookup_GetKey_Read(options->srclookup, keyName, RLOOKUP_F_HIDDEN);
  if (!*out) {
    if (options->loadKeys) {
      *out = RLookup_GetKey_Load(options->srclookup, keyName, keyName, RLOOKUP_F_HIDDEN);
      *options->loadKeys = array_ensure_append_1(*options->loadKeys, *out);
    }
    // We currently allow implicit loading only for known fields from the schema.
    // If we can't load keys, or the key we loaded is not in the schema, we fail.
    if (!options->loadKeys || !((*out)->flags & RLOOKUP_F_SCHEMASRC)) {
      QueryError_SetWithUserDataFmt(options->status, QUERY_ERROR_CODE_NO_PROP_KEY, "Property is not present in document or pipeline", ": `%s`", keyName);
      return 0;
    }
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
