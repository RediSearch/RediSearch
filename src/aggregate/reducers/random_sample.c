/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <aggregate/reducer.h>
#include "reducers_ffi.h"

Reducer *RDCRRandomSample_New(const ReducerOptions *options) {
  const RLookupKey *srckey;
  if (!ReducerOptions_GetKey(options, &srckey)) {
    return NULL;
  }
  // Get the number of samples..
  unsigned samplesize;
  int rc = AC_GetUnsigned(options->args, &samplesize, 0);
  if (rc != AC_OK) {
    QERR_MKBADARGS_AC(options->status, "<sample size>", rc);
    return NULL;
  }
  if (samplesize > MAX_SAMPLE_SIZE) {
    QueryError_SetError(options->status, QUERY_ERROR_CODE_PARSE_ARGS, "Sample size too large");
    return NULL;
  }
  return RandomSampleReducer_Create(srckey, samplesize);
}
