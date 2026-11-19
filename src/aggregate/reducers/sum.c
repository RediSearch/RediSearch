/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <aggregate/reducer.h>
#include <stdbool.h>

#include "reducers_ffi.h"

static Reducer *newReducerCommon(const ReducerOptions *options, SumReducerMode mode) {
  const RLookupKey *srckey;
  if (!ReducerOpts_GetValuesKey(options, &srckey)) {
    return NULL;
  }
  return SumReducer_Create(srckey, mode);
}

Reducer *RDCRSum_New(const ReducerOptions *options) {
  return newReducerCommon(options, SumReducerMode_Sum);
}

Reducer *RDCRAvg_New(const ReducerOptions *options) {
  return newReducerCommon(options, SumReducerMode_Average);
}

Reducer *RDCRCountNumbers_New(const ReducerOptions *options) {
  // Only shards run it, as half of a distributed AVG (see dist_plan.cpp).
  if (!ReducerOpts_IsInternal(options)) {
    QueryError_SetWithUserDataFmt(options->status, QUERY_ERROR_CODE_NO_REDUCER, "No such reducer",
                                  ": %s", options->name);
    return NULL;
  }
  return newReducerCommon(options, SumReducerMode_Count);
}
