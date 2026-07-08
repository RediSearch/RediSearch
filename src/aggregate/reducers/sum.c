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

static Reducer *newReducerCommon(const ReducerOptions *options, bool isAvg) {
  const RLookupKey *srckey;
  if (!ReducerOpts_GetKey(options, &srckey)) {
    return NULL;
  }
  return SumReducer_Create(srckey, isAvg);
}

Reducer *RDCRSum_New(const ReducerOptions *options) {
  return newReducerCommon(options, false);
}

Reducer *RDCRAvg_New(const ReducerOptions *options) {
  return newReducerCommon(options, true);
}
