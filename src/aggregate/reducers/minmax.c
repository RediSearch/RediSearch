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

static Reducer *newMinMax(const ReducerOptions *options, bool isMax) {
  const RLookupKey *srckey;
  if (!ReducerOpts_GetKey(options, &srckey)) {
    return NULL;
  }
  return MinMaxReducer_Create(srckey, isMax);
}

Reducer *RDCRMin_New(const ReducerOptions *options) {
  return newMinMax(options, false);
}

Reducer *RDCRMax_New(const ReducerOptions *options) {
  return newMinMax(options, true);
}
