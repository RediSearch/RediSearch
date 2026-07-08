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

Reducer *RDCRStdDev_New(const ReducerOptions *options) {
  const RLookupKey *srckey;
  if (!ReducerOptions_GetKey(options, &srckey)) {
    return NULL;
  }
  return StdDevReducer_Create(srckey);
}
