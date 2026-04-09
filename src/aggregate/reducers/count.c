/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <aggregate/reducer.h>
#include <util/block_alloc.h>
#include "reducers_rs.h"

Reducer *RDCRCount_New(const ReducerOptions *options) {
  if (options->args->argc != 0) {
    QueryError_SetError(options->status, QUERY_ERROR_CODE_BAD_ATTR, "Count accepts 0 values only");
    return NULL;
  }
  Reducer *r = rm_calloc(1, sizeof(*r));
  r->Add = counterAdd;
  r->Finalize = counterFinalize;
  r->Free = Reducer_GenericFree;
  r->NewInstance = counterNewInstance;
  r->FreeInstance = counterFreeInstance;
  return r;
}
