/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <aggregate/reducer.h>
#include <util/block_alloc.h>

typedef struct {
  size_t count;
} counterData;

#define COUNTER_BLOCK_SIZE 32 * sizeof(counterData)

static void *counterNewInstance(Reducer *r) {
  counterData *dd = BlkAlloc_Alloc(&r->alloc, sizeof(counterData), COUNTER_BLOCK_SIZE);
  dd->count = 0;
  return dd;
}

static int counterAdd(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  ((counterData *)ctx)->count++;
  return 1;
}

static RSValue *counterFinalize(Reducer *r, void *instance) {
  counterData *dd = instance;
  return RS_NumVal(dd->count);
}

Reducer *RDCRCount_New(const ReducerOptions *options) {
  if (options->args->argc != 0) {
    QueryError_SetError(options->status, QUERY_EBADATTR, "Count accepts 0 values only");
    return NULL;
  }
  Reducer *r = rm_calloc(1, sizeof(*r));
  r->Add = counterAdd;
  r->Finalize = counterFinalize;
  r->Free = Reducer_GenericFree;
  r->NewInstance = counterNewInstance;
  return r;
}
