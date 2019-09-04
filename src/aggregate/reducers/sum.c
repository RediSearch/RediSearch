#include <aggregate/reducer.h>

typedef struct {
  size_t count;
  double total;
} sumCtx;

typedef struct {
  Reducer base;
  int isAvg;
  const RLookupKey *srckey;
} SumReducer;

#define BLOCK_SIZE 32 * sizeof(sumCtx)

static void *sumNewInstance(Reducer *r) {
  sumCtx *ctx = BlkAlloc_Alloc(&r->alloc, sizeof(*ctx), BLOCK_SIZE);
  ctx->count = 0;
  ctx->total = 0;
  return ctx;
}

static int sumAdd(Reducer *baseparent, void *instance, const RLookupRow *row) {
  sumCtx *ctr = instance;
  const SumReducer *parent = (const SumReducer *)baseparent;
  ctr->count++;
  const RSValue *v = RLookup_GetItem(parent->srckey, row);
  if (v && v->t == RSValue_Number) {
    ctr->total += v->numval;
  } else {  // try to convert value to number
    double d = 0;
    if (RSValue_ToNumber(v, &d)) {
      ctr->total += d;
    }
  }
  return 1;
}

static RSValue *sumFinalize(Reducer *baseparent, void *instance) {
  sumCtx *ctr = instance;
  SumReducer *parent = (SumReducer *)baseparent;
  double v = 0;
  if (parent->isAvg) {
    if (ctr->count) {
      v = ctr->total / ctr->count;
    }
  } else {
    v = ctr->total;
  }
  return RS_NumVal(v);
}

static Reducer *newReducerCommon(const ReducerOptions *options, int isAvg) {
  SumReducer *r = rm_calloc(1, sizeof(*r));
  if (!ReducerOpts_GetKey(options, &r->srckey)) {
    rm_free(r);
    return NULL;
  }
  r->base.NewInstance = sumNewInstance;
  r->base.Add = sumAdd;
  r->base.Finalize = sumFinalize;
  r->base.Free = Reducer_GenericFree;
  r->isAvg = isAvg;
  return &r->base;
}

Reducer *RDCRSum_New(const ReducerOptions *options) {
  return newReducerCommon(options, 0);
}

Reducer *RDCRAvg_New(const ReducerOptions *options) {
  return newReducerCommon(options, 1);
}
