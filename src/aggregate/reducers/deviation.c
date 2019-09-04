#include <aggregate/reducer.h>
#include <math.h>

typedef struct {
  const RLookupKey *srckey;
  size_t n;
  double oldM, newM, oldS, newS;
} devCtx;

#define BLOCK_SIZE 1024 * sizeof(devCtx)

static void *stddevNewInstance(Reducer *rbase) {
  devCtx *dctx = BlkAlloc_Alloc(&rbase->alloc, sizeof(*dctx), BLOCK_SIZE);
  memset(dctx, 0, sizeof(*dctx));
  dctx->srckey = rbase->srckey;
  return dctx;
}

static void stddevAddInternal(devCtx *dctx, double d) {
  // https://www.johndcook.com/blog/standard_deviation/
  dctx->n++;
  if (dctx->n == 1) {
    dctx->oldM = dctx->newM = d;
    dctx->oldS = 0.0;
  } else {
    dctx->newM = dctx->oldM + (d - dctx->oldM) / dctx->n;
    dctx->newS = dctx->oldS + (d - dctx->oldM) * (d - dctx->newM);

    // set up for next iteration
    dctx->oldM = dctx->newM;
    dctx->oldS = dctx->newS;
  }
}

static int stddevAdd(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  devCtx *dctx = ctx;
  double d;
  RSValue *v = RLookup_GetItem(dctx->srckey, srcrow);
  if (v) {
    if (v->t != RSValue_Array) {
      if (RSValue_ToNumber(v, &d)) {
        stddevAddInternal(dctx, d);
      }
    } else {
      uint32_t sz = RSValue_ArrayLen(v);
      for (uint32_t i = 0; i < sz; i++) {
        if (RSValue_ToNumber(RSValue_ArrayItem(v, i), &d)) {
          stddevAddInternal(dctx, d);
        }
      }
    }
  }
  return 1;
}

static RSValue *stddevFinalize(Reducer *parent, void *instance) {
  devCtx *dctx = instance;
  double variance = ((dctx->n > 1) ? dctx->newS / (dctx->n - 1) : 0.0);
  double stddev = sqrt(variance);
  return RS_NumVal(stddev);
}

Reducer *RDCRStdDev_New(const ReducerOptions *options) {
  Reducer *r = rm_calloc(1, sizeof(*r));
  if (!ReducerOptions_GetKey(options, &r->srckey)) {
    rm_free(r);
    return NULL;
  }
  r->Add = stddevAdd;
  r->Finalize = stddevFinalize;
  r->Free = Reducer_GenericFree;
  r->NewInstance = stddevNewInstance;
  r->reducerId = REDUCER_T_STDDEV;
  return r;
}
