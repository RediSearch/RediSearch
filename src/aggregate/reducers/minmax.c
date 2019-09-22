#include <aggregate/reducer.h>
#include <float.h>

typedef enum { Minmax_Min = 1, Minmax_Max = 2 } MinmaxMode;

typedef struct {
  const RLookupKey *srckey;
  double val;
  MinmaxMode mode;
  size_t numMatches;
} minmaxCtx;

typedef struct {
  Reducer base;
  MinmaxMode mode;
} MinmaxReducer;

static void *minmaxNewInstance(Reducer *rbase) {
  MinmaxReducer *r = (MinmaxReducer *)rbase;
  minmaxCtx *m = BlkAlloc_Alloc(&rbase->alloc, sizeof(*m), 1024);
  m->mode = r->mode;
  m->srckey = r->base.srckey;
  m->numMatches = 0;
  if (m->mode == Minmax_Min) {
    m->val = DBL_MAX;
  } else if (m->mode == Minmax_Max) {
    m->val = DBL_MIN;
  } else {
    m->val = 0;
  }
  return m;
}

static int minmaxAdd(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  minmaxCtx *m = ctx;
  double val;
  RSValue *v = RLookup_GetItem(m->srckey, srcrow);
  if (!RSValue_ToNumber(v, &val)) {
    return 1;
  }

  if (m->mode == Minmax_Max && val > m->val) {
    m->val = val;
  } else if (m->mode == Minmax_Min && val < m->val) {
    m->val = val;
  }

  m->numMatches++;
  return 1;
}

static RSValue *minmaxFinalize(Reducer *parent, void *instance) {
  minmaxCtx *ctx = instance;
  return RS_NumVal(ctx->numMatches ? ctx->val : 0);
}

static Reducer *newMinMax(const ReducerOptions *options, MinmaxMode mode) {
  MinmaxReducer *r = rm_calloc(1, sizeof(*r));
  if (!ReducerOpts_GetKey(options, &r->base.srckey)) {
    rm_free(r);
    return NULL;
  }
  r->base.NewInstance = minmaxNewInstance;
  r->base.Add = minmaxAdd;
  r->base.Finalize = minmaxFinalize;
  r->base.Free = Reducer_GenericFree;
  r->mode = mode;
  return &r->base;
}

Reducer *RDCRMin_New(const ReducerOptions *options) {
  return newMinMax(options, Minmax_Min);
}

Reducer *RDCRMax_New(const ReducerOptions *options) {
  return newMinMax(options, Minmax_Max);
}
