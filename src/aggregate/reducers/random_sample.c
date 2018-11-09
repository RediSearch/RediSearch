#include <aggregate/reducer.h>

typedef struct {
  Reducer base;
  size_t len;
} RSMPLReducer;

typedef struct {
  size_t seen;  // how many items we've seen
  RSValue *samples[];
} rsmplCtx;

static void *sampleNewInstance(Reducer *base) {
  RSMPLReducer *r = (RSMPLReducer *)base;
  rsmplCtx *ctx = Reducer_BlkAlloc(base, sizeof(*ctx) + r->len * sizeof(RSValue *), 10000);
  ctx->seen = 0;
  return ctx;
}

static int sampleAdd(Reducer *rbase, void *ctx, const RLookupRow *srcrow) {
  RSMPLReducer *r = (RSMPLReducer *)rbase;
  rsmplCtx *sc = ctx;
  RSValue *v = RLookup_GetItem(rbase->srckey, srcrow);
  if (!v) {
    return 1;
  }

  if (sc->seen < r->len) {
    sc->samples[sc->seen++] = RSValue_IncrRef(v);
  } else {
    int i = rand() % sc->seen++;
    if (i < r->len) {
      RSVALUE_REPLACE(sc->samples + i, v);
    }
  }
  return 1;
}

static RSValue *sampleFinalize(Reducer *rbase, void *ctx) {
  rsmplCtx *sc = ctx;
  RSMPLReducer *r = (RSMPLReducer *)rbase;
  size_t len = MIN(r->len, sc->seen);
  RSValue *ret = RSValue_NewArrayEx(sc->samples, len, RSVAL_ARRAY_ALLOC);
  return ret;
}

static void sampleFreeInstance(Reducer *rbase, void *p) {
  rsmplCtx *sc = p;
  RSMPLReducer *r = (RSMPLReducer *)rbase;
  size_t len = MIN(r->len, sc->seen);
  for (size_t ii = 0; ii < len; ++ii) {
    RSValue_Decref(sc->samples[ii]);
  }
}

Reducer *RDCRRandomSample_New(const ReducerOptions *options) {
  RSMPLReducer *ret = calloc(1, sizeof(*ret));
  if (!ReducerOptions_GetKey(options, &ret->base.srckey)) {
    free(ret);
    return NULL;
  }
  // Get the number of samples..
  unsigned samplesize;
  int rc = AC_GetUnsigned(options->args, &samplesize, 0);
  if (rc != AC_OK) {
    QERR_MKBADARGS_AC(options->status, "<sample size>", rc);
    free(ret);
    return NULL;
  }
  if (samplesize > MAX_SAMPLE_SIZE) {
    QERR_MKBADARGS_FMT(options->status, "Sample size too large");
    free(ret);
    return NULL;
  }
  ret->len = samplesize;
  Reducer *rbase = &ret->base;
  rbase->Add = sampleAdd;
  rbase->Finalize = sampleFinalize;
  rbase->Free = Reducer_GenericFree;
  rbase->FreeInstance = sampleFreeInstance;
  rbase->NewInstance = sampleNewInstance;
  return rbase;
}