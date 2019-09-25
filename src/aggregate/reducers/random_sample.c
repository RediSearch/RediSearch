#include <aggregate/reducer.h>

typedef struct {
  Reducer base;
  size_t len;
} RSMPLReducer;

typedef struct {
  size_t seen;  // how many items we've seen
  RSValue *samplesArray;
} rsmplCtx;

static void *sampleNewInstance(Reducer *base) {
  RSMPLReducer *r = (RSMPLReducer *)base;
  size_t blocksize = MAX(10000, sizeof(rsmplCtx) + r->len * sizeof(RSValue *));
  rsmplCtx *ctx = Reducer_BlkAlloc(base, sizeof(*ctx) + r->len * sizeof(RSValue *), blocksize);
  ctx->seen = 0;
  ctx->samplesArray = RSValue_NewArrayEx(NULL, r->len, 0);
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
    RSVALUE_ARRELEM(sc->samplesArray, sc->seen) = RSValue_IncrRef(v);
    RSVALUE_ARRLEN(sc->samplesArray)++;
    assert(RSVALUE_ARRLEN(sc->samplesArray) <= r->len);
  } else {
    size_t i = rand() % (sc->seen + 1);
    if (i < r->len) {
      RSVALUE_REPLACE(&RSVALUE_ARRELEM(sc->samplesArray, i), v);
    }
  }
  sc->seen++;
  return 1;
}

static RSValue *sampleFinalize(Reducer *rbase, void *ctx) {
  rsmplCtx *sc = ctx;
  RSMPLReducer *r = (RSMPLReducer *)rbase;
  size_t len = MIN(r->len, sc->seen);
  RSValue *ret = sc->samplesArray;
  sc->samplesArray = NULL;
  return ret;
}

static void sampleFreeInstance(Reducer *rbase, void *p) {
  rsmplCtx *sc = p;
  RSMPLReducer *r = (RSMPLReducer *)rbase;
  if (sc->samplesArray) {
    RSValue_Decref(sc->samplesArray);
  }
}

Reducer *RDCRRandomSample_New(const ReducerOptions *options) {
  RSMPLReducer *ret = rm_calloc(1, sizeof(*ret));
  if (!ReducerOptions_GetKey(options, &ret->base.srckey)) {
    rm_free(ret);
    return NULL;
  }
  // Get the number of samples..
  unsigned samplesize;
  int rc = AC_GetUnsigned(options->args, &samplesize, 0);
  if (rc != AC_OK) {
    QERR_MKBADARGS_AC(options->status, "<sample size>", rc);
    rm_free(ret);
    return NULL;
  }
  if (samplesize > MAX_SAMPLE_SIZE) {
    QERR_MKBADARGS_FMT(options->status, "Sample size too large");
    rm_free(ret);
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
