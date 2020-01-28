#include <aggregate/reducer.h>
#include "util/quantile.h"

typedef struct {
  Reducer base;
  double pct;
  unsigned resolution;
} QTLReducer;

static void *quantileNewInstance(Reducer *parent) {
  QTLReducer *qt = (QTLReducer *)parent;
  return NewQuantileStream(&qt->pct, 0, qt->resolution);
}

static int quantileAdd(Reducer *rbase, void *ctx, const RLookupRow *row) {
  double d;
  QTLReducer *qt = (QTLReducer *)rbase;
  QuantStream *qs = ctx;
  RSValue *v = RLookup_GetItem(rbase->srckey, row);
  if (!v) {
    return 1;
  }

  if (v->t != RSValue_Array) {
    if (RSValue_ToNumber(v, &d)) {
      QS_Insert(qs, d);
    }
  } else {
    uint32_t sz = RSValue_ArrayLen(v);
    for (uint32_t i = 0; i < sz; i++) {
      if (RSValue_ToNumber(RSValue_ArrayItem(v, i), &d)) {
        QS_Insert(qs, d);
      }
    }
  }
  return 1;
}

static RSValue *quantileFinalize(Reducer *r, void *ctx) {
  QuantStream *qs = ctx;
  QTLReducer *qt = (QTLReducer *)r;
  double value = QS_Query(qs, qt->pct);
  return RS_NumVal(value);
}

static void quantileFreeInstance(Reducer *unused, void *p) {
  QS_Free(p);
}

Reducer *RDCRQuantile_New(const ReducerOptions *options) {
  QTLReducer *r = rm_calloc(1, sizeof(*r));
  r->resolution = 500;  // Fixed, i guess?

  if (!ReducerOptions_GetKey(options, &r->base.srckey)) {
    goto error;
  }
  int rv;
  if ((rv = AC_GetDouble(options->args, &r->pct, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(options->status, options->name, rv);
    goto error;
  }
  if (!(r->pct >= 0 && r->pct <= 1.0)) {
    QERR_MKBADARGS_FMT(options->status, "Percentage must be between 0.0 and 1.0");
    goto error;
  }

  if (!AC_IsAtEnd(options->args)) {
    if ((rv = AC_GetUnsigned(options->args, &r->resolution, 0)) != AC_OK) {
      QERR_MKBADARGS_AC(options->status, "<resolution>", rv);
      goto error;
    }
    if (r->resolution < 1 || r->resolution > MAX_SAMPLE_SIZE) {
      QERR_MKBADARGS_FMT(options->status, "Invalid resolution");
      goto error;
    }
  }

  if (!ReducerOpts_EnsureArgsConsumed(options)) {
    goto error;
  }

  r->base.NewInstance = quantileNewInstance;
  r->base.Add = quantileAdd;
  r->base.Free = Reducer_GenericFree;
  r->base.FreeInstance = quantileFreeInstance;
  r->base.Finalize = quantileFinalize;
  return &r->base;

error:
  rm_free(r);
  return NULL;
}
