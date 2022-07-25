
#include "aggregate/reducer.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0
typedef struct {
  Reducer base;
  double pct;
  unsigned resolution;
} QTLReducer;

//---------------------------------------------------------------------------------------------

static void *quantileNewInstance(Reducer *parent) {
  QTLReducer *qt = (QTLReducer *)parent;
  return new QuantileStream(&qt->pct, 0, qt->resolution);
}
#endif

//---------------------------------------------------------------------------------------------

int RDCRQuantile::Add(const RLookupRow *row) {
  double d;
  RSValue *v = row->GetItem(srckey);
  if (!v) {
    return 1;
  }

  if (v->t != RSValue_Array) {
    if (v->ToNumber(&d)) {
      data.qs->Insert(d);
    }
  } else {
    uint32_t sz = v->ArrayLen();
    for (uint32_t i = 0; i < sz; i++) {
      if (v->ArrayItem(i)->ToNumber(&d)) {
        data.qs->Insert(d);
      }
    }
  }
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRQuantile::Finalize() {
  double value = data.qs->Query(pct);
  return RS_NumVal(value);
}

//---------------------------------------------------------------------------------------------

RDCRQuantile::RDCRQuantile(const ReducerOptions *options) {
  resolution = 500;  // Fixed, i guess?

  if (!options->GetKey(&srckey)) {
    throw Error("RDCRQuantile: no key found");
  }

  int rv;
  if ((rv = options->args->GetDouble(&pct, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(options->status, options->name, rv);
    throw Error("RDCRQuantile: Bad arguments");
  }
  if (!(pct >= 0 && pct <= 1.0)) {
    QERR_MKBADARGS_FMT(options->status, "Percentage must be between 0.0 and 1.0");
    throw Error("Percentage must be between 0.0 and 1.0");
  }

  if (!options->args->IsAtEnd()) {
    if ((rv = options->args->GetUnsigned(&resolution, 0)) != AC_OK) {
      QERR_MKBADARGS_AC(options->status, "<resolution>", rv);
      throw Error("RDCRQuantile: Bad arguments");
    }
    if (resolution < 1 || resolution > MAX_SAMPLE_SIZE) {
      QERR_MKBADARGS_FMT(options->status, "Invalid resolution");
      throw Error("RDCRQuantile: Invalid resolution");
    }
  }

  if (!options->EnsureArgsConsumed()) {
    throw Error("RDCRQuantile: ");
  }

  data.qs = new QuantStream(&pct, 0, resolution);

  reducerId = REDUCER_T_QUANTILE;
}

///////////////////////////////////////////////////////////////////////////////////////////////
