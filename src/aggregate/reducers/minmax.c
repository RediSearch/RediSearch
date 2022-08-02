
#include "aggregate/reducer.h"
#include <float.h>

///////////////////////////////////////////////////////////////////////////////////////////////

int RDCRMin::Add(const RLookupRow *srcrow) {
  double val;
  RSValue *v = srcrow->GetItem(srckey);
  if (!v->ToNumber(&val)) {
    return 1;
  }

  if (val < data.val) {
    data.val = val;
  }

  data.numMatches++;
  return 1;
}

int RDCRMax::Add(const RLookupRow *srcrow) {
  double val;
  RSValue *v = srcrow->GetItem(srckey);
  if (!v->ToNumber(&val)) {
    return 1;
  }

  if (val > data.val) {
    data.val = val;
  }

  data.numMatches++;
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRMin::Finalize() {
  return RS_NumVal(data.numMatches ? data.val : 0);
}

RSValue *RDCRMax::Finalize() {
  return RS_NumVal(data.numMatches ? data.val : 0);
}

//---------------------------------------------------------------------------------------------

RDCRMin::RDCRMin(const ReducerOptions *options) {
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRMin: no key found");
  }

  data.numMatches = 0;
  data.val = DBL_MIN;

  reducerId = REDUCER_T_MIN;
}

//---------------------------------------------------------------------------------------------

RDCRMax::RDCRMax(const ReducerOptions *options) {
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRMax: no key found");
  }

  data.numMatches = 0;
  data.val = DBL_MAX;

  reducerId = REDUCER_T_MAX;
}

///////////////////////////////////////////////////////////////////////////////////////////////
