
#include "aggregate/reducer.h"
#include <float.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#ifdef 0
enum MinMaxMode { Minmax_Min = 1, Minmax_Max = 2 };

struct MinMaxReducer : public Reducer {
  MinMaxMode mode;
  double val;
  size_t numMatches;

  MinMaxReducer(MinMaxMode mode_);

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};

//---------------------------------------------------------------------------------------------

MinMaxReducer::MinMaxReducer(MinMaxMode mode_) {
  numMatches = 0;
  mode = mode_;

  if (mode == Minmax_Min) {
    val = DBL_MAX;
  } else if (mode == Minmax_Max) {
    val = DBL_MIN;
  } else {
    val = 0;
  }
}
#endif //0

//---------------------------------------------------------------------------------------------

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
