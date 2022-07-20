
#include "aggregate/reducer.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#ifdef 0
struct SumReducer : public Reducer {
  bool isAvg;
  size_t count;
  double total;

  SumReducer(bool avg);

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};

// //---------------------------------------------------------------------------------------------

SumReducer::SumReducer(bool avg) {
  isAvg = avg;
  count = 0;
  total = 0;
}
#endif //0

//---------------------------------------------------------------------------------------------

int RDCRSum::Add(const RLookupRow *srcrow) {
  const RSValue *v = srcrow->GetItem(srckey);
  if (v && v->t == RSValue_Number) {
    data.total += v->numval;
  } else {  // try to convert value to number
    double d = 0;
    if (v->ToNumber(&d)) {
      data.total += d;
    }
  }
  return 1;
}

int RDCRAvg::Add(const RLookupRow *srcrow) {
  data.count++;
  const RSValue *v = srcrow->GetItem(srckey);
  if (v && v->t == RSValue_Number) {
    data.total += v->numval;
  } else {  // try to convert value to number
    double d = 0;
    if (v->ToNumber(&d)) {
      data.total += d;
    }
  }
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRSum::Finalize() {
  return RS_NumVal(data.total);
}

RSValue *RDCRAvg::Finalize() {
  double v = 0;
  if (data.count) {
    v = data.total / data.count;
  }

  return RS_NumVal(v);
}

//---------------------------------------------------------------------------------------------

RDCRSum::RDCRSum(const ReducerOptions *options) {
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRSum: no key found");

  }
  reducerId = REDUCER_T_SUM;
}

//---------------------------------------------------------------------------------------------

RDCRAvg::RDCRAvg(const ReducerOptions *options) {
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRAvg: no key found");

  }
  reducerId = REDUCER_T_AVG;
}

///////////////////////////////////////////////////////////////////////////////////////////////
