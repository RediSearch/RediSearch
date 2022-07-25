
#include "aggregate/reducer.h"
#include <math.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0
#define BLOCK_SIZE 1024 * sizeof(devCtx)

//---------------------------------------------------------------------------------------------

RDCRStdDev::Data *RDCRStdDev::NewInstance() {
  Data *dd = alloc.Alloc(sizeof(*dd), BLOCK_SIZE);
  memset(dd, 0, sizeof(*dd));
  dd->srckey = srckey;
  return dd;
}
#endif

//---------------------------------------------------------------------------------------------

void RDCRStdDev::Data::Add(double d) {
  // https://www.johndcook.com/blog/standard_deviation/
  n++;
  if (n == 1) {
    oldM = newM = d;
    oldS = 0.0;
  } else {
    newM = oldM + (d - oldM) / n;
    newS = oldS + (d - oldM) * (d - newM);

    // set up for next iteration
    oldM = newM;
    oldS = newS;
  }
}

//---------------------------------------------------------------------------------------------

int RDCRStdDev::Add(const RLookupRow *srcrow) {
  double d;
  RSValue *v = srcrow->GetItem(srckey);
  if (v) {
    if (v->t != RSValue_Array) {
      if (v->ToNumber(&d)) {
        data.Add(d);
      }
    } else {
      uint32_t sz = v->ArrayLen();
      for (uint32_t i = 0; i < sz; i++) {
        RSValue *v1 = v->ArrayItem(i);
        if (v1->ToNumber(&d)) {
          data.Add(d);
        }
      }
    }
  }
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRStdDev::Finalize() {
  double variance = ((data.n > 1) ? data.newS / (data.n - 1) : 0.0);
  double stddev = sqrt(variance);
  return RS_NumVal(stddev);
}

//---------------------------------------------------------------------------------------------

RDCRStdDev::RDCRStdDev(const ReducerOptions *options) {
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRStdDev: no key found");
  }
  reducerId = REDUCER_T_STDDEV;
}

///////////////////////////////////////////////////////////////////////////////////////////////
