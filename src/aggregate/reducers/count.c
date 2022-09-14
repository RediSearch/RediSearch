#include "aggregate/reducer.h"

///////////////////////////////////////////////////////////////////////////////////////////////

RDCRCount::RDCRCount(const ReducerOptions *options) {
  if (options->args->argc != 0) {
    throw Error("Count accepts 0 values only");
  }
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRCount: no key found");
  }

  reducerId = REDUCER_T_COUNT;
}

//---------------------------------------------------------------------------------------------

int RDCRCount::Add(const RLookupRow *srcrow) {
  data.count++;
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRCount::Finalize() {
  return RS_NumVal(data.count);
}

///////////////////////////////////////////////////////////////////////////////////////////////
