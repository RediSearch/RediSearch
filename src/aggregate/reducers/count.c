
#include "aggregate/reducer.h"
#include "util/block_alloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define COUNTER_BLOCK_SIZE (32 * sizeof(RDCRCount::Data))

//---------------------------------------------------------------------------------------------

Data *RDCRCount::NewInstance() {
  Data *dd = alloc.Alloc(sizeof(Data), COUNTER_BLOCK_SIZE);
  dd->count = 0;
  return dd;
}

//---------------------------------------------------------------------------------------------

int RDCRCount::Add(Data *dd, const RLookupRow *srcrow) {
  dd->count++;
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRCount::Finalize(Data *dd) {
  return RS_NumVal(dd->count);
}

///////////////////////////////////////////////////////////////////////////////////////////////
