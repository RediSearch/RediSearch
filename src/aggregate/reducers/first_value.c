
#include "aggregate/reducer.h"

///////////////////////////////////////////////////////////////////////////////////////////////

int RDCRFirstValue::noSort(const RLookupRow *srcrow) {
  if (data.value) {
    return 1;
  }

  RSValue *val = srcrow->GetItem(srckey);
  if (!val) {
    data.value = RS_NullVal();
    return 1;
  }
  data.value = val->IncrRef();
  return 1;
}

//---------------------------------------------------------------------------------------------

int RDCRFirstValue::sort(const RLookupRow *srcrow) {
  RSValue *val = srcrow->GetItem(srckey);
  if (!val) {
    return 1;
  }

  RSValue *curSortval = srcrow->GetItem(data.sortprop);
  if (!curSortval) {
    curSortval = &RS_StaticNull;
  }

  if (!data.sortval) {
    // No current value: assign value and continue
    data.value = val->IncrRef();
    data.sortval = curSortval->IncrRef();
    return 1;
  }

  int rc = (ascending ? -1 : 1) * RSValue::Cmp(curSortval, data.sortval, NULL);
  int isnull = data.sortval->IsNull();

  if (!data.value || (!isnull && rc > 0) || (isnull && rc < 0)) {
    RSVALUE_REPLACE(&data.sortval, curSortval);
    RSVALUE_REPLACE(&data.value, val);
  }

  return 1;
}

//---------------------------------------------------------------------------------------------

int RDCRFirstValue::Add(const RLookupRow *srcrow) {
  if (data.sortprop) {
    sort(srcrow);
  }
  else {
    noSort(srcrow);
  }
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRFirstValue::Finalize() {
  return data.value->IncrRef();
}

//---------------------------------------------------------------------------------------------

RDCRFirstValue::~RDCRFirstValue() {
  RSVALUE_CLEARVAR(data.value);
  RSVALUE_CLEARVAR(data.sortval);
}

//---------------------------------------------------------------------------------------------

RDCRFirstValue::RDCRFirstValue(const ReducerOptions *options) {
  ascending = true;

  if (!options->GetKey(&srckey)) {
    throw Error("RDCRFirstValue: no key found");
  }

  if (options->args->AdvanceIfMatch("BY")) {
    // Get the next field...
    if (!options->GetKey(&data.sortprop)) {
      throw Error("RDCRFirstValue: no sort by found");
    }
    if (options->args->AdvanceIfMatch("ASC")) {
      ascending = true;
    } else if (options->args->AdvanceIfMatch("DESC")) {
      ascending = false;
    }
  }

  if (!options->EnsureArgsConsumed()) {
    throw Error("RDCRFirstValue: args not consumed");
  }

  data.value = NULL;
  data.sortval = NULL;
  //@@ reducerId = ?;
}

///////////////////////////////////////////////////////////////////////////////////////////////
