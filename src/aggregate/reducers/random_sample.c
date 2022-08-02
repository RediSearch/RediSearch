
#include "aggregate/reducer.h"

///////////////////////////////////////////////////////////////////////////////////////////////

int RDCRRandomSample::Add(const RLookupRow *srcrow) {
  RSValue *v = srcrow->GetItem(srckey);
  if (!v) {
    return 1;
  }

  if (data.seen < len) {
    RSVALUE_ARRELEM(data.samplesArray, data.seen) = v->IncrRef();
    RSVALUE_ARRLEN(data.samplesArray)++;
    assert(RSVALUE_ARRLEN(data.samplesArray) <= len);
  } else {
    size_t i = rand() % (data.seen + 1);
    if (i < len) {
      RSVALUE_REPLACE(&RSVALUE_ARRELEM(data.samplesArray, i), v);
    }
  }
  data.seen++;
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRRandomSample::Finalize() {
  RSValue *ret = data.samplesArray;
  data.samplesArray = NULL;
  return ret;
}

//---------------------------------------------------------------------------------------------

RDCRRandomSample::~RDCRRandomSample() {
  delete data.samplesArray;
}

//---------------------------------------------------------------------------------------------

RDCRRandomSample::RDCRRandomSample(const ReducerOptions *options) {
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRRandomSample: no key found");
  }

  // Get the number of samples..
  unsigned samplesize;
  int rc = options->args->GetUnsigned(&samplesize, 0);
  if (rc != AC_OK) {
    QERR_MKBADARGS_AC(options->status, "<sample size>", rc);
    throw Error("RDCRRandomSample: Bad arguments");
  }
  if (samplesize > MAX_SAMPLE_SIZE) {
    QERR_MKBADARGS_FMT(options->status, "Sample size too large");
    throw Error("RDCRRandomSample: Sample size too large");
  }

  len = samplesize;
  data.seen = 0;
  data.samplesArray = RSValue::NewArray(NULL, len, 0);
  //@@ reducerId = ?;
}

///////////////////////////////////////////////////////////////////////////////////////////////
