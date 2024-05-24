/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisearch.h"
#include "index.h"
#include "inverted_index.h"
#include "spec.h"
#include "rmutil/alloc.h"
#include "time_sample.h"

#define NUM_ENTRIES 5000000
#define MY_FLAGS Index_StoreFreqs | Index_StoreFieldFlags

static void writeEntry(InvertedIndex *idx, size_t id) {
  ForwardIndexEntry ent = {0};
  ent.docId = id;
  ent.docScore = 1.0;
  ent.fieldMask = RS_FIELDMASK_ALL;
  ent.freq = 3;
  ent.term = "foo";
  ent.vw = NULL;
  ent.len = 3;
  InvertedIndex_WriteEntry(idx, &ent);
}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  size_t index_memsize;
  InvertedIndex *idx = NewInvertedIndex(MY_FLAGS, 1, &index_memsize);
  for (size_t ii = 0; ii < NUM_ENTRIES; ++ii) {
    writeEntry(idx, ii);
  }

  for (size_t ii = 0; ii < 100; ++ii) {
    IndexReader *r = NewIndexReader(idx, NULL, RS_FIELDMASK_ALL, MY_FLAGS, NULL, 0);
    IndexIterator *it = NewReadIterator(r);
    TimeSample ts;
    TimeSampler_Start(&ts);
    RSIndexResult *res;
    while (INDEXREAD_EOF != it->Read(it->ctx, &res)) {
      TimeSampler_Tick(&ts);
    }
    TimeSampler_End(&ts);
    printf("%d iterations in %lldms, %fns/iter\n", ts.num, TimeSampler_DurationMS(&ts),
           TimeSampler_IterationMS(&ts) * 1000000);
    ReadIterator_Free(it);
  }
  return 0;
}
