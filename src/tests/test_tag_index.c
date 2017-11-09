#include "test_util.h"
#include "../tag_index.h"
#include "../rmutil/alloc.h"
#include "time_sample.h"

int testTagIndexCreate() {
  TagIndex *idx = NewTagIndex();
  ASSERT(idx);
  // ASSERT_STRING_EQ(idx->)
  int N = 100000;
  Vector *v = NewVector(char *, 8);
  Vector_Push(v, strdup("hello"));
  Vector_Push(v, strdup("world"));
  Vector_Push(v, strdup("foo"));
  size_t totalSZ = 0;
  for (t_docId d = 1; d <= N; d++) {
    size_t sz = TagIndex_Index(idx, v, d);
    ASSERT(sz > 0);
    totalSZ += sz;
  }
  ASSERT_EQUAL(idx->values->cardinality, Vector_Size(v));

  printf("%zd\n", totalSZ);
  IndexIterator *it = TagIndex_OpenReader(idx, NULL, "hello", 5, NULL, NULL, NULL);
  ASSERT(it != NULL);
  RSIndexResult *r;
  t_docId n = 1;

  TimeSample ts;
  TimeSampler_Start(&ts);
  while (INDEXREAD_EOF != it->Read(it->ctx, &r)) {
    // printf("DocId: %d\n", r->docId);
    ASSERT_EQUAL(n++, r->docId);
    TimeSampler_Tick(&ts);
  }
  TimeSampler_End(&ts);
  printf("%d iterations in %lldns, rate %fns/iter\n", N, ts.durationNS,
         TimeSampler_IterationMS(&ts) * 1000000);
  ASSERT_EQUAL(N + 1, n);
  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testTagIndexCreate);
});