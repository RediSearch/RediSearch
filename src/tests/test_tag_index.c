#include "test_util.h"
#include "../tag_index.h"
#include "../rmutil/alloc.h"
#include "time_sample.h"
#include "../util/arr.h"
int testTagIndexCreate() {
  TagIndex *idx = NewTagIndex();
  ASSERT(idx);
  // ASSERT_STRING_EQ(idx->)
  int N = 100000;
  char **v = array_newlen(char *, 3);
  v[0] = strdup("hello");
  v[1] = strdup("world");
  v[2] = strdup("foo");
  size_t totalSZ = 0;
  for (t_docId d = 1; d <= N; d++) {
    size_t sz = TagIndex_Index(idx, v, d);
    ASSERT(sz > 0);
    totalSZ += sz;
    // make sure repeating push of the same vector doesn't get indexed
    sz = TagIndex_Index(idx, v, d);
    ASSERT(sz == 0);
  }

  ASSERT_EQUAL(idx->values->cardinality, array_len(v));
  ASSERT_EQUAL(305496, totalSZ);

  IndexIterator *it = TagIndex_OpenReader(idx, NULL, "hello", 5, NULL, NULL, NULL, 1);
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
  it->Free(it);
  array_free(v);
  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testTagIndexCreate);
});