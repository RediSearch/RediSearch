#include "tag_index.h"
#include <gtest/gtest.h>
#include <vector>
#include <string>

class TagIndexTest : public ::testing::Test {};

TEST_F(TagIndexTest, testCreate) {
  TagIndex *idx = NewTagIndex();
  ASSERT_FALSE(idx == NULL);
  // ASSERT_STRING_EQ(idx->)
  const size_t N = 100000;
  std::vector<const char *> v{"hello", "world", "foo"};
  // for (auto s : v) {
  //   printf("V[n]: %s\n", s);
  // }
  size_t totalSZ = 0;
  for (t_docId d = 1; d <= N; d++) {
    size_t sz = TagIndex_Index(idx, &v[0], v.size(), d);
    ASSERT_GT(sz, 0);
    totalSZ += sz;
    // make sure repeating push of the same vector doesn't get indexed
    sz = TagIndex_Index(idx, &v[0], v.size(), d);
    ASSERT_EQ(0, sz);
  }

  ASSERT_EQ(v.size(), idx->values->cardinality);
  ASSERT_EQ(300000, totalSZ);

  IndexIterator *it = TagIndex_OpenReader(idx, NULL, "hello", 5, 1);
  ASSERT_TRUE(it != NULL);
  RSIndexResult *r;
  t_docId n = 1;

  // TimeSample ts;
  // TimeSampler_Start(&ts);
  while (INDEXREAD_EOF != it->Read(it->ctx, &r)) {
    // printf("DocId: %d\n", r->docId);
    ASSERT_EQ(n++, r->docId);
    // TimeSampler_Tick(&ts);
  }

  // TimeSampler_End(&ts);
  // printf("%d iterations in %lldns, rate %fns/iter\n", N, ts.durationNS,
  //        TimeSampler_IterationMS(&ts) * 1000000);
  ASSERT_EQ(N + 1, n);
  it->Free(it);
  TagIndex_Free(idx);
}

#define TEST_MY_SEP(sep, str)                     \
  orig = s = strdup(str);                         \
  token = TagIndex_SepString(sep, &s, &tokenLen); \
  EXPECT_STREQ(token, "foo");                     \
  ASSERT_EQ(tokenLen, 3);                         \
  token = TagIndex_SepString(sep, &s, &tokenLen); \
  EXPECT_STREQ(token, "bar");                     \
  ASSERT_EQ(tokenLen, 3);                         \
  token = TagIndex_SepString(sep, &s, &tokenLen); \
  ASSERT_FALSE(token);                            \
  free(orig);

TEST_F(TagIndexTest, testSepString) {
  char *orig, *s;
  size_t tokenLen;
  char *token;

  orig = s = strdup(" , , , , , , ,   , , , ,,,,   ,,,");
  token = TagIndex_SepString(',', &s, &tokenLen);
  ASSERT_FALSE(token);
  token = TagIndex_SepString(',', &s, &tokenLen);
  ASSERT_FALSE(token);
  free(orig);

  orig = s = strdup("");
  token = TagIndex_SepString(',', &s, &tokenLen);
  ASSERT_FALSE(token);
  token = TagIndex_SepString(',', &s, &tokenLen);
  ASSERT_FALSE(token);
  free(orig);

  TEST_MY_SEP(',', "foo,bar")

  TEST_MY_SEP(',', "  foo  ,   bar   ")

  TEST_MY_SEP(',', " ,,  foo  ,   bar ,,  ")

  TEST_MY_SEP(',', " ,,  foo  , ,   bar ,,  ")

  TEST_MY_SEP(' ', "   foo    bar   ")
}
