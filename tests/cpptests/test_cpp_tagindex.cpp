#include "tag_index.h"
#include "gtest/gtest.h"

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
  t_docId d;
  for (d = 1; d <= N; d++) {
    size_t sz = TagIndex_Index(idx, &v[0], v.size(), d);
    totalSZ += sz;
    // make sure repeating push of the same vector doesn't get indexed
    sz = TagIndex_Index(idx, &v[0], v.size(), d);
    ASSERT_EQ(0, sz);
  }

  ASSERT_EQ(v.size(), idx->values->cardinality);

  // expectedTotalSZ should include the memory occupied by the inverted index 
  // structure and its blocks.

  // Buffer grows up to 1077 bytes trying to store 1000 bytes. See Buffer_Grow()
  size_t buffer_cap = 1077;
  size_t num_blocks = N / INDEX_BLOCK_SIZE_DOCID_ONLY;

  // The size of the inverted index structure is 32 bytes
  size_t iv_index_size = sizeof_InvertedIndex(Index_DocIdsOnly);

  size_t expectedTotalSZ = v.size() * (iv_index_size + ((buffer_cap + sizeof(IndexBlock)) * num_blocks));
  ASSERT_EQ(expectedTotalSZ, totalSZ);

  // Add a new entry to and check the last block size
  std::vector<const char *> v2{"bye"};
  size_t sz = TagIndex_Index(idx, &v2[0], v2.size(), ++d);
  size_t last_block_size = sizeof_InvertedIndex(Index_DocIdsOnly) +
                            sizeof(IndexBlock) + INDEX_BLOCK_INITIAL_CAP;
  ASSERT_EQ(expectedTotalSZ + last_block_size, totalSZ + sz);

  IndexIterator *it = TagIndex_OpenReader(idx, NULL, "hello", 5, 1, RS_INVALID_FIELD_INDEX);
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

#define TEST_MY_SEP(sep, str)                            \
  orig = s = strdup(str);                                \
  token = TagIndex_SepString(sep, &s, &tokenLen, false); \
  EXPECT_STREQ(token, "foo");                            \
  ASSERT_EQ(tokenLen, 3);                                \
  token = TagIndex_SepString(sep, &s, &tokenLen, false); \
  EXPECT_STREQ(token, "bar");                            \
  ASSERT_EQ(tokenLen, 3);                                \
  token = TagIndex_SepString(sep, &s, &tokenLen, false); \
  ASSERT_FALSE(token);                                   \
  free(orig);

TEST_F(TagIndexTest, testSepString) {
  char *orig, *s;
  size_t tokenLen;
  char *token;

  orig = s = strdup(" , , , , , , ,   , , , ,,,,   ,,,");
  token = TagIndex_SepString(',', &s, &tokenLen, false);
  ASSERT_FALSE(token);
  token = TagIndex_SepString(',', &s, &tokenLen, false);
  ASSERT_FALSE(token);
  free(orig);

  orig = s = strdup("");
  token = TagIndex_SepString(',', &s, &tokenLen, false);
  ASSERT_FALSE(token);
  token = TagIndex_SepString(',', &s, &tokenLen, false);
  ASSERT_FALSE(token);
  free(orig);

  TEST_MY_SEP(',', "foo,bar")

  TEST_MY_SEP(',', "  foo  ,   bar   ")

  TEST_MY_SEP(',', " ,,  foo  ,   bar ,,  ")

  TEST_MY_SEP(',', " ,,  foo  , ,   bar ,,  ")

  TEST_MY_SEP(' ', "   foo    bar   ")
}
