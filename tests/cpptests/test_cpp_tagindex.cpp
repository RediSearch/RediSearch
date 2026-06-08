/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "tag_index.h"
#include "triemap_ffi.h"
#include "gtest/gtest.h"
#include "index_utils.h"

#include <vector>
#include <string>

class TagIndexTest : public ::testing::Test {};

TEST_F(TagIndexTest, testCreate) {
  TagIndex *idx = NewTagIndex(NULL, 0);
  ASSERT_FALSE(idx == NULL);
  // ASSERT_STRING_EQ(idx->)
  const size_t N = 100000;
  std::vector<const char *> v{"hello", "world", "foo"};
  // for (auto s : v) {
  //   printf("V[n]: %s\n", s);
  // }
  t_docId d;
  IndexStats stats = {0};
  for (d = 1; d <= N; d++) {
    TagIndex_Index(NULL, idx, NULL, &v[0], v.size(), d, &stats);
    const size_t sz = stats.invertedSize;
    // make sure repeating push of the same vector doesn't get indexed
    TagIndex_Index(NULL, idx, NULL, &v[0], v.size(), d, &stats);
    ASSERT_EQ(sz, stats.invertedSize);
  }

  ASSERT_EQ(v.size(), TrieMap_NUniqueKeys(idx->values));

  // expectedTotalSZ should include the memory occupied by the inverted index
  // structure and its blocks.

  // Buffer grows up to 1106 bytes trying to store 1000 bytes - it doubles each time
  size_t buffer_cap = 1106;
  size_t num_blocks = N / 1000;

  // The base size of an inverted index is 72 bytes: 48 bytes for the struct itself
  // (including the `pending: Vec<Arc<IndexBlock>>` field) plus 24 bytes for the
  // Arc<ThinVec> heap allocation for the empty `sealed` region (Arc refcount header
  // + ThinVec stack representation).
  size_t iv_index_size = 72;

  // Each block in `pending` costs PER_NEW_BLOCK_BYTES + buffer capacity:
  //   48  IndexBlock inline (first_doc_id + last_doc_id + num_entries + buffer triple)
  //   16  Arc refcount header (strong + weak counter)
  //    8  one pointer slot in the `pending` Vec (reserve_exact strategy)
  size_t per_block_overhead = 48 + 16 + 8;
  size_t expectedTotalSZ =
      v.size() * (iv_index_size + (buffer_cap + per_block_overhead) * num_blocks);
  ASSERT_EQ(expectedTotalSZ, stats.invertedSize);

  // Add a new entry to and check the last block size
  std::vector<const char *> v2{"bye"};
  TagIndex_Index(NULL, idx, NULL, &v2[0], v2.size(), ++d, &stats);
  // A base inverted index is 72 bytes (48 struct + 24 Arc<ThinVec> heap),
  // a block in `pending` is 48 (IndexBlock inline) + 16 (Arc header) + 8 (Vec slot),
  // and after the first insert the buffer capacity is 1 byte.
  size_t last_block_size = iv_index_size + per_block_overhead + 1;
  ASSERT_EQ(expectedTotalSZ + last_block_size, stats.invertedSize);

  MockQueryEvalCtx mockQctx(N, N);
  QueryIterator *it = TagIndex_OpenReader(idx, &mockQctx.sctx, "hello", 5, 1, RS_INVALID_FIELD_INDEX);
  ASSERT_TRUE(it != NULL);
  t_docId n = 1;

  // TimeSample ts;
  // TimeSampler_Start(&ts);
  while (ITERATOR_EOF != it->Read(it)) {
    // printf("DocId: %d\n", r->docId);
    ASSERT_EQ(n++, it->lastDocId);
    // TimeSampler_Tick(&ts);
  }

  // TimeSampler_End(&ts);
  // printf("%d iterations in %lldns, rate %fns/iter\n", N, ts.durationNS,
  //        TimeSampler_IterationMS(&ts) * 1000000);
  ASSERT_EQ(N + 1, n);
  it->Free(it);
  TagIndex_Free(idx);
}

TEST_F(TagIndexTest, testSkipToLastId) {
  TagIndex *idx = NewTagIndex(NULL, 0);
  ASSERT_FALSE(idx == NULL);
  std::vector<const char *> v{"hello"};
  t_docId docId = 1;
  IndexStats stats = {0};
  TagIndex_Index(NULL, idx, NULL, &v[0], v.size(), docId, &stats);
  MockQueryEvalCtx mockQctx(1, 1);
  QueryIterator *it = TagIndex_OpenReader(idx, &mockQctx.sctx, "hello", 5, 1, RS_INVALID_FIELD_INDEX);
  IteratorStatus rc = it->Read(it);
  ASSERT_EQ(rc, ITERATOR_OK);
  ASSERT_EQ(it->lastDocId, docId);
  rc = it->SkipTo(it, docId + 1);
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_GE(it->lastDocId, docId);
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
