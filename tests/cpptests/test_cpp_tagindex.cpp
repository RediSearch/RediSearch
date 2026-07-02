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

  // The base size of an inverted index is 120 bytes: 96 for the struct
  // (sealed Arc + pending Vec triple + Option<IndexBlock> in_progress + small
  // fields) plus 24 bytes for the Arc<ThinVec> heap allocation for the empty
  // `sealed` region (Arc refcount header + ThinVec stack representation). The
  // 48-byte `IndexBlock` slot inside `Option<in_progress>` is included in the
  // 96 — it is not double-counted as a separate block below.
  size_t iv_index_size = 120;

  // The last block of each tag lives in `in_progress` (owned directly on the
  // struct); only its buffer counts here. The other (num_blocks - 1) blocks
  // are in `pending`, each costing 48 (IndexBlock inline inside Arc) + 16
  // (Arc refcount header).
  size_t per_pending_block_overhead = 48 + 16;
  // `pending: Vec<Arc<IndexBlock>>` uses Rust's `Vec::push` doubling growth
  // strategy: 0 -> 4 -> 8 -> 16 -> 32 -> 64 -> 128. For 99 pushes (num_blocks - 1)
  // capacity lands at 128 slots * 8 bytes = 1024 bytes.
  size_t pending_vec_heap = 128 * 8;
  size_t expectedTotalSZ = v.size() *
                           (iv_index_size +
                            (num_blocks - 1) * (buffer_cap + per_pending_block_overhead) +
                            pending_vec_heap +
                            buffer_cap /* in_progress buffer */);
  ASSERT_EQ(expectedTotalSZ, stats.invertedSize);

  // Add a new entry to and check the last block size
  std::vector<const char *> v2{"bye"};
  TagIndex_Index(NULL, idx, NULL, &v2[0], v2.size(), ++d, &stats);
  // The "bye" tag adds a brand-new InvertedIndex with one block — that block
  // stays in `in_progress` (no rollover), so the only on-top-of-base cost is
  // the 1-byte buffer growth after the first insert.
  size_t last_block_size = iv_index_size + 1;
  ASSERT_EQ(expectedTotalSZ + last_block_size, stats.invertedSize);

  MockQueryEvalCtx mockQctx(N, N);
  QueryIterator *it = TagIndex_OpenReader(idx, &mockQctx.sctx, "hello", 5, 1, RS_INVALID_FIELD_INDEX, NULL);
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
  QueryIterator *it = TagIndex_OpenReader(idx, &mockQctx.sctx, "hello", 5, 1, RS_INVALID_FIELD_INDEX, NULL);
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
