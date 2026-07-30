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
#include "field_spec.h"
#include "document.h"
#include "indexer.h"
#include "util/arr.h"
#include "gtest/gtest.h"
#include "index_utils.h"

#include <vector>
#include <string>

class TagIndexTest : public ::testing::Test {};

// Build a minimal stack FieldSpec for a TAG field. `tokenizeTagString` /
// `TagIndex_Preprocess` only read `tagOpts.tagSep`, `tagOpts.tagFlags` and
// `options` (via FieldSpec_IndexesEmpty), so no full IndexSpec is required.
static FieldSpec makeTagFieldSpec(char sep, TagFieldFlags flags, FieldSpecOptions options) {
  FieldSpec fs{};
  fs.types = INDEXFLD_T_TAG;
  fs.options = options;
  fs.tagOpts.tagFlags = flags;
  fs.tagOpts.tagSep = sep;
  fs.tagOpts.tagIndex = NULL;
  return fs;
}

// Run TagIndex_Preprocess and assert the returned status and tag list.
static void checkPreprocess(const FieldSpec &fs, const DocumentField &df, int expectedRet,
                            const char *const *expected, size_t nExpected) {
  FieldIndexerData fdata{};
  int ret = TagIndex_Preprocess(&fs, &df, &fdata);
  EXPECT_EQ(ret, expectedRet);
  ASSERT_EQ(array_len(fdata.tags), nExpected);
  for (size_t i = 0; i < nExpected; i++) {
    EXPECT_STREQ(fdata.tags[i], expected[i]);
  }
  TagIndex_FreePreprocessedData(fdata.tags);
}

// Tokenize a single C-string TAG value and assert the resulting tags.
static void checkPreprocessCStr(char sep, TagFieldFlags flags, FieldSpecOptions options,
                                const char *value, const char *const *expected, size_t nExpected) {
  FieldSpec fs = makeTagFieldSpec(sep, flags, options);
  DocumentField df{};
  df.unionType = FLD_VAR_T_CSTR;
  df.strval = (char *)value;
  df.strlen = strlen(value);
  checkPreprocess(fs, df, /*expectedRet=*/1, expected, nExpected);
}

TEST_F(TagIndexTest, testCreate) {
  TagIndex *idx = NewTagIndex(NULL, 0, false);
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
    const size_t numRecords = stats.numRecords;
    // make sure repeating push of the same vector doesn't get indexed
    TagIndex_Index(NULL, idx, NULL, &v[0], v.size(), d, &stats);
    ASSERT_EQ(sz, stats.invertedSize);
    ASSERT_EQ(numRecords, stats.numRecords);
  }

  ASSERT_EQ(v.size(), TagIndex_NUniqueValues(idx));
  ASSERT_EQ(N * v.size(), stats.numRecords);

  // expectedTotalSZ should include the memory occupied by the inverted index
  // structure and its blocks.

  // Buffer grows up to 1106 bytes trying to store 1000 bytes - it doubles each time
  size_t buffer_cap = 1106;
  size_t num_blocks = N / 1000;

  // The size of the inverted index structure is 24 bytes
  size_t iv_index_size = 24;

  // Each index block is 56 bytes + its buffer capacity + the header of the block vector
  size_t expectedTotalSZ = v.size() * (iv_index_size + (8 + (buffer_cap + 56) * num_blocks));
  ASSERT_EQ(expectedTotalSZ, stats.invertedSize);

  // Add a new entry to and check the last block size
  std::vector<const char *> v2{"bye"};
  TagIndex_Index(NULL, idx, NULL, &v2[0], v2.size(), ++d, &stats);
  // A base inverted index is 24 bytes
  // The header of the block vector is 8 bytes
  // An index block is 56 bytes
  // And after the first insert the buffer capacity is 1 byte
  size_t last_block_size = 24 + 8 + 56 + 1;
  ASSERT_EQ(expectedTotalSZ + last_block_size, stats.invertedSize);
  ASSERT_EQ(N * v.size() + v2.size(), stats.numRecords);

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

TEST_F(TagIndexTest, testDuplicateTagValuesCountOnce) {
  TagIndex *idx = NewTagIndex(NULL, 0, false);
  const char *v[] = {"foo", "foo", "bar"};
  IndexStats stats = {0};

  TagIndex_Index(NULL, idx, NULL, &v[0], 3, 1, &stats);
  TagIndex_Commit(idx, &v[0], 3, &stats);

  ASSERT_EQ(2u, stats.numRecords);
  ASSERT_EQ(2u, TagIndex_NUniqueValues(idx));

  TagIndex_Free(idx);
}

TEST_F(TagIndexTest, testSkipToLastId) {
  TagIndex *idx = NewTagIndex(NULL, 0, false);
  ASSERT_FALSE(idx == NULL);
  std::vector<const char *> v{"hello"};
  t_docId docId = 1;
  IndexStats stats = {0};
  TagIndex_Index(NULL, idx, NULL, &v[0], v.size(), docId, &stats);
  MockQueryEvalCtx mockQctx(1, 1);
  QueryIterator *it =
      TagIndex_OpenReader(idx, &mockQctx.sctx, "hello", 5, 1, RS_INVALID_FIELD_INDEX, NULL);
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

TEST_F(TagIndexTest, testSepStringIndexEmpty) {
  char *orig, *s, *token;
  size_t tokenLen;

  // Leading space then a separator -> an empty value is returned, then the
  // following non-empty token.
  orig = s = strdup(" ,foo");
  token = TagIndex_SepString(',', &s, &tokenLen, true);
  EXPECT_STREQ(token, "");
  token = TagIndex_SepString(',', &s, &tokenLen, true);
  EXPECT_STREQ(token, "foo");
  ASSERT_EQ(tokenLen, 3);
  token = TagIndex_SepString(',', &s, &tokenLen, true);
  ASSERT_FALSE(token);
  free(orig);

  // Leading spaces then end-of-string -> one empty value, then NULL.
  orig = s = strdup("   ");
  token = TagIndex_SepString(',', &s, &tokenLen, true);
  EXPECT_STREQ(token, "");
  token = TagIndex_SepString(',', &s, &tokenLen, true);
  ASSERT_FALSE(token);
  free(orig);
}

TEST_F(TagIndexTest, testTokenizeViaPreprocess) {
  // --- normal separator, case-insensitive (ASCII, in-place lowercase) ---
  {
    const char *e[] = {"hello", "world"};
    checkPreprocessCStr(',', (TagFieldFlags)0, (FieldSpecOptions)0, "Hello,World", e, 2);
  }

  // --- normal separator, case-insensitive, lowercase needs MORE bytes ---
  // 'İ' (U+0130, 2 bytes) lowercases to 'i' + combining dot above (3 bytes),
  // so unicode_tolower returns a freshly allocated longer buffer.
  {
    const char *e[] = {"i̇stanbul"};
    checkPreprocessCStr(',', (TagFieldFlags)0, (FieldSpecOptions)0, "İSTANBUL", e, 1);
  }

  // --- normal separator, case-sensitive (no lowercasing) ---
  {
    const char *e[] = {"Hello", "World"};
    checkPreprocessCStr(',', TagField_CaseSensitive, (FieldSpecOptions)0, "Hello,World", e, 2);
  }

  // --- JSON default separator: the whole string is a single token ---
  {
    // case-insensitive, ASCII -> lowercased in place
    const char *e[] = {"hello, world"};
    checkPreprocessCStr(TAG_FIELD_DEFAULT_JSON_SEP, (TagFieldFlags)0, (FieldSpecOptions)0,
                        "Hello, World", e, 1);
  }
  {
    // case-insensitive, longer-output realloc branch
    const char *e[] = {"i̇stanbul"};
    checkPreprocessCStr(TAG_FIELD_DEFAULT_JSON_SEP, (TagFieldFlags)0, (FieldSpecOptions)0,
                        "İSTANBUL", e, 1);
  }
  {
    // case-sensitive: tolower block skipped entirely
    const char *e[] = {"Hello, World"};
    checkPreprocessCStr(TAG_FIELD_DEFAULT_JSON_SEP, TagField_CaseSensitive, (FieldSpecOptions)0,
                        "Hello, World", e, 1);
  }

  // --- index-empty: empty input and trailing-separator input both add "" ---
  {
    const char *e[] = {""};
    checkPreprocessCStr(',', (TagFieldFlags)0, FieldSpec_IndexEmpty, "", e, 1);
  }
  {
    const char *e[] = {"foo", ""};
    checkPreprocessCStr(',', (TagFieldFlags)0, FieldSpec_IndexEmpty, "foo,", e, 2);
  }

  // --- FLD_VAR_T_ARRAY: each element is tokenized independently ---
  {
    FieldSpec fs = makeTagFieldSpec(',', (TagFieldFlags)0, (FieldSpecOptions)0);
    DocumentField df{};
    df.unionType = FLD_VAR_T_ARRAY;
    const char *vals[] = {"red,green", "blue"};
    df.multiVal = (char **)vals;
    df.arrayLen = 2;
    const char *e[] = {"red", "green", "blue"};
    checkPreprocess(fs, df, /*expectedRet=*/1, e, 3);
  }

  // --- FLD_VAR_T_NULL: nothing to index, isNull is set, returns 0 ---
  {
    FieldSpec fs = makeTagFieldSpec(',', (TagFieldFlags)0, (FieldSpecOptions)0);
    DocumentField df{};
    df.unionType = FLD_VAR_T_NULL;
    FieldIndexerData fdata{};
    int ret = TagIndex_Preprocess(&fs, &df, &fdata);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(fdata.isNull, 1);
    ASSERT_EQ(array_len(fdata.tags), 0u);
    TagIndex_FreePreprocessedData(fdata.tags);
  }
}

TEST_F(TagIndexTest, testOpenReaderEdgeCases) {
  MockQueryEvalCtx mockQctx(1, 1);

  // NULL index -> NULL reader
  {
    ASSERT_TRUE(
        TagIndex_OpenReader(NULL, &mockQctx.sctx, "x", 1, 1, RS_INVALID_FIELD_INDEX, NULL) == NULL);
  }

  // Absent tag -> NULL reader
  {
    TagIndex *idx = NewTagIndex(NULL, 0, false);
    const char *v[] = {"hello"};
    IndexStats stats = {0};
    TagIndex_Index(NULL, idx, NULL, &v[0], 1, 1, &stats);

    ASSERT_TRUE(TagIndex_OpenReader(idx, &mockQctx.sctx, "missing", 7, 1, RS_INVALID_FIELD_INDEX,
                                    NULL) == NULL);
    TagIndex_Free(idx);
  }
}

TEST_F(TagIndexTest, testCommitAndOverheadWithSuffix) {
  TagIndex *idx = NewTagIndex(NULL, 0, false);
  idx->suffix = NewTrieMap();

  const char *v[] = {"hello", "world"};
  IndexStats stats = {0};
  TagIndex_Commit(idx, &v[0], 1, &stats);
  ASSERT_EQ(stats.numRecords, 0u);

  // Overhead with a live index that has a populated suffix trie.
  FieldSpec fs{};
  fs.types = INDEXFLD_T_TAG;
  fs.tagOpts.tagIndex = idx;
  size_t overhead = TagIndex_GetOverhead(&fs);
  ASSERT_GT(overhead, 0u);

  TagIndex_Free(idx);

  // No tag index -> zero overhead.
  FieldSpec emptyFs{};
  emptyFs.types = INDEXFLD_T_TAG;
  emptyFs.tagOpts.tagIndex = NULL;
  ASSERT_EQ(TagIndex_GetOverhead(&emptyFs), 0u);
}
