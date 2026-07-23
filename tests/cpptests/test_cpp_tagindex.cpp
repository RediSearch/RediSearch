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
