/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "forward_index.h"
#include "fnv_ffi.h"
#include "spec.h"

#include <deque>
#include <string>
#include <vector>

class ForwardIndexTest : public ::testing::Test {
 protected:
  // Each document has a single TEXT field holding `text`; only its length matters to the index.
  struct TestDoc {
    std::string text;
    DocumentField field = {};
    Document doc = {};

    explicit TestDoc(size_t textLen, FieldType indexAs = INDEXFLD_T_FULLTEXT) : text(textLen, 'x') {
      field.unionType = FLD_VAR_T_CSTR;
      field.strval = text.data();
      field.strlen = text.size();
      field.indexAs = indexAs;
      doc.fields = &field;
      doc.numFields = 1;
      doc.language = RS_LANG_ENGLISH;
    }
  };

  // Tokens point into these strings, so they must neither move nor die while the index uses them.
  std::deque<std::string> terms;

  void addTerms(ForwardIndex *idx, size_t first, size_t count, uint32_t repeats = 1) {
    ForwardIndexTokenizerCtx ctx;
    ForwardIndexTokenizerCtx_Init(&ctx, idx, NULL, NULL, 0, 1);
    uint32_t pos = 0;
    for (size_t i = first; i < first + count; ++i) {
      const std::string &term = termAt(i);
      for (uint32_t r = 0; r < repeats; ++r) {
        Token tok = {};
        tok.tok = term.c_str();
        tok.tokLen = term.size();
        tok.pos = ++pos;
        forwardIndexTokenFunc(&ctx, &tok);
      }
    }
  }

  const std::string &termAt(size_t i) {
    while (terms.size() <= i) {
      terms.push_back("t" + std::to_string(terms.size()));
    }
    return terms[i];
  }

  ForwardIndexEntry *find(ForwardIndex *idx, size_t i) {
    const std::string &term = termAt(i);
    return ForwardIndex_Find(idx, term.c_str(), term.size(), rs_fnv_32a_buf(term.c_str(), term.size(), 0));
  }

  static std::vector<std::string> iterate(ForwardIndex *idx) {
    std::vector<std::string> out;
    ForwardIndexIterator it = ForwardIndex_Iterate(idx);
    for (ForwardIndexEntry *e = ForwardIndexIterator_Next(&it); e; e = ForwardIndexIterator_Next(&it)) {
      out.emplace_back(e->term, e->len);
    }
    return out;
  }
};

TEST_F(ForwardIndexTest, iteratesEachEntryOnceInInsertionOrderAcrossRehash) {
  TestDoc small(1);
  ForwardIndex *idx = NewForwardIndex(&small.doc, Index_StoreTermOffsets);
  size_t initialBuckets = idx->hits->numBuckets;
  addTerms(idx, 0, 1000);
  addTerms(idx, 0, 1000);  // repeated terms must not add entries
  ASSERT_GT(idx->hits->numBuckets, initialBuckets);

  std::vector<std::string> got = iterate(idx);
  ASSERT_EQ(got.size(), 1000);
  for (size_t i = 0; i < got.size(); ++i) {
    ASSERT_EQ(got[i], termAt(i));
    ForwardIndexEntry *e = find(idx, i);
    ASSERT_NE(e, nullptr);
    ASSERT_EQ(e->freq, 2);
    ASSERT_EQ(e->numOffsets, 2);
  }
  ForwardIndexFree(idx);
}

TEST_F(ForwardIndexTest, resetForgetsPreviousEntries) {
  TestDoc doc(100);
  ForwardIndex *idx = NewForwardIndex(&doc.doc, Index_StoreTermOffsets);
  addTerms(idx, 0, 5000);
  size_t buckets = idx->hits->numBuckets;
  ASSERT_LE(buckets, FORWARD_INDEX_MAX_RETAINED_BUCKETS);

  ForwardIndex_Reset(idx, &doc.doc, Index_StoreTermOffsets);
  ASSERT_EQ(idx->hits->numBuckets, buckets);  // small enough to be kept
  ASSERT_EQ(idx->hits->numItems, 0);
  ASSERT_EQ(idx->entries.root, nullptr);
  for (size_t i = 0; i < 5000; ++i) {
    ASSERT_EQ(find(idx, i), nullptr) << termAt(i);
  }

  addTerms(idx, 4990, 20, 3);
  std::vector<std::string> got = iterate(idx);
  ASSERT_EQ(got.size(), 20);
  for (size_t i = 0; i < got.size(); ++i) {
    ASSERT_EQ(got[i], termAt(4990 + i));
    ForwardIndexEntry *e = find(idx, 4990 + i);
    ASSERT_EQ(e->freq, 3);
    ASSERT_EQ(e->numOffsets, 3);
  }
  ForwardIndexFree(idx);
}

TEST_F(ForwardIndexTest, resetReleasesTableGrownByLargeDocument) {
  TestDoc small(10);
  ForwardIndex *idx = NewForwardIndex(&small.doc, Index_StoreTermOffsets);
  addTerms(idx, 0, 2 * FORWARD_INDEX_MAX_RETAINED_BUCKETS);
  ASSERT_GT(idx->hits->numBuckets, FORWARD_INDEX_MAX_RETAINED_BUCKETS);

  ForwardIndex_Reset(idx, &small.doc, Index_StoreTermOffsets);
  ASSERT_LE(idx->hits->numBuckets, FORWARD_INDEX_MAX_RETAINED_BUCKETS);
  ASSERT_EQ(idx->entries.root, nullptr);

  addTerms(idx, 0, 10);
  ASSERT_EQ(iterate(idx).size(), 10);
  ForwardIndexFree(idx);
}

TEST_F(ForwardIndexTest, offsetsAreEncodedPerTermAfterReleasingLargePositionsLog) {
  TestDoc doc(100);
  ForwardIndex *idx = NewForwardIndex(&doc.doc, Index_StoreTermOffsets);
  addTerms(idx, 0, 4, 20000);
  iterate(idx);
  ASSERT_EQ(find(idx, 0)->numOffsets, 20000);

  ForwardIndex_Reset(idx, &doc.doc, Index_StoreTermOffsets);
  ASSERT_EQ(idx->positionsCap, 0);  // released, not kept for the next document

  // Interleave the terms so that each term's positions are scattered across the log.
  ForwardIndexTokenizerCtx ctx;
  ForwardIndexTokenizerCtx_Init(&ctx, idx, NULL, NULL, 0, 1);
  for (uint32_t pos = 1; pos <= 16; ++pos) {
    const std::string &term = termAt((pos - 1) % 8);
    Token tok = {};
    tok.tok = term.c_str();
    tok.tokLen = term.size();
    tok.pos = pos;
    forwardIndexTokenFunc(&ctx, &tok);
  }
  iterate(idx);
  for (size_t i = 0; i < 8; ++i) {
    ForwardIndexEntry *e = find(idx, i);
    ASSERT_EQ(e->numOffsets, 2);
    // Positions i+1 and i+9, delta-encoded as single-byte varints.
    ASSERT_EQ(std::vector<uint8_t>(e->offsets, e->offsets + e->offsetsLen), (std::vector<uint8_t>{uint8_t(i + 1), 8}));
  }
  ForwardIndexFree(idx);
}

TEST_F(ForwardIndexTest, noOffsetsWithoutStoreTermOffsets) {
  TestDoc doc(100);
  ForwardIndex *idx = NewForwardIndex(&doc.doc, 0);
  addTerms(idx, 0, 10, 3);
  iterate(idx);
  for (size_t i = 0; i < 10; ++i) {
    ForwardIndexEntry *e = find(idx, i);
    ASSERT_EQ(e->offsets, nullptr);
    ASSERT_EQ(e->numOffsets, 0);
  }
  ASSERT_EQ(idx->numPositions, 0);
  ForwardIndexFree(idx);
}

TEST_F(ForwardIndexTest, presizeIgnoresNonTextFieldsAndIsCapped) {
  TestDoc huge(64 << 20);
  ForwardIndex *idx = NewForwardIndex(&huge.doc, 0);
  ASSERT_LE(idx->hits->numBuckets, FORWARD_INDEX_MAX_RETAINED_BUCKETS);
  ForwardIndexFree(idx);

  TestDoc tagOnly(64 << 10, INDEXFLD_T_TAG);
  TestDoc textOnly(1);
  ForwardIndex *tagIdx = NewForwardIndex(&tagOnly.doc, 0);
  ForwardIndex *textIdx = NewForwardIndex(&textOnly.doc, 0);
  ASSERT_EQ(tagIdx->hits->numBuckets, textIdx->hits->numBuckets);
  ForwardIndexFree(tagIdx);
  ForwardIndexFree(textIdx);
}
