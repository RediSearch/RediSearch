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
#include "indexes.h"
#include "util/mempool/mempool.h"

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
    ASSERT_EQ(VVW_GetCount(e->vw), 2);
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
  ASSERT_TRUE(iterate(idx).empty());
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
    ASSERT_EQ(VVW_GetCount(e->vw), 3);
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
  ASSERT_TRUE(iterate(idx).empty());

  addTerms(idx, 0, 10);
  ASSERT_EQ(iterate(idx).size(), 10);
  ForwardIndexFree(idx);
}

TEST_F(ForwardIndexTest, offsetsAreFreshAfterReusingLargeWriters) {
  TestDoc doc(100);
  ForwardIndex *idx = NewForwardIndex(&doc.doc, Index_StoreTermOffsets);
  addTerms(idx, 0, 4, 10000);
  ASSERT_EQ(VVW_GetCount(find(idx, 0)->vw), 10000);

  ForwardIndex_Reset(idx, &doc.doc, Index_StoreTermOffsets);
  addTerms(idx, 0, 8, 2);
  for (size_t i = 0; i < 8; ++i) {
    VarintVectorWriter *vw = find(idx, i)->vw;
    ASSERT_EQ(VVW_GetCount(vw), 2);
    ASSERT_EQ(VVW_GetByteLength(vw), 2);  // positions 1..16 delta-encode to one byte each
  }
  ForwardIndexFree(idx);
}

TEST_F(ForwardIndexTest, resetPoolsOnlySmallWritersUpToTheCap) {
  TestDoc doc(100);
  ForwardIndex *idx = NewForwardIndex(&doc.doc, Index_StoreTermOffsets);
  // Term 0's writer grows past the size limit; terms 1..2 stay small.
  addTerms(idx, 0, 1, 2000);
  addTerms(idx, 1, 2);
  ASSERT_GT(VVW_GetByteLength(find(idx, 0)->vw), 1024);
  ForwardIndex_Reset(idx, &doc.doc, Index_StoreTermOffsets);
  ASSERT_EQ(mempool_size(idx->vvwPool), 2);

  // More writers than the pool keeps, without growing the table past its limit.
  addTerms(idx, 0, 4000);
  ForwardIndex_Reset(idx, &doc.doc, Index_StoreTermOffsets);
  ASSERT_LE(idx->hits->numBuckets, FORWARD_INDEX_MAX_RETAINED_BUCKETS);
  ASSERT_EQ(mempool_size(idx->vvwPool), 1024);
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

TEST_F(ForwardIndexTest, documentContextPoolFreesContextsPastItsCap) {
  static const char *args[] = {"SCHEMA", "t", "TEXT"};
  QueryError err = QueryError_Default();
  StrongRef ref = IndexSpec_ParseC(NULL, "fwidx_pool", args, sizeof(args) / sizeof(*args), &err);
  ASSERT_TRUE(QueryError_IsOk(&err)) << QueryError_GetUserError(&err);
  IndexSpec *sp = (IndexSpec *)StrongRef_Get(ref);

  // More contexts in flight than the pool keeps, released twice so pooled contexts are also reused.
  constexpr size_t numContexts = 40;
  for (int round = 0; round < 2; ++round) {
    std::vector<Document> docs(numContexts);
    std::vector<RSAddDocumentCtx *> ctxs;
    for (size_t i = 0; i < numContexts; ++i) {
      std::string key = "doc" + std::to_string(i);
      Document_Init(&docs[i], RedisModule_CreateString(NULL, key.c_str(), key.size()), 1, RS_LANG_ENGLISH, DocumentType_Hash);
      docs[i].flags = DOCUMENT_F_OWNSTRINGS;
      Document_AddFieldC(&docs[i], "t", "hello world", strlen("hello world"), INDEXFLD_T_FULLTEXT);
      RSAddDocumentCtx *aCtx = NewAddDocumentCtx(sp, &docs[i], &err);
      ASSERT_NE(aCtx, nullptr) << QueryError_GetUserError(&err);
      aCtx->stateFlags |= ACTX_F_NOFREEDOC;
      ctxs.push_back(aCtx);
    }
    for (size_t i = 0; i < numContexts; ++i) {
      AddDocumentCtx_Free(ctxs[i]);
      Document_Free(&docs[i]);
    }
  }
  Indexes_RemoveSpecFromGlobals(ref, false);
}
