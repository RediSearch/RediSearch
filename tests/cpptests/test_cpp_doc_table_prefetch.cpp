/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"

#include <string>
#include <vector>

#include "doc_table.h"

namespace {

class DocTablePrefetchTest : public ::testing::Test {
 protected:
  // A small maxSize, so docIds past it share buckets and form chains.
  static constexpr t_docId MAX_SIZE = 8;
  DocTable dt = NewDocTable(2, MAX_SIZE);

  void TearDown() override { DocTable_Free(&dt); }

  t_docId put(const std::string &key) {
    RSDocumentMetadata *dmd =
        DocTable_Put(&dt, key.data(), key.size(), 1.0, Document_DefaultFlags, nullptr, 0, DocumentType_Hash);
    t_docId id = dmd->id;
    DMD_Return(dmd);
    return id;
  }

  void deleteDoc(t_docId id) { DMD_Return(DocTable_DeleteById(&dt, id)); }

  // The prefetch reads bucket memory, so with ASan this catches any out-of-bounds read.
  void prefetchAllAndBeyond() {
    for (t_docId id = 0; id <= dt.maxDocId + 2 * MAX_SIZE; ++id) {
      DocTable_Prefetch(&dt, id);
    }
  }
};

}  // namespace

TEST_F(DocTablePrefetchTest, EmptyTable) {
  prefetchAllAndBeyond();
  EXPECT_EQ(DocTable_Borrow(&dt, 1), nullptr);
}

TEST_F(DocTablePrefetchTest, LeavesDocsAndRefCountsUnchanged) {
  std::vector<t_docId> ids;
  for (int i = 0; i < 3 * MAX_SIZE; ++i) {
    ids.push_back(put("doc" + std::to_string(i)));
  }
  // Holes in the id range: a bucket left empty and a chain with a removed link.
  deleteDoc(ids[2]);
  deleteDoc(ids[MAX_SIZE + 3]);
  deleteDoc(ids[3]);
  deleteDoc(ids[2 * MAX_SIZE + 3]);

  std::vector<uint16_t> refCounts;
  for (t_docId id : ids) {
    const RSDocumentMetadata *dmd = DocTable_Borrow(&dt, id);
    refCounts.push_back(dmd ? dmd->ref_count : 0);
    DMD_Return(dmd);
  }

  prefetchAllAndBeyond();

  for (size_t i = 0; i < ids.size(); ++i) {
    const RSDocumentMetadata *dmd = DocTable_Borrow(&dt, ids[i]);
    EXPECT_EQ(dmd ? dmd->ref_count : 0, refCounts[i]) << "docId " << ids[i];
    if (dmd) {
      EXPECT_EQ(dmd->id, ids[i]);
    }
    DMD_Return(dmd);
  }
}

TEST_F(DocTablePrefetchTest, IdBeyondCapacity) {
  put("doc1");
  ASSERT_LT(dt.cap, MAX_SIZE);
  // DocTable_Put keeps the buckets ahead of maxDocId, so fake an id past them: the bounds check
  // must not rely on that.
  dt.maxDocId = MAX_SIZE - 1;
  DocTable_Prefetch(&dt, dt.maxDocId);
  dt.maxDocId = 1;
}
