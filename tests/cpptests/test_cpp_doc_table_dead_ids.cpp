/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"

#include <vector>

#include "doc_table.h"
#include "rmalloc.h"

namespace {

class DocTableDeadIdsTest : public ::testing::Test {
 protected:
  DocTable dt = NewDocTable(4, 4);

  void TearDown() override { DocTable_Free(&dt); }

  t_docId put(const std::string &key) {
    RSDocumentMetadata *dmd =
        DocTable_Put(&dt, key.data(), key.size(), 1.0, Document_DefaultFlags, nullptr, 0, DocumentType_Hash);
    t_docId id = dmd->id;
    DMD_Return(dmd);
    return id;
  }

  // Sweep `first..=last` in steps of `cap` IDs, as the GC does between lock releases.
  std::vector<t_docId> deadIds(t_docId first, t_docId last, size_t cap) {
    std::vector<t_docId> dead;
    std::vector<t_docId> out(cap);
    t_docId next = first;
    while (next <= last) {
      size_t n = DocTable_CollectDeadIds(&dt, &next, last, out.data(), out.size());
      dead.insert(dead.end(), out.begin(), out.begin() + n);
    }
    return dead;
  }
};

TEST_F(DocTableDeadIdsTest, DeletedIdsAreDeadAndLiveOnesAreNot) {
  for (int i = 0; i < 6; i++) {
    put("doc" + std::to_string(i));  // ids 1..6, more than the table's initial buckets
  }
  DMD_Return(DocTable_DeleteById(&dt, 2));
  DMD_Return(DocTable_DeleteById(&dt, 5));

  EXPECT_EQ(deadIds(1, dt.maxDocId, 100), (std::vector<t_docId>{2, 5}));
  // Small steps split the sweep across calls without changing what it finds.
  EXPECT_EQ(deadIds(1, dt.maxDocId, 1), (std::vector<t_docId>{2, 5}));
  // A sweep bounded below the highest ID does not look past its bound.
  EXPECT_EQ(deadIds(1, 4, 100), (std::vector<t_docId>{2}));
}

TEST_F(DocTableDeadIdsTest, AFullBufferStopsTheSweepWhereItCanResume) {
  for (int i = 0; i < 4; i++) {
    put("doc" + std::to_string(i));
  }
  for (t_docId id = 1; id <= 4; id++) {
    DMD_Return(DocTable_DeleteById(&dt, id));
  }
  t_docId out[3];
  t_docId next = 1;
  ASSERT_EQ(DocTable_CollectDeadIds(&dt, &next, 4, out, 3), 3u);
  EXPECT_EQ(next, 4u);
  ASSERT_EQ(DocTable_CollectDeadIds(&dt, &next, 4, out, 3), 1u);
  EXPECT_EQ(out[0], 4u);
  EXPECT_EQ(next, 5u);
  EXPECT_EQ(DocTable_CollectDeadIds(&dt, &next, 4, out, 3), 0u);
}

TEST_F(DocTableDeadIdsTest, ATableWithoutASetRecordsNothingOnDelete) {
  put("doc");
  ASSERT_EQ(dt.deletedDocs, nullptr);
  DMD_Return(DocTable_DeleteById(&dt, 1));
  EXPECT_EQ(dt.deletedDocs, nullptr);
}

}  // namespace
