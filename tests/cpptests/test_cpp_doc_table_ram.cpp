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

#include "doc_table.h"

namespace {

class DocTableRamTest : public ::testing::Test {
 protected:
  DocTable dt = NewDocTable(2, 8, false);
  struct timespec now = {.tv_sec = 100, .tv_nsec = 0};

  void TearDown() override { DocTable_Free(&dt); }

  RSDocumentMetadata *putWithId(t_docId id, const std::string &key) {
    return DocTable_PutWithId(&dt, id, key.data(), key.size(), 2.5, Document_DefaultFlags,
                              DocumentType_Hash);
  }

  static RSDocumentMetadata blankCopy() {
    RSDocumentMetadata out = {};
    out.ref_count = 1;
    return out;
  }
};

}  // namespace

TEST_F(DocTableRamTest, PutWithIdRaisesMaxDocIdAndNeverLowersIt) {
  DMD_Return(putWithId(5, "a"));
  EXPECT_EQ(5, dt.maxDocId);
  DMD_Return(putWithId(3, "b"));
  EXPECT_EQ(5, dt.maxDocId);
  EXPECT_TRUE(DocTable_Exists(&dt, 3));
  EXPECT_TRUE(DocTable_Exists(&dt, 5));
  EXPECT_FALSE(DocTable_Exists(&dt, 4));
}

TEST_F(DocTableRamTest, CopyByIdOwnsItsKeyAcrossRenameAndDelete) {
  RSDocumentMetadata *dmd = putWithId(7, "old-key");
  dmd->docLen = 11;
  dmd->maxTermFreq = 4;
  const uint16_t slot = dmd->slot;
  DocTable_SetDocExpiration(dmd, {.tv_sec = 500, .tv_nsec = 0});
  DMD_Return(dmd);

  RSDocumentMetadata out = blankCopy();
  ASSERT_TRUE(DocTable_CopyById(&dt, 7, &out, &now));
  EXPECT_EQ(7, out.id);
  EXPECT_EQ(11, out.docLen);
  EXPECT_EQ(4, out.maxTermFreq);
  EXPECT_FLOAT_EQ(2.5f, out.score);
  EXPECT_EQ(500LL * 1000000000LL, out.expirationTimeNs);
  EXPECT_EQ(slot, out.slot);

  DocTable_SetKeyById(&dt, 7, "new-key", 7);
  DMD_Return(DocTable_DeleteById(&dt, 7));
  EXPECT_STREQ("old-key", out.keyPtr);
  sdsfree(out.keyPtr);
}

TEST_F(DocTableRamTest, CopyByIdRejectsMissingDeletedAndExpired) {
  RSDocumentMetadata *dmd = putWithId(2, "k");
  DocTable_SetDocExpiration(dmd, {.tv_sec = 100, .tv_nsec = 0});
  DMD_Return(dmd);
  DMD_Return(putWithId(3, "j"));
  DMD_Return(DocTable_DeleteById(&dt, 3));

  RSDocumentMetadata out = blankCopy();
  EXPECT_FALSE(DocTable_CopyById(&dt, 2, &out, &now));  // expires at `now`
  EXPECT_FALSE(DocTable_CopyById(&dt, 3, &out, &now));
  EXPECT_FALSE(DocTable_CopyById(&dt, 9, &out, &now));
  EXPECT_EQ(nullptr, out.keyPtr);
}
