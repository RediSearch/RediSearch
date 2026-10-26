/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"

#include <initializer_list>
#include <string>
#include <utility>

#include "doc_table.h"
#include "rmalloc.h"
#include "slot_ranges.h"

namespace {

// Slots as reported by `CLUSTER KEYSLOT` on a real Redis.
constexpr uint16_t SLOT_A = 15495;  // "{a}..."
constexpr uint16_t SLOT_B = 3300;   // "{b}..."

RedisModuleSlotRangeArray *makeRanges(std::initializer_list<std::pair<uint16_t, uint16_t>> ranges) {
  auto *array = static_cast<RedisModuleSlotRangeArray *>(rm_malloc(SlotRangeArray_SizeOf(ranges.size())));
  array->num_ranges = ranges.size();
  int i = 0;
  for (const auto &r : ranges) {
    array->ranges[i].start = r.first;
    array->ranges[i].end = r.second;
    ++i;
  }
  return array;
}

class DocTableSlotsTest : public ::testing::Test {
 protected:
  DocTable dt = NewDocTable(16, 16);

  void TearDown() override { DocTable_Free(&dt); }

  t_docId put(const std::string &key) {
    RSDocumentMetadata *dmd =
        DocTable_Put(&dt, key.data(), key.size(), 1.0, Document_DefaultFlags, nullptr, 0, DocumentType_Hash);
    t_docId id = dmd->id;
    DMD_Return(dmd);
    return id;
  }

  bool visible(t_docId id) {
    const RSDocumentMetadata *dmd = DocTable_Borrow(&dt, id);
    DMD_Return(dmd);
    return dmd != nullptr;
  }

  PendingSlotDeletion *addPending(std::initializer_list<std::pair<uint16_t, uint16_t>> ranges) {
    RedisModuleSlotRangeArray *slots = makeRanges(ranges);
    PendingSlotDeletion *pending = DocTable_AddPendingDeletion(&dt, slots);
    rm_free(slots);
    return pending;
  }
};

}  // namespace

TEST_F(DocTableSlotsTest, PutStoresClusterSlot) {
  const std::pair<const char *, uint16_t> cases[] = {
      {"foo", 12182}, {"bar", 5061}, {"{user1000}.following", 3443}, {"foo{}{bar}", 8363}, {"{a}x", SLOT_A},
  };
  for (const auto &[key, slot] : cases) {
    const RSDocumentMetadata *dmd = DocTable_Borrow(&dt, put(key));
    ASSERT_NE(dmd, nullptr);
    EXPECT_EQ(dmd->slot, slot) << key;
    DMD_Return(dmd);
  }
}

TEST_F(DocTableSlotsTest, IsEmptyTracksDocs) {
  EXPECT_TRUE(DocTable_IsEmpty(&dt));
  t_docId id = put("{a}1");
  EXPECT_FALSE(DocTable_IsEmpty(&dt));
  DMD_Return(DocTable_DeleteById(&dt, id));
  EXPECT_TRUE(DocTable_IsEmpty(&dt));
}

TEST_F(DocTableSlotsTest, RenameRecomputesSlot) {
  t_docId id = put("{a}x");
  DocTable_SetKeyById(&dt, id, "{b}x", 4);
  const RSDocumentMetadata *dmd = DocTable_Borrow(&dt, id);
  ASSERT_NE(dmd, nullptr);
  EXPECT_EQ(dmd->slot, SLOT_B);
  DMD_Return(dmd);
}

TEST_F(DocTableSlotsTest, PendingDeletionHidesOnlyOlderDocsInItsSlots) {
  t_docId inSlot = put("{a}1");
  t_docId otherSlot = put("{b}1");
  EXPECT_FALSE(DocTable_HasPendingDeletions(&dt));
  const size_t memsize = dt.memsize;

  PendingSlotDeletion *pending = addPending({{SLOT_A, SLOT_A}});
  EXPECT_TRUE(DocTable_HasPendingDeletions(&dt));
  const size_t pendingSize = dt.memsize - memsize;
  EXPECT_GT(pendingSize, 0);
  EXPECT_EQ(pending->maxDocId, otherSlot);

  // A doc added after the announcement belongs to the new owner of the slot.
  t_docId newInSlot = put("{a}2");

  EXPECT_FALSE(visible(inSlot));
  EXPECT_TRUE(visible(otherSlot));
  EXPECT_TRUE(visible(newInSlot));
  // Hidden docs are still physically present until deleted.
  EXPECT_TRUE(DocTable_Exists(&dt, inSlot));

  const size_t beforeRemove = dt.memsize;
  DocTable_RemovePendingDeletion(&dt, pending);
  EXPECT_FALSE(DocTable_HasPendingDeletions(&dt));
  EXPECT_EQ(dt.memsize, beforeRemove - pendingSize);
  EXPECT_TRUE(visible(inSlot));
}

TEST_F(DocTableSlotsTest, OverlappingDeletionsKeepTheirOwnCutoff) {
  t_docId first = put("{a}1");
  PendingSlotDeletion *older = addPending({{SLOT_B, SLOT_B}, {SLOT_A, SLOT_A}});
  t_docId second = put("{a}2");
  PendingSlotDeletion *newer = addPending({{SLOT_A, SLOT_A}});
  t_docId third = put("{a}3");

  EXPECT_FALSE(visible(first));
  EXPECT_FALSE(visible(second));
  EXPECT_TRUE(visible(third));

  DocTable_RemovePendingDeletion(&dt, newer);
  EXPECT_FALSE(visible(first));
  EXPECT_TRUE(visible(second));

  DocTable_RemovePendingDeletion(&dt, older);
  EXPECT_TRUE(visible(first));
}

TEST_F(DocTableSlotsTest, DeleteByIdRemovesHiddenDoc) {
  t_docId id = put("{a}1");
  addPending({{SLOT_A, SLOT_A}});
  RSDocumentMetadata *removed = DocTable_DeleteById(&dt, id);
  ASSERT_NE(removed, nullptr);
  EXPECT_EQ(removed->id, id);
  DMD_Return(removed);
  EXPECT_FALSE(DocTable_Exists(&dt, id));
}

TEST_F(DocTableSlotsTest, FreeReleasesPendingDeletions) {
  put("{a}1");
  addPending({{0, 16383}});
  addPending({{SLOT_A, SLOT_A}});
  // TearDown frees the table; the sanitizer build reports the nodes if they leak.
}
