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
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "doc_table.h"
#include "inverted_index.h"
#include "iterators_ffi.h"
#include "numeric_range_tree_ffi.h"
#include "rmalloc.h"
#include "slot_ranges.h"
#include "spec.h"

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
    EXPECT_EQ(DocTable_IsVisible(&dt, id), dmd != nullptr);
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
  EXPECT_FALSE(visible(id));
  EXPECT_FALSE(visible(0));
  EXPECT_FALSE(visible(id + 1));
}

TEST_F(DocTableSlotsTest, NumericTopKExcludesPendingDeletionsBeforeFillingHeap) {
  const t_docId hidden = put("{a}old");
  const t_docId otherSlot = put("{b}live");
  addPending({{SLOT_A, SLOT_A}});
  const t_docId newInSlot = put("{a}new");
  const t_docId deleted = put("{b}deleted");
  DMD_Return(DocTable_DeleteById(&dt, deleted));

  auto tree = std::unique_ptr<NumericRangeTree, decltype(&NumericRangeTree_Free)>(
      NewNumericRangeTree(false), NumericRangeTree_Free);
  _NumericRangeTree_Add(tree.get(), hidden, 1.0, false, false, 0);
  _NumericRangeTree_Add(tree.get(), otherSlot, 3.0, false, false, 0);
  _NumericRangeTree_Add(tree.get(), newInSlot, 2.0, false, false, 0);
  _NumericRangeTree_Add(tree.get(), deleted, 0.0, false, false, 0);

  ASSERT_TRUE(DocTable_Exists(&dt, hidden));
  ASSERT_FALSE(visible(hidden));
  ASSERT_TRUE(visible(newInSlot));

  // Borrow the fixture's table without transferring ownership to the search context.
  IndexSpec spec = {};
  spec.docs = dt;
  RedisSearchCtx sctx = {};
  sctx.spec = &spec;
  NumericFilter filter = {};
  filter.min = -std::numeric_limits<double>::infinity();
  filter.max = std::numeric_limits<double>::infinity();
  filter.minInclusive = filter.maxInclusive = true;
  FieldFilterContext filterCtx = {};
  filterCtx.field.index_tag = FieldMaskOrIndex_Index;
  filterCtx.field.index = 0;
  filterCtx.predicate = FIELD_EXPIRATION_PREDICATE_DEFAULT;

  const t_docId expected[] = {newInSlot, otherSlot};
  for (size_t k : {1, 2}) {
    SCOPED_TRACE(k);
    auto freeIterator = [](QueryIterator *it) { it->Free(it); };
    auto it = std::unique_ptr<QueryIterator, decltype(freeIterator)>(
        NewNumericTopKIterator(tree.get(), &filter, true, k, 3, nullptr, &sctx, &filterCtx),
        freeIterator);
    ASSERT_NE(it, nullptr);
    for (size_t i = 0; i < k; ++i) {
      ASSERT_EQ(it->Read(it.get()), ITERATOR_OK);
      EXPECT_EQ(it->current->docId, expected[i]);
    }
    EXPECT_EQ(it->Read(it.get()), ITERATOR_EOF);
  }
}

TEST_F(DocTableSlotsTest, FreeReleasesPendingDeletions) {
  put("{a}1");
  addPending({{0, 16383}});
  addPending({{SLOT_A, SLOT_A}});
  // TearDown frees the table; the sanitizer build reports the nodes if they leak.
}

TEST_F(DocTableSlotsTest, KeyLookupRejectsPendingDeletionCandidates) {
  const std::string key = "{a}same";
  const t_docId oldId = put(key);
  const t_docId unaffectedId = put("{b}other");
  PendingSlotDeletion *pending = addPending({{SLOT_A, SLOT_A}});
  ASSERT_TRUE(DocTable_StartKeyMap(&dt));
  DocTableKeyMapIterator iter = {};
  ASSERT_TRUE(DocTable_BuildKeyMapBatch(&dt, &iter, SIZE_MAX));

  EXPECT_FALSE(DocTable_KeyEquals(&dt, oldId, key.data(), key.size()));
  EXPECT_EQ(DocTable_GetIdByKey(&dt, key.data(), key.size()), 0u);
  EXPECT_EQ(DocTable_GetIdByKey(&dt, "{b}other", 8), unaffectedId);

  // Slot trim detaches Redis keys before asynchronous index cleanup. A recreated
  // key can therefore coexist with the hidden DMD until cleanup deletes the old ID.
  const t_docId replacementId = put(key);
  ASSERT_GT(replacementId, pending->maxDocId);
  EXPECT_TRUE(DocTable_KeyEquals(&dt, replacementId, key.data(), key.size()));
  EXPECT_EQ(DocTable_GetIdByKey(&dt, key.data(), key.size()), replacementId);

  // Rehashing can change which of the identical-key candidates is encountered first.
  for (int i = 0; i < 100; ++i) {
    put("{b}fill:" + std::to_string(i));
    EXPECT_EQ(DocTable_GetIdByKey(&dt, key.data(), key.size()), replacementId);
  }
  DMD_Return(DocTable_DeleteById(&dt, oldId));
  DocTable_RemovePendingDeletion(&dt, pending);
  EXPECT_EQ(DocTable_GetIdByKey(&dt, key.data(), key.size()), replacementId);
}
