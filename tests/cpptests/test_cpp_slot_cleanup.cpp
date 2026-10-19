/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"

#include "common.h"
#include "doc_id_meta.h"
#include "indexes.h"
#include "rmalloc.h"
#include "slot_cleanup.h"
#include "slot_ranges.h"
#include "spec.h"
#include "VecSim/vec_sim.h"

extern "C" {
#include "vector_index.h"
#include "redis_index.h"
}

#include <string>
#include <vector>

namespace {

// Slots as reported by `CLUSTER KEYSLOT` on a real Redis.
constexpr uint16_t SLOT_A = 15495;  // "{a}..."

// FLOAT32 DIM 4.
const char *const kVec = "aaaabbbbccccdddd";

class SlotCleanupTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx = nullptr;
  IndexSpec *spec = nullptr;
  RedisModuleSlotRangeArray *slotA = nullptr;

  void SetUp() override {
    CleanPool_ThreadPoolStart();
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    RMCK::flushdb(ctx);
    static int counter = 0;
    std::string name = "slotcleanup" + std::to_string(++counter);
    QueryError err = QueryError_Default();
    RMCK::ArgvList args(ctx, "FT.CREATE", name.c_str(), "ON", "HASH", "SCHEMA", "title", "TEXT", "vec",
                        "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2");
    spec = Indexes_CreateNewSpec(ctx, args, args.size(), &err);
    ASSERT_NE(spec, nullptr) << QueryError_GetUserError(&err);

    slotA = static_cast<RedisModuleSlotRangeArray *>(rm_malloc(SlotRangeArray_SizeOf(1)));
    slotA->num_ranges = 1;
    slotA->ranges[0] = {SLOT_A, SLOT_A};
  }

  void TearDown() override {
    if (spec) {
      Indexes_RemoveSpecFromGlobals(IndexSpec_GetStrongRefUnsafe(spec), false);
    }
    // Slot cleanup tasks and the dropped spec's teardown both run there.
    CleanPool_Wait();
    rm_free(slotA);
    RedisModule_FreeThreadSafeContext(ctx);
  }

  t_docId index(const char *key, const char *title = "hello") {
    RMCK::hset(ctx, key, "title", title);
    RMCK::hset(ctx, key, "vec", kVec);
    EXPECT_EQ(IndexSpec_UpdateDoc(spec, ctx, RMCK::RString(key), DocumentType_Hash, nullptr, nullptr, 0),
              REDISMODULE_OK);
    uint64_t docId = 0;
    EXPECT_EQ(DocIdMeta_Get(ctx, RMCK::RString(key), spec->specId, &docId), REDISMODULE_OK);
    return docId;
  }

  bool visible(t_docId id) {
    IndexSpec_LockRead(spec);
    const RSDocumentMetadata *dmd = DocTable_Borrow(&spec->docs, id);
    IndexSpec_Unlock(spec);
    DMD_Return(dmd);
    return dmd != nullptr;
  }

  bool present(t_docId id) {
    IndexSpec_LockRead(spec);
    bool exists = DocTable_Exists(&spec->docs, id);
    IndexSpec_Unlock(spec);
    return exists;
  }

  bool hasPending() {
    IndexSpec_LockRead(spec);
    bool pending = DocTable_HasPendingDeletions(&spec->docs);
    IndexSpec_Unlock(spec);
    return pending;
  }

  PendingSlotDeletion *addPending() {
    IndexSpec_LockWrite(spec);
    PendingSlotDeletion *pending = DocTable_AddPendingDeletion(&spec->docs, slotA);
    IndexSpec_Unlock(spec);
    return pending;
  }

  void removePending(PendingSlotDeletion *pending) {
    IndexSpec_LockWrite(spec);
    DocTable_RemovePendingDeletion(&spec->docs, pending);
    IndexSpec_Unlock(spec);
  }

  size_t vectorCount() {
    for (size_t i = 0; i < spec->numFields; ++i) {
      if (spec->fields[i].types & INDEXFLD_T_VECTOR) {
        return VecSimIndex_IndexSize(openVectorIndex(ctx, &spec->fields[i], DONT_CREATE_INDEX));
      }
    }
    return 0;
  }
};

}  // namespace

TEST_F(SlotCleanupTest, DeleteDocByIdUnsafeUpdatesIndexState) {
  t_docId kept = index("{b}1");
  t_docId id = index("{a}1", "hello world");
  const size_t docsLen = spec->stats.scoring.totalDocsLen;
  ASSERT_EQ(spec->stats.scoring.numDocuments, 2);
  ASSERT_EQ(vectorCount(), 2);

  IndexSpec_LockWrite(spec);
  IndexSpec_DeleteDocById_Unsafe(spec, id);
  IndexSpec_Unlock(spec);

  EXPECT_FALSE(present(id));
  EXPECT_TRUE(present(kept));
  EXPECT_EQ(spec->stats.scoring.numDocuments, 1);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, docsLen - 2);
  EXPECT_EQ(vectorCount(), 1);

  // A docId that is already gone, or was never assigned, leaves the index untouched.
  IndexSpec_LockWrite(spec);
  IndexSpec_DeleteDocById_Unsafe(spec, id);
  IndexSpec_DeleteDocById_Unsafe(spec, id + 100);
  IndexSpec_Unlock(spec);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 1);
  EXPECT_EQ(vectorCount(), 1);
}

TEST_F(SlotCleanupTest, CleanupDeletesOnlyDocsTheDeletionCovers) {
  t_docId a1 = index("{a}1");
  t_docId a2 = index("{a}2");
  t_docId b1 = index("{b}1");
  PendingSlotDeletion *pending = addPending();
  // Written after the announcement, so the deletion does not cover it.
  t_docId a3 = index("{a}3");

  arrayof(t_docId) docIds = SlotCleanup_CollectDocIds(spec, pending);
  EXPECT_EQ(std::vector<t_docId>(docIds, docIds + array_len(docIds)), (std::vector<t_docId>{a1, a2}));
  SlotCleanup_ApplyDeletes(spec, docIds);
  array_free(docIds);
  removePending(pending);

  EXPECT_FALSE(present(a1));
  EXPECT_FALSE(present(a2));
  EXPECT_TRUE(visible(b1));
  EXPECT_TRUE(visible(a3));
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2);
  EXPECT_EQ(vectorCount(), 2);
}

TEST_F(SlotCleanupTest, DocChangedBetweenPhasesIsNotDeletedTwice) {
  t_docId rewritten = index("{a}1");
  t_docId deleted = index("{a}2");
  t_docId untouched = index("{a}3");
  PendingSlotDeletion *pending = addPending();
  arrayof(t_docId) docIds = SlotCleanup_CollectDocIds(spec, pending);
  ASSERT_EQ(array_len(docIds), 3);

  // Re-indexing a key gives it a fresh docId and drops the old one; a plain delete drops it too.
  t_docId rewrittenNow = index("{a}1", "changed");
  ASSERT_NE(rewrittenNow, rewritten);
  IndexSpec_DeleteDocById(spec, deleted);
  ASSERT_EQ(spec->stats.scoring.numDocuments, 2);

  SlotCleanup_ApplyDeletes(spec, docIds);
  array_free(docIds);
  removePending(pending);

  EXPECT_FALSE(present(untouched));
  EXPECT_TRUE(visible(rewrittenNow));
  EXPECT_EQ(spec->stats.scoring.numDocuments, 1);
}

TEST_F(SlotCleanupTest, CollectSpansManyReadLockPasses) {
  const size_t n = 2 * SLOT_CLEANUP_SCAN_DOC_LIMIT + 1;
  for (size_t i = 0; i < n; ++i) {
    index(("{a}" + std::to_string(i)).c_str());
  }
  t_docId other = index("{b}1");
  PendingSlotDeletion *pending = addPending();

  arrayof(t_docId) docIds = SlotCleanup_CollectDocIds(spec, pending);
  EXPECT_EQ(array_len(docIds), n);
  SlotCleanup_ApplyDeletes(spec, docIds);
  array_free(docIds);
  removePending(pending);

  EXPECT_EQ(spec->stats.scoring.numDocuments, 1);
  EXPECT_TRUE(visible(other));
}

TEST_F(SlotCleanupTest, DeleteSlotsHidesAtOnceAndCleansInBackground) {
  t_docId a1 = index("{a}1");
  t_docId b1 = index("{b}1");

  Indexes_DeleteSlots(slotA);
  EXPECT_FALSE(visible(a1));
  EXPECT_TRUE(visible(b1));

  CleanPool_Wait();
  EXPECT_FALSE(hasPending());
  EXPECT_FALSE(present(a1));
  EXPECT_TRUE(visible(b1));
  EXPECT_EQ(spec->stats.scoring.numDocuments, 1);
}

TEST_F(SlotCleanupTest, OverlappingDeletionsEachKeepTheirCutoff) {
  t_docId first = index("{a}1");
  Indexes_DeleteSlots(slotA);
  t_docId second = index("{a}2");
  Indexes_DeleteSlots(slotA);
  t_docId third = index("{a}3");

  CleanPool_Wait();
  EXPECT_FALSE(hasPending());
  EXPECT_FALSE(present(first));
  EXPECT_FALSE(present(second));
  EXPECT_TRUE(visible(third));
}

TEST_F(SlotCleanupTest, SpecDroppedWithCleanupQueuedIsSafe) {
  index("{a}1");
  Indexes_DeleteSlots(slotA);
  Indexes_RemoveSpecFromGlobals(IndexSpec_GetStrongRefUnsafe(spec), false);
  spec = nullptr;
  // The task either finds the spec gone or keeps it alive until it finishes; the sanitizer build
  // reports a use-after-free or a leaked node otherwise.
  CleanPool_Wait();
}
