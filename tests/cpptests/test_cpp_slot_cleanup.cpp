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
#include "index_utils.h"
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
#include "search_disk.h"
#include "result_processor.h"
#include "search_result_ffi.h"
#include "query_request.h"
#include "types_ffi.h"
extern RedisSearchDiskAPI *disk;
extern RedisSearchDisk *disk_db;
}

#include <algorithm>
#include <functional>
#include <string>
#include <utility>
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

  SlotCleanupCursor cursor = {};
  arrayof(t_docId) docIds = SlotCleanup_CollectDocIds(spec, pending, &cursor);
  EXPECT_TRUE(SlotCleanup_CursorDone(&cursor));
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
  SlotCleanupCursor cursor = {};
  arrayof(t_docId) docIds = SlotCleanup_CollectDocIds(spec, pending, &cursor);
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

TEST_F(SlotCleanupTest, CleanupRunsInBoundedChunks) {
  const size_t n = 2 * SLOT_CLEANUP_SCAN_LIMIT + 1;
  for (size_t i = 0; i < n; ++i) {
    index(("{a}" + std::to_string(i)).c_str());
  }
  t_docId other = index("{b}1");
  PendingSlotDeletion *pending = addPending();

  SlotCleanupCursor cursor = {};
  size_t total = 0, chunks = 0;
  while (!SlotCleanup_CursorDone(&cursor)) {
    arrayof(t_docId) docIds = SlotCleanup_CollectDocIds(spec, pending, &cursor);
    // One doc per bucket here, so a chunk never overshoots its limit.
    EXPECT_LE(array_len(docIds), SLOT_CLEANUP_CHUNK_DOC_LIMIT);
    total += array_len(docIds);
    ++chunks;
    SlotCleanup_ApplyDeletes(spec, docIds);
    array_free(docIds);
  }
  removePending(pending);

  EXPECT_EQ(total, n);
  EXPECT_GT(chunks, n / SLOT_CLEANUP_CHUNK_DOC_LIMIT);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 1);
  EXPECT_TRUE(visible(other));
}

TEST_F(SlotCleanupTest, DeleteSlotsHidesAtOnceAndCleansInBackground) {
  t_docId a1 = index("{a}1");
  t_docId a2 = index("{a}2");
  t_docId b1 = index("{b}1");

  Indexes_DeleteSlots(slotA);
  EXPECT_FALSE(visible(a1));
  EXPECT_TRUE(visible(b1));

  CleanPool_Wait();
  EXPECT_FALSE(hasPending());
  EXPECT_FALSE(present(a1));
  EXPECT_FALSE(present(a2));
  EXPECT_TRUE(visible(b1));
  EXPECT_EQ(spec->stats.scoring.numDocuments, 1);
  // Every deletion is left for the GC to take on its next run, which schedules the inverted-index
  // cleanup from that count.
  EXPECT_EQ(IndexSpec_TakeGCDeferredDeletes(spec), 2);
}

TEST_F(SlotCleanupTest, ScheduledCleanupFinishesAcrossRequeues) {
  const size_t n = 2 * SLOT_CLEANUP_CHUNK_DOC_LIMIT + 1;
  for (size_t i = 0; i < n; ++i) {
    index(("{a}" + std::to_string(i)).c_str());
  }
  t_docId other = index("{b}1");

  Indexes_DeleteSlots(slotA);
  CleanPool_Wait();

  EXPECT_FALSE(hasPending());
  EXPECT_EQ(spec->stats.scoring.numDocuments, 1);
  EXPECT_TRUE(visible(other));
  EXPECT_EQ(IndexSpec_TakeGCDeferredDeletes(spec), n);
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

TEST_F(SlotCleanupTest, ExplicitCutoffDoesNotUseRamDocTableMaxId) {
  IndexSpec_LockWrite(spec);
  EXPECT_EQ(spec->docs.maxDocId, 0);
  PendingSlotDeletion *first = DocTable_AddPendingDeletionWithCutoff(&spec->docs, slotA, 10);
  PendingSlotDeletion *second = DocTable_AddPendingDeletionWithCutoff(&spec->docs, slotA, 20);
  EXPECT_TRUE(DocTable_IsDocPendingDeletionForSlot(&spec->docs, 10, SLOT_A));
  EXPECT_TRUE(DocTable_IsDocPendingDeletionForSlot(&spec->docs, 20, SLOT_A));
  EXPECT_FALSE(DocTable_IsDocPendingDeletionForSlot(&spec->docs, 21, SLOT_A));
  EXPECT_FALSE(DocTable_IsDocPendingDeletionForSlot(&spec->docs, 10, SLOT_A - 1));

  DocTable_RemovePendingDeletion(&spec->docs, second);
  EXPECT_TRUE(DocTable_IsDocPendingDeletionForSlot(&spec->docs, 10, SLOT_A));
  EXPECT_FALSE(DocTable_IsDocPendingDeletionForSlot(&spec->docs, 11, SLOT_A));
  DocTable_RemovePendingDeletion(&spec->docs, first);
  EXPECT_FALSE(DocTable_IsDocPendingDeletionForSlot(&spec->docs, 10, SLOT_A));
  IndexSpec_Unlock(spec);
}

namespace {

std::function<void(IndexSpec *, t_docId)> onDiskMetadataRead;
bool firstDiskDocDeleted = false;
std::vector<std::pair<t_docId, DiskReadFailureReason>> metadataFailures;

class DiskSlotVisibilityTest : public SlotCleanupTest {
 protected:
  RedisSearchDiskAPI api{};
  RedisSearchDiskAPI *savedDisk = nullptr;
  RedisSearchDisk *savedDiskDb = nullptr;

  void SetUp() override {
    SlotCleanupTest::SetUp();
    savedDisk = disk;
    savedDiskDb = disk_db;
    disk = &api;
    disk_db = nullptr;
    spec->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(spec);
    firstDiskDocDeleted = false;
    metadataFailures.clear();
    onDiskMetadataRead = {};
    api.docTable.isDocIdDeleted = [](RedisSearchDiskIndexSpec *, t_docId id) {
      return firstDiskDocDeleted && id == 1;
    };
    api.docTable.getDocumentMetadata = [](RedisSearchDiskIndexSpec *handle, t_docId id,
                                          RSDocumentMetadata *dmd, AllocateKeyCallback allocateKey,
                                          const t_expirationTimePoint *,
                                          RedisSearchDiskSnapshot *, DiskReadFailureReason *reason) {
      EXPECT_FALSE(IndexSpec_IsLocked(reinterpret_cast<IndexSpec *>(handle)));
      if (onDiskMetadataRead) onDiskMetadataRead(reinterpret_cast<IndexSpec *>(handle), id);
      if (firstDiskDocDeleted && id == 1) {
        *reason = DISK_READ_DELETED;
        return false;
      }
      for (const auto &[failedId, failure] : metadataFailures) {
        if (failedId == id) {
          *reason = failure;
          return false;
        }
      }
      const char *key = id == 2 ? "{b}other" : "{a}moving";
      dmd->id = id;
      dmd->keyPtr = allocateKey(key, strlen(key));
      dmd->slot = 0;
      return true;
    };
  }

  void TearDown() override {
    onDiskMetadataRead = {};
    spec->diskSpec = nullptr;
    disk = savedDisk;
    disk_db = savedDiskDb;
    SlotCleanupTest::TearDown();
  }

  std::vector<t_docId> queryIds(std::pair<uint64_t, uint64_t> *counts = nullptr) {
    struct Iterator {
      QueryIterator base{};
      RSIndexResult current{};
      Iterator() {
        base.Free = [](QueryIterator *) {};
        base.Read = [](QueryIterator *base) {
          auto *self = reinterpret_cast<Iterator *>(base);
          if (base->lastDocId == 4) {
            base->atEOF = true;
            base->current = nullptr;
            return ITERATOR_EOF;
          }
          self->current.docId = ++base->lastDocId;
          base->current = &self->current;
          return ITERATOR_OK;
        };
      }
    } iterator;
    MockQueryEvalCtx queryContext;
    RedisSearchCtx &sctx = queryContext.sctx;
    sctx.redisCtx = ctx;
    sctx.spec = spec;
    QueryProcessingCtx processing{};
    processing.isProfile = counts != nullptr;
    ResultProcessor *processor =
        RPQueryIterator_New(&iterator.base, nullptr, 0, &sctx, counts != nullptr, false);
    processor->parent = &processing;
    SearchResult result = SearchResult_New();
    std::vector<t_docId> ids;
    while (processor->Next(processor, &result) == RS_RESULT_OK) {
      ids.push_back(SearchResult_GetDocId(&result));
      SearchResult_Clear(&result);
    }
    if (counts) RPQueryIterator_GetCandidateCounts(processor, &counts->first, &counts->second);
    SearchResult_Destroy(&result);
    processor->Free(processor);
    return ids;
  }
};

}  // namespace

TEST_F(DiskSlotVisibilityTest, BackendDeletionReasonsReclassifySyncCandidates) {
  IndexSpec_LockWrite(spec);
  PendingSlotDeletion *pending = DocTable_AddPendingDeletionWithCutoff(&spec->docs, slotA, 3);
  IndexSpec_Unlock(spec);

  metadataFailures = {{1, DISK_READ_DELETED}, {3, DISK_READ_DELETED}};
  std::pair<uint64_t, uint64_t> counts;
  EXPECT_EQ(queryIds(&counts), (std::vector<t_docId>{2, 4}));
  EXPECT_EQ(counts, (std::pair<uint64_t, uint64_t>{2, 2}));
  removePending(pending);
  metadataFailures.clear();
  EXPECT_EQ(queryIds(&counts), (std::vector<t_docId>{1, 2, 3, 4}));
  EXPECT_EQ(counts, (std::pair<uint64_t, uint64_t>{0, 4}));
}

TEST_F(DiskSlotVisibilityTest, MetadataReadBeforeFinalBatchRemainsHiddenAfterFilterRemoval) {
  IndexSpec_LockWrite(spec);
  PendingSlotDeletion *pending = DocTable_AddPendingDeletionWithCutoff(&spec->docs, slotA, 1);
  IndexSpec_Unlock(spec);
  onDiskMetadataRead = [pending](IndexSpec *spec, t_docId id) {
    if (id != 1) return;
    IndexSpec_LockWrite(spec);
    firstDiskDocDeleted = true;
    DocTable_RemovePendingDeletion(&spec->docs, pending);
    IndexSpec_Unlock(spec);
  };

  EXPECT_EQ(queryIds(), (std::vector<t_docId>{2, 3, 4}));
  EXPECT_FALSE(hasPending());
}

namespace {

struct BufferedAsyncPool {
  struct Read {
    t_docId id;
    uint64_t userData;
  };
  std::vector<Read> reads;
  std::vector<RSDocumentMetadata *> decoded;
  std::vector<t_docId> deletedIds;
  uint64_t snapshotMarker = 0;
  size_t creates = 0, adds = 0, polls = 0, frees = 0, syncReads = 0;
};

BufferedAsyncPool *bufferedAsyncPool = nullptr;

class BufferedDiskQuery {
  struct Iterator {
    QueryIterator base{};
    RSIndexResult *current = nullptr;
    Iterator() {
      current = NewVirtualResult(1.0, RS_FIELDMASK_ALL);
      base.Free = [](QueryIterator *base) {
        auto *self = reinterpret_cast<Iterator *>(base);
        IndexResult_Free(self->current);
        self->current = nullptr;
      };
      base.Read = [](QueryIterator *base) {
        auto *self = reinterpret_cast<Iterator *>(base);
        if (base->lastDocId == 6) {
          base->atEOF = true;
          base->current = nullptr;
          return ITERATOR_EOF;
        }
        self->current->docId = ++base->lastDocId;
        base->current = self->current;
        return ITERATOR_OK;
      };
    }
  } iterator;
  MockQueryEvalCtx queryContext;
  RedisSearchCtx &sctx = queryContext.sctx;
  QueryProcessingCtx processing{}, hybridProcessing{};
  QueryProcessingCtx *activeProcessing = &processing;
  RLookup sourceLookup = RLookup_New(), tailLookup = RLookup_New();
  RPStatus upstreamStatus = RS_RESULT_OK;
  ResultProcessor *processor = nullptr;
  SearchResult result;

 public:
  BufferedDiskQuery(RedisModuleCtx *ctx, IndexSpec *spec, RedisSearchDiskSnapshot *snapshot,
                    bool profiled = false)
      : result(SearchResult_New()) {
    sctx.redisCtx = ctx;
    sctx.spec = spec;
    sctx.diskSnapshot = snapshot;
    processing.isProfile = profiled;
    processor = RPQueryIterator_New(&iterator.base, nullptr, 0, &sctx, profiled, false);
    QITR_PushRP(&processing, processor);
  }

  void mergeHybrid() {
    auto **upstreams = array_new(ResultProcessor *, 1);
    array_append(upstreams, processor);
    auto *lookups = static_cast<HybridLookupContext *>(rm_calloc(1, sizeof(HybridLookupContext)));
    lookups->sourceLookups = array_new(const RLookup *, 1);
    array_append(lookups->sourceLookups, &sourceLookup);
    lookups->tailLookup = &tailLookup;
    auto *scoring = HybridScoringContext_NewRRF(60, 6, false);
    processor = RPHybridMerger_New(&sctx, scoring, upstreams, 1, nullptr, nullptr,
                                  &upstreamStatus, lookups, nullptr);
    activeProcessing = &hybridProcessing;
    QITR_PushRP(activeProcessing, processor);
  }

  void sortByScore() {
    auto *sorter = RPSorter_NewByScore(6, nullptr);
    QITR_PushRP(activeProcessing, sorter);
    processor = sorter;
  }

  void normalizeScores() {
    processor = RPMaxScoreNormalizer_New(nullptr);
    QITR_PushRP(activeProcessing, processor);
  }

  std::pair<uint64_t, uint64_t> candidateCounts() const {
    std::pair<uint64_t, uint64_t> counts;
    RPQueryIterator_GetCandidateCounts(processor, &counts.first, &counts.second);
    return counts;
  }

  uint32_t skipped() const {
    return activeProcessing->skippedResults;
  }

  ~BufferedDiskQuery() {
    SearchResult_Destroy(&result);
    QITR_FreeChain(&hybridProcessing);
    QITR_FreeChain(&processing);
    RLookup_Cleanup(&sourceLookup);
    RLookup_Cleanup(&tailLookup);
  }

  t_docId first() {
    EXPECT_EQ(processor->Next(processor, &result), RS_RESULT_OK);
    const auto id = SearchResult_GetDocId(&result);
    SearchResult_Clear(&result);
    return id;
  }

  std::vector<t_docId> remaining() {
    std::vector<t_docId> ids;
    int rc;
    while ((rc = processor->Next(processor, &result)) == RS_RESULT_OK) {
      ids.push_back(SearchResult_GetDocId(&result));
      SearchResult_Clear(&result);
    }
    EXPECT_EQ(rc, RS_RESULT_EOF);
    return ids;
  }
};

class DiskSlotAsyncVisibilityTest : public DiskSlotVisibilityTest {
 protected:
  BufferedAsyncPool pool;
  bool savedAsyncEnabled = false;
  decltype(RSGlobalConfig.diskAsyncReadPoolSize) savedPoolSize{};
  decltype(RSGlobalConfig.diskAsyncReadQueueFactor) savedQueueFactor{};

  void SetUp() override {
    DiskSlotVisibilityTest::SetUp();
    bufferedAsyncPool = &pool;
    disk_db = reinterpret_cast<RedisSearchDisk *>(&pool);
    savedAsyncEnabled = SearchDisk_GetAsyncIOEnabled();
    savedPoolSize = RSGlobalConfig.diskAsyncReadPoolSize;
    savedQueueFactor = RSGlobalConfig.diskAsyncReadQueueFactor;
    SearchDisk_SetAsyncIOEnabled(true);
    RSGlobalConfig.diskAsyncReadPoolSize = 8;
    RSGlobalConfig.diskAsyncReadQueueFactor = 1;
    api.basic.isAsyncIOSupported = [](RedisSearchDisk *) { return true; };
    api.docTable.isDocIdDeleted = [](RedisSearchDiskIndexSpec *, t_docId id) {
      for (const auto deleted : bufferedAsyncPool->deletedIds) {
        if (deleted == id) return true;
      }
      return false;
    };
    api.docTable.getDocumentMetadata = [](RedisSearchDiskIndexSpec *, t_docId, RSDocumentMetadata *,
                                          AllocateKeyCallback, const t_expirationTimePoint *,
                                          RedisSearchDiskSnapshot *, DiskReadFailureReason *reason) {
      *reason = DISK_READ_ERROR;
      ++bufferedAsyncPool->syncReads;
      ADD_FAILURE() << "the async query must not fall back to synchronous metadata reads";
      return false;
    };
    api.docTable.createAsyncReadPool = [](RedisSearchDiskIndexSpec *, uint16_t capacity,
                                          RedisSearchDiskSnapshot *snapshot) {
      ++bufferedAsyncPool->creates;
      EXPECT_EQ(capacity, 8);
      EXPECT_EQ(snapshot,
                reinterpret_cast<RedisSearchDiskSnapshot *>(&bufferedAsyncPool->snapshotMarker));
      return static_cast<RedisSearchDiskAsyncReadPool>(bufferedAsyncPool);
    };
    api.docTable.addAsyncRead = [](RedisSearchDiskAsyncReadPool handle, t_docId id,
                                   uint64_t userData) {
      auto *pool = static_cast<BufferedAsyncPool *>(handle);
      ++pool->adds;
      pool->reads.push_back({id, userData});
      return true;
    };
    api.docTable.pollAsyncReads = [](RedisSearchDiskAsyncReadPool handle, uint32_t,
                                     AsyncReadResult *results, uint16_t capacity, AsyncReadFailure *failures,
                                     uint16_t failureCapacity, t_expirationTimePoint,
                                     AllocateDMDCallback allocate) {
      auto *pool = static_cast<BufferedAsyncPool *>(handle);
      ++pool->polls;
      EXPECT_LE(pool->reads.size(), capacity);
      uint16_t ready = 0, failed = 0;
      for (const auto &read : pool->reads) {
        const auto failure = std::find_if(metadataFailures.begin(), metadataFailures.end(),
            [&](const auto &entry) { return entry.first == read.id; });
        if (failure != metadataFailures.end()) {
          EXPECT_LT(failed, failureCapacity);
          failures[failed++] = {read.userData, failure->second};
          continue;
        }
        const std::string key = std::string(read.id == 1 || read.id == 5 ? "{b}other:" : "{a}moving:") +
                                std::to_string(read.id);
        auto *dmd = allocate(key.data(), key.size());
        dmd->id = read.id;
        dmd->slot = 0;
        DMD_Incref(dmd);
        pool->decoded.push_back(dmd);
        results[ready++] = {dmd, read.userData};
      }
      pool->reads.clear();
      return AsyncPollResult{ready, failed, 0};
    };
    api.docTable.freeAsyncReadPool = [](RedisSearchDiskAsyncReadPool handle) {
      auto *pool = static_cast<BufferedAsyncPool *>(handle);
      EXPECT_TRUE(pool->reads.empty());
      ++pool->frees;
    };
  }

  void TearDown() override {
    EXPECT_EQ(pool.creates, pool.frees);
    for (auto *dmd : pool.decoded) {
      EXPECT_EQ(dmd->ref_count, 1) << "query retained ownership of decoded metadata";
      DMD_Return(dmd);
    }
    SearchDisk_SetAsyncIOEnabled(savedAsyncEnabled);
    RSGlobalConfig.diskAsyncReadPoolSize = savedPoolSize;
    RSGlobalConfig.diskAsyncReadQueueFactor = savedQueueFactor;
    bufferedAsyncPool = nullptr;
    DiskSlotVisibilityTest::TearDown();
  }

  RedisSearchDiskSnapshot *snapshot() {
    return reinterpret_cast<RedisSearchDiskSnapshot *>(&pool.snapshotMarker);
  }

  PendingSlotDeletion *registerTrim() {
    IndexSpec_LockWrite(spec);
    auto *pending = DocTable_AddPendingDeletionWithCutoff(&spec->docs, slotA, 3);
    IndexSpec_Unlock(spec);
    return pending;
  }

  void expectDecodedBeforeTrim() {
    ASSERT_EQ(pool.creates, 1);
    ASSERT_EQ(pool.adds, 6);
    ASSERT_EQ(pool.decoded.size(), 6);
    EXPECT_GT(pool.polls, 0);
    EXPECT_EQ(pool.syncReads, 0);
    EXPECT_EQ(pool.decoded[1]->ref_count, 2);
    EXPECT_FALSE(hasPending());
  }
};

}  // namespace

TEST_F(DiskSlotAsyncVisibilityTest, DeliveredMetadataSurvivesTrimBeforeConsumption) {
  {
    BufferedDiskQuery query(ctx, spec, snapshot(), true);
    ASSERT_EQ(query.first(), 1);
    expectDecodedBeforeTrim();
    EXPECT_EQ(query.candidateCounts(), (std::pair<uint64_t, uint64_t>{0, 6}));
    auto *pending = registerTrim();
    EXPECT_EQ(query.remaining(), (std::vector<t_docId>{2, 3, 4, 5, 6}));
    EXPECT_EQ(query.candidateCounts(), (std::pair<uint64_t, uint64_t>{0, 6}));
    EXPECT_TRUE(hasPending());
    removePending(pending);
  }
  EXPECT_EQ(pool.frees, 1);
}

TEST_F(DiskSlotAsyncVisibilityTest, DeliveredMetadataSurvivesFinalBatchCompletion) {
  {
    BufferedDiskQuery query(ctx, spec, snapshot());
    ASSERT_EQ(query.first(), 1);
    expectDecodedBeforeTrim();
    auto *pending = registerTrim();
    IndexSpec_LockWrite(spec);
    pool.deletedIds = {2, 3};
    DocTable_RemovePendingDeletion(&spec->docs, pending);
    IndexSpec_Unlock(spec);
    EXPECT_FALSE(hasPending());
    EXPECT_EQ(query.remaining(), (std::vector<t_docId>{2, 3, 4, 5, 6}));
  }
  EXPECT_EQ(pool.frees, 1);
}

TEST_F(DiskSlotAsyncVisibilityTest, EarlyQueryFreeReleasesEveryBufferedMetadataReference) {
  {
    BufferedDiskQuery query(ctx, spec, snapshot());
    ASSERT_EQ(query.first(), 1);
    expectDecodedBeforeTrim();
    registerTrim();
  }
  EXPECT_EQ(pool.frees, 1);
  ASSERT_EQ(pool.decoded.size(), 6);
  for (auto *dmd : pool.decoded) EXPECT_EQ(dmd->ref_count, 1);
}

enum class AcceptedResultBuffer { Sorter, Normalizer, HybridMerger };

class AcceptedDiskResultTest : public DiskSlotAsyncVisibilityTest,
                               public ::testing::WithParamInterface<AcceptedResultBuffer> {};

TEST_P(AcceptedDiskResultTest, AcceptedRowsSurviveLaterTrimRegistration) {
  BufferedDiskQuery query(ctx, spec, snapshot());
  switch (GetParam()) {
    case AcceptedResultBuffer::Sorter:
      query.sortByScore();
      break;
    case AcceptedResultBuffer::Normalizer:
      query.normalizeScores();
      break;
    case AcceptedResultBuffer::HybridMerger:
      query.mergeHybrid();
      break;
  }
  const auto first = query.first();
  EXPECT_EQ(pool.decoded.size(), 6u);
  EXPECT_EQ(pool.syncReads, 0u);
  EXPECT_FALSE(hasPending());
  auto *pending = registerTrim();
  auto ids = query.remaining();
  ids.push_back(first);
  std::sort(ids.begin(), ids.end());
  EXPECT_EQ(ids, (std::vector<t_docId>{1, 2, 3, 4, 5, 6}));
  EXPECT_EQ(query.skipped(), 0u);
  removePending(pending);
}

INSTANTIATE_TEST_SUITE_P(BufferStage, AcceptedDiskResultTest,
                         ::testing::Values(AcceptedResultBuffer::Sorter,
                                           AcceptedResultBuffer::Normalizer,
                                           AcceptedResultBuffer::HybridMerger));

TEST_F(DiskSlotVisibilityTest, OnlyDeletedSyncFailuresChangeDeletedCandidateCount) {
  metadataFailures = {{1, DISK_READ_DELETED}, {2, DISK_READ_EXPIRED},
                      {3, DISK_READ_NOT_FOUND}, {4, DISK_READ_ERROR}};
  std::pair<uint64_t, uint64_t> counts;
  EXPECT_TRUE(queryIds(&counts).empty());
  EXPECT_EQ(counts, (std::pair<uint64_t, uint64_t>{1, 3}));
}

TEST_F(DiskSlotAsyncVisibilityTest, OnlyDeletedAsyncFailuresChangeDeletedCandidateCount) {
  metadataFailures = {{2, DISK_READ_DELETED}, {3, DISK_READ_EXPIRED},
                      {4, DISK_READ_NOT_FOUND}, {5, DISK_READ_ERROR}};
  {
    BufferedDiskQuery query(ctx, spec, snapshot(), true);
    EXPECT_EQ(query.remaining(), (std::vector<t_docId>{1, 6}));
    EXPECT_EQ(query.candidateCounts(), (std::pair<uint64_t, uint64_t>{1, 5}));
  }
  EXPECT_EQ(pool.decoded.size(), 2u);
  EXPECT_EQ(pool.frees, 1u);
}
